#ifndef COLUMNAR_STORAGE_H
#define COLUMNAR_STORAGE_H

#include "postgres.h"
#include "pg_compat.h"

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"

/*
 * Compression types for stripe storage.
 */
typedef enum ColumnarCompression
{
	COLUMNAR_COMPRESSION_NONE = 0,
	COLUMNAR_COMPRESSION_LZ4 = 1,
	COLUMNAR_COMPRESSION_ZSTD = 2
} ColumnarCompression;

/* GUC variable — current compression setting */
extern int	columnar_compression;

/*
 * Metadata for a single stripe.
 */
typedef struct StripeMetadata
{
	int			stripe_id;
	int64_t		row_count;
	int64_t		file_size;
	int			compression;		/* ColumnarCompression enum value */
	int64_t		uncompressed_size;	/* original IPC stream size (0 if none) */
} StripeMetadata;

/*
 * Metadata for the entire columnar table storage.
 */
typedef struct ColumnarMetadata
{
	int			num_stripes;
	int64_t		total_rows;
	StripeMetadata *stripes;	/* palloc'd array */
} ColumnarMetadata;

/*
 * Build the directory path for a columnar table's storage.
 * Returns a palloc'd string: $PGDATA/columnar/<dbOid>/<relNumber>
 */
extern char *columnar_dir_path(const RelFileLocator *locator);

/*
 * Build the file path for a specific stripe.
 * Returns a palloc'd string.
 */
extern char *columnar_stripe_path(const RelFileLocator *locator, int stripe_id);

/*
 * Build the metadata file path.
 */
extern char *columnar_metadata_path(const RelFileLocator *locator);

/*
 * Create the storage directory for a new columnar table.
 */
extern void columnar_create_storage(const RelFileLocator *locator);

/*
 * Remove all storage for a columnar table.
 */
extern void columnar_remove_storage(const RelFileLocator *locator);

/*
 * Read the metadata for a table. Returns a palloc'd ColumnarMetadata.
 */
extern ColumnarMetadata *columnar_read_metadata(const RelFileLocator *locator);

/*
 * Write metadata for a table.
 */
extern void columnar_write_metadata(const RelFileLocator *locator,
									ColumnarMetadata *meta);

/*
 * Write a stripe (single RecordBatch) to an Arrow IPC file.
 * Updates the metadata file.
 */
extern void columnar_write_stripe(const RelFileLocator *locator,
								  struct ArrowSchema *schema,
								  struct ArrowArrayView *batch_view,
								  int64_t nrows);

/*
 * Compute the total size of all files in the columnar storage directory.
 */
extern uint64 columnar_storage_size(const RelFileLocator *locator);

/* ----------------------------------------------------------------
 * Delete bitmap API
 *
 * Each stripe that has at least one deleted row gets a companion file:
 *   $PGDATA/columnar/<dbOid>/<relfilenode>/stripe_XXXXXX.deleted
 *
 * The file is a packed bitset of ceil(row_count / 8) bytes.
 * Bit i (0-based) set means row i in that stripe is logically deleted.
 * ----------------------------------------------------------------
 */

/*
 * Build the path for a stripe's delete bitmap file.
 * Returns a palloc'd string.
 */
extern char *columnar_deleted_path(const RelFileLocator *locator, int stripe_id);

/*
 * Read the delete bitmap for a stripe.
 * Returns a palloc'd byte array of ceil(row_count/8) bytes, or NULL if the
 * stripe has no deletions (file does not exist).
 */
extern uint8_t *columnar_read_delete_bitmap(const RelFileLocator *locator,
											int stripe_id, int64_t row_count);

/*
 * Mark a single row as deleted in the stripe's bitmap file.
 * Reads the existing file (if any), sets the bit, then rewrites the file.
 */
extern void columnar_set_deleted_bit(const RelFileLocator *locator,
									 int stripe_id,
									 int64_t row_offset,
									 int64_t row_count);

/*
 * Inline helper: test whether bit row_offset is set in the bitmap.
 * bits must be non-NULL.
 */
static inline bool
columnar_is_deleted(const uint8_t *bits, int64_t row_offset)
{
	return (bits[row_offset / 8] & (1 << (row_offset % 8))) != 0;
}

/* ----------------------------------------------------------------
 * Per-stripe min/max statistics
 *
 * Each stripe that has trackable columns gets a companion file:
 *   $PGDATA/columnar/<dbOid>/<relfilenode>/stripe_XXXXXX.stats
 *
 * File format (text):
 *   Line 1:           "PGCS 1 <ncols>"
 *   Lines 2..ncols+1: "<stat_type> <has_stats> <min_val> <max_val>"
 *
 * stat_type: 0=NONE, 1=INT, 2=FLOAT
 * has_stats: 0 or 1 (0 means the column was entirely NULL in this stripe)
 * min_val, max_val: int64 text representation
 *   - INT:   value in PG-epoch units (int2/4/8 → raw; date → PG days;
 *             timestamp → PG microseconds)
 *   - FLOAT: IEEE-754 bit pattern of the double value via memcpy
 *   - NONE / has_stats=0: both written as 0 (ignored on read)
 *
 * An absent stats file means stats are not available for that stripe
 * (e.g. the stripe was written before Level-3 was installed).  Pruning
 * is silently skipped for such stripes.  Stats failures emit a WARNING
 * but never ERROR — stats are purely advisory.
 * ----------------------------------------------------------------
 */

typedef enum ColumnarStatType
{
	COLUMNAR_STAT_TYPE_NONE  = 0,	/* not tracked (text, bool, etc.) */
	COLUMNAR_STAT_TYPE_INT   = 1,	/* min/max stored as int64 in PG-epoch units */
	COLUMNAR_STAT_TYPE_FLOAT = 2,	/* min/max stored as double */
} ColumnarStatType;

/*
 * Min/max statistics for one non-dropped column within a stripe.
 * Columns are indexed in Arrow schema child order (= non-dropped att order).
 */
typedef struct ColumnarColumnStats
{
	int		stat_type;		/* ColumnarStatType */
	bool	has_stats;		/* false if the column was entirely NULL */
	int64_t min_int;		/* STAT_TYPE_INT: PG-epoch minimum */
	int64_t max_int;		/* STAT_TYPE_INT: PG-epoch maximum */
	double	min_float;		/* STAT_TYPE_FLOAT: minimum as double */
	double	max_float;		/* STAT_TYPE_FLOAT: maximum as double */
} ColumnarColumnStats;

/*
 * All column statistics for a single stripe.
 */
typedef struct ColumnarStripeStats
{
	int					ncols;	/* entries in cols[] */
	ColumnarColumnStats *cols;	/* palloc'd array */
} ColumnarStripeStats;

/*
 * Build the path for a stripe's statistics file.
 * Returns a palloc'd string.
 */
extern char *columnar_stats_path(const RelFileLocator *locator, int stripe_id);

/*
 * Write per-stripe min/max stats to stripe_XXXXXX.stats.
 * Failures are emitted as WARNINGs; stats are advisory.
 */
extern void columnar_write_stripe_stats(const RelFileLocator *locator,
										int stripe_id,
										const ColumnarStripeStats *stats);

/*
 * Read stats for a stripe.
 * Returns a palloc'd ColumnarStripeStats, or NULL if the file is absent
 * or incompatible (ncols mismatch after an ALTER TABLE ADD/DROP COLUMN).
 * expected_ncols must equal the number of non-dropped columns.
 */
extern ColumnarStripeStats *columnar_read_stripe_stats(const RelFileLocator *locator,
													   int stripe_id,
													   int expected_ncols);

/*
 * Free a ColumnarStripeStats returned by columnar_read_stripe_stats.
 */
extern void columnar_free_stripe_stats(ColumnarStripeStats *stats);

/*
 * Evict the stats cache entry for a single stripe.
 * Called from columnar_relation_vacuum() when the .stats file is removed
 * for a fully-deleted stripe.
 */
extern void columnar_stats_cache_evict_stripe(const RelFileLocator *locator,
											  int stripe_id);

#endif /* COLUMNAR_STORAGE_H */
