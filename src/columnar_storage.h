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

#endif /* COLUMNAR_STORAGE_H */
