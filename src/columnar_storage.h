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

#endif /* COLUMNAR_STORAGE_H */
