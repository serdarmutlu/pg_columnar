#include "postgres.h"

#include "miscadmin.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include <dirent.h>
#include <inttypes.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef HAVE_LIBZSTD
#include <zstd.h>
#endif
#ifdef HAVE_LIBLZ4
#include <lz4frame.h>
#endif

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"
#include "columnar_storage.h"

/* Metadata format:
 *
 * Line 1: num_stripes total_rows
 * Lines 2..N+1: stripe_id row_count file_size compression uncompressed_size
 *
 * For backward compatibility, the reader also accepts the old 3-field format
 * (stripe_id row_count file_size) and defaults compression to NONE.
 */

/* ----------------------------------------------------------------
 * Backend-local metadata cache
 *
 * Avoids re-reading the metadata file on every columnar_read_metadata()
 * call.  The primary beneficiary is columnar_index_fetch_tuple(), which
 * is invoked for every TID during an index scan and previously incurred
 * a full metadata-file read per call.  columnar_tuple_delete() and
 * columnar_relation_estimate_size() also benefit.
 *
 * Key:   (dbOid, relNumber) — the two fields used to build the columnar
 *        directory path, matching PG_LOCATOR_DB/REL.
 * Value: full copy of ColumnarMetadata (num_stripes, total_rows, stripes[]).
 *
 * Invalidation rules:
 *   columnar_write_metadata() updates the cache after every successful
 *   write, keeping it in sync with the on-disk file for this backend.
 *
 *   columnar_remove_storage() evicts the entry when a table is dropped
 *   or truncated.
 *
 * Cross-backend consistency: the cache is intentionally backend-local and
 * does not handle concurrent modifications from other backends, matching
 * the existing no-MVCC stance of the columnar TAM.
 * ----------------------------------------------------------------
 */

typedef struct ColumnarMetadataCacheKey
{
	Oid			dbOid;		/* PG_LOCATOR_DB value */
	Oid			relNumber;	/* PG_LOCATOR_REL value (Oid on all PG versions) */
} ColumnarMetadataCacheKey;

typedef struct ColumnarMetadataCacheEntry
{
	ColumnarMetadataCacheKey key;	/* MUST be first for HTAB */
	int			num_stripes;
	int64_t		total_rows;
	StripeMetadata *stripes;		/* palloc'd in metadata_cache_ctx; NULL if 0 stripes */
} ColumnarMetadataCacheEntry;

static HTAB *metadata_cache = NULL;
static MemoryContext metadata_cache_ctx = NULL;

/*
 * Initialize the metadata cache.  Called lazily on first use.
 */
static void
columnar_metadata_cache_init(void)
{
	HASHCTL		ctl;

	metadata_cache_ctx = AllocSetContextCreate(TopMemoryContext,
											   "columnar metadata cache",
											   ALLOCSET_SMALL_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ColumnarMetadataCacheKey);
	ctl.entrysize = sizeof(ColumnarMetadataCacheEntry);
	ctl.hcxt = metadata_cache_ctx;

	metadata_cache = hash_create("columnar metadata cache",
								 16,	/* initial capacity; grows automatically */
								 &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Insert or overwrite a metadata cache entry.
 *
 * Called after every successful metadata file read (cache miss path) and
 * after every metadata file write, so the cache always reflects the latest
 * on-disk state for this backend.
 */
static void
columnar_metadata_cache_update(const RelFileLocator *locator,
							   const ColumnarMetadata *meta)
{
	ColumnarMetadataCacheKey key;
	ColumnarMetadataCacheEntry *entry;
	bool		found;
	MemoryContext oldcxt;

	if (metadata_cache == NULL)
		columnar_metadata_cache_init();

	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);

	entry = hash_search(metadata_cache, &key, HASH_ENTER, &found);

	/* Discard the old stripes array when overwriting an existing entry */
	if (found && entry->stripes != NULL)
	{
		pfree(entry->stripes);
		entry->stripes = NULL;
	}

	entry->num_stripes = meta->num_stripes;
	entry->total_rows = meta->total_rows;

	if (meta->num_stripes > 0 && meta->stripes != NULL)
	{
		/* Allocate in the long-lived cache context so it outlives the query */
		oldcxt = MemoryContextSwitchTo(metadata_cache_ctx);
		entry->stripes = palloc(sizeof(StripeMetadata) * meta->num_stripes);
		MemoryContextSwitchTo(oldcxt);
		memcpy(entry->stripes, meta->stripes,
			   sizeof(StripeMetadata) * meta->num_stripes);
	}
	else
		entry->stripes = NULL;
}

/*
 * Remove a metadata cache entry.
 *
 * Called when the underlying storage is removed (DROP TABLE, TRUNCATE) so
 * the cache cannot return stale data for a future table that reuses the
 * same relfilenode.
 */
static void
columnar_metadata_cache_evict(const RelFileLocator *locator)
{
	ColumnarMetadataCacheKey key;
	ColumnarMetadataCacheEntry *entry;
	bool		found;

	if (metadata_cache == NULL)
		return;					/* cache not yet initialised; nothing to evict */

	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);

	/* Free the stripes array before removing the HTAB entry to avoid a leak */
	entry = hash_search(metadata_cache, &key, HASH_FIND, &found);
	if (found && entry->stripes != NULL)
	{
		pfree(entry->stripes);
		entry->stripes = NULL;
	}

	hash_search(metadata_cache, &key, HASH_REMOVE, NULL);
}

char *
columnar_dir_path(const RelFileLocator *locator)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator));
	return path;
}

char *
columnar_stripe_path(const RelFileLocator *locator, int stripe_id)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u/stripe_%06d.arrow",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator), stripe_id);
	return path;
}

char *
columnar_metadata_path(const RelFileLocator *locator)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u/metadata",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator));
	return path;
}

void
columnar_create_storage(const RelFileLocator *locator)
{
	char		base[MAXPGPATH];
	char		dbdir[MAXPGPATH];
	char	   *reldir;
	ColumnarMetadata meta;
	int			rc;

	/* $PGDATA/columnar */
	snprintf(base, MAXPGPATH, "%s/columnar", DataDir);
	(void) mkdir(base, S_IRWXU);

	/* $PGDATA/columnar/<dbOid> */
	snprintf(dbdir, MAXPGPATH, "%s/columnar/%u", DataDir, PG_LOCATOR_DB(locator));
	(void) mkdir(dbdir, S_IRWXU);

	/* $PGDATA/columnar/<dbOid>/<relNumber> */
	reldir = columnar_dir_path(locator);
	rc = mkdir(reldir, S_IRWXU);
	if (rc < 0 && errno == EEXIST)
	{
		/*
		 * Directory already exists (e.g. relNumber reused after DROP+CREATE).
		 * Remove any stale stripe files before writing fresh metadata.
		 */
		columnar_remove_storage(locator);
		if (mkdir(reldir, S_IRWXU) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create columnar directory \"%s\": %m",
							reldir)));
	}
	else if (rc < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create columnar directory \"%s\": %m",
						reldir)));
	}

	/* Write initial empty metadata */
	meta.num_stripes = 0;
	meta.total_rows = 0;
	meta.stripes = NULL;
	columnar_write_metadata(locator, &meta);

	pfree(reldir);
}

void
columnar_remove_storage(const RelFileLocator *locator)
{
	char	   *dirpath = columnar_dir_path(locator);
	DIR		   *dir;
	struct dirent *de;

	/*
	 * Evict before touching the files so that any subsequent
	 * columnar_read_metadata call for this locator is forced back to disk
	 * (or gets an empty result for a freshly-created replacement table).
	 * We evict unconditionally — even if the directory does not exist —
	 * because the cache might still hold a stale entry from a previous
	 * incarnation of the same relfilenode.
	 */
	columnar_metadata_cache_evict(locator);

	dir = opendir(dirpath);
	if (dir == NULL)
	{
		pfree(dirpath);
		return;					/* directory doesn't exist, nothing to do */
	}

	while ((de = readdir(dir)) != NULL)
	{
		char		filepath[MAXPGPATH];

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, de->d_name);
		(void) unlink(filepath);
	}
	closedir(dir);
	(void) rmdir(dirpath);

	pfree(dirpath);
}

ColumnarMetadata *
columnar_read_metadata(const RelFileLocator *locator)
{
	char	   *metapath;
	FILE	   *fp;
	ColumnarMetadata *meta;
	int			i;

	/*
	 * Cache lookup.  On a hit, return a palloc'd copy so the caller can
	 * pfree it in the usual way without disturbing the cached state.
	 */
	{
		ColumnarMetadataCacheKey ckey;
		ColumnarMetadataCacheEntry *centry;
		bool		found;

		if (metadata_cache == NULL)
			columnar_metadata_cache_init();

		ckey.dbOid = (Oid) PG_LOCATOR_DB(locator);
		ckey.relNumber = (Oid) PG_LOCATOR_REL(locator);

		centry = hash_search(metadata_cache, &ckey, HASH_FIND, &found);
		if (found)
		{
			ColumnarMetadata *result = palloc(sizeof(ColumnarMetadata));

			result->num_stripes = centry->num_stripes;
			result->total_rows = centry->total_rows;
			if (centry->num_stripes > 0 && centry->stripes != NULL)
			{
				result->stripes = palloc(sizeof(StripeMetadata) * centry->num_stripes);
				memcpy(result->stripes, centry->stripes,
					   sizeof(StripeMetadata) * centry->num_stripes);
			}
			else
				result->stripes = NULL;
			return result;
		}
	}

	/* Cache miss — read from the metadata file on disk. */
	metapath = columnar_metadata_path(locator);
	meta = palloc0(sizeof(ColumnarMetadata));

	fp = fopen(metapath, "r");
	if (fp == NULL)
	{
		/* No metadata file yet -- return empty (do not cache; no valid data) */
		pfree(metapath);
		return meta;
	}

	{
		char	header_line[256];

		if (fgets(header_line, sizeof(header_line), fp) == NULL ||
			sscanf(header_line, "%d %" SCNd64, &meta->num_stripes, &meta->total_rows) != 2)
		{
			/* Unparseable header — return what we have, do not cache */
			fclose(fp);
			pfree(metapath);
			return meta;
		}
	}

	if (meta->num_stripes > 0)
	{
		meta->stripes = palloc0(sizeof(StripeMetadata) * meta->num_stripes);
		for (i = 0; i < meta->num_stripes; i++)
		{
			char		line[256];
			int			fields;

			if (fgets(line, sizeof(line), fp) == NULL)
			{
				meta->num_stripes = i;
				break;
			}

			fields = sscanf(line, "%d %" SCNd64 " %" SCNd64 " %d %" SCNd64,
							&meta->stripes[i].stripe_id,
							&meta->stripes[i].row_count,
							&meta->stripes[i].file_size,
							&meta->stripes[i].compression,
							&meta->stripes[i].uncompressed_size);
			if (fields == 3)
			{
				/* Old format without compression fields */
				meta->stripes[i].compression = COLUMNAR_COMPRESSION_NONE;
				meta->stripes[i].uncompressed_size = meta->stripes[i].file_size;
			}
			else if (fields != 5)
			{
				meta->num_stripes = i;
				break;
			}
		}
	}

	fclose(fp);
	pfree(metapath);

	/* Populate the cache so subsequent calls for this table are served from RAM */
	columnar_metadata_cache_update(locator, meta);

	return meta;
}

void
columnar_write_metadata(const RelFileLocator *locator, ColumnarMetadata *meta)
{
	char	   *metapath = columnar_metadata_path(locator);
	FILE	   *fp;
	int			i;

	fp = fopen(metapath, "w");
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write columnar metadata \"%s\": %m", metapath)));

	fprintf(fp, "%d %" PRId64 "\n", meta->num_stripes, meta->total_rows);
	for (i = 0; i < meta->num_stripes; i++)
	{
		fprintf(fp, "%d %" PRId64 " %" PRId64 " %d %" PRId64 "\n",
				meta->stripes[i].stripe_id,
				meta->stripes[i].row_count,
				meta->stripes[i].file_size,
				meta->stripes[i].compression,
				meta->stripes[i].uncompressed_size);
	}

	fclose(fp);
	pfree(metapath);

	/*
	 * Keep the backend-local cache in sync with what we just wrote.
	 * This is the single update point: every on-disk metadata change
	 * goes through columnar_write_metadata, so updating here is sufficient
	 * to maintain cache consistency within this backend.
	 */
	columnar_metadata_cache_update(locator, meta);
}

/* ----------------------------------------------------------------
 * Per-stripe min/max statistics implementation
 * ----------------------------------------------------------------
 */

char *
columnar_stats_path(const RelFileLocator *locator, int stripe_id)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u/stripe_%06d.stats",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator),
			 stripe_id);
	return path;
}

/*
 * Collect per-column min/max statistics from a finished RecordBatch.
 *
 * Uses Arrow schema format strings to identify column types, which avoids
 * introducing a dependency on the PG TupleDesc in the storage layer.
 *
 * Epoch conversion (Arrow → PG) is applied to DATE32 and TIMESTAMP so that
 * stored stats can be compared directly with PG Datum values in the pruning
 * layer without any additional conversion.
 *
 * Tracked types and their storage format:
 *   INT16  ("s"), INT32 ("i"), INT64 ("l") → STAT_TYPE_INT, raw value
 *   DATE32 ("tdD")                          → STAT_TYPE_INT, PG days
 *   TIMESTAMP ("tsu:...")                   → STAT_TYPE_INT, PG microseconds
 *   FLOAT  ("f"), DOUBLE ("g")             → STAT_TYPE_FLOAT, double value
 *   Everything else (bool, text, …)        → STAT_TYPE_NONE (not tracked)
 */
static ColumnarStripeStats *
columnar_collect_stripe_stats(struct ArrowSchema *schema,
							  struct ArrowArrayView *batch_view,
							  int64_t nrows)
{
	ColumnarStripeStats *stats;
	int			ncols = (int) schema->n_children;
	int			i;

	stats = palloc(sizeof(ColumnarStripeStats));
	stats->ncols = ncols;
	if (ncols == 0)
	{
		stats->cols = NULL;
		return stats;
	}
	stats->cols = palloc0(sizeof(ColumnarColumnStats) * ncols);

	for (i = 0; i < ncols; i++)
	{
		struct ArrowArrayView *child = batch_view->children[i];
		ColumnarColumnStats *cs = &stats->cols[i];
		const char *fmt = schema->children[i]->format;
		int			stat_type;
		bool		date_epoch = false;
		bool		ts_epoch = false;
		int64_t		j;
		bool		has_non_null = false;

		/* Classify the column by its Arrow format string */
		if (strcmp(fmt, "s") == 0)			/* INT16 */
			stat_type = COLUMNAR_STAT_TYPE_INT;
		else if (strcmp(fmt, "i") == 0)		/* INT32 */
			stat_type = COLUMNAR_STAT_TYPE_INT;
		else if (strcmp(fmt, "l") == 0)		/* INT64 */
			stat_type = COLUMNAR_STAT_TYPE_INT;
		else if (strcmp(fmt, "tdD") == 0)	/* DATE32 */
		{
			stat_type = COLUMNAR_STAT_TYPE_INT;
			date_epoch = true;
		}
		else if (strncmp(fmt, "tsu:", 4) == 0)	/* TIMESTAMP µs (any tz) */
		{
			stat_type = COLUMNAR_STAT_TYPE_INT;
			ts_epoch = true;
		}
		else if (strcmp(fmt, "f") == 0)		/* FLOAT32 */
			stat_type = COLUMNAR_STAT_TYPE_FLOAT;
		else if (strcmp(fmt, "g") == 0)		/* FLOAT64 */
			stat_type = COLUMNAR_STAT_TYPE_FLOAT;
		else
		{
			cs->stat_type = COLUMNAR_STAT_TYPE_NONE;
			cs->has_stats = false;
			continue;
		}

		cs->stat_type = stat_type;

		/* Scan all rows to find min/max */
		for (j = 0; j < nrows; j++)
		{
			if (ArrowArrayViewIsNull(child, j))
				continue;

			if (stat_type == COLUMNAR_STAT_TYPE_INT)
			{
				int64_t		raw = ArrowArrayViewGetIntUnsafe(child, j);
				int64_t		val;

				/* Convert from Arrow epoch to PG epoch */
				if (date_epoch)
					val = raw - INT64CONST(10957);			/* days */
				else if (ts_epoch)
					val = raw - INT64CONST(946684800000000);	/* µs */
				else
					val = raw;

				if (!has_non_null)
				{
					cs->min_int = val;
					cs->max_int = val;
				}
				else
				{
					if (val < cs->min_int)
						cs->min_int = val;
					if (val > cs->max_int)
						cs->max_int = val;
				}
			}
			else					/* FLOAT */
			{
				double		val = ArrowArrayViewGetDoubleUnsafe(child, j);

				if (!has_non_null)
				{
					cs->min_float = val;
					cs->max_float = val;
				}
				else
				{
					if (val < cs->min_float)
						cs->min_float = val;
					if (val > cs->max_float)
						cs->max_float = val;
				}
			}
			has_non_null = true;
		}

		cs->has_stats = has_non_null;
	}

	return stats;
}

void
columnar_write_stripe_stats(const RelFileLocator *locator, int stripe_id,
							const ColumnarStripeStats *stats)
{
	char	   *path = columnar_stats_path(locator, stripe_id);
	FILE	   *fp;
	int			i;

	fp = fopen(path, "w");
	if (fp == NULL)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("columnar: could not write stripe stats \"%s\": %m",
						path)));
		pfree(path);
		return;
	}

	fprintf(fp, "PGCS 1 %d\n", stats->ncols);

	for (i = 0; i < stats->ncols; i++)
	{
		const ColumnarColumnStats *cs = &stats->cols[i];

		if (!cs->has_stats || cs->stat_type == COLUMNAR_STAT_TYPE_NONE)
		{
			/* No usable stats: write zeros */
			fprintf(fp, "%d 0 0 0\n", cs->stat_type);
		}
		else if (cs->stat_type == COLUMNAR_STAT_TYPE_INT)
		{
			fprintf(fp, "1 1 %" PRId64 " %" PRId64 "\n",
					cs->min_int, cs->max_int);
		}
		else					/* COLUMNAR_STAT_TYPE_FLOAT */
		{
			int64_t		min_bits,
						max_bits;

			memcpy(&min_bits, &cs->min_float, sizeof(min_bits));
			memcpy(&max_bits, &cs->max_float, sizeof(max_bits));
			fprintf(fp, "2 1 %" PRId64 " %" PRId64 "\n",
					min_bits, max_bits);
		}
	}

	fclose(fp);
	pfree(path);
}

ColumnarStripeStats *
columnar_read_stripe_stats(const RelFileLocator *locator,
						   int stripe_id, int expected_ncols)
{
	char	   *path = columnar_stats_path(locator, stripe_id);
	FILE	   *fp;
	ColumnarStripeStats *stats;
	char		header[64];
	int			version,
				ncols;
	int			i;

	fp = fopen(path, "r");
	pfree(path);
	if (fp == NULL)
		return NULL;			/* no stats file */

	/* Validate header: "PGCS <version> <ncols>" */
	if (fgets(header, sizeof(header), fp) == NULL ||
		sscanf(header, "PGCS %d %d", &version, &ncols) != 2 ||
		version != 1 || ncols != expected_ncols)
	{
		fclose(fp);
		return NULL;			/* corrupt or schema mismatch */
	}

	stats = palloc(sizeof(ColumnarStripeStats));
	stats->ncols = ncols;
	stats->cols = palloc0(sizeof(ColumnarColumnStats) * ncols);

	for (i = 0; i < ncols; i++)
	{
		char		line[128];
		int			stat_type,
					has_stats_int;
		int64_t		min_raw,
					max_raw;

		if (fgets(line, sizeof(line), fp) == NULL ||
			sscanf(line, "%d %d %" SCNd64 " %" SCNd64,
				   &stat_type, &has_stats_int, &min_raw, &max_raw) != 4)
		{
			fclose(fp);
			columnar_free_stripe_stats(stats);
			return NULL;
		}

		stats->cols[i].stat_type = stat_type;
		stats->cols[i].has_stats = (has_stats_int != 0);

		if (stats->cols[i].has_stats)
		{
			if (stat_type == COLUMNAR_STAT_TYPE_INT)
			{
				stats->cols[i].min_int = min_raw;
				stats->cols[i].max_int = max_raw;
			}
			else if (stat_type == COLUMNAR_STAT_TYPE_FLOAT)
			{
				memcpy(&stats->cols[i].min_float, &min_raw, sizeof(double));
				memcpy(&stats->cols[i].max_float, &max_raw, sizeof(double));
			}
		}
	}

	fclose(fp);
	return stats;
}

void
columnar_free_stripe_stats(ColumnarStripeStats *stats)
{
	if (stats == NULL)
		return;
	if (stats->cols)
		pfree(stats->cols);
	pfree(stats);
}

void
columnar_write_stripe(const RelFileLocator *locator,
					  struct ArrowSchema *schema,
					  struct ArrowArrayView *batch_view,
					  int64_t nrows)
{
	ColumnarMetadata *meta;
	int			new_stripe_id;
	char	   *filepath;
	FILE	   *fp;
	struct ArrowBuffer ipc_buffer;
	struct ArrowIpcOutputStream output_stream;
	struct ArrowIpcWriter writer;
	struct ArrowError arrow_error;
	int			rc;
	int			compression = columnar_compression;
	const void *write_data;
	int64_t		write_size;
	int64_t		uncompressed_size;
	void	   *compressed = NULL;

	/* Get next stripe ID */
	meta = columnar_read_metadata(locator);
	new_stripe_id = meta->num_stripes + 1;
	filepath = columnar_stripe_path(locator, new_stripe_id);

	/* Write IPC stream to in-memory buffer */
	ArrowBufferInit(&ipc_buffer);

	rc = ArrowIpcOutputStreamInitBuffer(&output_stream, &ipc_buffer);
	if (rc != NANOARROW_OK)
		ereport(ERROR, (errmsg("columnar: failed to init IPC buffer output stream")));

	rc = ArrowIpcWriterInit(&writer, &output_stream);
	if (rc != NANOARROW_OK)
	{
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR, (errmsg("columnar: failed to init IPC writer")));
	}

	/* Write IPC stream: schema + batch + EOS */
	rc = ArrowIpcWriterWriteSchema(&writer, schema, &arrow_error);
	if (rc != NANOARROW_OK)
	{
		ArrowIpcWriterReset(&writer);
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR, (errmsg("columnar: IPC write schema failed: %s",
							   arrow_error.message)));
	}

	rc = ArrowIpcWriterWriteArrayView(&writer, batch_view, &arrow_error);
	if (rc != NANOARROW_OK)
	{
		ArrowIpcWriterReset(&writer);
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR, (errmsg("columnar: IPC write batch failed: %s",
							   arrow_error.message)));
	}

	ArrowIpcWriterReset(&writer);

	/* ipc_buffer now contains the complete uncompressed IPC stream */
	uncompressed_size = ipc_buffer.size_bytes;
	write_data = ipc_buffer.data;
	write_size = ipc_buffer.size_bytes;

	/* Compress if requested */
	if (compression == COLUMNAR_COMPRESSION_ZSTD)
	{
#ifdef HAVE_LIBZSTD
		size_t		bound = ZSTD_compressBound(ipc_buffer.size_bytes);
		size_t		csize;

		compressed = palloc(bound);
		csize = ZSTD_compress(compressed, bound,
							  ipc_buffer.data, ipc_buffer.size_bytes,
							  3 /* default level */);
		if (ZSTD_isError(csize))
		{
			pfree(compressed);
			ArrowBufferReset(&ipc_buffer);
			ereport(ERROR, (errmsg("columnar: ZSTD compression failed: %s",
								   ZSTD_getErrorName(csize))));
		}
		write_data = compressed;
		write_size = (int64_t) csize;
#else
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnar: ZSTD compression not available"),
				 errhint("Rebuild pg_columnar with ZSTD support (install libzstd-dev).")));
#endif
	}
	else if (compression == COLUMNAR_COMPRESSION_LZ4)
	{
#ifdef HAVE_LIBLZ4
		LZ4F_preferences_t prefs;
		size_t		bound;
		size_t		csize;

		memset(&prefs, 0, sizeof(prefs));
		prefs.frameInfo.contentSize = ipc_buffer.size_bytes;

		bound = LZ4F_compressFrameBound(ipc_buffer.size_bytes, &prefs);
		compressed = palloc(bound);
		csize = LZ4F_compressFrame(compressed, bound,
								   ipc_buffer.data, ipc_buffer.size_bytes,
								   &prefs);
		if (LZ4F_isError(csize))
		{
			pfree(compressed);
			ArrowBufferReset(&ipc_buffer);
			ereport(ERROR, (errmsg("columnar: LZ4 compression failed: %s",
								   LZ4F_getErrorName(csize))));
		}
		write_data = compressed;
		write_size = (int64_t) csize;
#else
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnar: LZ4 compression not available"),
				 errhint("Rebuild pg_columnar with LZ4 support (install liblz4-dev).")));
#endif
	}

	/* Write to file */
	fp = fopen(filepath, "wb");
	if (fp == NULL)
	{
		if (compressed)
			pfree(compressed);
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create stripe file \"%s\": %m", filepath)));
	}

	if (fwrite(write_data, 1, write_size, fp) != (size_t) write_size)
	{
		fclose(fp);
		if (compressed)
			pfree(compressed);
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write stripe file \"%s\": %m", filepath)));
	}

	fclose(fp);
	if (compressed)
		pfree(compressed);
	ArrowBufferReset(&ipc_buffer);

	/*
	 * Collect and write per-stripe min/max statistics.
	 * batch_view is still valid here (the IPC writer only reads it).
	 * Failures emit a WARNING but do not abort the write.
	 */
	{
		ColumnarStripeStats *stripe_stats;

		stripe_stats = columnar_collect_stripe_stats(schema, batch_view, nrows);
		columnar_write_stripe_stats(locator, new_stripe_id, stripe_stats);
		columnar_free_stripe_stats(stripe_stats);
	}

	/* Update metadata */
	{
		ColumnarMetadata new_meta;
		int			i;

		new_meta.num_stripes = meta->num_stripes + 1;
		new_meta.total_rows = meta->total_rows + nrows;
		new_meta.stripes = palloc(sizeof(StripeMetadata) * new_meta.num_stripes);

		for (i = 0; i < meta->num_stripes; i++)
			new_meta.stripes[i] = meta->stripes[i];

		new_meta.stripes[meta->num_stripes].stripe_id = new_stripe_id;
		new_meta.stripes[meta->num_stripes].row_count = nrows;
		new_meta.stripes[meta->num_stripes].file_size = write_size;
		new_meta.stripes[meta->num_stripes].compression = compression;
		new_meta.stripes[meta->num_stripes].uncompressed_size = uncompressed_size;

		columnar_write_metadata(locator, &new_meta);

		pfree(new_meta.stripes);
	}

	if (meta->stripes)
		pfree(meta->stripes);
	pfree(meta);
	pfree(filepath);
}

/* ----------------------------------------------------------------
 * Delete bitmap implementation
 * ----------------------------------------------------------------
 */

char *
columnar_deleted_path(const RelFileLocator *locator, int stripe_id)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u/stripe_%06d.deleted",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator),
			 stripe_id);
	return path;
}

uint8_t *
columnar_read_delete_bitmap(const RelFileLocator *locator,
							int stripe_id, int64_t row_count)
{
	char	   *path = columnar_deleted_path(locator, stripe_id);
	FILE	   *fp;
	int64_t		nbytes = (row_count + 7) / 8;
	uint8_t    *bits;

	fp = fopen(path, "rb");
	pfree(path);

	if (fp == NULL)
		return NULL;			/* no deletions for this stripe */

	bits = palloc0(nbytes);
	(void) fread(bits, 1, nbytes, fp);
	fclose(fp);
	return bits;
}

void
columnar_set_deleted_bit(const RelFileLocator *locator,
						 int stripe_id,
						 int64_t row_offset,
						 int64_t row_count)
{
	char	   *path = columnar_deleted_path(locator, stripe_id);
	int64_t		nbytes = (row_count + 7) / 8;
	uint8_t    *bits;
	FILE	   *fp;

	/* Read existing bitmap or allocate a fresh zeroed one */
	fp = fopen(path, "rb");
	if (fp != NULL)
	{
		bits = palloc0(nbytes);
		(void) fread(bits, 1, nbytes, fp);
		fclose(fp);
	}
	else
	{
		bits = palloc0(nbytes);
	}

	/* Set the bit for this row */
	bits[row_offset / 8] |= (uint8_t) (1 << (row_offset % 8));

	/* Write the updated bitmap back */
	fp = fopen(path, "wb");
	if (fp == NULL)
	{
		char	   *errmsg_path = pstrdup(path);

		pfree(bits);
		pfree(path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write delete bitmap \"%s\": %m",
						errmsg_path)));
	}

	if (fwrite(bits, 1, nbytes, fp) != (size_t) nbytes)
	{
		char	   *errmsg_path = pstrdup(path);

		fclose(fp);
		pfree(bits);
		pfree(path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write delete bitmap \"%s\": %m",
						errmsg_path)));
	}

	fclose(fp);
	pfree(bits);
	pfree(path);
}

uint64
columnar_storage_size(const RelFileLocator *locator)
{
	char	   *dirpath = columnar_dir_path(locator);
	DIR		   *dir;
	struct dirent *de;
	uint64		total = 0;

	dir = opendir(dirpath);
	if (dir == NULL)
	{
		pfree(dirpath);
		return 0;
	}

	while ((de = readdir(dir)) != NULL)
	{
		struct stat st;
		char		filepath[MAXPGPATH];

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, de->d_name);
		if (stat(filepath, &st) == 0)
			total += st.st_size;
	}

	closedir(dir);
	pfree(dirpath);
	return total;
}
