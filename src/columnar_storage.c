#include "postgres.h"

#include "miscadmin.h"
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
	char	   *metapath = columnar_metadata_path(locator);
	FILE	   *fp;
	ColumnarMetadata *meta;
	int			i;

	meta = palloc0(sizeof(ColumnarMetadata));

	fp = fopen(metapath, "r");
	if (fp == NULL)
	{
		/* No metadata file yet -- return empty */
		pfree(metapath);
		return meta;
	}

	{
		char	header_line[256];

		if (fgets(header_line, sizeof(header_line), fp) == NULL ||
			sscanf(header_line, "%d %" SCNd64, &meta->num_stripes, &meta->total_rows) != 2)
		{
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
