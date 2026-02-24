#include "postgres.h"

#include "access/multixact.h"
#include "access/parallel.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
#include "executor/executor.h"
#include "executor/tuptable.h"
#include "miscadmin.h"
#include "storage/bufmgr.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/rel.h"
#include "utils/sampling.h"
#include "utils/timestamp.h"

#ifdef HAVE_LIBZSTD
#include <zstd.h>
#endif
#ifdef HAVE_LIBLZ4
#include <lz4frame.h>
#endif

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"
#include "pg_compat.h"
#include "columnar_storage.h"
#include "columnar_write_buffer.h"
#include "columnar_typemap.h"

/* ----------------------------------------------------------------
 * Scan descriptor for columnar tables.
 * ----------------------------------------------------------------
 */
typedef struct ColumnarScanDescData
{
	TableScanDescData base;

	/* Stripe iteration state */
	int			current_stripe;
	int			num_stripes;
	StripeMetadata *stripe_meta;	/* cached stripe metadata array */

	/* Arrow IPC reader */
	struct ArrowArrayStream arrow_stream;
	struct ArrowSchema arrow_schema;
	bool		stream_open;

	/* Current batch */
	struct ArrowArray current_batch;
	struct ArrowArrayView batch_view;
	bool		batch_view_valid;
	bool		has_batch;
	int64_t		batch_row_index;
	int64_t		batch_length;

	/* Write buffer scan */
	bool		include_write_buffer;
	int64_t		wb_row_index;

	/*
	 * Delete bitmap for the currently open stripe.
	 * NULL means no rows are deleted in this stripe.
	 * Allocated in CurrentMemoryContext; freed in columnar_close_stripe.
	 */
	uint8_t    *current_delete_bitmap;
} ColumnarScanDescData;

typedef ColumnarScanDescData *ColumnarScanDesc;

/* ----------------------------------------------------------------
 * Index fetch descriptor for columnar tables.
 *
 * TID encoding used by this TAM:
 *   BlockNumber  = 1-based stripe index (1 = first on-disk stripe)
 *   OffsetNumber = 1-based row offset within that stripe
 *   BlockNumber  = num_stripes + 1 for rows still in the write buffer
 * ----------------------------------------------------------------
 */
typedef struct ColumnarIndexFetchData
{
	IndexFetchTableData base;

	/* Currently cached stripe (-1 = nothing cached) */
	int			cached_stripe_id;

	/* Arrow IPC reader state for the cached stripe */
	struct ArrowArrayStream arrow_stream;
	struct ArrowSchema arrow_schema;
	bool		stream_open;

	/* The single RecordBatch that forms the cached stripe */
	struct ArrowArray cached_batch;
	struct ArrowArrayView batch_view;
	bool		has_batch;
	bool		batch_view_valid;

	/*
	 * Delete bitmap for the currently cached stripe.
	 * NULL means no rows in this stripe are deleted.
	 */
	uint8_t    *cached_delete_bitmap;
} ColumnarIndexFetchData;

/* ----------------------------------------------------------------
 * Helper: open a stripe's IPC stream into caller-supplied state.
 *
 * Initialises *out_stream and *out_schema from the stripe file.
 * The caller owns both and must release them when done.
 * ----------------------------------------------------------------
 */
static void
columnar_open_stripe_stream(const RelFileLocator *locator,
							StripeMetadata *sm,
							struct ArrowArrayStream *out_stream,
							struct ArrowSchema *out_schema)
{
	char	   *filepath;
	FILE	   *fp;
	struct ArrowIpcInputStream input_stream;
	int			rc;

	filepath = columnar_stripe_path(locator, sm->stripe_id);

	if (sm->compression == COLUMNAR_COMPRESSION_NONE)
	{
		/* Uncompressed: read directly from file */
		fp = fopen(filepath, "rb");
		if (fp == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open stripe file \"%s\": %m", filepath)));

		rc = ArrowIpcInputStreamInitFile(&input_stream, fp, 1);
		if (rc != NANOARROW_OK)
		{
			fclose(fp);
			ereport(ERROR, (errmsg("columnar: failed to init IPC input stream")));
		}
	}
	else
	{
		/* Compressed: read file, decompress, use buffer input stream */
		void	   *file_data;
		struct ArrowBuffer decompressed;

		fp = fopen(filepath, "rb");
		if (fp == NULL)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not open stripe file \"%s\": %m", filepath)));

		file_data = palloc(sm->file_size);
		if (fread(file_data, 1, sm->file_size, fp) != (size_t) sm->file_size)
		{
			fclose(fp);
			pfree(file_data);
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read stripe file \"%s\": %m", filepath)));
		}
		fclose(fp);

		/* Decompress into an ArrowBuffer (nanoarrow-managed, malloc-backed) */
		ArrowBufferInit(&decompressed);
		ArrowBufferReserve(&decompressed, sm->uncompressed_size);

		if (sm->compression == COLUMNAR_COMPRESSION_ZSTD)
		{
#ifdef HAVE_LIBZSTD
			size_t		result;

			result = ZSTD_decompress(decompressed.data, sm->uncompressed_size,
									 file_data, sm->file_size);
			if (ZSTD_isError(result))
			{
				pfree(file_data);
				ArrowBufferReset(&decompressed);
				ereport(ERROR,
						(errmsg("columnar: ZSTD decompression failed: %s",
								ZSTD_getErrorName(result))));
			}
			decompressed.size_bytes = (int64_t) result;
#else
			pfree(file_data);
			ArrowBufferReset(&decompressed);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("columnar: ZSTD support not compiled in")));
#endif
		}
		else if (sm->compression == COLUMNAR_COMPRESSION_LZ4)
		{
#ifdef HAVE_LIBLZ4
			LZ4F_dctx  *dctx = NULL;
			LZ4F_errorCode_t err;
			size_t		src_size;
			size_t		dst_size;
			size_t		ret;

			err = LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION);
			if (LZ4F_isError(err))
			{
				pfree(file_data);
				ArrowBufferReset(&decompressed);
				ereport(ERROR,
						(errmsg("columnar: LZ4 decompression context failed")));
			}

			src_size = sm->file_size;
			dst_size = sm->uncompressed_size;
			ret = LZ4F_decompress(dctx,
								  decompressed.data, &dst_size,
								  file_data, &src_size,
								  NULL);
			LZ4F_freeDecompressionContext(dctx);

			if (LZ4F_isError(ret))
			{
				pfree(file_data);
				ArrowBufferReset(&decompressed);
				ereport(ERROR,
						(errmsg("columnar: LZ4 decompression failed: %s",
								LZ4F_getErrorName(ret))));
			}
			decompressed.size_bytes = (int64_t) dst_size;
#else
			pfree(file_data);
			ArrowBufferReset(&decompressed);
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("columnar: LZ4 support not compiled in")));
#endif
		}

		pfree(file_data);

		/* ArrowIpcInputStreamInitBuffer takes ownership of decompressed */
		rc = ArrowIpcInputStreamInitBuffer(&input_stream, &decompressed);
		if (rc != NANOARROW_OK)
		{
			ArrowBufferReset(&decompressed);
			ereport(ERROR, (errmsg("columnar: failed to init IPC buffer input stream")));
		}
	}

	rc = ArrowIpcArrayStreamReaderInit(out_stream, &input_stream, NULL);
	if (rc != NANOARROW_OK)
		ereport(ERROR, (errmsg("columnar: failed to init IPC reader")));

	/* Read schema */
	rc = out_stream->get_schema(out_stream, out_schema);
	if (rc != 0)
		ereport(ERROR, (errmsg("columnar: failed to read schema from stripe %d",
							   sm->stripe_id)));

	pfree(filepath);
}

static void
columnar_open_stripe(ColumnarScanDesc scan, int stripe_idx)
{
	Relation	rel = scan->base.rs_rd;
	StripeMetadata *sm = &scan->stripe_meta[stripe_idx];

	columnar_open_stripe_stream(RelationGetLocator(rel), sm,
								&scan->arrow_stream, &scan->arrow_schema);

	/* Load delete bitmap for this stripe (NULL if no rows deleted) */
	if (scan->current_delete_bitmap)
	{
		pfree(scan->current_delete_bitmap);
		scan->current_delete_bitmap = NULL;
	}
	scan->current_delete_bitmap =
		columnar_read_delete_bitmap(RelationGetLocator(rel),
									sm->stripe_id, sm->row_count);

	scan->stream_open = true;
	scan->has_batch = false;
}

static void
columnar_close_stripe(ColumnarScanDesc scan)
{
	if (scan->current_delete_bitmap)
	{
		pfree(scan->current_delete_bitmap);
		scan->current_delete_bitmap = NULL;
	}

	if (scan->has_batch)
	{
		if (scan->batch_view_valid)
		{
			ArrowArrayViewReset(&scan->batch_view);
			scan->batch_view_valid = false;
		}
		if (scan->current_batch.release)
			scan->current_batch.release(&scan->current_batch);
		scan->has_batch = false;
	}

	if (scan->stream_open)
	{
		if (scan->arrow_schema.release)
			scan->arrow_schema.release(&scan->arrow_schema);
		if (scan->arrow_stream.release)
			scan->arrow_stream.release(&scan->arrow_stream);
		scan->stream_open = false;
	}
}

/* ----------------------------------------------------------------
 * Slot callbacks
 * ----------------------------------------------------------------
 */
static const TupleTableSlotOps *
columnar_slot_callbacks(Relation rel)
{
	return &TTSOpsVirtual;
}

/* ----------------------------------------------------------------
 * Min/max stripe pruning
 *
 * Returns true when it is guaranteed that no row in the stripe can
 * satisfy all of the supplied scan keys, so the stripe can be skipped
 * entirely.  Returns false (conservatively) when stats are unavailable.
 *
 * Important: PostgreSQL's sequential scan path typically passes nkeys = 0
 * because WHERE-clause quals are applied by the executor after the TAM
 * returns each row, not pushed down to the TAM.  Pruning therefore only
 * fires when a caller explicitly sets rs_nkeys > 0 (e.g. a custom scan
 * provider planned for Level 4).  The infrastructure is correct and safe;
 * it simply has no opportunity to prune during plain seq-scan queries today.
 * ----------------------------------------------------------------
 */
static bool
columnar_stripe_should_skip(const RelFileLocator *locator,
							StripeMetadata *sm,
							struct ScanKeyData *keys,
							int nkeys,
							TupleDesc tupdesc)
{
	ColumnarStripeStats *stats;
	int			k;
	bool		skip = false;
	int			ncols;
	int			a;

	/* Count non-dropped columns — must match the stats file's ncols */
	ncols = 0;
	for (a = 0; a < tupdesc->natts; a++)
	{
		if (!TupleDescAttr(tupdesc, a)->attisdropped)
			ncols++;
	}

	stats = columnar_read_stripe_stats(locator, sm->stripe_id, ncols);
	if (stats == NULL)
		return false;			/* no stats → cannot prune */

	for (k = 0; k < nkeys && !skip; k++)
	{
		struct ScanKeyData *key = &keys[k];
		AttrNumber	attno = key->sk_attno;	/* 1-based */
		Form_pg_attribute attr;
		ColumnarColumnStats *cs;
		int			child_idx;

		/* Ignore NULL-related scan flags */
		if (key->sk_flags & (SK_ISNULL | SK_SEARCHNULL | SK_SEARCHNOTNULL))
			continue;

		if (attno < 1 || attno > tupdesc->natts)
			continue;

		attr = TupleDescAttr(tupdesc, attno - 1);
		if (attr->attisdropped)
			continue;

		/* child_idx = number of non-dropped columns before this attribute */
		child_idx = 0;
		for (a = 0; a < (int) attno - 1; a++)
		{
			if (!TupleDescAttr(tupdesc, a)->attisdropped)
				child_idx++;
		}

		if (child_idx >= stats->ncols)
			continue;

		cs = &stats->cols[child_idx];

		if (!cs->has_stats || cs->stat_type == COLUMNAR_STAT_TYPE_NONE)
			continue;

		if (cs->stat_type == COLUMNAR_STAT_TYPE_INT)
		{
			int64_t		key_val;
			int64_t		stripe_min = cs->min_int;
			int64_t		stripe_max = cs->max_int;

			/*
			 * Extract the comparison value in PG-epoch units.  These
			 * match exactly what columnar_collect_stripe_stats stored:
			 *   INT2/4/8  → raw integer
			 *   DATE       → days since PG epoch (2000-01-01)
			 *   TIMESTAMP  → µs since PG epoch
			 */
			switch (attr->atttypid)
			{
				case INT2OID:
					key_val = (int64_t) DatumGetInt16(key->sk_argument);
					break;
				case INT4OID:
					key_val = (int64_t) DatumGetInt32(key->sk_argument);
					break;
				case INT8OID:
					key_val = DatumGetInt64(key->sk_argument);
					break;
				case DATEOID:
					key_val = (int64_t) DatumGetDateADT(key->sk_argument);
					break;
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
					key_val = (int64_t) DatumGetTimestamp(key->sk_argument);
					break;
				default:
					continue;	/* unsupported type — skip this key */
			}

			switch (key->sk_strategy)
			{
				case BTLessStrategyNumber:
					/* col < key: impossible if stripe_min >= key */
					if (stripe_min >= key_val)
						skip = true;
					break;
				case BTLessEqualStrategyNumber:
					/* col <= key: impossible if stripe_min > key */
					if (stripe_min > key_val)
						skip = true;
					break;
				case BTEqualStrategyNumber:
					/* col = key: impossible if key outside [min, max] */
					if (key_val < stripe_min || key_val > stripe_max)
						skip = true;
					break;
				case BTGreaterEqualStrategyNumber:
					/* col >= key: impossible if stripe_max < key */
					if (stripe_max < key_val)
						skip = true;
					break;
				case BTGreaterStrategyNumber:
					/* col > key: impossible if stripe_max <= key */
					if (stripe_max <= key_val)
						skip = true;
					break;
				default:
					break;
			}
		}
		else					/* COLUMNAR_STAT_TYPE_FLOAT */
		{
			double		key_val;
			double		stripe_min = cs->min_float;
			double		stripe_max = cs->max_float;

			switch (attr->atttypid)
			{
				case FLOAT4OID:
					key_val = (double) DatumGetFloat4(key->sk_argument);
					break;
				case FLOAT8OID:
					key_val = DatumGetFloat8(key->sk_argument);
					break;
				default:
					continue;
			}

			switch (key->sk_strategy)
			{
				case BTLessStrategyNumber:
					if (stripe_min >= key_val)
						skip = true;
					break;
				case BTLessEqualStrategyNumber:
					if (stripe_min > key_val)
						skip = true;
					break;
				case BTEqualStrategyNumber:
					if (key_val < stripe_min || key_val > stripe_max)
						skip = true;
					break;
				case BTGreaterEqualStrategyNumber:
					if (stripe_max < key_val)
						skip = true;
					break;
				case BTGreaterStrategyNumber:
					if (stripe_max <= key_val)
						skip = true;
					break;
				default:
					break;
			}
		}
	}

	columnar_free_stripe_stats(stats);
	return skip;
}

/* ----------------------------------------------------------------
 * Sequential scan
 * ----------------------------------------------------------------
 */
static TableScanDesc
columnar_scan_begin(Relation rel, Snapshot snapshot,
					int nkeys, struct ScanKeyData *key,
					ParallelTableScanDesc pscan, uint32 flags)
{
	ColumnarScanDesc scan;
	ColumnarMetadata *meta;

	scan = palloc0(sizeof(ColumnarScanDescData));
	scan->base.rs_rd = rel;
	scan->base.rs_snapshot = snapshot;
	scan->base.rs_nkeys = nkeys;
	scan->base.rs_key = key;
	scan->base.rs_flags = flags;
	scan->base.rs_parallel = pscan;

	meta = columnar_read_metadata(RelationGetLocator(rel));
	scan->current_stripe = 0;
	scan->stream_open = false;
	scan->has_batch = false;
	scan->batch_view_valid = false;
	scan->wb_row_index = 0;

	/*
	 * For a coordinated Parallel Sequential Scan (pscan != NULL), only
	 * the leader should scan stripes; workers return no rows to prevent
	 * duplicate results, because the leader will cover the full table.
	 *
	 * For a Parallel Append query (pscan == NULL even when
	 * IsParallelWorker() is true), each sub-plan is assigned to exactly
	 * one process with no overlap, so parallel workers must scan their
	 * assigned table normally.
	 */
	if (IsParallelWorker() && pscan != NULL)
	{
		scan->num_stripes = 0;
		scan->stripe_meta = NULL;
		scan->include_write_buffer = false;
	}
	else
	{
		scan->num_stripes = meta->num_stripes;
		if (meta->num_stripes > 0)
		{
			/* Take ownership of the stripes array */
			scan->stripe_meta = meta->stripes;
			meta->stripes = NULL;
		}
		else
			scan->stripe_meta = NULL;
		scan->include_write_buffer =
			(columnar_find_write_buffer(rel) != NULL);
	}

	if (meta->stripes)
		pfree(meta->stripes);
	pfree(meta);

	return (TableScanDesc) scan;
}

static void
columnar_scan_end(TableScanDesc sscan)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;

	columnar_close_stripe(scan);

	if (scan->stripe_meta)
		pfree(scan->stripe_meta);

	if (sscan->rs_flags & SO_TEMP_SNAPSHOT)
		UnregisterSnapshot(sscan->rs_snapshot);

	pfree(scan);
}

static void
columnar_scan_rescan(TableScanDesc sscan, struct ScanKeyData *key,
					 bool set_params, bool allow_strat,
					 bool allow_sync, bool allow_pagemode)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;

	columnar_close_stripe(scan);

	scan->current_stripe = 0;
	scan->has_batch = false;
	scan->stream_open = false;
	scan->wb_row_index = 0;
	scan->include_write_buffer =
		(columnar_find_write_buffer(sscan->rs_rd) != NULL);

	if (key != NULL)
		sscan->rs_key = key;
}

static bool
columnar_scan_getnextslot(TableScanDesc sscan, ScanDirection direction,
						   TupleTableSlot *slot)
{
	ColumnarScanDesc scan = (ColumnarScanDesc) sscan;
	TupleDesc	tupdesc = RelationGetDescr(sscan->rs_rd);
	struct ArrowError arrow_error;

	ExecClearTuple(slot);

	for (;;)
	{
		/* Try to return a row from the current batch */
		if (scan->has_batch && scan->batch_row_index < scan->batch_length)
		{
			/* Skip rows that have been logically deleted */
			if (scan->current_delete_bitmap != NULL &&
				columnar_is_deleted(scan->current_delete_bitmap,
									scan->batch_row_index))
			{
				scan->batch_row_index++;
				continue;
			}

			columnar_populate_slot(slot, &scan->batch_view,
								   scan->batch_row_index, tupdesc);

			/* Synthesize a TID */
			ItemPointerSet(&slot->tts_tid,
						   (BlockNumber) scan->current_stripe,
						   (OffsetNumber) (scan->batch_row_index + 1));

			scan->batch_row_index++;
			ExecStoreVirtualTuple(slot);
			return true;
		}

		/* Try to get the next batch from the current stream */
		if (scan->stream_open)
		{
			struct ArrowArray next_batch;
			int			rc;

			memset(&next_batch, 0, sizeof(next_batch));
			rc = scan->arrow_stream.get_next(&scan->arrow_stream, &next_batch);

			if (rc == 0 && next_batch.release != NULL)
			{
				/* Release previous batch if any */
				if (scan->has_batch)
				{
					if (scan->batch_view_valid)
					{
						ArrowArrayViewReset(&scan->batch_view);
						scan->batch_view_valid = false;
					}
					if (scan->current_batch.release)
						scan->current_batch.release(&scan->current_batch);
				}

				scan->current_batch = next_batch;
				scan->batch_row_index = 0;
				scan->batch_length = next_batch.length;
				scan->has_batch = true;

				/* Set up ArrayView for reading */
				ArrowArrayViewInitFromSchema(&scan->batch_view,
											 &scan->arrow_schema,
											 &arrow_error);
				rc = ArrowArrayViewSetArray(&scan->batch_view,
											&scan->current_batch,
											&arrow_error);
				if (rc != NANOARROW_OK)
					ereport(ERROR,
							(errmsg("columnar: failed to set batch view: %s",
									arrow_error.message)));
				scan->batch_view_valid = true;
				continue;
			}

			/* Stream exhausted, move to next stripe */
			columnar_close_stripe(scan);
		}

		/* Try to open the next stripe */
		if (scan->current_stripe < scan->num_stripes)
		{
			StripeMetadata *sm = &scan->stripe_meta[scan->current_stripe];

			/*
			 * Skip stripes that were fully vacuumed (row_count == 0).
			 * Their files have been removed; there is nothing to read.
			 */
			if (sm->row_count == 0)
			{
				scan->current_stripe++;
				continue;
			}

			/*
			 * Min/max stripe pruning: skip stripes whose value ranges
			 * cannot possibly satisfy the scan keys.
			 *
			 * Note: PostgreSQL's seq-scan path passes nkeys = 0 (quals
			 * are filtered by the executor, not pushed to the TAM), so
			 * pruning today is a no-op for plain WHERE-clause queries.
			 * The infrastructure is ready for a Level-4 custom scan node
			 * that will pass explicit scan keys.
			 */
			if (sscan->rs_nkeys > 0 &&
				columnar_stripe_should_skip(RelationGetLocator(sscan->rs_rd),
											sm,
											sscan->rs_key,
											sscan->rs_nkeys,
											tupdesc))
			{
				scan->current_stripe++;
				continue;
			}

			columnar_open_stripe(scan, scan->current_stripe);
			scan->current_stripe++;
			continue;
		}

		/* All stripes exhausted, check write buffer */
		if (scan->include_write_buffer)
		{
			ColumnarWriteBuffer *wb = columnar_find_write_buffer(sscan->rs_rd);

			if (wb != NULL && scan->wb_row_index < wb->nrows)
			{
				int			child_idx = 0;
				int			natts = tupdesc->natts;
				int			i;

				/* Read directly from the write buffer's in-progress arrays */
				for (i = 0; i < natts; i++)
				{
					Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

					if (attr->attisdropped)
					{
						slot->tts_isnull[i] = true;
						slot->tts_values[i] = (Datum) 0;
						continue;
					}

					/*
					 * We need to read from the in-progress Arrow array.
					 * Create a temporary view for this column.
					 */
					{
						struct ArrowArrayView col_view;
						struct ArrowError err;
						int			rc2;

						ArrowArrayViewInitFromSchema(&col_view,
													 wb->schema.children[child_idx],
													 &err);
						rc2 = ArrowArrayViewSetArray(&col_view,
													 &wb->columns[i].array,
													 &err);
						if (rc2 != NANOARROW_OK)
						{
							/* Array not finished yet - try finishing a copy */
							slot->tts_isnull[i] = true;
							slot->tts_values[i] = (Datum) 0;
							ArrowArrayViewReset(&col_view);
							child_idx++;
							continue;
						}

						if (ArrowArrayViewIsNull(&col_view, scan->wb_row_index))
						{
							slot->tts_isnull[i] = true;
							slot->tts_values[i] = (Datum) 0;
						}
						else
						{
							slot->tts_isnull[i] = false;

							switch (attr->atttypid)
							{
								case BOOLOID:
									slot->tts_values[i] = BoolGetDatum(
										ArrowArrayViewGetIntUnsafe(&col_view, scan->wb_row_index) != 0);
									break;
								case INT2OID:
									slot->tts_values[i] = Int16GetDatum(
										(int16) ArrowArrayViewGetIntUnsafe(&col_view, scan->wb_row_index));
									break;
								case INT4OID:
									slot->tts_values[i] = Int32GetDatum(
										(int32) ArrowArrayViewGetIntUnsafe(&col_view, scan->wb_row_index));
									break;
								case INT8OID:
									slot->tts_values[i] = Int64GetDatum(
										ArrowArrayViewGetIntUnsafe(&col_view, scan->wb_row_index));
									break;
								case FLOAT4OID:
									slot->tts_values[i] = Float4GetDatum(
										(float4) ArrowArrayViewGetDoubleUnsafe(&col_view, scan->wb_row_index));
									break;
								case FLOAT8OID:
									slot->tts_values[i] = Float8GetDatum(
										ArrowArrayViewGetDoubleUnsafe(&col_view, scan->wb_row_index));
									break;
								case TEXTOID:
								case VARCHAROID:
								{
									struct ArrowStringView sv =
										ArrowArrayViewGetStringUnsafe(&col_view, scan->wb_row_index);
									text   *t = (text *) palloc(VARHDRSZ + sv.size_bytes);

									SET_VARSIZE(t, VARHDRSZ + sv.size_bytes);
									memcpy(VARDATA(t), sv.data, sv.size_bytes);
									slot->tts_values[i] = PointerGetDatum(t);
									break;
								}
								case BYTEAOID:
								{
									struct ArrowBufferView bv =
										ArrowArrayViewGetBytesUnsafe(&col_view, scan->wb_row_index);
									bytea  *b = (bytea *) palloc(VARHDRSZ + bv.size_bytes);

									SET_VARSIZE(b, VARHDRSZ + bv.size_bytes);
									memcpy(VARDATA(b), bv.data.data, bv.size_bytes);
									slot->tts_values[i] = PointerGetDatum(b);
									break;
								}
								case DATEOID:
								{
									int32_t arrow_date = (int32_t)
										ArrowArrayViewGetIntUnsafe(&col_view, scan->wb_row_index);

									slot->tts_values[i] = DateADTGetDatum(
										(DateADT)(arrow_date - 10957));
									break;
								}
								case TIMESTAMPOID:
								case TIMESTAMPTZOID:
								{
									int64_t arrow_ts =
										ArrowArrayViewGetIntUnsafe(&col_view, scan->wb_row_index);

									slot->tts_values[i] = TimestampGetDatum(
										(Timestamp)(arrow_ts - INT64CONST(946684800000000)));
									break;
								}
								default:
									slot->tts_isnull[i] = true;
									slot->tts_values[i] = (Datum) 0;
									break;
							}
						}
						ArrowArrayViewReset(&col_view);
					}
					child_idx++;
				}

				ItemPointerSet(&slot->tts_tid,
							   (BlockNumber)(scan->num_stripes + 1),
							   (OffsetNumber)(scan->wb_row_index + 1));
				scan->wb_row_index++;
				ExecStoreVirtualTuple(slot);
				return true;
			}
			scan->include_write_buffer = false;
		}

		/* No more data */
		return false;
	}
}

/* ----------------------------------------------------------------
 * TID range scan (not supported)
 * ----------------------------------------------------------------
 */
static void
columnar_scan_set_tidrange(TableScanDesc sscan,
						   ItemPointer mintid,
						   ItemPointer maxtid)
{
	/* no-op: TID range scans not meaningful for columnar */
}

static bool
columnar_scan_getnextslot_tidrange(TableScanDesc sscan,
								   ScanDirection direction,
								   TupleTableSlot *slot)
{
	/* Not supported -- just return no rows */
	return false;
}

/* ----------------------------------------------------------------
 * Parallel scan stubs
 * ----------------------------------------------------------------
 */
static Size
columnar_parallelscan_estimate(Relation rel)
{
	return sizeof(ParallelTableScanDescData);
}

static Size
columnar_parallelscan_initialize(Relation rel, ParallelTableScanDesc pscan)
{
	pscan->phs_relid = RelationGetRelid(rel);
	pscan->phs_syncscan = false;
	return sizeof(ParallelTableScanDescData);
}

static void
columnar_parallelscan_reinitialize(Relation rel, ParallelTableScanDesc pscan)
{
	/* no-op */
}

/* ----------------------------------------------------------------
 * Index fetch implementation
 * ----------------------------------------------------------------
 */

/* Release any cached stripe held by an index fetch descriptor. */
static void
columnar_index_release_cached_stripe(ColumnarIndexFetchData *iscan)
{
	if (iscan->cached_delete_bitmap)
	{
		pfree(iscan->cached_delete_bitmap);
		iscan->cached_delete_bitmap = NULL;
	}
	if (iscan->batch_view_valid)
	{
		ArrowArrayViewReset(&iscan->batch_view);
		iscan->batch_view_valid = false;
	}
	if (iscan->has_batch && iscan->cached_batch.release)
	{
		iscan->cached_batch.release(&iscan->cached_batch);
		iscan->has_batch = false;
	}
	if (iscan->stream_open)
	{
		if (iscan->arrow_schema.release)
			iscan->arrow_schema.release(&iscan->arrow_schema);
		if (iscan->arrow_stream.release)
			iscan->arrow_stream.release(&iscan->arrow_stream);
		iscan->stream_open = false;
	}
	iscan->cached_stripe_id = -1;
}

static struct IndexFetchTableData *
columnar_index_fetch_begin(Relation rel)
{
	ColumnarIndexFetchData *iscan = palloc0(sizeof(ColumnarIndexFetchData));

	iscan->base.rel = rel;
	iscan->cached_stripe_id = -1;
	iscan->stream_open = false;
	iscan->has_batch = false;
	iscan->batch_view_valid = false;
	iscan->cached_delete_bitmap = NULL;

	return (struct IndexFetchTableData *) iscan;
}

static void
columnar_index_fetch_reset(struct IndexFetchTableData *scan)
{
	/*
	 * Keep any cached stripe to benefit repeated lookups in the same
	 * stripe within one index scan.
	 */
}

static void
columnar_index_fetch_end(struct IndexFetchTableData *scan)
{
	ColumnarIndexFetchData *iscan = (ColumnarIndexFetchData *) scan;

	columnar_index_release_cached_stripe(iscan);
	pfree(iscan);
}

static bool
columnar_index_fetch_tuple(struct IndexFetchTableData *scan,
						   ItemPointer tid,
						   Snapshot snapshot,
						   TupleTableSlot *slot,
						   bool *call_again, bool *all_dead)
{
	ColumnarIndexFetchData *iscan = (ColumnarIndexFetchData *) scan;
	Relation	rel = scan->rel;
	TupleDesc	tupdesc = RelationGetDescr(rel);
	BlockNumber stripe_1based = ItemPointerGetBlockNumber(tid);
	OffsetNumber row_off_1based = ItemPointerGetOffsetNumber(tid);
	int64_t		row_idx = (int64_t) row_off_1based - 1;
	ColumnarMetadata *meta;
	int			num_stripes;

	*call_again = false;
	if (all_dead)
		*all_dead = false;

	ExecClearTuple(slot);

	meta = columnar_read_metadata(RelationGetLocator(rel));
	num_stripes = meta->num_stripes;

	/* ---- Write-buffer row ---- */
	if ((int) stripe_1based == num_stripes + 1)
	{
		ColumnarWriteBuffer *wb = columnar_find_write_buffer(rel);

		if (wb == NULL || row_idx < 0 || row_idx >= wb->nrows)
		{
			if (meta->stripes)
				pfree(meta->stripes);
			pfree(meta);
			return false;
		}

		/*
		 * Populate the slot from the in-progress write buffer using the
		 * same column-reading pattern as the sequential scan.
		 */
		{
			int			child_idx = 0;
			int			natts = tupdesc->natts;
			int			i;

			for (i = 0; i < natts; i++)
			{
				Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

				if (attr->attisdropped)
				{
					slot->tts_isnull[i] = true;
					slot->tts_values[i] = (Datum) 0;
					continue;
				}

				{
					struct ArrowArrayView col_view;
					struct ArrowError err;
					int			rc;

					ArrowArrayViewInitFromSchema(&col_view,
												 wb->schema.children[child_idx],
												 &err);
					rc = ArrowArrayViewSetArray(&col_view,
											   &wb->columns[i].array,
											   &err);
					if (rc != NANOARROW_OK)
					{
						slot->tts_isnull[i] = true;
						slot->tts_values[i] = (Datum) 0;
						ArrowArrayViewReset(&col_view);
						child_idx++;
						continue;
					}

					if (ArrowArrayViewIsNull(&col_view, row_idx))
					{
						slot->tts_isnull[i] = true;
						slot->tts_values[i] = (Datum) 0;
					}
					else
					{
						slot->tts_isnull[i] = false;

						switch (attr->atttypid)
						{
							case BOOLOID:
								slot->tts_values[i] = BoolGetDatum(
									ArrowArrayViewGetIntUnsafe(&col_view, row_idx) != 0);
								break;
							case INT2OID:
								slot->tts_values[i] = Int16GetDatum(
									(int16) ArrowArrayViewGetIntUnsafe(&col_view, row_idx));
								break;
							case INT4OID:
								slot->tts_values[i] = Int32GetDatum(
									(int32) ArrowArrayViewGetIntUnsafe(&col_view, row_idx));
								break;
							case INT8OID:
								slot->tts_values[i] = Int64GetDatum(
									ArrowArrayViewGetIntUnsafe(&col_view, row_idx));
								break;
							case FLOAT4OID:
								slot->tts_values[i] = Float4GetDatum(
									(float4) ArrowArrayViewGetDoubleUnsafe(&col_view, row_idx));
								break;
							case FLOAT8OID:
								slot->tts_values[i] = Float8GetDatum(
									ArrowArrayViewGetDoubleUnsafe(&col_view, row_idx));
								break;
							case TEXTOID:
							case VARCHAROID:
							{
								struct ArrowStringView sv =
									ArrowArrayViewGetStringUnsafe(&col_view, row_idx);
								text   *t = (text *) palloc(VARHDRSZ + sv.size_bytes);

								SET_VARSIZE(t, VARHDRSZ + sv.size_bytes);
								memcpy(VARDATA(t), sv.data, sv.size_bytes);
								slot->tts_values[i] = PointerGetDatum(t);
								break;
							}
							case BYTEAOID:
							{
								struct ArrowBufferView bv =
									ArrowArrayViewGetBytesUnsafe(&col_view, row_idx);
								bytea  *b = (bytea *) palloc(VARHDRSZ + bv.size_bytes);

								SET_VARSIZE(b, VARHDRSZ + bv.size_bytes);
								memcpy(VARDATA(b), bv.data.data, bv.size_bytes);
								slot->tts_values[i] = PointerGetDatum(b);
								break;
							}
							case DATEOID:
							{
								int32_t arrow_date = (int32_t)
									ArrowArrayViewGetIntUnsafe(&col_view, row_idx);

								slot->tts_values[i] = DateADTGetDatum(
									(DateADT)(arrow_date - 10957));
								break;
							}
							case TIMESTAMPOID:
							case TIMESTAMPTZOID:
							{
								int64_t arrow_ts =
									ArrowArrayViewGetIntUnsafe(&col_view, row_idx);

								slot->tts_values[i] = TimestampGetDatum(
									(Timestamp)(arrow_ts - INT64CONST(946684800000000)));
								break;
							}
							default:
								slot->tts_isnull[i] = true;
								slot->tts_values[i] = (Datum) 0;
								break;
						}
					}
					ArrowArrayViewReset(&col_view);
				}
				child_idx++;
			}
		}

		ItemPointerCopy(tid, &slot->tts_tid);
		ExecStoreVirtualTuple(slot);

		if (meta->stripes)
			pfree(meta->stripes);
		pfree(meta);
		return true;
	}

	/* ---- On-disk stripe row ---- */
	if ((int) stripe_1based < 1 || (int) stripe_1based > num_stripes)
	{
		/* TID out of range */
		if (meta->stripes)
			pfree(meta->stripes);
		pfree(meta);
		return false;
	}

	/*
	 * Guard against stripes that were fully vacuumed (row_count == 0).
	 * Their files have been removed, so attempting to open them would
	 * fail.  Any index entries that still reference these TIDs are stale
	 * dead entries; returning false tells the executor to skip them.
	 */
	if (meta->stripes[stripe_1based - 1].row_count == 0)
	{
		if (meta->stripes)
			pfree(meta->stripes);
		pfree(meta);
		return false;
	}

	/* Load the stripe into cache if it is not already there */
	if (iscan->cached_stripe_id != (int) stripe_1based)
	{
		StripeMetadata *sm = &meta->stripes[stripe_1based - 1];
		struct ArrowError arrow_error;
		int			rc;

		/* Drop any previously cached stripe */
		columnar_index_release_cached_stripe(iscan);

		/* Open the IPC stream for this stripe */
		columnar_open_stripe_stream(RelationGetLocator(rel), sm,
									&iscan->arrow_stream,
									&iscan->arrow_schema);
		iscan->stream_open = true;

		/*
		 * Each stripe is written as a single RecordBatch.  Read it now so
		 * that subsequent lookups in the same stripe are served from RAM.
		 */
		memset(&iscan->cached_batch, 0, sizeof(iscan->cached_batch));
		rc = iscan->arrow_stream.get_next(&iscan->arrow_stream,
										  &iscan->cached_batch);
		if (rc != 0 || iscan->cached_batch.release == NULL)
		{
			/* Empty stripe — nothing to fetch */
			if (meta->stripes)
				pfree(meta->stripes);
			pfree(meta);
			return false;
		}
		iscan->has_batch = true;

		/* Build the array view for typed column access */
		ArrowArrayViewInitFromSchema(&iscan->batch_view,
									 &iscan->arrow_schema,
									 &arrow_error);
		rc = ArrowArrayViewSetArray(&iscan->batch_view,
									&iscan->cached_batch,
									&arrow_error);
		if (rc != NANOARROW_OK)
			ereport(ERROR,
					(errmsg("columnar: failed to set index fetch batch view: %s",
							arrow_error.message)));
		iscan->batch_view_valid = true;

		/* Load the delete bitmap for this stripe (NULL if no deletions) */
		iscan->cached_delete_bitmap =
			columnar_read_delete_bitmap(RelationGetLocator(rel),
										sm->stripe_id, sm->row_count);

		iscan->cached_stripe_id = (int) stripe_1based;
	}

	if (meta->stripes)
		pfree(meta->stripes);
	pfree(meta);

	/* Validate that the row offset is within the cached batch */
	if (row_idx < 0 || row_idx >= iscan->cached_batch.length)
		return false;

	/* Skip logically deleted rows */
	if (iscan->cached_delete_bitmap != NULL &&
		columnar_is_deleted(iscan->cached_delete_bitmap, row_idx))
		return false;

	/* Populate the slot from the cached batch */
	columnar_populate_slot(slot, &iscan->batch_view, row_idx, tupdesc);
	ItemPointerCopy(tid, &slot->tts_tid);
	ExecStoreVirtualTuple(slot);

	return true;
}

/* ----------------------------------------------------------------
 * Non-modifying tuple operations (stubs)
 * ----------------------------------------------------------------
 */
static bool
columnar_tuple_fetch_row_version(Relation rel, ItemPointer tid,
								 Snapshot snapshot, TupleTableSlot *slot)
{
	struct IndexFetchTableData *iscan;
	bool		call_again = false;
	bool		all_dead = false;
	bool		found;

	/*
	 * Reuse the index fetch path, which already handles both on-disk
	 * stripe rows and write-buffer rows, and respects the delete bitmap.
	 */
	iscan = columnar_index_fetch_begin(rel);
	found = columnar_index_fetch_tuple(iscan, tid, snapshot, slot,
									   &call_again, &all_dead);
	columnar_index_fetch_end(iscan);
	return found;
}

static bool
columnar_tuple_tid_valid(TableScanDesc scan, ItemPointer tid)
{
	return false;
}

static void
columnar_tuple_get_latest_tid(TableScanDesc scan, ItemPointer tid)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support get_latest_tid")));
}

static bool
columnar_tuple_satisfies_snapshot(Relation rel, TupleTableSlot *slot,
								  Snapshot snapshot)
{
	/* All rows are visible (no MVCC) */
	return true;
}

#if PG_VERSION_NUM >= 150000
static TransactionId
columnar_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	/*
	 * Columnar tables do not support DELETE or UPDATE, so no tuples can
	 * ever become dead.  Return InvalidTransactionId to indicate there
	 * are no deletable index entries.
	 */
	return InvalidTransactionId;
}
#else
static TransactionId
columnar_index_delete_tuples(Relation rel, TM_IndexDelete *delstate,
							 int ndelstate, TM_IndexStatus *status,
							 int nstatus, TransactionId oldestXmin,
							 bool knowndead)
{
	return InvalidTransactionId;
}
#endif

/* ----------------------------------------------------------------
 * Tuple insert / multi_insert
 * ----------------------------------------------------------------
 */
static void
columnar_tuple_insert(Relation rel, TupleTableSlot *slot,
					  CommandId cid, int options,
					  struct BulkInsertStateData *bistate)
{
	ColumnarWriteBuffer *buf;
	TupleDesc	tupdesc;
	int			natts;
	int			i;


	buf = columnar_get_write_buffer(rel);
	tupdesc = RelationGetDescr(rel);
	natts = tupdesc->natts;


	slot_getallattrs(slot);

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

		if (attr->attisdropped)
			continue;


		if (slot->tts_isnull[i])
			columnar_append_null(&buf->columns[i].array);
		else
			columnar_append_datum(&buf->columns[i].array,
								  slot->tts_values[i],
								  attr->atttypid);
	}
	buf->nrows++;


	/*
	 * Synthesize a TID that matches where this row will land once the
	 * write buffer is flushed to disk.  wb_stripe_block tracks the
	 * 1-based stripe index that the current write batch will become,
	 * so the TID is (wb_stripe_block, row_offset_within_batch).
	 */
	ItemPointerSet(&slot->tts_tid,
				   (BlockNumber) buf->wb_stripe_block,
				   (OffsetNumber) buf->nrows);


	/* Auto-flush when buffer is full */
	if (buf->nrows >= COLUMNAR_FLUSH_THRESHOLD)
		columnar_flush_write_buffer(buf);
}

static void
columnar_tuple_insert_speculative(Relation rel, TupleTableSlot *slot,
								  CommandId cid, int options,
								  struct BulkInsertStateData *bistate,
								  uint32 specToken)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support speculative insertion")));
}

static void
columnar_tuple_complete_speculative(Relation rel, TupleTableSlot *slot,
									uint32 specToken, bool succeeded)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support speculative insertion")));
}

static void
columnar_multi_insert(Relation rel, TupleTableSlot **slots, int nslots,
					  CommandId cid, int options,
					  struct BulkInsertStateData *bistate)
{
	int			s;

	for (s = 0; s < nslots; s++)
		columnar_tuple_insert(rel, slots[s], cid, options, bistate);
}

/* ----------------------------------------------------------------
 * Tuple delete / update / lock (not supported)
 * ----------------------------------------------------------------
 */
static TM_Result
columnar_tuple_delete(Relation rel, ItemPointer tid,
					  CommandId cid, Snapshot snapshot,
					  Snapshot crosscheck, bool wait,
					  TM_FailureData *tmfd, bool changingPart)
{
	BlockNumber stripe_1based = ItemPointerGetBlockNumber(tid);
	OffsetNumber row_off_1based = ItemPointerGetOffsetNumber(tid);
	int64_t		row_idx = (int64_t) row_off_1based - 1;
	ColumnarMetadata *meta;
	int			num_stripes;

	memset(tmfd, 0, sizeof(*tmfd));

	meta = columnar_read_metadata(RelationGetLocator(rel));
	num_stripes = meta->num_stripes;

	if ((int) stripe_1based == num_stripes + 1)
	{
		/*
		 * The target row is in the write buffer (not yet on disk).
		 * Flush the write buffer first so the row becomes part of an
		 * on-disk stripe with the same stripe ID as its TID block number.
		 * After the flush the stripe_1based value is valid on disk.
		 */
		ColumnarWriteBuffer *wb = columnar_find_write_buffer(rel);

		if (wb != NULL && wb->nrows > 0)
			columnar_flush_write_buffer(wb);

		/* Re-read metadata now that the stripe is on disk */
		if (meta->stripes)
			pfree(meta->stripes);
		pfree(meta);
		meta = columnar_read_metadata(RelationGetLocator(rel));
		num_stripes = meta->num_stripes;
	}

	/* Mark the row deleted in the on-disk stripe's bitmap */
	if ((int) stripe_1based >= 1 && (int) stripe_1based <= num_stripes)
	{
		StripeMetadata *sm = &meta->stripes[stripe_1based - 1];

		columnar_set_deleted_bit(RelationGetLocator(rel),
								 sm->stripe_id, row_idx, sm->row_count);
	}

	if (meta->stripes)
		pfree(meta->stripes);
	pfree(meta);

	return TM_Ok;
}

#if PG_VERSION_NUM >= 150000
static TM_Result
columnar_tuple_update(Relation rel, ItemPointer otid,
					  TupleTableSlot *slot, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck,
					  bool wait, TM_FailureData *tmfd,
					  LockTupleMode *lockmode,
					  TU_UpdateIndexes *update_indexes)
{
	TM_FailureData del_tmfd;
	TM_Result	result;

	memset(tmfd, 0, sizeof(*tmfd));
	if (lockmode)
		*lockmode = LockTupleExclusive;

	/* Step 1: logically delete the old row */
	result = columnar_tuple_delete(rel, otid, cid, snapshot, crosscheck,
								   wait, &del_tmfd, false);
	if (result != TM_Ok)
	{
		*tmfd = del_tmfd;
		return result;
	}

	/* Step 2: insert the new row; slot->tts_tid gets the new TID */
	columnar_tuple_insert(rel, slot, cid, 0, NULL);

	/* Tell the executor to update all index entries for this row */
	*update_indexes = TU_All;

	return TM_Ok;
}
#else
static TM_Result
columnar_tuple_update(Relation rel, ItemPointer otid,
					  TupleTableSlot *slot, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck,
					  bool wait, TM_FailureData *tmfd,
					  LockTupleMode *lockmode,
					  bool *update_indexes)
{
	TM_FailureData del_tmfd;
	TM_Result	result;

	memset(tmfd, 0, sizeof(*tmfd));
	if (lockmode)
		*lockmode = LockTupleExclusive;

	/* Step 1: logically delete the old row */
	result = columnar_tuple_delete(rel, otid, cid, snapshot, crosscheck,
								   wait, &del_tmfd, false);
	if (result != TM_Ok)
	{
		*tmfd = del_tmfd;
		return result;
	}

	/* Step 2: insert the new row; slot->tts_tid gets the new TID */
	columnar_tuple_insert(rel, slot, cid, 0, NULL);

	/* Tell the executor to update all index entries for this row */
	*update_indexes = true;

	return TM_Ok;
}
#endif

static TM_Result
columnar_tuple_lock(Relation rel, ItemPointer tid,
					Snapshot snapshot, TupleTableSlot *slot,
					CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, uint8 flags,
					TM_FailureData *tmfd)
{
	memset(tmfd, 0, sizeof(*tmfd));

	/*
	 * Columnar tables have no MVCC and no row-level locking.
	 * Populate the slot with the current row version so the caller
	 * (e.g. the UPDATE executor) can compute the new tuple.
	 */
	if (!columnar_tuple_fetch_row_version(rel, tid, snapshot, slot))
	{
		/* Row was already deleted */
		return TM_Deleted;
	}

	return TM_Ok;
}

static void
columnar_finish_bulk_insert(Relation rel, int options)
{
	ColumnarWriteBuffer *buf = columnar_find_write_buffer(rel);

	if (buf != NULL && buf->nrows > 0)
		columnar_flush_write_buffer(buf);
}

/* ----------------------------------------------------------------
 * DDL callbacks
 * ----------------------------------------------------------------
 */
#if PG_VERSION_NUM >= 160000
static void
columnar_relation_set_new_filelocator(Relation rel,
									  const RelFileLocator *newrlocator,
									  char persistence,
									  TransactionId *freezeXid,
									  MultiXactId *minmulti)
{
	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	columnar_create_storage(newrlocator);
}
#else
static void
columnar_relation_set_new_filenode(Relation rel,
								   const RelFileNode *newrnode,
								   char persistence,
								   TransactionId *freezeXid,
								   MultiXactId *minmulti)
{
	*freezeXid = InvalidTransactionId;
	*minmulti = InvalidMultiXactId;

	columnar_create_storage(newrnode);
}
#endif

static void
columnar_relation_nontransactional_truncate(Relation rel)
{
	columnar_remove_storage(RelationGetLocator(rel));
	columnar_create_storage(RelationGetLocator(rel));
}

#if PG_VERSION_NUM >= 160000
static void
columnar_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support tablespace changes")));
}
#else
static void
columnar_relation_copy_data(Relation rel, const RelFileNode *newrnode)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support tablespace changes")));
}
#endif

static void
columnar_relation_copy_for_cluster(Relation OldTable, Relation NewTable,
								   Relation OldIndex, bool use_sort,
								   TransactionId OldestXmin,
								   TransactionId *xid_cutoff,
								   MultiXactId *multi_cutoff,
								   double *num_tuples,
								   double *tups_vacuumed,
								   double *tups_recently_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support CLUSTER")));
}

static void
columnar_relation_vacuum(Relation rel, struct VacuumParams *params,
						 BufferAccessStrategy bstrategy)
{
	const RelFileLocator *locator = RelationGetLocator(rel);
	ColumnarMetadata *meta;
	int64_t		new_total_rows = 0;
	bool		any_changes = false;
	int			i;

	meta = columnar_read_metadata(locator);
	if (meta->num_stripes == 0)
	{
		pfree(meta);
		return;
	}

	for (i = 0; i < meta->num_stripes; i++)
	{
		StripeMetadata *sm = &meta->stripes[i];
		uint8_t    *del_bits;
		int64_t		del_count;
		int64_t		j;

		CHECK_FOR_INTERRUPTS();

		if (sm->row_count == 0)
			continue;			/* already vacuumed in a previous pass */

		del_bits = columnar_read_delete_bitmap(locator,
											   sm->stripe_id, sm->row_count);

		if (del_bits == NULL)
		{
			/* No deletions in this stripe */
			new_total_rows += sm->row_count;
			continue;
		}

		/* Count deleted rows */
		del_count = 0;
		for (j = 0; j < sm->row_count; j++)
		{
			if (columnar_is_deleted(del_bits, j))
				del_count++;
		}
		pfree(del_bits);

		if (del_count == 0)
		{
			/*
			 * Bitmap file exists but contains no set bits — stale file.
			 * Remove it and carry on.
			 */
			char	   *del_path = columnar_deleted_path(locator, sm->stripe_id);

			(void) unlink(del_path);
			pfree(del_path);
			new_total_rows += sm->row_count;
			continue;
		}

		if (del_count == sm->row_count)
		{
			/*
			 * Every row in this stripe is deleted.  Reclaim the disk space
			 * by removing the stripe file, its delete bitmap, and its
			 * min/max stats file.  Mark the stripe as empty in metadata
			 * (row_count = 0); we keep the entry in the stripes array so
			 * that TID block-numbers for subsequent stripes do not shift,
			 * which would invalidate existing index entries.
			 */
			char	   *stripe_path = columnar_stripe_path(locator, sm->stripe_id);
			char	   *del_path = columnar_deleted_path(locator, sm->stripe_id);
			char	   *stats_path = columnar_stats_path(locator, sm->stripe_id);

			(void) unlink(stripe_path);
			(void) unlink(del_path);
			(void) unlink(stats_path);	/* optional; ignore if absent */
			pfree(stripe_path);
			pfree(del_path);
			pfree(stats_path);

			/*
			 * Evict both the stats cache and the bitmap cache for this
			 * stripe.  Their files have been removed from disk, and the
			 * stripe's row_count is about to be zeroed.  Evicting keeps
			 * memory clean and removes any file_absent marker that a
			 * future table reusing the same relfilenode might inherit.
			 */
			columnar_stats_cache_evict_stripe(locator, sm->stripe_id);
			columnar_bitmap_cache_evict_stripe(locator, sm->stripe_id);

			sm->row_count = 0;
			sm->file_size = 0;
			sm->uncompressed_size = 0;
			any_changes = true;
			/* new_total_rows not incremented: no surviving rows */
		}
		else
		{
			/*
			 * Partial deletion: some rows survive, some are deleted.
			 * We intentionally do NOT rewrite the stripe file because
			 * doing so would shift the row offsets of surviving rows and
			 * invalidate the TIDs held by existing indexes.  The delete
			 * bitmap remains in place so that scans and index fetches
			 * continue to skip deleted rows correctly.
			 *
			 * Full rewriting with index rebuilding is a future Phase 3
			 * enhancement.
			 */
			new_total_rows += (sm->row_count - del_count);
		}
	}

	/*
	 * Update total_rows if it changed.  This keeps columnar_relation_estimate_size
	 * accurate for the query planner without requiring ANALYZE.
	 */
	if (new_total_rows != meta->total_rows)
	{
		meta->total_rows = new_total_rows;
		any_changes = true;
	}

	if (any_changes)
		columnar_write_metadata(locator, meta);

	if (meta->stripes)
		pfree(meta->stripes);
	pfree(meta);
}

/* ----------------------------------------------------------------
 * ANALYZE stubs
 * ----------------------------------------------------------------
 */
#if PG_VERSION_NUM >= 170000
static bool
columnar_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	return false;
}
#elif PG_VERSION_NUM >= 160000
static bool
columnar_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno,
								 BufferAccessStrategy bstrategy)
{
	return false;
}
#else
static bool
columnar_scan_analyze_next_block(TableScanDesc scan, BlockNumber blockno)
{
	return false;
}
#endif

static bool
columnar_scan_analyze_next_tuple(TableScanDesc scan,
								 TransactionId OldestXmin,
								 double *liverows, double *deadrows,
								 TupleTableSlot *slot)
{
	return false;
}

/* ----------------------------------------------------------------
 * Index build
 *
 * Performs a full sequential scan of the columnar table and calls the
 * supplied callback for each row with its synthesised TID and index-key
 * values.  This is the entry-point used by CREATE INDEX.
 * ----------------------------------------------------------------
 */
static double
columnar_index_build_range_scan(Relation table_rel, Relation index_rel,
								struct IndexInfo *index_info,
								bool allow_sync, bool anyvisible,
								bool progress, BlockNumber start_blockno,
								BlockNumber numblocks,
								IndexBuildCallback callback,
								void *callback_state,
								TableScanDesc scan)
{
	TupleTableSlot *slot;
	EState	   *estate;
	ExprContext *econtext;
	double		ntuples = 0;
	Datum		values[INDEX_MAX_KEYS];
	bool		isnull[INDEX_MAX_KEYS];
	bool		own_scan = false;

	/*
	 * If no scan was provided by the caller (the common case for a
	 * non-concurrent index build), create one over the entire table.
	 */
	if (scan == NULL)
	{
		scan = columnar_scan_begin(table_rel,
								  SnapshotAny,
								  0, NULL, NULL,
								  SO_TYPE_SEQSCAN);
		own_scan = true;
	}

	slot = table_slot_create(table_rel, NULL);

	/*
	 * An EState is needed so that FormIndexDatum can evaluate index
	 * expressions and partial-index predicates.
	 */
	estate = CreateExecutorState();
	econtext = GetPerTupleExprContext(estate);
	econtext->ecxt_scantuple = slot;

	while (columnar_scan_getnextslot(scan, ForwardScanDirection, slot))
	{
		CHECK_FOR_INTERRUPTS();

		/* Compute index key values (handles expressions too). */
		FormIndexDatum(index_info, slot, estate, values, isnull);

		/*
		 * All columnar rows are treated as live (tupleIsAlive = true)
		 * because columnar tables have no MVCC and no DELETE support.
		 */
		callback(index_rel, &slot->tts_tid, values, isnull,
				 true /* tupleIsAlive */, callback_state);

		ntuples += 1;
	}

	ExecDropSingleTupleTableSlot(slot);
	FreeExecutorState(estate);

	if (own_scan)
		columnar_scan_end(scan);

	return ntuples;
}

/* ----------------------------------------------------------------
 * Index validate scan
 *
 * Called during REINDEX CONCURRENTLY to validate newly-inserted index
 * entries against the visible table contents.  Since columnar tables
 * have no MVCC and do not support DELETE/UPDATE, every tuple that
 * existed at initial scan time remains valid — nothing to validate.
 * ----------------------------------------------------------------
 */
static void
columnar_index_validate_scan(Relation table_rel, Relation index_rel,
							 struct IndexInfo *index_info,
							 Snapshot snapshot,
							 struct ValidateIndexState *state)
{
	/* No-op: all tuples are always live in a columnar table. */
}

/* ----------------------------------------------------------------
 * Miscellaneous
 * ----------------------------------------------------------------
 */
static uint64
columnar_relation_size(Relation rel, ForkNumber forkNumber)
{
	if (forkNumber != MAIN_FORKNUM)
		return 0;

	return columnar_storage_size(RelationGetLocator(rel));
}

static bool
columnar_relation_needs_toast_table(Relation rel)
{
	return false;
}

static void
columnar_relation_estimate_size(Relation rel, int32 *attr_widths,
								BlockNumber *pages, double *tuples,
								double *allvisfrac)
{
	ColumnarMetadata *meta;
	ColumnarWriteBuffer *wb;
	uint64		total_size;
	int64_t		total_rows;

	meta = columnar_read_metadata(RelationGetLocator(rel));
	total_rows = meta->total_rows;
	if (meta->stripes)
		pfree(meta->stripes);
	pfree(meta);

	wb = columnar_find_write_buffer(rel);
	if (wb != NULL)
		total_rows += wb->nrows;

	total_size = columnar_storage_size(RelationGetLocator(rel));

	*pages = (BlockNumber) Max(1, total_size / BLCKSZ);
	*tuples = (double) total_rows;
	*allvisfrac = 1.0;
}

/* ----------------------------------------------------------------
 * TABLESAMPLE stubs
 * ----------------------------------------------------------------
 */
static bool
columnar_scan_sample_next_block(TableScanDesc scan,
								struct SampleScanState *scanstate)
{
	return false;
}

static bool
columnar_scan_sample_next_tuple(TableScanDesc scan,
								struct SampleScanState *scanstate,
								TupleTableSlot *slot)
{
	return false;
}

/* ----------------------------------------------------------------
 * The TableAmRoutine struct
 * ----------------------------------------------------------------
 */
const TableAmRoutine columnar_am_methods = {
	.type = T_TableAmRoutine,

	.slot_callbacks = columnar_slot_callbacks,

	.scan_begin = columnar_scan_begin,
	.scan_end = columnar_scan_end,
	.scan_rescan = columnar_scan_rescan,
	.scan_getnextslot = columnar_scan_getnextslot,

	.scan_set_tidrange = columnar_scan_set_tidrange,
	.scan_getnextslot_tidrange = columnar_scan_getnextslot_tidrange,

	.parallelscan_estimate = columnar_parallelscan_estimate,
	.parallelscan_initialize = columnar_parallelscan_initialize,
	.parallelscan_reinitialize = columnar_parallelscan_reinitialize,

	.index_fetch_begin = columnar_index_fetch_begin,
	.index_fetch_reset = columnar_index_fetch_reset,
	.index_fetch_end = columnar_index_fetch_end,
	.index_fetch_tuple = columnar_index_fetch_tuple,

	.tuple_fetch_row_version = columnar_tuple_fetch_row_version,
	.tuple_tid_valid = columnar_tuple_tid_valid,
	.tuple_get_latest_tid = columnar_tuple_get_latest_tid,
	.tuple_satisfies_snapshot = columnar_tuple_satisfies_snapshot,
	.index_delete_tuples = columnar_index_delete_tuples,

	.tuple_insert = columnar_tuple_insert,
	.tuple_insert_speculative = columnar_tuple_insert_speculative,
	.tuple_complete_speculative = columnar_tuple_complete_speculative,
	.multi_insert = columnar_multi_insert,
	.tuple_delete = columnar_tuple_delete,
	.tuple_update = columnar_tuple_update,
	.tuple_lock = columnar_tuple_lock,
	.finish_bulk_insert = columnar_finish_bulk_insert,

#if PG_VERSION_NUM >= 160000
	.relation_set_new_filelocator = columnar_relation_set_new_filelocator,
#else
	.relation_set_new_filenode = columnar_relation_set_new_filenode,
#endif
	.relation_nontransactional_truncate = columnar_relation_nontransactional_truncate,
	.relation_copy_data = columnar_relation_copy_data,
	.relation_copy_for_cluster = columnar_relation_copy_for_cluster,
	.relation_vacuum = columnar_relation_vacuum,

	.scan_analyze_next_block = columnar_scan_analyze_next_block,
	.scan_analyze_next_tuple = columnar_scan_analyze_next_tuple,

	.index_build_range_scan = columnar_index_build_range_scan,
	.index_validate_scan = columnar_index_validate_scan,

	.relation_size = columnar_relation_size,
	.relation_needs_toast_table = columnar_relation_needs_toast_table,
#if PG_VERSION_NUM >= 160000
	.relation_toast_am = NULL,
	.relation_fetch_toast_slice = NULL,
#endif

	.relation_estimate_size = columnar_relation_estimate_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = columnar_scan_sample_next_block,
	.scan_sample_next_tuple = columnar_scan_sample_next_tuple,
};
