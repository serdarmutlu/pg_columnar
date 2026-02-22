#include "postgres.h"

#include "access/multixact.h"
#include "access/parallel.h"
#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/index.h"
#include "commands/vacuum.h"
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
} ColumnarScanDescData;

typedef ColumnarScanDescData *ColumnarScanDesc;

/* ----------------------------------------------------------------
 * Helper: open/close stripe IPC reader
 * ----------------------------------------------------------------
 */
static void
columnar_open_stripe(ColumnarScanDesc scan, int stripe_idx)
{
	Relation	rel = scan->base.rs_rd;
	StripeMetadata *sm = &scan->stripe_meta[stripe_idx];
	char	   *filepath;
	FILE	   *fp;
	struct ArrowIpcInputStream input_stream;
	int			rc;

	filepath = columnar_stripe_path(&rel->rd_locator, sm->stripe_id);

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

	rc = ArrowIpcArrayStreamReaderInit(&scan->arrow_stream, &input_stream, NULL);
	if (rc != NANOARROW_OK)
		ereport(ERROR, (errmsg("columnar: failed to init IPC reader")));

	/* Read schema */
	rc = scan->arrow_stream.get_schema(&scan->arrow_stream, &scan->arrow_schema);
	if (rc != 0)
		ereport(ERROR, (errmsg("columnar: failed to read schema from stripe %d",
							   sm->stripe_id)));

	scan->stream_open = true;
	scan->has_batch = false;

	pfree(filepath);
}

static void
columnar_close_stripe(ColumnarScanDesc scan)
{
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

	meta = columnar_read_metadata(&rel->rd_locator);
	scan->current_stripe = 0;
	scan->stream_open = false;
	scan->has_batch = false;
	scan->batch_view_valid = false;
	scan->wb_row_index = 0;

	/*
	 * Parallel scans are not yet supported.  Only the leader process
	 * should scan stripes; parallel workers return no rows to prevent
	 * duplicate results.
	 */
	if (IsParallelWorker())
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
 * Index fetch stubs
 * ----------------------------------------------------------------
 */
static struct IndexFetchTableData *
columnar_index_fetch_begin(Relation rel)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index scans")));
	return NULL;
}

static void
columnar_index_fetch_reset(struct IndexFetchTableData *data)
{
}

static void
columnar_index_fetch_end(struct IndexFetchTableData *data)
{
}

static bool
columnar_index_fetch_tuple(struct IndexFetchTableData *scan,
						   ItemPointer tid,
						   Snapshot snapshot,
						   TupleTableSlot *slot,
						   bool *call_again, bool *all_dead)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index scans")));
	return false;
}

/* ----------------------------------------------------------------
 * Non-modifying tuple operations (stubs)
 * ----------------------------------------------------------------
 */
static bool
columnar_tuple_fetch_row_version(Relation rel, ItemPointer tid,
								 Snapshot snapshot, TupleTableSlot *slot)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support tuple fetch by TID")));
	return false;
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

static TransactionId
columnar_index_delete_tuples(Relation rel, TM_IndexDeleteOp *delstate)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index deletion")));
	return InvalidTransactionId;
}

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


	/* Synthesize a TID */
	ItemPointerSet(&slot->tts_tid, 0, (OffsetNumber) buf->nrows);


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
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support DELETE")));
	return TM_Ok;
}

static TM_Result
columnar_tuple_update(Relation rel, ItemPointer otid,
					  TupleTableSlot *slot, CommandId cid,
					  Snapshot snapshot, Snapshot crosscheck,
					  bool wait, TM_FailureData *tmfd,
					  LockTupleMode *lockmode,
					  TU_UpdateIndexes *update_indexes)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support UPDATE")));
	return TM_Ok;
}

static TM_Result
columnar_tuple_lock(Relation rel, ItemPointer tid,
					Snapshot snapshot, TupleTableSlot *slot,
					CommandId cid, LockTupleMode mode,
					LockWaitPolicy wait_policy, uint8 flags,
					TM_FailureData *tmfd)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support row locking")));
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

static void
columnar_relation_nontransactional_truncate(Relation rel)
{
	columnar_remove_storage(&rel->rd_locator);
	columnar_create_storage(&rel->rd_locator);
}

static void
columnar_relation_copy_data(Relation rel, const RelFileLocator *newrlocator)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support tablespace changes")));
}

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
	/* no-op for Phase 1 */
}

/* ----------------------------------------------------------------
 * ANALYZE stubs
 * ----------------------------------------------------------------
 */
static bool
columnar_scan_analyze_next_block(TableScanDesc scan, ReadStream *stream)
{
	return false;
}

static bool
columnar_scan_analyze_next_tuple(TableScanDesc scan,
								 TransactionId OldestXmin,
								 double *liverows, double *deadrows,
								 TupleTableSlot *slot)
{
	return false;
}

/* ----------------------------------------------------------------
 * Index build stubs
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
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index creation")));
	return 0;
}

static void
columnar_index_validate_scan(Relation table_rel, Relation index_rel,
							 struct IndexInfo *index_info,
							 Snapshot snapshot,
							 struct ValidateIndexState *state)
{
	ereport(ERROR,
			(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
			 errmsg("columnar tables do not support index validation")));
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

	return columnar_storage_size(&rel->rd_locator);
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

	meta = columnar_read_metadata(&rel->rd_locator);
	total_rows = meta->total_rows;
	if (meta->stripes)
		pfree(meta->stripes);
	pfree(meta);

	wb = columnar_find_write_buffer(rel);
	if (wb != NULL)
		total_rows += wb->nrows;

	total_size = columnar_storage_size(&rel->rd_locator);

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

	.relation_set_new_filelocator = columnar_relation_set_new_filelocator,
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
	.relation_toast_am = NULL,
	.relation_fetch_toast_slice = NULL,

	.relation_estimate_size = columnar_relation_estimate_size,

	.scan_bitmap_next_block = NULL,
	.scan_bitmap_next_tuple = NULL,
	.scan_sample_next_block = columnar_scan_sample_next_block,
	.scan_sample_next_tuple = columnar_scan_sample_next_tuple,
};
