#include "postgres.h"

#include "access/tupdesc.h"
#include "utils/memutils.h"
#include "utils/rel.h"

#include "pg_compat.h"
#include "nanoarrow.h"
#include "nanoarrow_ipc.h"
#include "columnar_write_buffer.h"
#include "columnar_typemap.h"
#include "columnar_storage.h"

/* Global linked list of active write buffers for this backend */
static ColumnarWriteBuffer *write_buffer_list = NULL;

/*
 * Helper: initialize a single column array for appending.
 */
static void
init_column_array(ColumnBuffer *col, struct ArrowSchema *child_schema)
{
	struct ArrowError err;
	int			rc;

	memset(&col->array, 0, sizeof(col->array));
	rc = ArrowArrayInitFromSchema(&col->array, child_schema, &err);
	if (rc != NANOARROW_OK)
		ereport(ERROR,
				(errmsg("columnar: failed to init column array: %s", err.message)));

	rc = ArrowArrayStartAppending(&col->array);
	if (rc != NANOARROW_OK)
		ereport(ERROR,
				(errmsg("columnar: failed to start appending to column array")));
}

ColumnarWriteBuffer *
columnar_find_write_buffer(Relation rel)
{
	Oid			relid = RelationGetRelid(rel);
	ColumnarWriteBuffer *buf;

	for (buf = write_buffer_list; buf != NULL; buf = buf->next)
	{
		if (buf->relid == relid)
			return buf;
	}
	return NULL;
}

ColumnarWriteBuffer *
columnar_get_write_buffer(Relation rel)
{
	ColumnarWriteBuffer *buf;
	TupleDesc	tupdesc;
	int			natts;
	int			child_idx;
	int			i;

	buf = columnar_find_write_buffer(rel);
	if (buf != NULL)
		return buf;

	/* Create a new write buffer in TopTransactionContext */
	{
		MemoryContext oldcxt = MemoryContextSwitchTo(TopTransactionContext);

		buf = palloc0(sizeof(ColumnarWriteBuffer));
		buf->relid = RelationGetRelid(rel);
		buf->locator = *RelationGetLocator(rel);

		tupdesc = RelationGetDescr(rel);
		natts = tupdesc->natts;
		buf->natts = natts;
		buf->columns = palloc0(sizeof(ColumnBuffer) * natts);
		buf->nrows = 0;

		/*
		 * Determine which stripe block number this write buffer will
		 * correspond to once flushed.  Read the current on-disk stripe
		 * count so that TIDs assigned during INSERT stay consistent with
		 * the on-disk stripe numbering.
		 */
		{
			ColumnarMetadata *meta = columnar_read_metadata(&buf->locator);

			buf->wb_stripe_block = meta->num_stripes + 1;
			if (meta->stripes)
				pfree(meta->stripes);
			pfree(meta);
		}

		/* Build Arrow schema from TupleDesc */
		columnar_build_arrow_schema(&buf->schema, tupdesc);

		/* Initialize per-column array builders */
		child_idx = 0;
		for (i = 0; i < natts; i++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, i);

			if (attr->attisdropped)
			{
				buf->columns[i].arrow_type = NANOARROW_TYPE_NA;
				buf->columns[i].pg_type = InvalidOid;
				continue;
			}

			buf->columns[i].pg_type = attr->atttypid;
			buf->columns[i].arrow_type = pg_type_to_arrow_type(attr->atttypid);
			init_column_array(&buf->columns[i], buf->schema.children[child_idx]);
			child_idx++;
		}

		/* Link into global list */
		buf->next = write_buffer_list;
		write_buffer_list = buf;

		MemoryContextSwitchTo(oldcxt);
	}

	return buf;
}

void
columnar_flush_write_buffer(ColumnarWriteBuffer *buf)
{
	struct ArrowArray record_batch;
	struct ArrowArrayView batch_view;
	struct ArrowError arrow_error;
	int			child_idx;
	int			i;
	int			rc;

	if (buf->nrows == 0)
		return;


	/* Finish building each column array */
	for (i = 0; i < buf->natts; i++)
	{
		if (buf->columns[i].pg_type == InvalidOid)
			continue;


		rc = ArrowArrayFinishBuildingDefault(&buf->columns[i].array, &arrow_error);
		if (rc != NANOARROW_OK)
			ereport(ERROR,
					(errmsg("columnar: failed to finish building column %d: %s",
							i, arrow_error.message)));

	}

	/*
	 * Build the struct (RecordBatch) array.
	 * We initialize from the schema to get proper struct type, then
	 * swap in our finished column arrays as children.
	 */

	memset(&record_batch, 0, sizeof(record_batch));
	rc = ArrowArrayInitFromSchema(&record_batch, &buf->schema, &arrow_error);
	if (rc != NANOARROW_OK)
		ereport(ERROR,
				(errmsg("columnar: failed to init record batch: %s",
						arrow_error.message)));


	record_batch.length = buf->nrows;
	record_batch.null_count = 0;

	/*
	 * Now swap each child: release the empty child that ArrowArrayInitFromSchema
	 * created, and move our populated column array into its place.
	 */
	child_idx = 0;
	for (i = 0; i < buf->natts; i++)
	{
		if (buf->columns[i].pg_type == InvalidOid)
			continue;


		/* Release the placeholder child */
		if (record_batch.children[child_idx]->release)
			record_batch.children[child_idx]->release(record_batch.children[child_idx]);

		/* Move our column data into the child slot */
		memcpy(record_batch.children[child_idx], &buf->columns[i].array,
			   sizeof(struct ArrowArray));

		/* Null out the source so we don't double-free */
		memset(&buf->columns[i].array, 0, sizeof(struct ArrowArray));

		child_idx++;
	}


	/* Create an ArrowArrayView for the IPC writer */
	memset(&batch_view, 0, sizeof(batch_view));
	rc = ArrowArrayViewInitFromSchema(&batch_view, &buf->schema, &arrow_error);
	if (rc != NANOARROW_OK)
		ereport(ERROR,
				(errmsg("columnar: failed to init batch view: %s",
						arrow_error.message)));


	rc = ArrowArrayViewSetArray(&batch_view, &record_batch, &arrow_error);
	if (rc != NANOARROW_OK)
		ereport(ERROR,
				(errmsg("columnar: failed to set batch view array: %s",
						arrow_error.message)));


	/* Write the stripe to disk */
	columnar_write_stripe(&buf->locator, &buf->schema, &batch_view, buf->nrows);


	/* Clean up */
	ArrowArrayViewReset(&batch_view);

	if (record_batch.release)
		record_batch.release(&record_batch);

	/* Re-initialize column arrays for the next batch */
	child_idx = 0;
	for (i = 0; i < buf->natts; i++)
	{
		if (buf->columns[i].pg_type == InvalidOid)
			continue;
		init_column_array(&buf->columns[i], buf->schema.children[child_idx]);
		child_idx++;
	}

	buf->nrows = 0;

	/*
	 * Advance wb_stripe_block so that TIDs assigned to subsequent rows
	 * (after an auto-flush) correctly reference the next stripe slot.
	 */
	buf->wb_stripe_block++;
}

void
columnar_flush_all_write_buffers(void)
{
	ColumnarWriteBuffer *buf;

	for (buf = write_buffer_list; buf != NULL; buf = buf->next)
	{
		if (buf->nrows > 0)
			columnar_flush_write_buffer(buf);
	}
	columnar_discard_all_write_buffers();
}

void
columnar_discard_all_write_buffers(void)
{
	ColumnarWriteBuffer *buf = write_buffer_list;

	while (buf)
	{
		ColumnarWriteBuffer *next = buf->next;
		int			i;

		for (i = 0; i < buf->natts; i++)
		{
			if (buf->columns[i].array.release)
				buf->columns[i].array.release(&buf->columns[i].array);
		}
		if (buf->schema.release)
			buf->schema.release(&buf->schema);

		buf = next;
	}
	write_buffer_list = NULL;
}
