#ifndef COLUMNAR_WRITE_BUFFER_H
#define COLUMNAR_WRITE_BUFFER_H

#include "postgres.h"
#include "utils/relcache.h"

#include "pg_compat.h"
#include "nanoarrow.h"

/*
 * Flush threshold: number of rows to buffer before writing a stripe.
 */
#define COLUMNAR_FLUSH_THRESHOLD 10000

/*
 * Per-column Arrow array builder.
 */
typedef struct ColumnBuffer
{
	struct ArrowArray array;
	enum ArrowType arrow_type;
	Oid			pg_type;
} ColumnBuffer;

/*
 * Write buffer for a single columnar table.
 * Accumulates rows in memory, flushes to a stripe file when full.
 */
typedef struct ColumnarWriteBuffer
{
	Oid			relid;
	RelFileLocator locator;
	int			natts;
	ColumnBuffer *columns;
	int64_t		nrows;
	struct ArrowSchema schema;
	/*
	 * The 1-based block number that this write buffer will occupy once
	 * flushed to disk.  Equals (number of on-disk stripes at buffer
	 * creation time) + 1, and is incremented after each auto-flush so
	 * that TIDs assigned during INSERT remain correct after the flush
	 * makes the batch a new on-disk stripe.
	 */
	int			wb_stripe_block;
	struct ColumnarWriteBuffer *next;	/* linked list */
} ColumnarWriteBuffer;

/*
 * Get or create a write buffer for the given relation.
 */
extern ColumnarWriteBuffer *columnar_get_write_buffer(Relation rel);

/*
 * Find an existing write buffer for the relation (returns NULL if none).
 */
extern ColumnarWriteBuffer *columnar_find_write_buffer(Relation rel);

/*
 * Flush the write buffer to a stripe file on disk.
 */
extern void columnar_flush_write_buffer(ColumnarWriteBuffer *buf);

/*
 * Flush all active write buffers (called on transaction commit).
 */
extern void columnar_flush_all_write_buffers(void);

/*
 * Discard all write buffers without flushing (called on abort).
 */
extern void columnar_discard_all_write_buffers(void);

#endif /* COLUMNAR_WRITE_BUFFER_H */
