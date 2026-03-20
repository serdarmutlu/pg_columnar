#ifndef COLUMNAR_TYPEMAP_H
#define COLUMNAR_TYPEMAP_H

#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"
#include "nodes/bitmapset.h"

#include "nanoarrow.h"

/*
 * Convert a PostgreSQL type OID to a nanoarrow ArrowType.
 * Raises an error for unsupported types.
 */
extern enum ArrowType pg_type_to_arrow_type(Oid pg_type);

/*
 * Build an Arrow schema (struct type with children) from a TupleDesc.
 * Caller must call schema->release(schema) when done.
 */
extern void columnar_build_arrow_schema(struct ArrowSchema *schema,
										TupleDesc tupdesc);

/*
 * Append a PostgreSQL Datum to an Arrow array builder.
 */
extern void columnar_append_datum(struct ArrowArray *array,
								  Datum value, Oid pg_type);

/*
 * Append a NULL to an Arrow array builder.
 */
extern void columnar_append_null(struct ArrowArray *array);

/*
 * Populate a TupleTableSlot from an Arrow batch at the given row index.
 * Uses ArrowArrayView for type-safe access.
 *
 * required_cols: bitmapset of 0-based attribute indexes to materialise.
 * Columns not in the set are left as NULL in the slot (safe when the caller
 * guarantees those columns are not accessed above the scan node).
 * Pass NULL to materialise all columns (conservative default).
 */
extern void columnar_populate_slot(TupleTableSlot *slot,
								   struct ArrowArrayView *batch_view,
								   int64_t row_index,
								   TupleDesc tupdesc,
								   const Bitmapset *required_cols);

#endif /* COLUMNAR_TYPEMAP_H */
