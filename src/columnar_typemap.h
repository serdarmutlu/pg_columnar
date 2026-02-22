#ifndef COLUMNAR_TYPEMAP_H
#define COLUMNAR_TYPEMAP_H

#include "postgres.h"
#include "access/tupdesc.h"
#include "executor/tuptable.h"

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
 */
extern void columnar_populate_slot(TupleTableSlot *slot,
								   struct ArrowArrayView *batch_view,
								   int64_t row_index,
								   TupleDesc tupdesc);

#endif /* COLUMNAR_TYPEMAP_H */
