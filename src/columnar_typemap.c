#include "postgres.h"

#include "catalog/pg_type_d.h"
#include "executor/tuptable.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/numeric.h"
#include "utils/timestamp.h"
#include "utils/uuid.h"

#include "nanoarrow.h"
#include "columnar_typemap.h"

/*
 * Epoch offsets between PostgreSQL (2000-01-01) and Unix (1970-01-01).
 * PG dates: days since 2000-01-01 → Arrow Date32: days since 1970-01-01
 * Difference: 10957 days
 *
 * PG timestamps: microseconds since 2000-01-01
 * Arrow timestamps: microseconds since 1970-01-01
 * Difference: 10957 * 86400 * 1000000 = 946684800000000 microseconds
 */
#define PG_UNIX_EPOCH_DAYS		10957
#define PG_UNIX_EPOCH_USECS	INT64CONST(946684800000000)

enum ArrowType
pg_type_to_arrow_type(Oid pg_type)
{
	switch (pg_type)
	{
		case BOOLOID:
			return NANOARROW_TYPE_BOOL;
		case INT2OID:
			return NANOARROW_TYPE_INT16;
		case INT4OID:
			return NANOARROW_TYPE_INT32;
		case INT8OID:
			return NANOARROW_TYPE_INT64;
		case FLOAT4OID:
			return NANOARROW_TYPE_FLOAT;
		case FLOAT8OID:
			return NANOARROW_TYPE_DOUBLE;
		case TEXTOID:
		case VARCHAROID:
			return NANOARROW_TYPE_STRING;
		case BYTEAOID:
			return NANOARROW_TYPE_BINARY;
		case DATEOID:
			return NANOARROW_TYPE_DATE32;
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
			return NANOARROW_TYPE_TIMESTAMP;
		case UUIDOID:
			return NANOARROW_TYPE_FIXED_SIZE_BINARY;
		case NUMERICOID:
			return NANOARROW_TYPE_STRING;
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("columnar: unsupported column type OID %u", pg_type)));
			return NANOARROW_TYPE_UNINITIALIZED;
	}
}

void
columnar_build_arrow_schema(struct ArrowSchema *schema, TupleDesc tupdesc)
{
	int			natts = tupdesc->natts;
	int			nvalid = 0;
	int			i;

	/* Count non-dropped columns */
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		if (!attr->attisdropped)
			nvalid++;
	}

	ArrowSchemaInit(schema);
	ArrowSchemaSetTypeStruct(schema, nvalid);

	nvalid = 0;
	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		struct ArrowSchema *child;
		enum ArrowType arrow_type;

		if (attr->attisdropped)
			continue;

		child = schema->children[nvalid];
		arrow_type = pg_type_to_arrow_type(attr->atttypid);

		if (arrow_type == NANOARROW_TYPE_TIMESTAMP)
		{
			ArrowSchemaSetTypeDateTime(child, NANOARROW_TYPE_TIMESTAMP,
									   NANOARROW_TIME_UNIT_MICRO,
									   attr->atttypid == TIMESTAMPTZOID ? "UTC" : NULL);
		}
		else if (arrow_type == NANOARROW_TYPE_FIXED_SIZE_BINARY)
		{
			ArrowSchemaSetTypeFixedSize(child, NANOARROW_TYPE_FIXED_SIZE_BINARY,
										UUID_LEN);
		}
		else
		{
			ArrowSchemaSetType(child, arrow_type);
		}

		ArrowSchemaSetName(child, NameStr(attr->attname));

		if (attr->attnotnull)
			child->flags = 0;	/* not nullable */
		else
			child->flags = ARROW_FLAG_NULLABLE;

		nvalid++;
	}
}

void
columnar_append_datum(struct ArrowArray *array, Datum value, Oid pg_type)
{
	switch (pg_type)
	{
		case BOOLOID:
			ArrowArrayAppendInt(array, DatumGetBool(value) ? 1 : 0);
			break;
		case INT2OID:
			ArrowArrayAppendInt(array, DatumGetInt16(value));
			break;
		case INT4OID:
			ArrowArrayAppendInt(array, DatumGetInt32(value));
			break;
		case INT8OID:
			ArrowArrayAppendInt(array, DatumGetInt64(value));
			break;
		case FLOAT4OID:
			ArrowArrayAppendDouble(array, DatumGetFloat4(value));
			break;
		case FLOAT8OID:
			ArrowArrayAppendDouble(array, DatumGetFloat8(value));
			break;
		case TEXTOID:
		case VARCHAROID:
		{
			text	   *t = DatumGetTextPP(value);
			struct ArrowStringView sv;

			sv.data = VARDATA_ANY(t);
			sv.size_bytes = VARSIZE_ANY_EXHDR(t);
			ArrowArrayAppendString(array, sv);
			break;
		}
		case BYTEAOID:
		{
			bytea	   *b = DatumGetByteaPP(value);
			struct ArrowBufferView bv;

			bv.data.data = VARDATA_ANY(b);
			bv.size_bytes = VARSIZE_ANY_EXHDR(b);
			ArrowArrayAppendBytes(array, bv);
			break;
		}
		case DATEOID:
		{
			DateADT		pg_date = DatumGetDateADT(value);
			int32_t		arrow_date = (int32_t) pg_date + PG_UNIX_EPOCH_DAYS;

			ArrowArrayAppendInt(array, arrow_date);
			break;
		}
		case TIMESTAMPOID:
		case TIMESTAMPTZOID:
		{
			Timestamp	pg_ts = DatumGetTimestamp(value);
			int64_t		arrow_ts = pg_ts + PG_UNIX_EPOCH_USECS;

			ArrowArrayAppendInt(array, arrow_ts);
			break;
		}
		case UUIDOID:
		{
			pg_uuid_t  *uuid = DatumGetUUIDP(value);
			struct ArrowBufferView bv;

			bv.data.data = uuid->data;
			bv.size_bytes = UUID_LEN;
			ArrowArrayAppendBytes(array, bv);
			break;
		}
		case NUMERICOID:
		{
			Datum		cstr_datum;
			char	   *cstr;
			struct ArrowStringView sv;

			cstr_datum = DirectFunctionCall1(numeric_out,
											 value);
			cstr = DatumGetCString(cstr_datum);
			sv.data = cstr;
			sv.size_bytes = strlen(cstr);
			ArrowArrayAppendString(array, sv);
			break;
		}
		default:
			ereport(ERROR,
					(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
					 errmsg("columnar: unsupported type OID %u for insert", pg_type)));
	}
}

void
columnar_append_null(struct ArrowArray *array)
{
	ArrowArrayAppendNull(array, 1);
}

void
columnar_populate_slot(TupleTableSlot *slot,
					   struct ArrowArrayView *batch_view,
					   int64_t row_index,
					   TupleDesc tupdesc)
{
	int			natts = tupdesc->natts;
	int			child_idx = 0;
	int			i;

	for (i = 0; i < natts; i++)
	{
		Form_pg_attribute attr = TupleDescAttr(tupdesc, i);
		struct ArrowArrayView *col_view;

		if (attr->attisdropped)
		{
			slot->tts_isnull[i] = true;
			slot->tts_values[i] = (Datum) 0;
			continue;
		}

		col_view = batch_view->children[child_idx];

		if (ArrowArrayViewIsNull(col_view, row_index))
		{
			slot->tts_isnull[i] = true;
			slot->tts_values[i] = (Datum) 0;
			child_idx++;
			continue;
		}

		slot->tts_isnull[i] = false;

		switch (attr->atttypid)
		{
			case BOOLOID:
				slot->tts_values[i] = BoolGetDatum(
					ArrowArrayViewGetIntUnsafe(col_view, row_index) != 0);
				break;
			case INT2OID:
				slot->tts_values[i] = Int16GetDatum(
					(int16) ArrowArrayViewGetIntUnsafe(col_view, row_index));
				break;
			case INT4OID:
				slot->tts_values[i] = Int32GetDatum(
					(int32) ArrowArrayViewGetIntUnsafe(col_view, row_index));
				break;
			case INT8OID:
				slot->tts_values[i] = Int64GetDatum(
					ArrowArrayViewGetIntUnsafe(col_view, row_index));
				break;
			case FLOAT4OID:
				slot->tts_values[i] = Float4GetDatum(
					(float4) ArrowArrayViewGetDoubleUnsafe(col_view, row_index));
				break;
			case FLOAT8OID:
				slot->tts_values[i] = Float8GetDatum(
					ArrowArrayViewGetDoubleUnsafe(col_view, row_index));
				break;
			case TEXTOID:
			case VARCHAROID:
			{
				struct ArrowStringView sv =
					ArrowArrayViewGetStringUnsafe(col_view, row_index);
				text	   *t = (text *) palloc(VARHDRSZ + sv.size_bytes);

				SET_VARSIZE(t, VARHDRSZ + sv.size_bytes);
				memcpy(VARDATA(t), sv.data, sv.size_bytes);
				slot->tts_values[i] = PointerGetDatum(t);
				break;
			}
			case BYTEAOID:
			{
				struct ArrowBufferView bv =
					ArrowArrayViewGetBytesUnsafe(col_view, row_index);
				bytea	   *b = (bytea *) palloc(VARHDRSZ + bv.size_bytes);

				SET_VARSIZE(b, VARHDRSZ + bv.size_bytes);
				memcpy(VARDATA(b), bv.data.data, bv.size_bytes);
				slot->tts_values[i] = PointerGetDatum(b);
				break;
			}
			case DATEOID:
			{
				int32_t		arrow_date = (int32_t)
					ArrowArrayViewGetIntUnsafe(col_view, row_index);
				DateADT		pg_date = (DateADT)(arrow_date - PG_UNIX_EPOCH_DAYS);

				slot->tts_values[i] = DateADTGetDatum(pg_date);
				break;
			}
			case TIMESTAMPOID:
			case TIMESTAMPTZOID:
			{
				int64_t		arrow_ts =
					ArrowArrayViewGetIntUnsafe(col_view, row_index);
				Timestamp	pg_ts = (Timestamp)(arrow_ts - PG_UNIX_EPOCH_USECS);

				slot->tts_values[i] = TimestampGetDatum(pg_ts);
				break;
			}
			case UUIDOID:
			{
				struct ArrowBufferView bv =
					ArrowArrayViewGetBytesUnsafe(col_view, row_index);
				pg_uuid_t  *uuid = (pg_uuid_t *) palloc(sizeof(pg_uuid_t));

				memcpy(uuid->data, bv.data.data, UUID_LEN);
				slot->tts_values[i] = UUIDPGetDatum(uuid);
				break;
			}
			case NUMERICOID:
			{
				struct ArrowStringView sv =
					ArrowArrayViewGetStringUnsafe(col_view, row_index);
				char	   *cstr = pnstrdup(sv.data, sv.size_bytes);
				Datum		num;

				num = DirectFunctionCall3(numeric_in,
										  CStringGetDatum(cstr),
										  ObjectIdGetDatum(InvalidOid),
										  Int32GetDatum(-1));
				pfree(cstr);
				slot->tts_values[i] = num;
				break;
			}
			default:
				ereport(ERROR,
						(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
						 errmsg("columnar: unsupported type OID %u in scan",
								attr->atttypid)));
		}

		child_idx++;
	}
}
