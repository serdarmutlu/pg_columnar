#include "postgres.h"

#include "access/tableam.h"
#include "access/xact.h"
#include "catalog/objectaccess.h"
#include "catalog/pg_am.h"
#include "catalog/pg_class.h"
#include "fmgr.h"
#include "miscadmin.h"
#include "utils/guc.h"
#include "utils/syscache.h"

#include "pg_compat.h"
#include "columnar_storage.h"
#include "columnar_write_buffer.h"

PG_MODULE_MAGIC;

extern const TableAmRoutine columnar_am_methods;
extern void columnar_custom_scan_init(void);

void		_PG_init(void);

static void columnar_xact_callback(XactEvent event, void *arg);
static void columnar_object_access_hook(ObjectAccessType access,
										Oid classId,
										Oid objectId,
										int subId,
										void *arg);

static object_access_hook_type prev_object_access_hook = NULL;
static Oid	columnar_am_oid = InvalidOid;

/* GUC: columnar.compression */
int			columnar_compression = COLUMNAR_COMPRESSION_NONE;

/* GUC: columnar.stripe_cache_size_mb */
int			columnar_stripe_cache_size_mb = 256;

static const struct config_enum_entry columnar_compression_options[] = {
	{"none", COLUMNAR_COMPRESSION_NONE, false},
	{"lz4", COLUMNAR_COMPRESSION_LZ4, false},
	{"zstd", COLUMNAR_COMPRESSION_ZSTD, false},
	{NULL, 0, false}
};

PG_FUNCTION_INFO_V1(columnar_handler);

Datum
columnar_handler(PG_FUNCTION_ARGS)
{
	PG_RETURN_POINTER(&columnar_am_methods);
}

void
_PG_init(void)
{
	DefineCustomEnumVariable("columnar.compression",
							 "Compression algorithm for new columnar stripes.",
							 NULL,
							 &columnar_compression,
							 COLUMNAR_COMPRESSION_NONE,
							 columnar_compression_options,
							 PGC_USERSET,
							 0,
							 NULL,
							 NULL,
							 NULL);

	DefineCustomIntVariable("columnar.stripe_cache_size_mb",
							"Per-backend cache size for decompressed stripe IPC bytes (MB). 0 = disabled.",
							NULL,
							&columnar_stripe_cache_size_mb,
							256,	/* default: 256 MB */
							0,		/* min: 0 (disabled) */
							16384,	/* max: 16 GB */
							PGC_USERSET,
							GUC_UNIT_MB,
							NULL,
							NULL,
							NULL);

	RegisterXactCallback(columnar_xact_callback, NULL);

	prev_object_access_hook = object_access_hook;
	object_access_hook = columnar_object_access_hook;

	/* Level 4: register custom scan provider for stripe pruning */
	columnar_custom_scan_init();
}

/*
 * Look up the OID of the 'columnar' access method, caching the result.
 */
static Oid
get_columnar_am_oid(void)
{
	if (columnar_am_oid == InvalidOid)
	{
		HeapTuple	tup;

		tup = SearchSysCache1(AMNAME, CStringGetDatum("columnar"));
		if (HeapTupleIsValid(tup))
		{
			columnar_am_oid = ((Form_pg_am) GETSTRUCT(tup))->oid;
			ReleaseSysCache(tup);
		}
	}
	return columnar_am_oid;
}

/*
 * Object access hook: clean up columnar storage when a columnar table
 * is dropped.  Called BEFORE the catalog entry is removed.
 */
static void
columnar_object_access_hook(ObjectAccessType access,
							Oid classId,
							Oid objectId,
							int subId,
							void *arg)
{
	if (prev_object_access_hook)
		prev_object_access_hook(access, classId, objectId, subId, arg);

	if (access != OAT_DROP)
		return;

	/* Only interested in relations (pg_class entries) */
	if (classId != RelationRelationId)
		return;

	/* subId != 0 means a column drop, not a table drop */
	if (subId != 0)
		return;

	{
		HeapTuple		classtup;
		Form_pg_class	classform;
		RelFileLocator	rlocator;

		classtup = SearchSysCache1(RELOID, ObjectIdGetDatum(objectId));
		if (!HeapTupleIsValid(classtup))
			return;

		classform = (Form_pg_class) GETSTRUCT(classtup);

		/* Only clean up tables that use our columnar AM */
		if (classform->relam != get_columnar_am_oid())
		{
			ReleaseSysCache(classtup);
			return;
		}

		PG_LOCATOR_SPC(&rlocator) = classform->reltablespace ?
			classform->reltablespace : MyDatabaseTableSpace;
		PG_LOCATOR_DB(&rlocator) = MyDatabaseId;
		PG_LOCATOR_REL(&rlocator) = classform->relfilenode;

		ReleaseSysCache(classtup);

		columnar_remove_storage(&rlocator);
	}
}

static void
columnar_xact_callback(XactEvent event, void *arg)
{
	switch (event)
	{
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
			columnar_flush_all_write_buffers();
			break;

		case XACT_EVENT_ABORT:
			columnar_discard_all_write_buffers();
			break;

		default:
			break;
	}
}
