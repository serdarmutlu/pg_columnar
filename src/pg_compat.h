/*
 * pg_compat.h
 *
 * Compatibility macros for PostgreSQL 14-18 support.
 */
#ifndef PG_COMPAT_H
#define PG_COMPAT_H

#include "postgres.h"

/* Minimum supported version check */
#if PG_VERSION_NUM < 140000
#error "pg_columnar requires PostgreSQL 14 or later"
#endif

/*
 * PostgreSQL 16 renamed RelFileNode to RelFileLocator and changed field names:
 *   - spcNode -> spcOid
 *   - dbNode  -> dbOid
 *   - relNode -> relNumber
 *
 * The header also moved from storage/relfilenode.h to storage/relfilelocator.h.
 * The Relation struct field changed from rd_node to rd_locator.
 */
#if PG_VERSION_NUM >= 160000

#include "storage/relfilelocator.h"

/* Use native PG16+ types and field names */
#define PG_RELFILELOCATOR			RelFileLocator
#define PG_LOCATOR_SPC(loc)			((loc)->spcOid)
#define PG_LOCATOR_DB(loc)			((loc)->dbOid)
#define PG_LOCATOR_REL(loc)			((loc)->relNumber)
#define RelationGetLocator(rel)		(&(rel)->rd_locator)

#else /* PG14, PG15 */

#include "storage/relfilenode.h"

/* Map PG16+ names to PG14-15 equivalents */
typedef RelFileNode					PG_RELFILELOCATOR;
#define PG_LOCATOR_SPC(loc)			((loc)->spcNode)
#define PG_LOCATOR_DB(loc)			((loc)->dbNode)
#define PG_LOCATOR_REL(loc)			((loc)->relNode)
#define RelationGetLocator(rel)		(&(rel)->rd_node)

/* Provide RelFileLocator as an alias for older PG versions */
typedef RelFileNode					RelFileLocator;

#endif /* PG_VERSION_NUM >= 160000 */

/*
 * PostgreSQL 16 renamed the TableAmRoutine callback from
 * relation_set_new_filenode to relation_set_new_filelocator.
 */
#if PG_VERSION_NUM >= 160000
#define TABLEAM_SET_FILELOCATOR_NAME	relation_set_new_filelocator
#else
#define TABLEAM_SET_FILELOCATOR_NAME	relation_set_new_filenode
#endif

/*
 * The scan_analyze_next_block callback signature changed twice:
 *   PG17+ : (TableScanDesc, ReadStream *)          -- read-ahead support
 *   PG16  : (TableScanDesc, BlockNumber, BufferAccessStrategy)
 *   PG14/15: (TableScanDesc, BlockNumber)
 */
#if PG_VERSION_NUM >= 170000
#define ANALYZE_NEXT_BLOCK_ARGS		TableScanDesc scan, ReadStream *stream
#define ANALYZE_NEXT_BLOCK_PARAMS	scan, stream
#elif PG_VERSION_NUM >= 160000
#define ANALYZE_NEXT_BLOCK_ARGS		TableScanDesc scan, BlockNumber blockno, \
									BufferAccessStrategy bstrategy
#define ANALYZE_NEXT_BLOCK_PARAMS	scan, blockno, bstrategy
#else
#define ANALYZE_NEXT_BLOCK_ARGS		TableScanDesc scan, BlockNumber blockno
#define ANALYZE_NEXT_BLOCK_PARAMS	scan, blockno
#endif

/*
 * PostgreSQL 16 added relation_toast_am and relation_fetch_toast_slice
 * to TableAmRoutine. Earlier versions don't have these fields.
 */
#if PG_VERSION_NUM >= 160000
#define TABLEAM_HAS_TOAST_AM		1
#else
#define TABLEAM_HAS_TOAST_AM		0
#endif

/*
 * PostgreSQL 16 changed the index_delete_tuples signature.
 * PG16+ uses TM_IndexDeleteOp, earlier versions use different parameters.
 */
#if PG_VERSION_NUM >= 160000
#define INDEX_DELETE_USES_TM_OP		1
#else
#define INDEX_DELETE_USES_TM_OP		0
#endif

/*
 * PostgreSQL 15 added TU_UpdateIndexes enum for tuple_update.
 * Earlier versions use a bool pointer.
 */
#if PG_VERSION_NUM >= 150000
#define TUPLE_UPDATE_HAS_TU_ENUM	1
#else
#define TUPLE_UPDATE_HAS_TU_ENUM	0
#endif

/*
 * PostgreSQL 14 compatibility: some minor differences in TableAmRoutine.
 */
#if PG_VERSION_NUM >= 140000 && PG_VERSION_NUM < 150000
#define PG14_COMPAT					1
#else
#define PG14_COMPAT					0
#endif

/*
 * TID range scan support was added in PostgreSQL 14.
 * All our supported versions have it.
 */
#define TABLEAM_HAS_TIDRANGE		1

/*
 * Helper macro to set up RelFileLocator fields portably.
 */
#define PG_SET_LOCATOR(loc, spc, db, rel) \
	do { \
		PG_LOCATOR_SPC(loc) = (spc); \
		PG_LOCATOR_DB(loc) = (db); \
		PG_LOCATOR_REL(loc) = (rel); \
	} while (0)

#endif /* PG_COMPAT_H */
