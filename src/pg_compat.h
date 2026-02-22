#ifndef PG_COMPAT_H
#define PG_COMPAT_H

#include "postgres.h"

/*
 * Cross-version compatibility macros for pg_columnar.
 *
 * PostgreSQL 16 renamed RelFileNode -> RelFileLocator and changed field names:
 *   spcNode -> spcOid
 *   dbNode  -> dbOid
 *   relNode -> relNumber
 *
 * It also renamed rd_node -> rd_locator in RelationData.
 */

#if PG_VERSION_NUM < 160000

/*
 * PG14 / PG15: provide RelFileLocator as an alias for RelFileNode,
 * and define PG_LOCATOR_* accessors using the old field names.
 */
typedef RelFileNode RelFileLocator;

#define PG_LOCATOR_SPC(loc) ((loc)->spcNode)
#define PG_LOCATOR_DB(loc)  ((loc)->dbNode)
#define PG_LOCATOR_REL(loc) ((loc)->relNode)

#define RelationGetLocator(rel) (&(rel)->rd_node)

#else

/*
 * PG16+: RelFileLocator is natively defined in storage/relfilelocator.h
 * with new field names.
 */
#include "storage/relfilelocator.h"

#define PG_LOCATOR_SPC(loc) ((loc)->spcOid)
#define PG_LOCATOR_DB(loc)  ((loc)->dbOid)
#define PG_LOCATOR_REL(loc) ((loc)->relNumber)

#define RelationGetLocator(rel) (&(rel)->rd_locator)

#endif /* PG_VERSION_NUM < 160000 */

#endif /* PG_COMPAT_H */
