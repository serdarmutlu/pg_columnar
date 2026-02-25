#include "postgres.h"

#include "miscadmin.h"
#include "utils/hsearch.h"
#include "utils/memutils.h"

#include <dirent.h>
#include <inttypes.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#ifdef HAVE_LIBZSTD
#include <zstd.h>
#endif
#ifdef HAVE_LIBLZ4
#include <lz4frame.h>
#endif

#include "nanoarrow.h"
#include "nanoarrow_ipc.h"
#include "columnar_storage.h"

/* Metadata format:
 *
 * Line 1: num_stripes total_rows
 * Lines 2..N+1: stripe_id row_count file_size compression uncompressed_size
 *
 * For backward compatibility, the reader also accepts the old 3-field format
 * (stripe_id row_count file_size) and defaults compression to NONE.
 */

/* ----------------------------------------------------------------
 * Backend-local metadata cache
 *
 * Avoids re-reading the metadata file on every columnar_read_metadata()
 * call.  The primary beneficiary is columnar_index_fetch_tuple(), which
 * is invoked for every TID during an index scan and previously incurred
 * a full metadata-file read per call.  columnar_tuple_delete() and
 * columnar_relation_estimate_size() also benefit.
 *
 * Key:   (dbOid, relNumber) — the two fields used to build the columnar
 *        directory path, matching PG_LOCATOR_DB/REL.
 * Value: full copy of ColumnarMetadata (num_stripes, total_rows, stripes[]).
 *
 * Invalidation rules:
 *   columnar_write_metadata() updates the cache after every successful
 *   write, keeping it in sync with the on-disk file for this backend.
 *
 *   columnar_remove_storage() evicts the entry when a table is dropped
 *   or truncated.
 *
 * Cross-backend consistency: the cache is intentionally backend-local and
 * does not handle concurrent modifications from other backends, matching
 * the existing no-MVCC stance of the columnar TAM.
 * ----------------------------------------------------------------
 */

typedef struct ColumnarMetadataCacheKey
{
	Oid			dbOid;		/* PG_LOCATOR_DB value */
	Oid			relNumber;	/* PG_LOCATOR_REL value (Oid on all PG versions) */
} ColumnarMetadataCacheKey;

typedef struct ColumnarMetadataCacheEntry
{
	ColumnarMetadataCacheKey key;	/* MUST be first for HTAB */
	int			num_stripes;
	int64_t		total_rows;
	StripeMetadata *stripes;		/* palloc'd in metadata_cache_ctx; NULL if 0 stripes */
} ColumnarMetadataCacheEntry;

static HTAB *metadata_cache = NULL;
static MemoryContext metadata_cache_ctx = NULL;

/*
 * Initialize the metadata cache.  Called lazily on first use.
 */
static void
columnar_metadata_cache_init(void)
{
	HASHCTL		ctl;

	metadata_cache_ctx = AllocSetContextCreate(TopMemoryContext,
											   "columnar metadata cache",
											   ALLOCSET_SMALL_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ColumnarMetadataCacheKey);
	ctl.entrysize = sizeof(ColumnarMetadataCacheEntry);
	ctl.hcxt = metadata_cache_ctx;

	metadata_cache = hash_create("columnar metadata cache",
								 16,	/* initial capacity; grows automatically */
								 &ctl,
								 HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Insert or overwrite a metadata cache entry.
 *
 * Called after every successful metadata file read (cache miss path) and
 * after every metadata file write, so the cache always reflects the latest
 * on-disk state for this backend.
 */
static void
columnar_metadata_cache_update(const RelFileLocator *locator,
							   const ColumnarMetadata *meta)
{
	ColumnarMetadataCacheKey key;
	ColumnarMetadataCacheEntry *entry;
	bool		found;
	MemoryContext oldcxt;

	if (metadata_cache == NULL)
		columnar_metadata_cache_init();

	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);

	entry = hash_search(metadata_cache, &key, HASH_ENTER, &found);

	/* Discard the old stripes array when overwriting an existing entry */
	if (found && entry->stripes != NULL)
	{
		pfree(entry->stripes);
		entry->stripes = NULL;
	}

	entry->num_stripes = meta->num_stripes;
	entry->total_rows = meta->total_rows;

	if (meta->num_stripes > 0 && meta->stripes != NULL)
	{
		/* Allocate in the long-lived cache context so it outlives the query */
		oldcxt = MemoryContextSwitchTo(metadata_cache_ctx);
		entry->stripes = palloc(sizeof(StripeMetadata) * meta->num_stripes);
		MemoryContextSwitchTo(oldcxt);
		memcpy(entry->stripes, meta->stripes,
			   sizeof(StripeMetadata) * meta->num_stripes);
	}
	else
		entry->stripes = NULL;
}

/*
 * Remove a metadata cache entry.
 *
 * Called when the underlying storage is removed (DROP TABLE, TRUNCATE) so
 * the cache cannot return stale data for a future table that reuses the
 * same relfilenode.
 */
static void
columnar_metadata_cache_evict(const RelFileLocator *locator)
{
	ColumnarMetadataCacheKey key;
	ColumnarMetadataCacheEntry *entry;
	bool		found;

	if (metadata_cache == NULL)
		return;					/* cache not yet initialised; nothing to evict */

	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);

	/* Free the stripes array before removing the HTAB entry to avoid a leak */
	entry = hash_search(metadata_cache, &key, HASH_FIND, &found);
	if (found && entry->stripes != NULL)
	{
		pfree(entry->stripes);
		entry->stripes = NULL;
	}

	hash_search(metadata_cache, &key, HASH_REMOVE, NULL);
}

/* ----------------------------------------------------------------
 * Backend-local per-stripe statistics cache
 *
 * Stripe stats (.stats files) are written once at flush time and never
 * modified afterward (stripes are write-once / immutable).  Caching
 * them avoids one fopen+parse per stripe per call to
 * columnar_stripe_should_skip().
 *
 * Key:   (dbOid, relNumber, stripe_id)
 * Value: copy of ColumnarStripeStats, or a "file absent" marker.
 *
 * Invalidation rules:
 *   columnar_stats_cache_evict_stripe() — called from VACUUM when a
 *   fully-deleted stripe's .stats file is removed.
 *
 *   columnar_stats_cache_evict_relation() — called from
 *   columnar_remove_storage() on DROP TABLE / TRUNCATE.
 *
 * The "file absent" marker caches the fact that a .stats file does not
 * exist for a stripe (e.g. stripes written before Level-3 was installed).
 * This avoids repeated fopen() failures for every stripe on every scan.
 * ----------------------------------------------------------------
 */

typedef struct ColumnarStatsCacheKey
{
	Oid			dbOid;
	Oid			relNumber;
	int			stripe_id;
	/* no padding needed: 3 × 4 bytes = 12 bytes, naturally aligned */
} ColumnarStatsCacheKey;

typedef struct ColumnarStatsCacheEntry
{
	ColumnarStatsCacheKey key;			/* MUST be first for HTAB */
	bool		file_absent;			/* true = .stats file confirmed missing */
	int			ncols;
	ColumnarColumnStats *cols;			/* palloc'd in stats_cache_ctx; NULL if ncols==0 */
} ColumnarStatsCacheEntry;

static HTAB *stats_cache = NULL;
static MemoryContext stats_cache_ctx = NULL;

/*
 * Initialise the stats cache lazily on first use.
 */
static void
columnar_stats_cache_init(void)
{
	HASHCTL		ctl;

	stats_cache_ctx = AllocSetContextCreate(TopMemoryContext,
											"columnar stats cache",
											ALLOCSET_SMALL_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ColumnarStatsCacheKey);
	ctl.entrysize = sizeof(ColumnarStatsCacheEntry);
	ctl.hcxt = stats_cache_ctx;

	stats_cache = hash_create("columnar stats cache",
							  64,	/* initial buckets; grows automatically */
							  &ctl,
							  HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Insert or overwrite a stats cache entry.
 *
 * Pass stats=NULL to record that the .stats file is absent for this stripe
 * (avoids repeated fopen() failures for pre-Level-3 stripes).
 * Pass a valid ColumnarStripeStats pointer to cache the parsed stats.
 *
 * Called after every successful stats file write and after every
 * successful stats file read (cache-miss path).
 */
static void
columnar_stats_cache_update(const RelFileLocator *locator,
							int stripe_id,
							const ColumnarStripeStats *stats)
{
	ColumnarStatsCacheKey key;
	ColumnarStatsCacheEntry *entry;
	bool		found;
	MemoryContext oldcxt;

	if (stats_cache == NULL)
		columnar_stats_cache_init();

	memset(&key, 0, sizeof(key));
	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);
	key.stripe_id = stripe_id;

	entry = hash_search(stats_cache, &key, HASH_ENTER, &found);

	/* Free previous cols array when overwriting */
	if (found && entry->cols != NULL)
	{
		pfree(entry->cols);
		entry->cols = NULL;
	}

	if (stats == NULL)
	{
		/* Record that the file is absent */
		entry->file_absent = true;
		entry->ncols = 0;
		entry->cols = NULL;
		return;
	}

	entry->file_absent = false;
	entry->ncols = stats->ncols;

	if (stats->ncols > 0 && stats->cols != NULL)
	{
		oldcxt = MemoryContextSwitchTo(stats_cache_ctx);
		entry->cols = palloc(sizeof(ColumnarColumnStats) * stats->ncols);
		MemoryContextSwitchTo(oldcxt);
		memcpy(entry->cols, stats->cols,
			   sizeof(ColumnarColumnStats) * stats->ncols);
	}
	else
		entry->cols = NULL;
}

/*
 * Evict the stats cache entry for a single stripe.
 *
 * Called from columnar_relation_vacuum() when a fully-deleted stripe's
 * .stats file is removed from disk.  Declared extern so columnar_tableam.c
 * can call it directly.
 */
void
columnar_stats_cache_evict_stripe(const RelFileLocator *locator, int stripe_id)
{
	ColumnarStatsCacheKey key;
	ColumnarStatsCacheEntry *entry;
	bool		found;

	if (stats_cache == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);
	key.stripe_id = stripe_id;

	entry = hash_search(stats_cache, &key, HASH_FIND, &found);
	if (found && entry->cols != NULL)
	{
		pfree(entry->cols);
		entry->cols = NULL;
	}
	hash_search(stats_cache, &key, HASH_REMOVE, NULL);
}

/*
 * Evict all stats cache entries for a relation.
 *
 * Called from columnar_remove_storage() on DROP TABLE / TRUNCATE.
 * Since the cache key includes stripe_id we cannot do a single
 * HASH_REMOVE; instead we collect all matching keys then delete them.
 */
static void
columnar_stats_cache_evict_relation(const RelFileLocator *locator)
{
	Oid			target_db = (Oid) PG_LOCATOR_DB(locator);
	Oid			target_rel = (Oid) PG_LOCATOR_REL(locator);
	HASH_SEQ_STATUS seq;
	ColumnarStatsCacheEntry *entry;
	ColumnarStatsCacheKey *keys;
	int			nkeys = 0;
	int			maxkeys = 64;
	int			i;

	if (stats_cache == NULL)
		return;

	keys = palloc(sizeof(ColumnarStatsCacheKey) * maxkeys);

	hash_seq_init(&seq, stats_cache);
	while ((entry = (ColumnarStatsCacheEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->key.dbOid != target_db || entry->key.relNumber != target_rel)
			continue;

		if (nkeys >= maxkeys)
		{
			maxkeys *= 2;
			keys = repalloc(keys, sizeof(ColumnarStatsCacheKey) * maxkeys);
		}
		keys[nkeys++] = entry->key;
	}
	/* seq scan ran to completion (returned NULL); no need for hash_seq_term */

	for (i = 0; i < nkeys; i++)
	{
		bool		found;

		entry = hash_search(stats_cache, &keys[i], HASH_FIND, &found);
		if (found && entry->cols != NULL)
		{
			pfree(entry->cols);
			entry->cols = NULL;
		}
		hash_search(stats_cache, &keys[i], HASH_REMOVE, NULL);
	}

	pfree(keys);
}

/* ----------------------------------------------------------------
 * Backend-local delete-bitmap cache
 *
 * Each stripe that has at least one deleted row gets a companion
 * .deleted file (a packed bitset).  Before this cache, every seq-scan
 * stripe open, every index-fetch stripe load, every VACUUM pass, and
 * every DELETE operation incurred at least one fopen+fread.
 *
 * Unlike stats (.stats files are write-once), delete bitmaps are
 * mutable: every DELETE/UPDATE modifies the bitmap via
 * columnar_set_deleted_bit().  The cache is therefore updated after
 * every successful write, not just after the initial read.
 *
 * Key:   (dbOid, relNumber, stripe_id)
 * Value: copy of the bitmap bytes, or a "file absent" marker.
 *
 * "file absent" (file_absent=true) means no .deleted file exists for
 * this stripe — i.e. no rows have been deleted yet.  This avoids
 * repeated fopen() failures on clean stripes.
 *
 * Cross-backend staleness: intentionally not handled — matches the
 * no-MVCC stance of the entire TAM.
 *
 * Invalidation rules:
 *   columnar_bitmap_cache_evict_stripe() — called from VACUUM when a
 *   fully-deleted stripe's .deleted file is removed.
 *
 *   columnar_bitmap_cache_evict_relation() — called from
 *   columnar_remove_storage() on DROP TABLE / TRUNCATE.
 * ----------------------------------------------------------------
 */

typedef struct ColumnarBitmapCacheKey
{
	Oid			dbOid;
	Oid			relNumber;
	int			stripe_id;
	/* 3 × 4 bytes = 12 bytes, naturally aligned — no padding */
} ColumnarBitmapCacheKey;

typedef struct ColumnarBitmapCacheEntry
{
	ColumnarBitmapCacheKey key;		/* MUST be first for HTAB */
	bool		file_absent;		/* true = no .deleted file (stripe has no deletions) */
	int64_t		nbytes;				/* byte count of bits[]; 0 if file_absent */
	uint8_t    *bits;				/* palloc'd in bitmap_cache_ctx; NULL if file_absent */
} ColumnarBitmapCacheEntry;

static HTAB *bitmap_cache = NULL;
static MemoryContext bitmap_cache_ctx = NULL;

/*
 * Initialise the bitmap cache lazily on first use.
 */
static void
columnar_bitmap_cache_init(void)
{
	HASHCTL		ctl;

	bitmap_cache_ctx = AllocSetContextCreate(TopMemoryContext,
											 "columnar bitmap cache",
											 ALLOCSET_SMALL_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ColumnarBitmapCacheKey);
	ctl.entrysize = sizeof(ColumnarBitmapCacheEntry);
	ctl.hcxt = bitmap_cache_ctx;

	bitmap_cache = hash_create("columnar bitmap cache",
							   64,	/* initial buckets; grows automatically */
							   &ctl,
							   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Insert or overwrite a bitmap cache entry.
 *
 * Pass bits=NULL / nbytes=0 to record that the .deleted file is absent
 * for this stripe (stripe has no deletions yet).
 * Pass a valid bits pointer to cache the current bitmap contents.
 *
 * The cache is updated after every successful file read (cache-miss
 * path) and after every successful file write (columnar_set_deleted_bit).
 * This "update after write" discipline ensures we only cache data that
 * is safely on disk.
 */
static void
columnar_bitmap_cache_update(const RelFileLocator *locator,
							 int stripe_id,
							 const uint8_t *bits,
							 int64_t nbytes)
{
	ColumnarBitmapCacheKey key;
	ColumnarBitmapCacheEntry *entry;
	bool		found;
	MemoryContext oldcxt;

	if (bitmap_cache == NULL)
		columnar_bitmap_cache_init();

	memset(&key, 0, sizeof(key));
	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);
	key.stripe_id = stripe_id;

	entry = hash_search(bitmap_cache, &key, HASH_ENTER, &found);

	/* Free the old bits array when overwriting an existing entry */
	if (found && entry->bits != NULL)
	{
		pfree(entry->bits);
		entry->bits = NULL;
	}

	if (bits == NULL || nbytes == 0)
	{
		/* Record that the file is absent (stripe has no deletions) */
		entry->file_absent = true;
		entry->nbytes = 0;
		entry->bits = NULL;
		return;
	}

	entry->file_absent = false;
	entry->nbytes = nbytes;

	oldcxt = MemoryContextSwitchTo(bitmap_cache_ctx);
	entry->bits = palloc(nbytes);
	MemoryContextSwitchTo(oldcxt);
	memcpy(entry->bits, bits, nbytes);
}

/*
 * Evict the bitmap cache entry for a single stripe.
 *
 * Called from columnar_relation_vacuum() when a fully-deleted stripe's
 * .deleted file is removed from disk.  Declared extern so
 * columnar_tableam.c can call it alongside the stats cache eviction.
 */
void
columnar_bitmap_cache_evict_stripe(const RelFileLocator *locator, int stripe_id)
{
	ColumnarBitmapCacheKey key;
	ColumnarBitmapCacheEntry *entry;
	bool		found;

	if (bitmap_cache == NULL)
		return;

	memset(&key, 0, sizeof(key));
	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);
	key.stripe_id = stripe_id;

	entry = hash_search(bitmap_cache, &key, HASH_FIND, &found);
	if (found && entry->bits != NULL)
	{
		pfree(entry->bits);
		entry->bits = NULL;
	}
	hash_search(bitmap_cache, &key, HASH_REMOVE, NULL);
}

/*
 * Evict all bitmap cache entries for a relation.
 *
 * Called from columnar_remove_storage() on DROP TABLE / TRUNCATE.
 * Uses collect-then-delete to avoid modifying the HTAB during iteration.
 */
static void
columnar_bitmap_cache_evict_relation(const RelFileLocator *locator)
{
	Oid			target_db = (Oid) PG_LOCATOR_DB(locator);
	Oid			target_rel = (Oid) PG_LOCATOR_REL(locator);
	HASH_SEQ_STATUS seq;
	ColumnarBitmapCacheEntry *entry;
	ColumnarBitmapCacheKey *keys;
	int			nkeys = 0;
	int			maxkeys = 64;
	int			i;

	if (bitmap_cache == NULL)
		return;

	keys = palloc(sizeof(ColumnarBitmapCacheKey) * maxkeys);

	hash_seq_init(&seq, bitmap_cache);
	while ((entry = (ColumnarBitmapCacheEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->key.dbOid != target_db || entry->key.relNumber != target_rel)
			continue;

		if (nkeys >= maxkeys)
		{
			maxkeys *= 2;
			keys = repalloc(keys, sizeof(ColumnarBitmapCacheKey) * maxkeys);
		}
		keys[nkeys++] = entry->key;
	}
	/* seq scan ran to completion — no need for hash_seq_term */

	for (i = 0; i < nkeys; i++)
	{
		bool		found;

		entry = hash_search(bitmap_cache, &keys[i], HASH_FIND, &found);
		if (found && entry->bits != NULL)
		{
			pfree(entry->bits);
			entry->bits = NULL;
		}
		hash_search(bitmap_cache, &keys[i], HASH_REMOVE, NULL);
	}

	pfree(keys);
}

/* ----------------------------------------------------------------
 * Backend-local per-stripe IPC bytes cache (Level 4b)
 *
 * Caches the decompressed Arrow IPC stream bytes for each stripe so
 * that repeat accesses within the same backend session avoid disk I/O
 * and (for compressed stripes) decompression entirely.
 *
 * Key:   (dbOid, relNumber, stripe_id) — same three-field key as
 *        the stats and bitmap caches.
 * Value: palloc'd copy of the decompressed IPC bytes in
 *        stripe_ipc_cache_ctx.
 * Capacity: bounded by columnar.stripe_cache_size_mb (default 256 MB).
 *           When a new entry would exceed the limit, the LRU entry is
 *           evicted first.
 *
 * Stripes are write-once: once flushed, a stripe file is never modified.
 * Therefore no per-stripe invalidation on write is needed; only on
 * DROP TABLE / TRUNCATE (which evicts the whole relation).
 *
 * LRU tracking: each entry carries a lru_clock counter that is
 * incremented on every cache hit.  Eviction performs a linear scan to
 * find the entry with the smallest clock value.  This is acceptable
 * because (a) the cache is bounded in size, so the number of entries is
 * moderate, and (b) the eviction path is rare relative to the hit path.
 * ----------------------------------------------------------------
 */

typedef struct ColumnarIpcCacheKey
{
	Oid			dbOid;
	Oid			relNumber;
	int			stripe_id;
	/* 3 × 4 = 12 bytes, naturally aligned; no padding */
} ColumnarIpcCacheKey;

typedef struct ColumnarIpcCacheEntry
{
	ColumnarIpcCacheKey key;		/* MUST be first for HTAB */
	uint8_t    *ipc_bytes;			/* palloc'd in stripe_ipc_cache_ctx */
	size_t		ipc_size;
	uint64		lru_clock;			/* incremented on every hit */
} ColumnarIpcCacheEntry;

static HTAB *stripe_ipc_cache = NULL;
static MemoryContext stripe_ipc_cache_ctx = NULL;
static uint64 stripe_ipc_lru_clock = 0;
static size_t stripe_ipc_total_bytes = 0;

/*
 * Initialise the IPC bytes cache.  Called lazily on first use.
 */
static void
columnar_stripe_ipc_cache_init(void)
{
	HASHCTL		ctl;

	stripe_ipc_cache_ctx = AllocSetContextCreate(TopMemoryContext,
												  "columnar stripe ipc cache",
												  ALLOCSET_DEFAULT_SIZES);

	memset(&ctl, 0, sizeof(ctl));
	ctl.keysize = sizeof(ColumnarIpcCacheKey);
	ctl.entrysize = sizeof(ColumnarIpcCacheEntry);
	ctl.hcxt = stripe_ipc_cache_ctx;

	stripe_ipc_cache = hash_create("columnar stripe ipc cache",
								   256,	/* initial buckets; grows automatically */
								   &ctl,
								   HASH_ELEM | HASH_BLOBS | HASH_CONTEXT);
}

/*
 * Evict the single LRU entry from the IPC bytes cache.
 * No-op if the cache is empty.
 */
static void
columnar_stripe_ipc_cache_evict_one_lru(void)
{
	HASH_SEQ_STATUS seq;
	ColumnarIpcCacheEntry *entry;
	ColumnarIpcCacheKey lru_key;
	uint64		min_clock = PG_UINT64_MAX;
	bool		found_any = false;
	bool		found;

	if (stripe_ipc_cache == NULL)
		return;

	hash_seq_init(&seq, stripe_ipc_cache);
	while ((entry = (ColumnarIpcCacheEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->lru_clock < min_clock)
		{
			min_clock = entry->lru_clock;
			lru_key = entry->key;
			found_any = true;
		}
	}
	/* seq scan ran to completion — no need for hash_seq_term */

	if (!found_any)
		return;

	entry = hash_search(stripe_ipc_cache, &lru_key, HASH_FIND, &found);
	if (found && entry->ipc_bytes != NULL)
	{
		stripe_ipc_total_bytes -= entry->ipc_size;
		pfree(entry->ipc_bytes);
		entry->ipc_bytes = NULL;
	}
	hash_search(stripe_ipc_cache, &lru_key, HASH_REMOVE, NULL);
}

/*
 * Look up a stripe's IPC bytes in the cache.
 *
 * Returns NULL on a cache miss.  On a hit, returns a palloc'd copy of
 * the cached bytes in CurrentMemoryContext and sets *out_size.
 * The caller owns the returned allocation and should pfree it when done.
 *
 * Updates the LRU clock for the hit entry.
 */
uint8_t *
columnar_stripe_ipc_cache_lookup(const RelFileLocator *locator,
								  int stripe_id,
								  size_t *out_size)
{
	ColumnarIpcCacheKey key;
	ColumnarIpcCacheEntry *entry;
	bool		found;
	uint8_t    *copy;

	if (stripe_ipc_cache == NULL)
		return NULL;

	memset(&key, 0, sizeof(key));
	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);
	key.stripe_id = stripe_id;

	entry = hash_search(stripe_ipc_cache, &key, HASH_FIND, &found);
	if (!found || entry->ipc_bytes == NULL)
		return NULL;

	entry->lru_clock = ++stripe_ipc_lru_clock;
	*out_size = entry->ipc_size;

	/* Return a palloc'd copy in CurrentMemoryContext */
	copy = palloc(entry->ipc_size);
	memcpy(copy, entry->ipc_bytes, entry->ipc_size);
	return copy;
}

/*
 * Insert (or replace) a stripe's IPC bytes in the cache.
 *
 * Copies size bytes from bytes into the cache context.  Evicts the LRU
 * entry if adding the new entry would exceed the configured limit.
 * No-op if columnar.stripe_cache_size_mb == 0 (caching disabled) or if
 * size exceeds the entire configured limit.
 */
void
columnar_stripe_ipc_cache_insert(const RelFileLocator *locator,
								  int stripe_id,
								  const uint8_t *bytes,
								  size_t size)
{
	size_t		limit_bytes;
	ColumnarIpcCacheKey key;
	ColumnarIpcCacheEntry *entry;
	bool		found;
	MemoryContext oldcxt;

	/* columnar_stripe_cache_size_mb is defined in pg_columnar.c */
	limit_bytes = (size_t) columnar_stripe_cache_size_mb * 1024 * 1024;

	if (limit_bytes == 0 || size > limit_bytes)
		return;					/* caching disabled or stripe too large */

	if (stripe_ipc_cache == NULL)
		columnar_stripe_ipc_cache_init();

	/* Evict LRU entries until there is room for the new entry */
	while (stripe_ipc_total_bytes + size > limit_bytes &&
		   hash_get_num_entries(stripe_ipc_cache) > 0)
		columnar_stripe_ipc_cache_evict_one_lru();

	memset(&key, 0, sizeof(key));
	key.dbOid = (Oid) PG_LOCATOR_DB(locator);
	key.relNumber = (Oid) PG_LOCATOR_REL(locator);
	key.stripe_id = stripe_id;

	entry = hash_search(stripe_ipc_cache, &key, HASH_ENTER, &found);

	/* Free the old bytes if overwriting an existing entry */
	if (found && entry->ipc_bytes != NULL)
	{
		stripe_ipc_total_bytes -= entry->ipc_size;
		pfree(entry->ipc_bytes);
		entry->ipc_bytes = NULL;
	}

	oldcxt = MemoryContextSwitchTo(stripe_ipc_cache_ctx);
	entry->ipc_bytes = palloc(size);
	MemoryContextSwitchTo(oldcxt);

	memcpy(entry->ipc_bytes, bytes, size);
	entry->ipc_size = size;
	entry->lru_clock = ++stripe_ipc_lru_clock;
	stripe_ipc_total_bytes += size;
}

/*
 * Evict all IPC bytes cache entries for a relation.
 *
 * Called from columnar_remove_storage() on DROP TABLE / TRUNCATE.
 * Uses collect-then-delete to avoid modifying the HTAB during iteration.
 */
void
columnar_stripe_ipc_cache_evict_relation(const RelFileLocator *locator)
{
	Oid			target_db = (Oid) PG_LOCATOR_DB(locator);
	Oid			target_rel = (Oid) PG_LOCATOR_REL(locator);
	HASH_SEQ_STATUS seq;
	ColumnarIpcCacheEntry *entry;
	ColumnarIpcCacheKey *keys;
	int			nkeys = 0;
	int			maxkeys = 64;
	int			i;

	if (stripe_ipc_cache == NULL)
		return;

	keys = palloc(sizeof(ColumnarIpcCacheKey) * maxkeys);

	hash_seq_init(&seq, stripe_ipc_cache);
	while ((entry = (ColumnarIpcCacheEntry *) hash_seq_search(&seq)) != NULL)
	{
		if (entry->key.dbOid != target_db || entry->key.relNumber != target_rel)
			continue;

		if (nkeys >= maxkeys)
		{
			maxkeys *= 2;
			keys = repalloc(keys, sizeof(ColumnarIpcCacheKey) * maxkeys);
		}
		keys[nkeys++] = entry->key;
	}
	/* seq scan ran to completion — no need for hash_seq_term */

	for (i = 0; i < nkeys; i++)
	{
		bool		found;

		entry = hash_search(stripe_ipc_cache, &keys[i], HASH_FIND, &found);
		if (found && entry->ipc_bytes != NULL)
		{
			stripe_ipc_total_bytes -= entry->ipc_size;
			pfree(entry->ipc_bytes);
			entry->ipc_bytes = NULL;
		}
		hash_search(stripe_ipc_cache, &keys[i], HASH_REMOVE, NULL);
	}

	pfree(keys);
}

char *
columnar_dir_path(const RelFileLocator *locator)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator));
	return path;
}

char *
columnar_stripe_path(const RelFileLocator *locator, int stripe_id)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u/stripe_%06d.arrow",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator), stripe_id);
	return path;
}

char *
columnar_metadata_path(const RelFileLocator *locator)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u/metadata",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator));
	return path;
}

void
columnar_create_storage(const RelFileLocator *locator)
{
	char		base[MAXPGPATH];
	char		dbdir[MAXPGPATH];
	char	   *reldir;
	ColumnarMetadata meta;
	int			rc;

	/* $PGDATA/columnar */
	snprintf(base, MAXPGPATH, "%s/columnar", DataDir);
	(void) mkdir(base, S_IRWXU);

	/* $PGDATA/columnar/<dbOid> */
	snprintf(dbdir, MAXPGPATH, "%s/columnar/%u", DataDir, PG_LOCATOR_DB(locator));
	(void) mkdir(dbdir, S_IRWXU);

	/* $PGDATA/columnar/<dbOid>/<relNumber> */
	reldir = columnar_dir_path(locator);
	rc = mkdir(reldir, S_IRWXU);
	if (rc < 0 && errno == EEXIST)
	{
		/*
		 * Directory already exists (e.g. relNumber reused after DROP+CREATE).
		 * Remove any stale stripe files before writing fresh metadata.
		 */
		columnar_remove_storage(locator);
		if (mkdir(reldir, S_IRWXU) < 0)
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not create columnar directory \"%s\": %m",
							reldir)));
	}
	else if (rc < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create columnar directory \"%s\": %m",
						reldir)));
	}

	/* Write initial empty metadata */
	meta.num_stripes = 0;
	meta.total_rows = 0;
	meta.stripes = NULL;
	columnar_write_metadata(locator, &meta);

	pfree(reldir);
}

void
columnar_remove_storage(const RelFileLocator *locator)
{
	char	   *dirpath = columnar_dir_path(locator);
	DIR		   *dir;
	struct dirent *de;

	/*
	 * Evict both caches before touching the files so that any subsequent
	 * read call for this locator is forced back to disk (or gets an empty
	 * result for a freshly-created replacement table).
	 * We evict unconditionally — even if the directory does not exist —
	 * because the caches might still hold stale entries from a previous
	 * incarnation of the same relfilenode.
	 */
	columnar_metadata_cache_evict(locator);
	columnar_stats_cache_evict_relation(locator);
	columnar_bitmap_cache_evict_relation(locator);
	columnar_stripe_ipc_cache_evict_relation(locator);

	dir = opendir(dirpath);
	if (dir == NULL)
	{
		pfree(dirpath);
		return;					/* directory doesn't exist, nothing to do */
	}

	while ((de = readdir(dir)) != NULL)
	{
		char		filepath[MAXPGPATH];

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, de->d_name);
		(void) unlink(filepath);
	}
	closedir(dir);
	(void) rmdir(dirpath);

	pfree(dirpath);
}

ColumnarMetadata *
columnar_read_metadata(const RelFileLocator *locator)
{
	char	   *metapath;
	FILE	   *fp;
	ColumnarMetadata *meta;
	int			i;

	/*
	 * Cache lookup.  On a hit, return a palloc'd copy so the caller can
	 * pfree it in the usual way without disturbing the cached state.
	 */
	{
		ColumnarMetadataCacheKey ckey;
		ColumnarMetadataCacheEntry *centry;
		bool		found;

		if (metadata_cache == NULL)
			columnar_metadata_cache_init();

		ckey.dbOid = (Oid) PG_LOCATOR_DB(locator);
		ckey.relNumber = (Oid) PG_LOCATOR_REL(locator);

		centry = hash_search(metadata_cache, &ckey, HASH_FIND, &found);
		if (found)
		{
			ColumnarMetadata *result = palloc(sizeof(ColumnarMetadata));

			result->num_stripes = centry->num_stripes;
			result->total_rows = centry->total_rows;
			if (centry->num_stripes > 0 && centry->stripes != NULL)
			{
				result->stripes = palloc(sizeof(StripeMetadata) * centry->num_stripes);
				memcpy(result->stripes, centry->stripes,
					   sizeof(StripeMetadata) * centry->num_stripes);
			}
			else
				result->stripes = NULL;
			return result;
		}
	}

	/* Cache miss — read from the metadata file on disk. */
	metapath = columnar_metadata_path(locator);
	meta = palloc0(sizeof(ColumnarMetadata));

	fp = fopen(metapath, "r");
	if (fp == NULL)
	{
		/* No metadata file yet -- return empty (do not cache; no valid data) */
		pfree(metapath);
		return meta;
	}

	{
		char	header_line[256];

		if (fgets(header_line, sizeof(header_line), fp) == NULL ||
			sscanf(header_line, "%d %" SCNd64, &meta->num_stripes, &meta->total_rows) != 2)
		{
			/* Unparseable header — return what we have, do not cache */
			fclose(fp);
			pfree(metapath);
			return meta;
		}
	}

	if (meta->num_stripes > 0)
	{
		meta->stripes = palloc0(sizeof(StripeMetadata) * meta->num_stripes);
		for (i = 0; i < meta->num_stripes; i++)
		{
			char		line[256];
			int			fields;

			if (fgets(line, sizeof(line), fp) == NULL)
			{
				meta->num_stripes = i;
				break;
			}

			fields = sscanf(line, "%d %" SCNd64 " %" SCNd64 " %d %" SCNd64,
							&meta->stripes[i].stripe_id,
							&meta->stripes[i].row_count,
							&meta->stripes[i].file_size,
							&meta->stripes[i].compression,
							&meta->stripes[i].uncompressed_size);
			if (fields == 3)
			{
				/* Old format without compression fields */
				meta->stripes[i].compression = COLUMNAR_COMPRESSION_NONE;
				meta->stripes[i].uncompressed_size = meta->stripes[i].file_size;
			}
			else if (fields != 5)
			{
				meta->num_stripes = i;
				break;
			}
		}
	}

	fclose(fp);
	pfree(metapath);

	/* Populate the cache so subsequent calls for this table are served from RAM */
	columnar_metadata_cache_update(locator, meta);

	return meta;
}

void
columnar_write_metadata(const RelFileLocator *locator, ColumnarMetadata *meta)
{
	char	   *metapath = columnar_metadata_path(locator);
	FILE	   *fp;
	int			i;

	fp = fopen(metapath, "w");
	if (fp == NULL)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write columnar metadata \"%s\": %m", metapath)));

	fprintf(fp, "%d %" PRId64 "\n", meta->num_stripes, meta->total_rows);
	for (i = 0; i < meta->num_stripes; i++)
	{
		fprintf(fp, "%d %" PRId64 " %" PRId64 " %d %" PRId64 "\n",
				meta->stripes[i].stripe_id,
				meta->stripes[i].row_count,
				meta->stripes[i].file_size,
				meta->stripes[i].compression,
				meta->stripes[i].uncompressed_size);
	}

	fclose(fp);
	pfree(metapath);

	/*
	 * Keep the backend-local cache in sync with what we just wrote.
	 * This is the single update point: every on-disk metadata change
	 * goes through columnar_write_metadata, so updating here is sufficient
	 * to maintain cache consistency within this backend.
	 */
	columnar_metadata_cache_update(locator, meta);
}

/* ----------------------------------------------------------------
 * Per-stripe min/max statistics implementation
 * ----------------------------------------------------------------
 */

char *
columnar_stats_path(const RelFileLocator *locator, int stripe_id)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u/stripe_%06d.stats",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator),
			 stripe_id);
	return path;
}

/*
 * Collect per-column min/max statistics from a finished RecordBatch.
 *
 * Uses Arrow schema format strings to identify column types, which avoids
 * introducing a dependency on the PG TupleDesc in the storage layer.
 *
 * Epoch conversion (Arrow → PG) is applied to DATE32 and TIMESTAMP so that
 * stored stats can be compared directly with PG Datum values in the pruning
 * layer without any additional conversion.
 *
 * Tracked types and their storage format:
 *   INT16  ("s"), INT32 ("i"), INT64 ("l") → STAT_TYPE_INT, raw value
 *   DATE32 ("tdD")                          → STAT_TYPE_INT, PG days
 *   TIMESTAMP ("tsu:...")                   → STAT_TYPE_INT, PG microseconds
 *   FLOAT  ("f"), DOUBLE ("g")             → STAT_TYPE_FLOAT, double value
 *   Everything else (bool, text, …)        → STAT_TYPE_NONE (not tracked)
 */
static ColumnarStripeStats *
columnar_collect_stripe_stats(struct ArrowSchema *schema,
							  struct ArrowArrayView *batch_view,
							  int64_t nrows)
{
	ColumnarStripeStats *stats;
	int			ncols = (int) schema->n_children;
	int			i;

	stats = palloc(sizeof(ColumnarStripeStats));
	stats->ncols = ncols;
	if (ncols == 0)
	{
		stats->cols = NULL;
		return stats;
	}
	stats->cols = palloc0(sizeof(ColumnarColumnStats) * ncols);

	for (i = 0; i < ncols; i++)
	{
		struct ArrowArrayView *child = batch_view->children[i];
		ColumnarColumnStats *cs = &stats->cols[i];
		const char *fmt = schema->children[i]->format;
		int			stat_type;
		bool		date_epoch = false;
		bool		ts_epoch = false;
		int64_t		j;
		bool		has_non_null = false;

		/* Classify the column by its Arrow format string */
		if (strcmp(fmt, "s") == 0)			/* INT16 */
			stat_type = COLUMNAR_STAT_TYPE_INT;
		else if (strcmp(fmt, "i") == 0)		/* INT32 */
			stat_type = COLUMNAR_STAT_TYPE_INT;
		else if (strcmp(fmt, "l") == 0)		/* INT64 */
			stat_type = COLUMNAR_STAT_TYPE_INT;
		else if (strcmp(fmt, "tdD") == 0)	/* DATE32 */
		{
			stat_type = COLUMNAR_STAT_TYPE_INT;
			date_epoch = true;
		}
		else if (strncmp(fmt, "tsu:", 4) == 0)	/* TIMESTAMP µs (any tz) */
		{
			stat_type = COLUMNAR_STAT_TYPE_INT;
			ts_epoch = true;
		}
		else if (strcmp(fmt, "f") == 0)		/* FLOAT32 */
			stat_type = COLUMNAR_STAT_TYPE_FLOAT;
		else if (strcmp(fmt, "g") == 0)		/* FLOAT64 */
			stat_type = COLUMNAR_STAT_TYPE_FLOAT;
		else
		{
			cs->stat_type = COLUMNAR_STAT_TYPE_NONE;
			cs->has_stats = false;
			continue;
		}

		cs->stat_type = stat_type;

		/* Scan all rows to find min/max */
		for (j = 0; j < nrows; j++)
		{
			if (ArrowArrayViewIsNull(child, j))
				continue;

			if (stat_type == COLUMNAR_STAT_TYPE_INT)
			{
				int64_t		raw = ArrowArrayViewGetIntUnsafe(child, j);
				int64_t		val;

				/* Convert from Arrow epoch to PG epoch */
				if (date_epoch)
					val = raw - INT64CONST(10957);			/* days */
				else if (ts_epoch)
					val = raw - INT64CONST(946684800000000);	/* µs */
				else
					val = raw;

				if (!has_non_null)
				{
					cs->min_int = val;
					cs->max_int = val;
				}
				else
				{
					if (val < cs->min_int)
						cs->min_int = val;
					if (val > cs->max_int)
						cs->max_int = val;
				}
			}
			else					/* FLOAT */
			{
				double		val = ArrowArrayViewGetDoubleUnsafe(child, j);

				if (!has_non_null)
				{
					cs->min_float = val;
					cs->max_float = val;
				}
				else
				{
					if (val < cs->min_float)
						cs->min_float = val;
					if (val > cs->max_float)
						cs->max_float = val;
				}
			}
			has_non_null = true;
		}

		cs->has_stats = has_non_null;
	}

	return stats;
}

void
columnar_write_stripe_stats(const RelFileLocator *locator, int stripe_id,
							const ColumnarStripeStats *stats)
{
	char	   *path = columnar_stats_path(locator, stripe_id);
	FILE	   *fp;
	int			i;

	fp = fopen(path, "w");
	if (fp == NULL)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("columnar: could not write stripe stats \"%s\": %m",
						path)));
		pfree(path);
		return;
	}

	fprintf(fp, "PGCS 1 %d\n", stats->ncols);

	for (i = 0; i < stats->ncols; i++)
	{
		const ColumnarColumnStats *cs = &stats->cols[i];

		if (!cs->has_stats || cs->stat_type == COLUMNAR_STAT_TYPE_NONE)
		{
			/* No usable stats: write zeros */
			fprintf(fp, "%d 0 0 0\n", cs->stat_type);
		}
		else if (cs->stat_type == COLUMNAR_STAT_TYPE_INT)
		{
			fprintf(fp, "1 1 %" PRId64 " %" PRId64 "\n",
					cs->min_int, cs->max_int);
		}
		else					/* COLUMNAR_STAT_TYPE_FLOAT */
		{
			int64_t		min_bits,
						max_bits;

			memcpy(&min_bits, &cs->min_float, sizeof(min_bits));
			memcpy(&max_bits, &cs->max_float, sizeof(max_bits));
			fprintf(fp, "2 1 %" PRId64 " %" PRId64 "\n",
					min_bits, max_bits);
		}
	}

	fclose(fp);
	pfree(path);

	/*
	 * Keep the backend-local cache in sync with the file we just wrote.
	 * Stats are immutable once written, so this entry will be valid for
	 * the lifetime of the stripe.
	 */
	columnar_stats_cache_update(locator, stripe_id, stats);
}

ColumnarStripeStats *
columnar_read_stripe_stats(const RelFileLocator *locator,
						   int stripe_id, int expected_ncols)
{
	char	   *path;
	FILE	   *fp;
	ColumnarStripeStats *stats;
	char		header[64];
	int			version,
				ncols;
	int			i;

	/*
	 * Cache lookup.  On a hit we return a palloc'd copy so the caller
	 * can pfree it in the usual way without disturbing the cached state.
	 */
	{
		ColumnarStatsCacheKey ckey;
		ColumnarStatsCacheEntry *centry;
		bool		found;

		if (stats_cache == NULL)
			columnar_stats_cache_init();

		memset(&ckey, 0, sizeof(ckey));
		ckey.dbOid = (Oid) PG_LOCATOR_DB(locator);
		ckey.relNumber = (Oid) PG_LOCATOR_REL(locator);
		ckey.stripe_id = stripe_id;

		centry = hash_search(stats_cache, &ckey, HASH_FIND, &found);
		if (found)
		{
			/* File confirmed absent (pre-Level-3 stripe) */
			if (centry->file_absent)
				return NULL;

			/* Schema mismatch after ALTER TABLE ADD/DROP COLUMN */
			if (centry->ncols != expected_ncols)
				return NULL;

			/* Return a palloc'd copy; caller may pfree it freely */
			{
				ColumnarStripeStats *result = palloc(sizeof(ColumnarStripeStats));

				result->ncols = centry->ncols;
				if (centry->ncols > 0 && centry->cols != NULL)
				{
					result->cols = palloc(sizeof(ColumnarColumnStats) * centry->ncols);
					memcpy(result->cols, centry->cols,
						   sizeof(ColumnarColumnStats) * centry->ncols);
				}
				else
					result->cols = NULL;
				return result;
			}
		}
	}

	/* Cache miss — read the .stats file from disk */
	path = columnar_stats_path(locator, stripe_id);
	fp = fopen(path, "r");
	pfree(path);
	if (fp == NULL)
	{
		/*
		 * File absent (pre-Level-3 stripe or already vacuumed).  Cache
		 * the absence so subsequent calls skip the fopen() entirely.
		 */
		if (errno == ENOENT)
			columnar_stats_cache_update(locator, stripe_id, NULL);
		return NULL;
	}

	/* Validate header: "PGCS <version> <ncols>" */
	if (fgets(header, sizeof(header), fp) == NULL ||
		sscanf(header, "PGCS %d %d", &version, &ncols) != 2 ||
		version != 1 || ncols != expected_ncols)
	{
		fclose(fp);
		return NULL;			/* corrupt or schema mismatch */
	}

	stats = palloc(sizeof(ColumnarStripeStats));
	stats->ncols = ncols;
	stats->cols = palloc0(sizeof(ColumnarColumnStats) * ncols);

	for (i = 0; i < ncols; i++)
	{
		char		line[128];
		int			stat_type,
					has_stats_int;
		int64_t		min_raw,
					max_raw;

		if (fgets(line, sizeof(line), fp) == NULL ||
			sscanf(line, "%d %d %" SCNd64 " %" SCNd64,
				   &stat_type, &has_stats_int, &min_raw, &max_raw) != 4)
		{
			fclose(fp);
			columnar_free_stripe_stats(stats);
			return NULL;
		}

		stats->cols[i].stat_type = stat_type;
		stats->cols[i].has_stats = (has_stats_int != 0);

		if (stats->cols[i].has_stats)
		{
			if (stat_type == COLUMNAR_STAT_TYPE_INT)
			{
				stats->cols[i].min_int = min_raw;
				stats->cols[i].max_int = max_raw;
			}
			else if (stat_type == COLUMNAR_STAT_TYPE_FLOAT)
			{
				memcpy(&stats->cols[i].min_float, &min_raw, sizeof(double));
				memcpy(&stats->cols[i].max_float, &max_raw, sizeof(double));
			}
		}
	}

	fclose(fp);

	/*
	 * Populate the cache so subsequent calls for this stripe are served
	 * from RAM.  Stats are immutable once written; the entry stays valid
	 * until the stripe is vacuumed (fully deleted).
	 */
	columnar_stats_cache_update(locator, stripe_id, stats);

	return stats;
}

void
columnar_free_stripe_stats(ColumnarStripeStats *stats)
{
	if (stats == NULL)
		return;
	if (stats->cols)
		pfree(stats->cols);
	pfree(stats);
}

void
columnar_write_stripe(const RelFileLocator *locator,
					  struct ArrowSchema *schema,
					  struct ArrowArrayView *batch_view,
					  int64_t nrows)
{
	ColumnarMetadata *meta;
	int			new_stripe_id;
	char	   *filepath;
	FILE	   *fp;
	struct ArrowBuffer ipc_buffer;
	struct ArrowIpcOutputStream output_stream;
	struct ArrowIpcWriter writer;
	struct ArrowError arrow_error;
	int			rc;
	int			compression = columnar_compression;
	const void *write_data;
	int64_t		write_size;
	int64_t		uncompressed_size;
	void	   *compressed = NULL;

	/* Get next stripe ID */
	meta = columnar_read_metadata(locator);
	new_stripe_id = meta->num_stripes + 1;
	filepath = columnar_stripe_path(locator, new_stripe_id);

	/* Write IPC stream to in-memory buffer */
	ArrowBufferInit(&ipc_buffer);

	rc = ArrowIpcOutputStreamInitBuffer(&output_stream, &ipc_buffer);
	if (rc != NANOARROW_OK)
		ereport(ERROR, (errmsg("columnar: failed to init IPC buffer output stream")));

	rc = ArrowIpcWriterInit(&writer, &output_stream);
	if (rc != NANOARROW_OK)
	{
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR, (errmsg("columnar: failed to init IPC writer")));
	}

	/* Write IPC stream: schema + batch + EOS */
	rc = ArrowIpcWriterWriteSchema(&writer, schema, &arrow_error);
	if (rc != NANOARROW_OK)
	{
		ArrowIpcWriterReset(&writer);
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR, (errmsg("columnar: IPC write schema failed: %s",
							   arrow_error.message)));
	}

	rc = ArrowIpcWriterWriteArrayView(&writer, batch_view, &arrow_error);
	if (rc != NANOARROW_OK)
	{
		ArrowIpcWriterReset(&writer);
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR, (errmsg("columnar: IPC write batch failed: %s",
							   arrow_error.message)));
	}

	ArrowIpcWriterReset(&writer);

	/* ipc_buffer now contains the complete uncompressed IPC stream */
	uncompressed_size = ipc_buffer.size_bytes;
	write_data = ipc_buffer.data;
	write_size = ipc_buffer.size_bytes;

	/* Compress if requested */
	if (compression == COLUMNAR_COMPRESSION_ZSTD)
	{
#ifdef HAVE_LIBZSTD
		size_t		bound = ZSTD_compressBound(ipc_buffer.size_bytes);
		size_t		csize;

		compressed = palloc(bound);
		csize = ZSTD_compress(compressed, bound,
							  ipc_buffer.data, ipc_buffer.size_bytes,
							  3 /* default level */);
		if (ZSTD_isError(csize))
		{
			pfree(compressed);
			ArrowBufferReset(&ipc_buffer);
			ereport(ERROR, (errmsg("columnar: ZSTD compression failed: %s",
								   ZSTD_getErrorName(csize))));
		}
		write_data = compressed;
		write_size = (int64_t) csize;
#else
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnar: ZSTD compression not available"),
				 errhint("Rebuild pg_columnar with ZSTD support (install libzstd-dev).")));
#endif
	}
	else if (compression == COLUMNAR_COMPRESSION_LZ4)
	{
#ifdef HAVE_LIBLZ4
		LZ4F_preferences_t prefs;
		size_t		bound;
		size_t		csize;

		memset(&prefs, 0, sizeof(prefs));
		prefs.frameInfo.contentSize = ipc_buffer.size_bytes;

		bound = LZ4F_compressFrameBound(ipc_buffer.size_bytes, &prefs);
		compressed = palloc(bound);
		csize = LZ4F_compressFrame(compressed, bound,
								   ipc_buffer.data, ipc_buffer.size_bytes,
								   &prefs);
		if (LZ4F_isError(csize))
		{
			pfree(compressed);
			ArrowBufferReset(&ipc_buffer);
			ereport(ERROR, (errmsg("columnar: LZ4 compression failed: %s",
								   LZ4F_getErrorName(csize))));
		}
		write_data = compressed;
		write_size = (int64_t) csize;
#else
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("columnar: LZ4 compression not available"),
				 errhint("Rebuild pg_columnar with LZ4 support (install liblz4-dev).")));
#endif
	}

	/* Write to file */
	fp = fopen(filepath, "wb");
	if (fp == NULL)
	{
		if (compressed)
			pfree(compressed);
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not create stripe file \"%s\": %m", filepath)));
	}

	if (fwrite(write_data, 1, write_size, fp) != (size_t) write_size)
	{
		fclose(fp);
		if (compressed)
			pfree(compressed);
		ArrowBufferReset(&ipc_buffer);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write stripe file \"%s\": %m", filepath)));
	}

	fclose(fp);
	if (compressed)
		pfree(compressed);
	ArrowBufferReset(&ipc_buffer);

	/*
	 * Collect and write per-stripe min/max statistics.
	 * batch_view is still valid here (the IPC writer only reads it).
	 * Failures emit a WARNING but do not abort the write.
	 */
	{
		ColumnarStripeStats *stripe_stats;

		stripe_stats = columnar_collect_stripe_stats(schema, batch_view, nrows);
		columnar_write_stripe_stats(locator, new_stripe_id, stripe_stats);
		columnar_free_stripe_stats(stripe_stats);
	}

	/* Update metadata */
	{
		ColumnarMetadata new_meta;
		int			i;

		new_meta.num_stripes = meta->num_stripes + 1;
		new_meta.total_rows = meta->total_rows + nrows;
		new_meta.stripes = palloc(sizeof(StripeMetadata) * new_meta.num_stripes);

		for (i = 0; i < meta->num_stripes; i++)
			new_meta.stripes[i] = meta->stripes[i];

		new_meta.stripes[meta->num_stripes].stripe_id = new_stripe_id;
		new_meta.stripes[meta->num_stripes].row_count = nrows;
		new_meta.stripes[meta->num_stripes].file_size = write_size;
		new_meta.stripes[meta->num_stripes].compression = compression;
		new_meta.stripes[meta->num_stripes].uncompressed_size = uncompressed_size;

		columnar_write_metadata(locator, &new_meta);

		pfree(new_meta.stripes);
	}

	if (meta->stripes)
		pfree(meta->stripes);
	pfree(meta);
	pfree(filepath);
}

/* ----------------------------------------------------------------
 * Delete bitmap implementation
 * ----------------------------------------------------------------
 */

char *
columnar_deleted_path(const RelFileLocator *locator, int stripe_id)
{
	char	   *path = palloc(MAXPGPATH);

	snprintf(path, MAXPGPATH, "%s/columnar/%u/%u/stripe_%06d.deleted",
			 DataDir, PG_LOCATOR_DB(locator), PG_LOCATOR_REL(locator),
			 stripe_id);
	return path;
}

uint8_t *
columnar_read_delete_bitmap(const RelFileLocator *locator,
							int stripe_id, int64_t row_count)
{
	char	   *path;
	FILE	   *fp;
	int64_t		nbytes = (row_count + 7) / 8;
	uint8_t    *bits;

	/*
	 * Cache lookup.  On a hit we return a palloc'd copy in
	 * CurrentMemoryContext so the caller can pfree it safely without
	 * disturbing the cached state.
	 */
	{
		ColumnarBitmapCacheKey ckey;
		ColumnarBitmapCacheEntry *centry;
		bool		found;

		if (bitmap_cache == NULL)
			columnar_bitmap_cache_init();

		memset(&ckey, 0, sizeof(ckey));
		ckey.dbOid = (Oid) PG_LOCATOR_DB(locator);
		ckey.relNumber = (Oid) PG_LOCATOR_REL(locator);
		ckey.stripe_id = stripe_id;

		centry = hash_search(bitmap_cache, &ckey, HASH_FIND, &found);
		if (found)
		{
			/* Stripe has no deletions (confirmed by a previous fopen miss) */
			if (centry->file_absent)
				return NULL;

			/* Return a palloc'd copy; caller may pfree it freely */
			bits = palloc(nbytes);
			memcpy(bits, centry->bits, nbytes);
			return bits;
		}
	}

	/* Cache miss — read the .deleted file from disk */
	path = columnar_deleted_path(locator, stripe_id);
	fp = fopen(path, "rb");
	pfree(path);

	if (fp == NULL)
	{
		/*
		 * File absent means no rows have been deleted in this stripe.
		 * Cache the absence so subsequent calls skip the fopen() entirely.
		 */
		if (errno == ENOENT)
			columnar_bitmap_cache_update(locator, stripe_id, NULL, 0);
		return NULL;
	}

	bits = palloc0(nbytes);
	(void) fread(bits, 1, nbytes, fp);
	fclose(fp);

	/*
	 * Populate the cache.  The bitmap is mutable (updated by DELETE), so
	 * the cached copy is kept in sync via columnar_set_deleted_bit.
	 */
	columnar_bitmap_cache_update(locator, stripe_id, bits, nbytes);

	return bits;
}

void
columnar_set_deleted_bit(const RelFileLocator *locator,
						 int stripe_id,
						 int64_t row_offset,
						 int64_t row_count)
{
	char	   *path = columnar_deleted_path(locator, stripe_id);
	int64_t		nbytes = (row_count + 7) / 8;
	uint8_t    *bits;
	FILE	   *fp;

	/*
	 * Obtain the current bitmap — from cache if available, otherwise
	 * from disk.  This eliminates the "read" half of the read-modify-write
	 * cycle for every DELETE after the first one on a given stripe.
	 */
	{
		ColumnarBitmapCacheKey ckey;
		ColumnarBitmapCacheEntry *centry;
		bool		found;

		if (bitmap_cache == NULL)
			columnar_bitmap_cache_init();

		memset(&ckey, 0, sizeof(ckey));
		ckey.dbOid = (Oid) PG_LOCATOR_DB(locator);
		ckey.relNumber = (Oid) PG_LOCATOR_REL(locator);
		ckey.stripe_id = stripe_id;

		centry = hash_search(bitmap_cache, &ckey, HASH_FIND, &found);

		if (found && !centry->file_absent && centry->bits != NULL)
		{
			/* Cache hit: copy cached bits into a local working buffer */
			bits = palloc(nbytes);
			memcpy(bits, centry->bits, nbytes);
		}
		else
		{
			/*
			 * Cache miss or file_absent (no deletions yet): allocate a
			 * zeroed bitmap and try to populate it from the existing file.
			 * If the file does not exist yet, we start from all-zeros.
			 */
			bits = palloc0(nbytes);
			fp = fopen(path, "rb");
			if (fp != NULL)
			{
				(void) fread(bits, 1, nbytes, fp);
				fclose(fp);
			}
		}
	}

	/* Set the bit for this row */
	bits[row_offset / 8] |= (uint8_t) (1 << (row_offset % 8));

	/* Write the updated bitmap back */
	fp = fopen(path, "wb");
	if (fp == NULL)
	{
		char	   *errmsg_path = pstrdup(path);

		pfree(bits);
		pfree(path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write delete bitmap \"%s\": %m",
						errmsg_path)));
	}

	if (fwrite(bits, 1, nbytes, fp) != (size_t) nbytes)
	{
		char	   *errmsg_path = pstrdup(path);

		fclose(fp);
		pfree(bits);
		pfree(path);
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write delete bitmap \"%s\": %m",
						errmsg_path)));
	}

	fclose(fp);

	/*
	 * Update the cache with the newly-written bitmap so that subsequent
	 * reads and deletes on this stripe are served from RAM.  We update
	 * after the fwrite to ensure the cache only reflects data that is
	 * safely on disk.
	 */
	columnar_bitmap_cache_update(locator, stripe_id, bits, nbytes);

	pfree(bits);
	pfree(path);
}

uint64
columnar_storage_size(const RelFileLocator *locator)
{
	char	   *dirpath = columnar_dir_path(locator);
	DIR		   *dir;
	struct dirent *de;
	uint64		total = 0;

	dir = opendir(dirpath);
	if (dir == NULL)
	{
		pfree(dirpath);
		return 0;
	}

	while ((de = readdir(dir)) != NULL)
	{
		struct stat st;
		char		filepath[MAXPGPATH];

		if (strcmp(de->d_name, ".") == 0 || strcmp(de->d_name, "..") == 0)
			continue;

		snprintf(filepath, MAXPGPATH, "%s/%s", dirpath, de->d_name);
		if (stat(filepath, &st) == 0)
			total += st.st_size;
	}

	closedir(dir);
	pfree(dirpath);
	return total;
}
