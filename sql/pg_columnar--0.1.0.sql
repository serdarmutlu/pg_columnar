-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_columnar" to load this extension. \quit

CREATE FUNCTION columnar_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE ACCESS METHOD columnar TYPE TABLE HANDLER columnar_handler;

COMMENT ON ACCESS METHOD columnar IS 'columnar table access method using Arrow IPC storage';

CREATE FUNCTION columnar_relation_size(regclass)
RETURNS bigint
AS 'MODULE_PATHNAME', 'columnar_relation_size_sql'
LANGUAGE C STRICT;

COMMENT ON FUNCTION columnar_relation_size(regclass) IS
'Returns the total on-disk size in bytes of all columnar stripe files for
the given columnar table. Use this instead of pg_total_relation_size(),
which always returns 0 for columnar tables.';

CREATE FUNCTION columnar_stripe_info(
    relation        regclass,
    OUT stripe_id   int,
    OUT row_count   bigint,
    OUT file_size   bigint,
    OUT compression text,
    OUT deleted_rows bigint,
    OUT has_stats   bool
)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'columnar_stripe_info'
LANGUAGE C STRICT;

COMMENT ON FUNCTION columnar_stripe_info(regclass) IS
'Returns one row per stripe for a columnar table with stripe_id, row_count,
file_size (bytes), compression algorithm, number of logically deleted rows,
and whether a min/max statistics file exists for the stripe.';

CREATE FUNCTION columnar_cache_stats(
    OUT metadata_hits    bigint,
    OUT metadata_misses  bigint,
    OUT stats_hits       bigint,
    OUT stats_misses     bigint,
    OUT bitmap_hits      bigint,
    OUT bitmap_misses    bigint,
    OUT ipc_hits         bigint,
    OUT ipc_misses       bigint,
    OUT ipc_bytes_cached bigint
)
RETURNS record
AS 'MODULE_PATHNAME', 'columnar_cache_stats'
LANGUAGE C STRICT;

CREATE FUNCTION columnar_compact(
    relation            regclass,
    OUT stripes_compacted int,
    OUT rows_compacted  bigint
)
RETURNS record
AS 'MODULE_PATHNAME', 'columnar_compact'
LANGUAGE C STRICT;

COMMENT ON FUNCTION columnar_compact(regclass) IS
'Rewrites all partially-deleted stripes, removing deleted rows and reclaiming
space.  Returns the number of stripes rewritten and rows copied.

After compaction the old stale index entries (pointing to the zeroed stripes)
are silently skipped by index scans.  Run REINDEX TABLE to remove them.

Acquires ExclusiveLock for the duration — no concurrent access.';

COMMENT ON FUNCTION columnar_cache_stats() IS
'Returns cumulative cache hit/miss counters for all four columnar caching
layers (metadata, stats, delete-bitmap, stripe IPC bytes) in the current
backend, plus the number of bytes currently resident in the IPC bytes cache.';

CREATE FUNCTION columnar_rebuild_metadata(
    relation            regclass,
    OUT stripes_rebuilt int,
    OUT rows_total      bigint
)
RETURNS record
AS 'MODULE_PATHNAME', 'columnar_rebuild_metadata'
LANGUAGE C STRICT;

COMMENT ON FUNCTION columnar_rebuild_metadata(regclass) IS
'Disaster-recovery function: reconstructs the metadata file by scanning the
stripe directory for .arrow files and reading each stripe''s Arrow IPC stream.

Useful when the metadata file has been lost or corrupted.  Safe to run on a
healthy table — the recovered metadata is equivalent to the original.

Stripes that cannot be read are skipped with a WARNING; the rest are still
recovered.  Returns the number of stripes recovered and the total live row
count (row_count minus deleted rows) across all recovered stripes.

Acquires ExclusiveLock for the duration — no concurrent access.';
