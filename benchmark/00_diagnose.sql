-- =============================================================================
-- pg_columnar diagnostic script
--
-- Run this BEFORE 02_load_data.sql to understand the current state of the
-- database and to isolate exactly where the columnar write path is failing.
--
-- Usage:
--   psql -d <your_db> -v ON_ERROR_STOP=0 -f benchmark/00_diagnose.sql
--
-- ON_ERROR_STOP=0 intentionally allows each test to run even if a prior
-- step fails, so you see the full picture in one pass.
-- =============================================================================

\echo ''
\echo '================================================================'
\echo ' pg_columnar DIAGNOSTIC REPORT'
\echo '================================================================'

-- ---------------------------------------------------------------------------
-- 1. Extension and access method presence
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 1. Extension and access method ==='
SELECT extname, extversion FROM pg_extension WHERE extname = 'pg_columnar';
SELECT amname, amtype FROM pg_am WHERE amname = 'columnar';

-- ---------------------------------------------------------------------------
-- 2. Confirm tables exist and are using the columnar AM
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 2. Table AM types (relam must map to "columnar" for columnar tables) ==='
SELECT
    c.relname,
    a.amname AS access_method,
    c.relfilenode,
    c.reltablespace
FROM pg_class c
LEFT JOIN pg_am a ON a.oid = c.relam
WHERE c.relname IN (
    'orders_heap','orders_columnar','orders_columnar_lz4','orders_columnar_zstd'
)
  AND c.relkind = 'r'
ORDER BY c.relname;

-- ---------------------------------------------------------------------------
-- 3. Current row counts (baseline – likely 0 if prior load failed)
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 3. Current row counts (expected 0 on first run) ==='
SELECT 'orders_heap'          AS table_name, COUNT(*) AS row_count FROM orders_heap
UNION ALL
SELECT 'orders_columnar',      COUNT(*) FROM orders_columnar
UNION ALL
SELECT 'orders_columnar_lz4',  COUNT(*) FROM orders_columnar_lz4
UNION ALL
SELECT 'orders_columnar_zstd', COUNT(*) FROM orders_columnar_zstd;

-- ---------------------------------------------------------------------------
-- 4. Storage sizes (columnar.storage_size() is custom; pg_relation_size
--    returns 0 or 8192 for the heap fork, which columnar tables do not use.
--    Total size includes the custom columnar stripe files.)
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 4. Storage sizes ==='
SELECT
    relname,
    pg_size_pretty(pg_relation_size(oid))       AS heap_fork_size,
    pg_size_pretty(pg_total_relation_size(oid))  AS total_size
FROM pg_class
WHERE relname IN (
    'orders_heap','orders_columnar','orders_columnar_lz4','orders_columnar_zstd'
)
  AND relkind = 'r'
ORDER BY relname;

-- ---------------------------------------------------------------------------
-- 5. PGDATA and columnar directory path
--    Stripe files live at:  $PGDATA/columnar/<db_oid>/<relfilenode>/
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 5. PGDATA and database OID (for manual stripe file inspection) ==='
SHOW data_directory;
SELECT oid AS db_oid, datname FROM pg_database WHERE datname = current_database();

\echo ''
\echo '=== 5b. Relfilenodes for columnar tables ==='
\echo '        Stripe files: $PGDATA/columnar/<db_oid>/<relfilenode>/stripe_*.arrow'
SELECT relname, relfilenode
FROM pg_class
WHERE relname IN ('orders_columnar','orders_columnar_lz4','orders_columnar_zstd')
  AND relkind = 'r'
ORDER BY relname;

-- ---------------------------------------------------------------------------
-- 6. Library load trigger
--    Opening a columnar relation loads pg_columnar.so and runs _PG_init(),
--    which registers the columnar.compression GUC and the xact callback.
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 6. Opening columnar relation to trigger library load ==='
SELECT COUNT(*) AS rows_before_test FROM orders_columnar;

-- ---------------------------------------------------------------------------
-- 7. GUC availability check
--    If the library loaded correctly, this must NOT raise:
--      ERROR: unrecognized configuration parameter "columnar.compression"
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 7. GUC check: columnar.compression (library must be loaded first) ==='
SHOW columnar.compression;

-- ---------------------------------------------------------------------------
-- 8. Minimal INSERT test – 3 rows, no compression
--    If this COUNT returns 0, the write path is broken.
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 8. Minimal INSERT test: 3 rows, no compression ==='
DROP TABLE IF EXISTS _diag_col_none;
CREATE TABLE _diag_col_none (id INTEGER, val TEXT) USING columnar;
SET columnar.compression = 'none';
INSERT INTO _diag_col_none VALUES (1, 'a'), (2, 'b'), (3, 'c');
\echo '    → COUNT(*) immediately after INSERT (same session, different txn):'
SELECT COUNT(*) AS rows_3_none FROM _diag_col_none;
SELECT pg_size_pretty(pg_total_relation_size('_diag_col_none')) AS size_3_rows;

-- ---------------------------------------------------------------------------
-- 9. Auto-flush test – 10 001 rows (crosses the 10 000-row stripe threshold)
--    After this INSERT exactly one full stripe (10 000 rows) must be on disk
--    and 1 row must be in the write buffer (visible via the scan).
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 9. Auto-flush test: 10 001 rows (should cross stripe threshold) ==='
DROP TABLE IF EXISTS _diag_col_flush;
CREATE TABLE _diag_col_flush (id BIGINT) USING columnar;
SET columnar.compression = 'none';
INSERT INTO _diag_col_flush SELECT generate_series(1, 10001);
\echo '    → COUNT(*) should be 10 001:'
SELECT COUNT(*) AS rows_10001 FROM _diag_col_flush;
SELECT pg_size_pretty(pg_total_relation_size('_diag_col_flush')) AS size_10001_rows;

-- ---------------------------------------------------------------------------
-- 10. Compression tests
--     If lz4/zstd support is missing the INSERT will raise an error that
--     will be visible even with ON_ERROR_STOP=0.
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 10a. LZ4 compression test: 3 rows ==='
DROP TABLE IF EXISTS _diag_col_lz4;
CREATE TABLE _diag_col_lz4 (id INTEGER) USING columnar;
SET columnar.compression = 'lz4';
INSERT INTO _diag_col_lz4 VALUES (1), (2), (3);
SELECT COUNT(*) AS rows_3_lz4 FROM _diag_col_lz4;

\echo ''
\echo '=== 10b. ZSTD compression test: 3 rows ==='
DROP TABLE IF EXISTS _diag_col_zstd;
CREATE TABLE _diag_col_zstd (id INTEGER) USING columnar;
SET columnar.compression = 'zstd';
INSERT INTO _diag_col_zstd VALUES (1), (2), (3);
SELECT COUNT(*) AS rows_3_zstd FROM _diag_col_zstd;

-- Reset GUC
RESET columnar.compression;

-- ---------------------------------------------------------------------------
-- 11. Large INSERT into the benchmark table directly
--     Uses only 1 000 rows so the diagnostic is fast.
--     If COUNT(*) still returns 0 here after a direct small insert,
--     the scan path itself is broken.
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 11. Direct small INSERT into orders_columnar (1 000 rows) ==='
SET columnar.compression = 'none';
INSERT INTO orders_columnar (
    order_id, customer_id, product_id, category, region,
    amount, quantity, discount, is_returned, order_date, created_at
)
SELECT
    gs,
    (gs % 50000) + 1,
    (gs % 5000) + 1,
    (ARRAY['Electronics','Clothing','Books'])[(gs % 3) + 1],
    (ARRAY['North','South','East'])[(gs % 3) + 1],
    100.0,
    1,
    0.0,
    false,
    DATE '2024-01-01',
    TIMESTAMP '2024-01-01 00:00:00'
FROM generate_series(1, 1000) gs;
RESET columnar.compression;

\echo '    → COUNT(*) in orders_columnar after 1 000-row INSERT:'
SELECT COUNT(*) AS orders_columnar_rows FROM orders_columnar;
SELECT pg_size_pretty(pg_total_relation_size('orders_columnar')) AS orders_columnar_size;

-- Undo the test rows so the table is clean for 02_load_data.sql
-- (columnar does not support DELETE/UPDATE; truncate instead)
TRUNCATE orders_columnar;
\echo '    → COUNT(*) after TRUNCATE (should be 0):'
SELECT COUNT(*) AS after_truncate FROM orders_columnar;

-- ---------------------------------------------------------------------------
-- 12. Cleanup diagnostic tables
-- ---------------------------------------------------------------------------
\echo ''
\echo '=== 12. Cleanup ==='
DROP TABLE IF EXISTS _diag_col_none;
DROP TABLE IF EXISTS _diag_col_flush;
DROP TABLE IF EXISTS _diag_col_lz4;
DROP TABLE IF EXISTS _diag_col_zstd;

\echo ''
\echo '================================================================'
\echo ' Diagnostic complete.'
\echo ''
\echo ' Interpreting results:'
\echo '   Step 7  → ERROR means library not loading (pg_columnar.so missing/broken)'
\echo '   Step 8  → 0 rows means 3-row write path broken (check PGDATA/columnar/)'
\echo '   Step 9  → 0 rows means flush path broken'
\echo '   Step 10 → ERROR means compression library (lz4/zstd) not compiled in'
\echo '   Step 11 → 0 rows means scan broken (stripe files exist but unreadable)'
\echo '================================================================'
