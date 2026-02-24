-- =============================================================================
-- pg_columnar benchmark – Step 4: Query plans
--
-- Shows EXPLAIN ANALYZE output for key queries across all four tables.
-- All columnar variants use the same scan node; the timing difference comes
-- from I/O cost when decompressing stripes (none vs. lz4 vs. zstd).
-- =============================================================================

-- ---------------------------------------------------------------------------
-- Pre-flight: same empty-table guard as 03_benchmark.sql.
-- ---------------------------------------------------------------------------
\timing off
\echo '=== PRE-FLIGHT: row counts (all must be > 0 for valid results) ==='
SELECT
    table_name,
    row_count,
    CASE
        WHEN row_count = 0
        THEN '*** EMPTY – run 02_load_data.sql first; plans below are MISLEADING ***'
        ELSE 'OK'
    END AS status
FROM (
    SELECT 'orders_heap'          AS table_name, COUNT(*) AS row_count FROM orders_heap
    UNION ALL
    SELECT 'orders_columnar'      AS table_name, COUNT(*) AS row_count FROM orders_columnar
    UNION ALL
    SELECT 'orders_columnar_lz4'  AS table_name, COUNT(*) AS row_count FROM orders_columnar_lz4
    UNION ALL
    SELECT 'orders_columnar_zstd' AS table_name, COUNT(*) AS row_count FROM orders_columnar_zstd
) counts;

SET max_parallel_workers_per_gather = 0;

-- ============================================================
-- Q2: SUM(amount) – single-column projection across all tables
-- ============================================================
\echo '=== Q2: SUM(amount) – heap ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT SUM(amount) FROM orders_heap;

\echo ''
\echo '=== Q2: SUM(amount) – columnar (none) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT SUM(amount) FROM orders_columnar;

\echo ''
\echo '=== Q2: SUM(amount) – columnar (lz4) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT SUM(amount) FROM orders_columnar_lz4;

\echo ''
\echo '=== Q2: SUM(amount) – columnar (zstd) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT SUM(amount) FROM orders_columnar_zstd;

-- ============================================================
-- Q6: GROUP BY category – shows HashAgg + scan cost difference
-- ============================================================
\echo ''
\echo '=== Q6: GROUP BY category – heap ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT category, COUNT(*), SUM(amount)
FROM orders_heap
GROUP BY category
ORDER BY SUM(amount) DESC;

\echo ''
\echo '=== Q6: GROUP BY category – columnar (none) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT category, COUNT(*), SUM(amount)
FROM orders_columnar
GROUP BY category
ORDER BY SUM(amount) DESC;

\echo ''
\echo '=== Q6: GROUP BY category – columnar (lz4) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT category, COUNT(*), SUM(amount)
FROM orders_columnar_lz4
GROUP BY category
ORDER BY SUM(amount) DESC;

\echo ''
\echo '=== Q6: GROUP BY category – columnar (zstd) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT category, COUNT(*), SUM(amount)
FROM orders_columnar_zstd
GROUP BY category
ORDER BY SUM(amount) DESC;

-- ============================================================
-- Q10: Complex analytics – most demanding query
-- ============================================================
\echo ''
\echo '=== Q10: Complex analytics – heap ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    region, category,
    COUNT(*), SUM(amount),
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM orders_heap
GROUP BY region, category
ORDER BY SUM(amount) DESC
LIMIT 20;

\echo ''
\echo '=== Q10: Complex analytics – columnar (none) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    region, category,
    COUNT(*), SUM(amount),
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM orders_columnar
GROUP BY region, category
ORDER BY SUM(amount) DESC
LIMIT 20;

\echo ''
\echo '=== Q10: Complex analytics – columnar (lz4) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    region, category,
    COUNT(*), SUM(amount),
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM orders_columnar_lz4
GROUP BY region, category
ORDER BY SUM(amount) DESC
LIMIT 20;

\echo ''
\echo '=== Q10: Complex analytics – columnar (zstd) ==='
EXPLAIN (ANALYZE, BUFFERS, FORMAT TEXT)
SELECT
    region, category,
    COUNT(*), SUM(amount),
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM orders_columnar_zstd
GROUP BY region, category
ORDER BY SUM(amount) DESC
LIMIT 20;
