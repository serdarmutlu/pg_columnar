-- =============================================================================
-- pg_columnar benchmark – Step 4: Query plans
--
-- Shows EXPLAIN ANALYZE output for key queries across all four tables.
-- All columnar variants use the same scan node; the timing difference comes
-- from I/O cost when decompressing stripes (none vs. lz4 vs. zstd).
-- =============================================================================

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
