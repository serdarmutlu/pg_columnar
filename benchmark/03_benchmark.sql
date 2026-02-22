-- =============================================================================
-- pg_columnar benchmark – Step 3: Performance queries
--
-- Each query runs against all four tables in order:
--   heap  →  columnar (none)  →  columnar (lz4)  →  columnar (zstd)
-- \timing is on so psql prints wall-clock time for every statement.
--
-- Query categories:
--   Q1  Full-table COUNT                       (scan all rows, zero projection)
--   Q2  Single-column aggregation              (columnar reads 1 of 11 cols)
--   Q3  Multi-column aggregation               (columnar reads 3 of 11 cols)
--   Q4  Filtered aggregation – high selectivity (~20% of rows)
--   Q5  Filtered aggregation – low selectivity  (~2% of rows)
--   Q6  GROUP BY with ORDER BY
--   Q7  Date range scan + aggregation
--   Q8  Two-column projection, full scan
--   Q9  Boolean filter + COUNT
--   Q10 Complex analytics (multiple aggregates by region & category)
-- =============================================================================

\timing on
SET max_parallel_workers_per_gather = 0;  -- disable parallelism for fair comparison

-- ============================================================
-- Q1: Full-table COUNT
-- ============================================================
\echo ''
\echo '--- Q1: Full-table COUNT (heap) ---'
SELECT COUNT(*) FROM orders_heap;

\echo '--- Q1: Full-table COUNT (columnar, none) ---'
SELECT COUNT(*) FROM orders_columnar;

\echo '--- Q1: Full-table COUNT (columnar, lz4) ---'
SELECT COUNT(*) FROM orders_columnar_lz4;

\echo '--- Q1: Full-table COUNT (columnar, zstd) ---'
SELECT COUNT(*) FROM orders_columnar_zstd;

-- ============================================================
-- Q2: Single-column SUM (columnar reads 1 column out of 11)
-- ============================================================
\echo ''
\echo '--- Q2: SUM(amount) (heap) ---'
SELECT SUM(amount) FROM orders_heap;

\echo '--- Q2: SUM(amount) (columnar, none) ---'
SELECT SUM(amount) FROM orders_columnar;

\echo '--- Q2: SUM(amount) (columnar, lz4) ---'
SELECT SUM(amount) FROM orders_columnar_lz4;

\echo '--- Q2: SUM(amount) (columnar, zstd) ---'
SELECT SUM(amount) FROM orders_columnar_zstd;

-- ============================================================
-- Q3: Multi-column aggregation (3 columns)
-- ============================================================
\echo ''
\echo '--- Q3: Multi-column aggregation (heap) ---'
SELECT
    SUM(amount)           AS total_revenue,
    AVG(discount)         AS avg_discount,
    SUM(quantity)         AS total_units
FROM orders_heap;

\echo '--- Q3: Multi-column aggregation (columnar, none) ---'
SELECT
    SUM(amount)           AS total_revenue,
    AVG(discount)         AS avg_discount,
    SUM(quantity)         AS total_units
FROM orders_columnar;

\echo '--- Q3: Multi-column aggregation (columnar, lz4) ---'
SELECT
    SUM(amount)           AS total_revenue,
    AVG(discount)         AS avg_discount,
    SUM(quantity)         AS total_units
FROM orders_columnar_lz4;

\echo '--- Q3: Multi-column aggregation (columnar, zstd) ---'
SELECT
    SUM(amount)           AS total_revenue,
    AVG(discount)         AS avg_discount,
    SUM(quantity)         AS total_units
FROM orders_columnar_zstd;

-- ============================================================
-- Q4: Filtered aggregation – high selectivity (~20% of rows)
-- ============================================================
\echo ''
\echo '--- Q4: High-selectivity filter + SUM (heap) ---'
SELECT SUM(amount)
FROM orders_heap
WHERE region = 'North';

\echo '--- Q4: High-selectivity filter + SUM (columnar, none) ---'
SELECT SUM(amount)
FROM orders_columnar
WHERE region = 'North';

\echo '--- Q4: High-selectivity filter + SUM (columnar, lz4) ---'
SELECT SUM(amount)
FROM orders_columnar_lz4
WHERE region = 'North';

\echo '--- Q4: High-selectivity filter + SUM (columnar, zstd) ---'
SELECT SUM(amount)
FROM orders_columnar_zstd
WHERE region = 'North';

-- ============================================================
-- Q5: Filtered aggregation – low selectivity (~2% of rows)
-- ============================================================
\echo ''
\echo '--- Q5: Low-selectivity filter + aggregation (heap) ---'
SELECT COUNT(*), AVG(amount), SUM(quantity)
FROM orders_heap
WHERE is_returned = TRUE AND region = 'South';

\echo '--- Q5: Low-selectivity filter + aggregation (columnar, none) ---'
SELECT COUNT(*), AVG(amount), SUM(quantity)
FROM orders_columnar
WHERE is_returned = TRUE AND region = 'South';

\echo '--- Q5: Low-selectivity filter + aggregation (columnar, lz4) ---'
SELECT COUNT(*), AVG(amount), SUM(quantity)
FROM orders_columnar_lz4
WHERE is_returned = TRUE AND region = 'South';

\echo '--- Q5: Low-selectivity filter + aggregation (columnar, zstd) ---'
SELECT COUNT(*), AVG(amount), SUM(quantity)
FROM orders_columnar_zstd
WHERE is_returned = TRUE AND region = 'South';

-- ============================================================
-- Q6: GROUP BY with ORDER BY
-- ============================================================
\echo ''
\echo '--- Q6: GROUP BY category (heap) ---'
SELECT
    category,
    COUNT(*)              AS orders,
    SUM(amount)           AS revenue,
    AVG(discount)         AS avg_discount
FROM orders_heap
GROUP BY category
ORDER BY revenue DESC;

\echo '--- Q6: GROUP BY category (columnar, none) ---'
SELECT
    category,
    COUNT(*)              AS orders,
    SUM(amount)           AS revenue,
    AVG(discount)         AS avg_discount
FROM orders_columnar
GROUP BY category
ORDER BY revenue DESC;

\echo '--- Q6: GROUP BY category (columnar, lz4) ---'
SELECT
    category,
    COUNT(*)              AS orders,
    SUM(amount)           AS revenue,
    AVG(discount)         AS avg_discount
FROM orders_columnar_lz4
GROUP BY category
ORDER BY revenue DESC;

\echo '--- Q6: GROUP BY category (columnar, zstd) ---'
SELECT
    category,
    COUNT(*)              AS orders,
    SUM(amount)           AS revenue,
    AVG(discount)         AS avg_discount
FROM orders_columnar_zstd
GROUP BY category
ORDER BY revenue DESC;

-- ============================================================
-- Q7: Date range scan + aggregation (~25% of rows)
-- ============================================================
\echo ''
\echo '--- Q7: Date range aggregation (heap) ---'
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(amount)                     AS monthly_revenue,
    COUNT(*)                        AS order_count
FROM orders_heap
WHERE order_date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY 1
ORDER BY 1;

\echo '--- Q7: Date range aggregation (columnar, none) ---'
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(amount)                     AS monthly_revenue,
    COUNT(*)                        AS order_count
FROM orders_columnar
WHERE order_date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY 1
ORDER BY 1;

\echo '--- Q7: Date range aggregation (columnar, lz4) ---'
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(amount)                     AS monthly_revenue,
    COUNT(*)                        AS order_count
FROM orders_columnar_lz4
WHERE order_date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY 1
ORDER BY 1;

\echo '--- Q7: Date range aggregation (columnar, zstd) ---'
SELECT
    DATE_TRUNC('month', order_date) AS month,
    SUM(amount)                     AS monthly_revenue,
    COUNT(*)                        AS order_count
FROM orders_columnar_zstd
WHERE order_date BETWEEN '2020-01-01' AND '2020-12-31'
GROUP BY 1
ORDER BY 1;

-- ============================================================
-- Q8: Two-column projection, full scan
-- ============================================================
\echo ''
\echo '--- Q8: 2-column projection full scan (heap) ---'
SELECT MIN(order_id), MAX(order_id), MIN(amount), MAX(amount)
FROM orders_heap;

\echo '--- Q8: 2-column projection full scan (columnar, none) ---'
SELECT MIN(order_id), MAX(order_id), MIN(amount), MAX(amount)
FROM orders_columnar;

\echo '--- Q8: 2-column projection full scan (columnar, lz4) ---'
SELECT MIN(order_id), MAX(order_id), MIN(amount), MAX(amount)
FROM orders_columnar_lz4;

\echo '--- Q8: 2-column projection full scan (columnar, zstd) ---'
SELECT MIN(order_id), MAX(order_id), MIN(amount), MAX(amount)
FROM orders_columnar_zstd;

-- ============================================================
-- Q9: Boolean filter + COUNT
-- ============================================================
\echo ''
\echo '--- Q9: COUNT returned orders (heap) ---'
SELECT COUNT(*) FROM orders_heap WHERE is_returned = TRUE;

\echo '--- Q9: COUNT returned orders (columnar, none) ---'
SELECT COUNT(*) FROM orders_columnar WHERE is_returned = TRUE;

\echo '--- Q9: COUNT returned orders (columnar, lz4) ---'
SELECT COUNT(*) FROM orders_columnar_lz4 WHERE is_returned = TRUE;

\echo '--- Q9: COUNT returned orders (columnar, zstd) ---'
SELECT COUNT(*) FROM orders_columnar_zstd WHERE is_returned = TRUE;

-- ============================================================
-- Q10: Complex analytics – multiple aggregates by region & category
-- ============================================================
\echo ''
\echo '--- Q10: Complex analytics by region & category (heap) ---'
SELECT
    region,
    category,
    COUNT(*)                          AS total_orders,
    SUM(amount)                       AS total_revenue,
    AVG(amount)                       AS avg_order_value,
    SUM(amount * (1 - discount))      AS net_revenue,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM orders_heap
GROUP BY region, category
ORDER BY total_revenue DESC
LIMIT 20;

\echo '--- Q10: Complex analytics by region & category (columnar, none) ---'
SELECT
    region,
    category,
    COUNT(*)                          AS total_orders,
    SUM(amount)                       AS total_revenue,
    AVG(amount)                       AS avg_order_value,
    SUM(amount * (1 - discount))      AS net_revenue,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM orders_columnar
GROUP BY region, category
ORDER BY total_revenue DESC
LIMIT 20;

\echo '--- Q10: Complex analytics by region & category (columnar, lz4) ---'
SELECT
    region,
    category,
    COUNT(*)                          AS total_orders,
    SUM(amount)                       AS total_revenue,
    AVG(amount)                       AS avg_order_value,
    SUM(amount * (1 - discount))      AS net_revenue,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM orders_columnar_lz4
GROUP BY region, category
ORDER BY total_revenue DESC
LIMIT 20;

\echo '--- Q10: Complex analytics by region & category (columnar, zstd) ---'
SELECT
    region,
    category,
    COUNT(*)                          AS total_orders,
    SUM(amount)                       AS total_revenue,
    AVG(amount)                       AS avg_order_value,
    SUM(amount * (1 - discount))      AS net_revenue,
    SUM(CASE WHEN is_returned THEN 1 ELSE 0 END) AS returns
FROM orders_columnar_zstd
GROUP BY region, category
ORDER BY total_revenue DESC
LIMIT 20;

\timing off
\echo ''
\echo '=== Benchmark complete ==='
