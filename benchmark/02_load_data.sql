-- =============================================================================
-- pg_columnar benchmark – Step 2: Load test data
--
-- Inserts ROW_COUNT rows into all four tables using generate_series().
-- Default: 1,000,000 rows  (~100 columnar stripes at the 10 000-row threshold)
--
-- The GUC columnar.compression is set to the appropriate value before each
-- columnar INSERT so that every stripe is written with the correct codec.
-- It is reset to 'none' at the end of the script.
--
-- Set the psql variable before running to override the row count, e.g.:
--   psql -v ROW_COUNT=500000 -f 02_load_data.sql
-- =============================================================================

-- Default row count if the caller did not supply one
\if :{?ROW_COUNT}
\else
  \set ROW_COUNT 1000000
\endif

\echo '=== Loading :ROW_COUNT rows into orders_heap ==='
\timing on

INSERT INTO orders_heap (
    order_id, customer_id, product_id, category, region,
    amount, quantity, discount, is_returned, order_date, created_at
)
SELECT
    gs                                                   AS order_id,
    (gs % 50000) + 1                                     AS customer_id,
    (gs % 5000)  + 1                                     AS product_id,
    (ARRAY['Electronics','Clothing','Books','Food',
            'Sports','Home','Toys','Automotive',
            'Health','Garden'])[(gs % 10) + 1]           AS category,
    (ARRAY['North','South','East','West','Central'])
        [(gs % 5) + 1]                                   AS region,
    round((random() * 999 + 1)::numeric, 2)::float8      AS amount,
    (gs % 20) + 1                                        AS quantity,
    round((random() * 0.3)::numeric, 4)::float8          AS discount,
    (gs % 10) = 0                                        AS is_returned,
    DATE '2020-01-01' + (gs % 1461)                      AS order_date,
    TIMESTAMP '2020-01-01' + (gs % 1461) * INTERVAL '1 day'
        + (gs % 86400) * INTERVAL '1 second'             AS created_at
FROM generate_series(1, :ROW_COUNT) gs;

-- columnar, no compression (default)
\echo '=== Loading :ROW_COUNT rows into orders_columnar (none) ==='
SET columnar.compression = 'none';

INSERT INTO orders_columnar (
    order_id, customer_id, product_id, category, region,
    amount, quantity, discount, is_returned, order_date, created_at
)
SELECT
    gs                                                   AS order_id,
    (gs % 50000) + 1                                     AS customer_id,
    (gs % 5000)  + 1                                     AS product_id,
    (ARRAY['Electronics','Clothing','Books','Food',
            'Sports','Home','Toys','Automotive',
            'Health','Garden'])[(gs % 10) + 1]           AS category,
    (ARRAY['North','South','East','West','Central'])
        [(gs % 5) + 1]                                   AS region,
    round((random() * 999 + 1)::numeric, 2)::float8      AS amount,
    (gs % 20) + 1                                        AS quantity,
    round((random() * 0.3)::numeric, 4)::float8          AS discount,
    (gs % 10) = 0                                        AS is_returned,
    DATE '2020-01-01' + (gs % 1461)                      AS order_date,
    TIMESTAMP '2020-01-01' + (gs % 1461) * INTERVAL '1 day'
        + (gs % 86400) * INTERVAL '1 second'             AS created_at
FROM generate_series(1, :ROW_COUNT) gs;

-- columnar, LZ4 compression
\echo '=== Loading :ROW_COUNT rows into orders_columnar_lz4 ==='
SET columnar.compression = 'lz4';

INSERT INTO orders_columnar_lz4 (
    order_id, customer_id, product_id, category, region,
    amount, quantity, discount, is_returned, order_date, created_at
)
SELECT
    gs                                                   AS order_id,
    (gs % 50000) + 1                                     AS customer_id,
    (gs % 5000)  + 1                                     AS product_id,
    (ARRAY['Electronics','Clothing','Books','Food',
            'Sports','Home','Toys','Automotive',
            'Health','Garden'])[(gs % 10) + 1]           AS category,
    (ARRAY['North','South','East','West','Central'])
        [(gs % 5) + 1]                                   AS region,
    round((random() * 999 + 1)::numeric, 2)::float8      AS amount,
    (gs % 20) + 1                                        AS quantity,
    round((random() * 0.3)::numeric, 4)::float8          AS discount,
    (gs % 10) = 0                                        AS is_returned,
    DATE '2020-01-01' + (gs % 1461)                      AS order_date,
    TIMESTAMP '2020-01-01' + (gs % 1461) * INTERVAL '1 day'
        + (gs % 86400) * INTERVAL '1 second'             AS created_at
FROM generate_series(1, :ROW_COUNT) gs;

-- columnar, Zstandard compression
\echo '=== Loading :ROW_COUNT rows into orders_columnar_zstd ==='
SET columnar.compression = 'zstd';

INSERT INTO orders_columnar_zstd (
    order_id, customer_id, product_id, category, region,
    amount, quantity, discount, is_returned, order_date, created_at
)
SELECT
    gs                                                   AS order_id,
    (gs % 50000) + 1                                     AS customer_id,
    (gs % 5000)  + 1                                     AS product_id,
    (ARRAY['Electronics','Clothing','Books','Food',
            'Sports','Home','Toys','Automotive',
            'Health','Garden'])[(gs % 10) + 1]           AS category,
    (ARRAY['North','South','East','West','Central'])
        [(gs % 5) + 1]                                   AS region,
    round((random() * 999 + 1)::numeric, 2)::float8      AS amount,
    (gs % 20) + 1                                        AS quantity,
    round((random() * 0.3)::numeric, 4)::float8          AS discount,
    (gs % 10) = 0                                        AS is_returned,
    DATE '2020-01-01' + (gs % 1461)                      AS order_date,
    TIMESTAMP '2020-01-01' + (gs % 1461) * INTERVAL '1 day'
        + (gs % 86400) * INTERVAL '1 second'             AS created_at
FROM generate_series(1, :ROW_COUNT) gs;

-- Restore default so the session is clean for subsequent steps
RESET columnar.compression;

\timing off

-- ---------------------------------------------------------------------------
-- Row count sanity check
-- ---------------------------------------------------------------------------
\echo '=== Row counts ==='
SELECT 'orders_heap'          AS table_name, COUNT(*) AS row_count FROM orders_heap
UNION ALL
SELECT 'orders_columnar'      AS table_name, COUNT(*) AS row_count FROM orders_columnar
UNION ALL
SELECT 'orders_columnar_lz4'  AS table_name, COUNT(*) AS row_count FROM orders_columnar_lz4
UNION ALL
SELECT 'orders_columnar_zstd' AS table_name, COUNT(*) AS row_count FROM orders_columnar_zstd;

-- ---------------------------------------------------------------------------
-- Storage size comparison (highlights compression ratio)
-- ---------------------------------------------------------------------------
\echo '=== Storage sizes ==='
SELECT
    relname                                          AS table_name,
    pg_size_pretty(pg_total_relation_size(oid))      AS total_size,
    pg_size_pretty(pg_relation_size(oid))            AS heap_size,
    pg_size_pretty(pg_indexes_size(oid))             AS index_size
FROM pg_class
WHERE relname IN (
    'orders_heap',
    'orders_columnar',
    'orders_columnar_lz4',
    'orders_columnar_zstd'
)
ORDER BY relname;

\echo '=== Data load complete ==='
