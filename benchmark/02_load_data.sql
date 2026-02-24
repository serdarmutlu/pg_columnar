-- =============================================================================
-- pg_columnar benchmark – Step 2: Load test data
--
-- Inserts NUM_ROWS rows into all four tables using generate_series().
-- Default: 1,000,000 rows  (~100 columnar stripes at the 10 000-row threshold)
--
-- The GUC columnar.compression is set to the appropriate value before each
-- columnar INSERT so that every stripe is written with the correct codec.
-- It is reset to 'none' at the end of the script.
--
-- Set the psql variable before running to override the row count, e.g.:
--   psql -v NUM_ROWS=500000 -f 02_load_data.sql
--
-- NOTE: We intentionally use NUM_ROWS instead of ROW_COUNT.
-- ROW_COUNT is a psql built-in variable that is automatically overwritten
-- after every command with the number of rows affected by that command.
-- Because SET commands affect 0 rows, using :ROW_COUNT after any SET would
-- silently expand to 0 and cause generate_series(1, 0) to produce no rows.
-- =============================================================================

-- Default row count if the caller did not supply one
\if :{?NUM_ROWS}
\else
  \set NUM_ROWS 1000000
\endif

\echo '=== Loading :NUM_ROWS rows into orders_heap ==='
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
FROM generate_series(1, :NUM_ROWS) gs;

-- ---------------------------------------------------------------------------
-- Load the pg_columnar library BEFORE any SET command references its GUC.
--
-- pg_columnar.so is loaded lazily: the first time a columnar relation is
-- opened in a session, PostgreSQL calls GetTableAmRoutine(), which loads
-- the library and runs _PG_init() – which registers the columnar.compression
-- enum GUC.
--
-- The heap INSERT above does NOT open any columnar table, so in a fresh
-- session the library is still absent at this point.  Calling
--   SET columnar.compression = 'none'
-- before the library is loaded either raises
--   ERROR: unrecognized configuration parameter "columnar.compression"
-- (aborting the script under ON_ERROR_STOP=1, so no columnar rows are ever
-- inserted) or stores a bare string placeholder that is silently dropped
-- when DefineCustomEnumVariable() later registers the enum type.
--
-- The zero-row SELECT below opens orders_columnar (empty at this point),
-- which triggers the lazy library load and registers the GUC correctly so
-- that every subsequent SET is applied as a typed enum value.
-- ---------------------------------------------------------------------------
\timing off
\echo '=== Loading pg_columnar library (opening columnar relation) ==='
SELECT COUNT(*) AS rows_in_orders_columnar_before_load FROM orders_columnar;
\timing on

-- columnar, no compression (default)
\echo '=== Loading :NUM_ROWS rows into orders_columnar (none) ==='
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
FROM generate_series(1, :NUM_ROWS) gs;

\timing off
\echo '--- orders_columnar row count (must equal :NUM_ROWS) ---'
SELECT COUNT(*) AS inserted_rows FROM orders_columnar;
\timing on

-- columnar, LZ4 compression
\echo '=== Loading :NUM_ROWS rows into orders_columnar_lz4 ==='
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
FROM generate_series(1, :NUM_ROWS) gs;

\timing off
\echo '--- orders_columnar_lz4 row count (must equal :NUM_ROWS) ---'
SELECT COUNT(*) AS inserted_rows FROM orders_columnar_lz4;
\timing on

-- columnar, Zstandard compression
\echo '=== Loading :NUM_ROWS rows into orders_columnar_zstd ==='
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
FROM generate_series(1, :NUM_ROWS) gs;

\timing off
\echo '--- orders_columnar_zstd row count (must equal :NUM_ROWS) ---'
SELECT COUNT(*) AS inserted_rows FROM orders_columnar_zstd;
\timing on

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
