-- =============================================================================
-- pg_columnar benchmark – Step 1: Setup
--
-- Creates the extension and four identical schemas:
--   orders_heap          – standard PostgreSQL heap table
--   orders_columnar      – columnar TAM, no compression (default)
--   orders_columnar_lz4  – columnar TAM, LZ4 compression
--   orders_columnar_zstd – columnar TAM, Zstandard compression
--
-- Compression is applied at write time via the GUC columnar.compression,
-- set in 02_load_data.sql before each INSERT.
--
-- Schema simulates an e-commerce orders fact table, which is intentionally
-- wide (11 columns) so that column-projection benefits are visible.
-- =============================================================================

\echo '=== Creating extension ==='
CREATE EXTENSION IF NOT EXISTS pg_columnar;

-- ---------------------------------------------------------------------------
-- Drop & recreate tables so the script is idempotent
-- ---------------------------------------------------------------------------
DROP TABLE IF EXISTS orders_heap;
DROP TABLE IF EXISTS orders_columnar;
DROP TABLE IF EXISTS orders_columnar_lz4;
DROP TABLE IF EXISTS orders_columnar_zstd;

\echo '=== Creating heap table ==='
CREATE TABLE orders_heap (
    order_id    BIGINT,
    customer_id INTEGER,
    product_id  INTEGER,
    category    VARCHAR(50),
    region      VARCHAR(20),
    amount      FLOAT8,
    quantity    INTEGER,
    discount    FLOAT8,
    is_returned BOOLEAN,
    order_date  DATE,
    created_at  TIMESTAMP
);

\echo '=== Creating columnar table (no compression) ==='
CREATE TABLE orders_columnar (
    order_id    BIGINT,
    customer_id INTEGER,
    product_id  INTEGER,
    category    VARCHAR(50),
    region      VARCHAR(20),
    amount      FLOAT8,
    quantity    INTEGER,
    discount    FLOAT8,
    is_returned BOOLEAN,
    order_date  DATE,
    created_at  TIMESTAMP
) USING columnar;

\echo '=== Creating columnar table (lz4) ==='
CREATE TABLE orders_columnar_lz4 (
    order_id    BIGINT,
    customer_id INTEGER,
    product_id  INTEGER,
    category    VARCHAR(50),
    region      VARCHAR(20),
    amount      FLOAT8,
    quantity    INTEGER,
    discount    FLOAT8,
    is_returned BOOLEAN,
    order_date  DATE,
    created_at  TIMESTAMP
) USING columnar;

\echo '=== Creating columnar table (zstd) ==='
CREATE TABLE orders_columnar_zstd (
    order_id    BIGINT,
    customer_id INTEGER,
    product_id  INTEGER,
    category    VARCHAR(50),
    region      VARCHAR(20),
    amount      FLOAT8,
    quantity    INTEGER,
    discount    FLOAT8,
    is_returned BOOLEAN,
    order_date  DATE,
    created_at  TIMESTAMP
) USING columnar;

\echo '=== Setup complete ==='
