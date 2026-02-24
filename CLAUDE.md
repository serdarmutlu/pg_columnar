# pg_columnar — Project Context for Claude

## What This Is

A PostgreSQL **Table Access Method (TAM)** extension that stores table data in
**Apache Arrow IPC format** using the embedded [nanoarrow](https://github.com/apache/arrow-nanoarrow)
library. Rows are buffered in memory and flushed to per-stripe `.arrow` files
on disk. Stripes can be compressed with LZ4 or ZSTD.

- **Extension name:** `pg_columnar`
- **Version:** `0.1.0`
- **PostgreSQL:** 17.8 (Homebrew, aarch64-apple-darwin)
- **PGDATA:** `/opt/homebrew/var/postgresql@17`
- **Dev database:** `omdb`
- **Install prefix:** `/opt/homebrew/lib/postgresql@17/` (lib) and `/opt/homebrew/share/postgresql@17/extension/` (control/sql)

---

## Repository Layout

```
pg_columnar/
├── src/
│   ├── pg_columnar.c          # _PG_init, GUC registration, xact callback
│   ├── columnar_tableam.c     # Table Access Method: all scan/insert/index/DDL hooks
│   ├── columnar_storage.c     # Stripe & metadata file I/O
│   ├── columnar_storage.h     # StripeMetadata, ColumnarMetadata, compression enum
│   ├── columnar_write_buffer.c # In-memory row accumulator, auto-flush logic
│   ├── columnar_write_buffer.h # ColumnarWriteBuffer struct, COLUMNAR_FLUSH_THRESHOLD
│   ├── columnar_typemap.c     # PostgreSQL Datum ↔ Arrow type conversion
│   ├── columnar_typemap.h
│   └── pg_compat.h            # PG_LOCATOR_DB/REL macros (PG15 vs PG16+ compat)
├── vendor/nanoarrow/          # Vendored nanoarrow (nanoarrow.c/h, nanoarrow_ipc.c/h, flatcc)
├── sql/pg_columnar--0.1.0.sql # CREATE TABLE ACCESS METHOD statement
├── benchmark/
│   ├── run_benchmark.sh       # Master orchestrator — runs steps 01–04
│   ├── 01_setup.sql           # CREATE EXTENSION + DROP/CREATE tables
│   ├── 02_load_data.sql       # INSERT 1M rows into all four tables
│   ├── 03_benchmark.sql       # 10 analytical queries, timed
│   ├── 04_explain.sql         # EXPLAIN ANALYZE for key queries
│   └── 00_diagnose.sql        # Step-by-step isolation diagnostic script
├── Makefile                   # PGXS build; auto-detects libzstd & liblz4 via pkg-config
└── pg_columnar.control
```

---

## Architecture

### Storage Layout (on disk)

```
$PGDATA/columnar/<dbOid>/<relfilenode>/
    metadata          # plain-text: "num_stripes total_rows\n" then one line per stripe
    stripe_000001.arrow
    stripe_000002.arrow
    ...
```

**Metadata file format:**
```
<num_stripes> <total_rows>
<stripe_id> <row_count> <file_size> <compression> <uncompressed_size>
...
```
`compression` is the `ColumnarCompression` enum: `0`=NONE, `1`=LZ4, `2`=ZSTD.
The reader also accepts the old 3-field format (no compression fields) and defaults to NONE.

Each `.arrow` file is a complete Arrow IPC stream (schema message + one RecordBatch + EOS).
Compressed stripes store the entire IPC stream compressed as a single blob; decompression
happens fully in memory before nanoarrow reads the stream.

### Key Constants

| Constant | Value | Defined in |
|---|---|---|
| `COLUMNAR_FLUSH_THRESHOLD` | 10 000 rows | `columnar_write_buffer.h` |
| `COLUMNAR_COMPRESSION_NONE` | 0 | `columnar_storage.h` |
| `COLUMNAR_COMPRESSION_LZ4` | 1 | `columnar_storage.h` |
| `COLUMNAR_COMPRESSION_ZSTD` | 2 | `columnar_storage.h` |

### GUC

`columnar.compression` — session-level enum GUC (`none` / `lz4` / `zstd`).
Registered in `_PG_init()` (lazy-loaded when first columnar relation is accessed).

**Critical:** The library is loaded lazily. Always access a columnar relation once
before issuing `SET columnar.compression = ...` or the SET will be silently ignored.
`02_load_data.sql` does this explicitly with a `SELECT COUNT(*) FROM orders_columnar`
before the first SET.

---

## Write Path

1. `columnar_tuple_insert` → appends datum to `ColumnarWriteBuffer` Arrow arrays.
2. Auto-flushes when `buf->nrows >= COLUMNAR_FLUSH_THRESHOLD` (every 10 000 rows).
3. `columnar_finish_bulk_insert` — called by executor after bulk INSERT; flushes any remaining rows.
4. `columnar_xact_callback` on **COMMIT** → `columnar_flush_all_write_buffers()` then `columnar_discard_all_write_buffers()`.
5. `columnar_xact_callback` on **ABORT** → `columnar_discard_all_write_buffers()` (no flush).

`columnar_write_stripe` (in `columnar_storage.c`):
- Serialises the Arrow array to an in-memory IPC buffer via nanoarrow.
- Optionally compresses with ZSTD or LZ4.
- Writes the result to `stripe_XXXXXX.arrow`.
- Reads the existing metadata, appends the new stripe entry, rewrites the metadata file.

---

## Read Path

`columnar_scan_begin` reads the metadata file and caches `StripeMetadata[]`.

`columnar_scan_getnextslot` iterates stripes:
1. Opens each stripe with `columnar_open_stripe_stream`.
2. For **NONE** compression: `ArrowIpcInputStreamInitFile` — reads from FILE* lazily.
3. For **LZ4/ZSTD** compression: reads entire file into memory, decompresses, then
   `ArrowIpcInputStreamInitBuffer` — reads from an in-memory `ArrowBuffer`.
4. Calls `get_schema` immediately; calls `get_next` per-batch in the scan loop.
5. After all on-disk stripes, optionally reads from the active write buffer (in-progress rows).

---

## Index Support

Index support is **fully implemented**. `CREATE INDEX`, `DROP INDEX`, `REINDEX`, and
index scans all work correctly.

### TID Encoding

Each row is assigned a synthetic TID that encodes its physical location:

| TID field | Meaning |
|---|---|
| `BlockNumber` | 1-based stripe index (stripe 1 = first on-disk stripe) |
| `OffsetNumber` | 1-based row offset within that stripe |
| `BlockNumber = num_stripes + 1` | Row is still in the write buffer (not yet flushed) |

TIDs are assigned in `columnar_tuple_insert` using `buf->wb_stripe_block` and `buf->nrows`.
After an auto-flush, `wb_stripe_block` is incremented so subsequent rows get a fresh block
number matching the next stripe.

### `CREATE INDEX` — `columnar_index_build_range_scan`

Called by PostgreSQL's index build machinery. Performs a full sequential scan of all
stripes (via `columnar_scan_begin` / `columnar_scan_getnextslot`), then for each row:
- Calls `FormIndexDatum` to evaluate index key expressions and partial-index predicates.
- Passes the row's synthesized TID and key values to the index build callback.
- All rows are treated as live (`tupleIsAlive = true`) — no MVCC, no dead rows.

### Index Scan — `columnar_index_fetch_tuple`

Called by the executor during an `Index Scan` plan node. Given a TID from the index:

1. Decodes stripe number and row offset from the TID.
2. **Stripe caching:** the entire stripe's RecordBatch is loaded into
   `ColumnarIndexFetchData.cached_batch` the first time a TID from that stripe is
   fetched. Subsequent lookups in the same stripe are served from RAM with no I/O.
3. Fetches the specific row by offset from the cached batch.
4. Also handles write-buffer TIDs (`BlockNumber = num_stripes + 1`).

### `REINDEX` — `columnar_index_validate_scan`

No-op. Since columnar tables have no MVCC and no DELETE/UPDATE support, every tuple
that existed at index-build time is always live — there is nothing to validate.

### `columnar_index_delete_tuples`

Returns `InvalidTransactionId`. No tuples can ever become dead, so there are no
deletable index entries.

### Index Limitations

- **Stripe caching is whole-stripe:** the entire 10 000-row RecordBatch is loaded into
  memory for each new stripe touched during an index scan. This is efficient for clustered
  access but can be memory-heavy for wide schemas with scattered lookups.
- **No min/max stripe skipping.** There is no per-stripe statistics metadata, so an
  index scan still has to open every stripe whose rows are referenced by the index.
  (Seq scan is often competitive for low-selectivity queries.)
- **No index-only scans.** The TAM always returns the full tuple; PostgreSQL's
  index-only scan optimisation is not applicable.

---

## Bugs Found and Fixed

### Bug 1 — `ROW_COUNT` psql built-in variable collision
**File:** `benchmark/02_load_data.sql`, `benchmark/run_benchmark.sh`
**Symptom:** All columnar tables showed 0 rows after running the load script.
**Root cause:** psql automatically overwrites the variable `ROW_COUNT` after every
command with the number of rows affected by that command. After
`SET columnar.compression = 'none'` (which affects 0 rows), `ROW_COUNT` became `0`,
so `generate_series(1, :ROW_COUNT)` silently became `generate_series(1, 0)` = no rows.
**Fix:** Renamed the variable from `ROW_COUNT` to `NUM_ROWS` everywhere in both files.
Added a comment in `02_load_data.sql` explaining why `NUM_ROWS` must not be renamed back.

### Bug 2 — Parallel Append zero-row scan
**File:** `src/columnar_tableam.c`, function `columnar_scan_begin`
**Symptom:** UNION ALL queries across all four benchmark tables returned 0 rows for
`orders_columnar` and `orders_columnar_lz4` but correct counts for `orders_columnar_zstd`.
Individual `SELECT COUNT(*)` on each table returned correct results.
**Root cause:** PostgreSQL optimised the UNION ALL into a **Parallel Append** plan
(Gather → Parallel Append). Each sub-table is assigned to exactly one process
(worker or leader) with no overlap. However, `columnar_scan_begin` had:
```c
if (IsParallelWorker())
    scan->num_stripes = 0;   // intentionally returns 0 rows
```
This guard was designed for **Parallel Seq Scan** on a single columnar table
(where only the leader should scan; workers must return nothing to avoid duplicates).
In Parallel Append, workers each own a distinct sub-table — no duplication is possible —
so returning 0 was wrong. The tables assigned to workers appeared empty.
`orders_columnar_zstd` happened to be assigned to the leader process, so it worked.
**Fix:** Changed the condition to `IsParallelWorker() && pscan != NULL`.
`pscan != NULL` means a coordinated Parallel Seq Scan is in progress (leader handles
all data). `pscan == NULL` with `IsParallelWorker() == true` means a Parallel Append —
the worker owns its assigned table exclusively and must scan it normally.

```c
// Before (too broad — broke Parallel Append):
if (IsParallelWorker())

// After (correct — only skips for coordinated Parallel Seq Scan):
if (IsParallelWorker() && pscan != NULL)
```

---

## Known Limitations

- **No MVCC.** All rows are always visible. No DELETE or UPDATE support.
- **No parallel Seq Scan.** For a `Gather → Parallel Seq Scan` on a single columnar
  table, only the leader process scans the stripes; workers return 0 rows. This means
  single-table parallel scans don't actually parallelise yet.
- **No TOAST.** `columnar_relation_needs_toast_table` returns false.
- **No ANALYZE.** `columnar_scan_analyze_next_block` always returns false (stub).
- **No VACUUM.** `columnar_relation_vacuum` is a no-op.
- **No tablespace support.** `columnar_relation_copy_data` raises an error.
- **Storage size not visible via `pg_total_relation_size`.** Stripe files live outside
  PostgreSQL's normal heap storage, so standard size functions report 0 bytes.
  Use `columnar_storage_size()` internally, or inspect the directory directly:
  `ls -lh $PGDATA/columnar/<dbOid>/<relfilenode>/`.
- **pg_class.relpages is stale.** Statistics are estimated from stripe metadata in
  `columnar_relation_estimate_size`; ANALYZE is a no-op, so the planner uses estimates.

---

## Build & Install

```bash
# One-time: ensure pg_config is on PATH (Homebrew)
export PATH="/opt/homebrew/opt/postgresql@17/bin:$PATH"

# Build and install
make && make install

# Reload the shared library in running sessions (new connections pick it up automatically)
psql -d omdb -c "SELECT pg_reload_conf();"
```

Optional compression libraries are detected automatically via `pkg-config`.
On this machine both `libzstd` and `liblz4` are present (Homebrew), so the build
compiles with `-DHAVE_LIBZSTD -DHAVE_LIBLZ4`.

---

## Benchmark Scripts

### Run full benchmark

```bash
# With default 1 000 000 rows
bash benchmark/run_benchmark.sh -d omdb

# With custom row count
bash benchmark/run_benchmark.sh -d omdb -n 50000

# Extra psql flags pass through
bash benchmark/run_benchmark.sh -d omdb -n 1000000 -h localhost -U myuser
```

Results are saved to `benchmark/results/benchmark_<timestamp>.log`.

### Individual scripts

| Script | Purpose |
|---|---|
| `01_setup.sql` | DROP + CREATE all four tables (heap + 3 columnar variants) |
| `02_load_data.sql` | INSERT `:NUM_ROWS` rows; validates row count after each INSERT |
| `03_benchmark.sql` | 10 analytical queries timed with `\timing on` |
| `04_explain.sql` | `EXPLAIN ANALYZE` for Q2, Q6, Q10 on all table variants |
| `00_diagnose.sql` | Step-by-step diagnostic: extension check, library load, per-type insert test |

**Passing row count:** use `-v NUM_ROWS=<n>` on the psql command line, or let `run_benchmark.sh`
handle it. Never use `ROW_COUNT` (it is a psql built-in that gets overwritten after every command).

### Four benchmark tables

| Table | Access Method | Compression |
|---|---|---|
| `orders_heap` | heap (standard) | — |
| `orders_columnar` | pg_columnar | none |
| `orders_columnar_lz4` | pg_columnar | lz4 |
| `orders_columnar_zstd` | pg_columnar | zstd |

### Reference benchmark results (1 000 000 rows, M-series Mac)

| Query | Heap | Columnar (none) | Columnar (lz4) | Columnar (zstd) |
|---|---|---|---|---|
| Q1 COUNT(*) | ~27ms | ~69ms | ~75ms | ~103ms |
| Q2 SUM(amount) | ~42ms | ~70ms | ~76ms | ~108ms |
| Q6 GROUP BY category | ~112ms | ~128ms | ~133ms | ~163ms |
| Q7 Date range agg | ~109ms | ~102ms | ~110ms | ~141ms |
| Q10 Complex analytics | ~477ms¹ | ~185ms | ~189ms | ~220ms |

¹ Heap requires an on-disk sort (35 MB temp); columnar uses in-memory hash aggregate.
Columnar is **2.2–2.6× faster** on complex analytics queries.

---

## Useful Diagnostic Queries

```sql
-- Check all four table row counts at once (uses Parallel Append — safe after Bug 2 fix)
SELECT 'orders_heap'          AS t, COUNT(*) FROM orders_heap
UNION ALL
SELECT 'orders_columnar'      AS t, COUNT(*) FROM orders_columnar
UNION ALL
SELECT 'orders_columnar_lz4'  AS t, COUNT(*) FROM orders_columnar_lz4
UNION ALL
SELECT 'orders_columnar_zstd' AS t, COUNT(*) FROM orders_columnar_zstd;

-- Show current compression GUC
SHOW columnar.compression;

-- Find relfilenodes (to locate stripe files on disk)
SELECT relname, relfilenode
FROM pg_class
WHERE relname IN ('orders_columnar','orders_columnar_lz4','orders_columnar_zstd');

-- Check stripe files on disk (substitute actual relfilenode)
-- ls /opt/homebrew/var/postgresql@17/columnar/16911/<relfilenode>/

-- List indexes on columnar tables
SELECT t.relname AS table_name, i.relname AS index_name, ix.indisprimary, a.attname AS key_column
FROM pg_index ix
JOIN pg_class t ON t.oid = ix.indrelid
JOIN pg_class i ON i.oid = ix.indexrelid
JOIN pg_am am ON am.oid = t.relam
JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
WHERE am.amname = 'pg_columnar'
ORDER BY t.relname, i.relname;

-- Force an index scan (disable seq scan) to test index fetch
SET enable_seqscan = off;
EXPLAIN (ANALYZE) SELECT order_id FROM orders_columnar WHERE category = 'Electronics' LIMIT 5;
RESET enable_seqscan;
```
