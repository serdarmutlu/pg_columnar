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
│   ├── columnar_storage.h     # StripeMetadata, ColumnarMetadata, ColumnarStripeStats, compression enum
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
    metadata                      # plain-text: "num_stripes total_rows\n" then one line per stripe
    stripe_000001.arrow           # Arrow IPC stream (one RecordBatch)
    stripe_000001.deleted         # delete bitmap — only present if rows were deleted
    stripe_000001.stats           # per-column min/max statistics — only present if Level 3 active
    stripe_000002.arrow
    stripe_000002.deleted
    stripe_000002.stats
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

After VACUUM, a fully-deleted stripe has `row_count=0` and no `.arrow`, `.deleted`, or `.stats` files
on disk. Its metadata entry is preserved at its original position for TID stability
(removing it would shift all later stripe block numbers and invalidate existing indexes).

Each `.arrow` file is a complete Arrow IPC stream (schema message + one RecordBatch + EOS).
Compressed stripes store the entire IPC stream compressed as a single blob; decompression
happens fully in memory before nanoarrow reads the stream.

**Stats (`.stats`) file format:**
```
PGCS 1 <ncols>
<stat_type> <has_stats> <min_val> <max_val>
...  (one line per non-dropped column, in Arrow schema child order)
```
`stat_type`: 0=NONE (text/bool/etc.), 1=INT, 2=FLOAT.
`has_stats`: 0 or 1 (0 means all-NULL column in this stripe).
`min_val`/`max_val`: int64 text.
- For INT: value in PG-epoch units (INT2/4/8 = raw int; DATE = days since 2000-01-01;
  TIMESTAMP = µs since 2000-01-01).
- For FLOAT: IEEE-754 bit pattern of the double value (via `memcpy`).
- For NONE / `has_stats=0`: both written as 0 (ignored on read).

Absent `.stats` file means stats were not collected (stripe predates Level 3).  Pruning
is silently skipped for such stripes — fully backward-compatible.

**Delete bitmap (`.deleted`) file format:**
A packed bitset of `ceil(row_count / 8)` bytes. Bit `i` (0-based) set means row `i`
within that stripe is logically deleted. The file is created on the first delete targeting
a row in that stripe, and is read/modified with read-modify-write semantics.

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

## Caching Architecture

Three caching / optimization levels are fully implemented; a fourth is planned:

### Level 1 — Backend-local Metadata Cache (implemented)

`columnar_read_metadata` was called on every TID lookup in `columnar_index_fetch_tuple`
(one file read per TID during an index scan).  A backend-local `HTAB` in `TopMemoryContext`
now caches `ColumnarMetadata` keyed on `(dbOid, relNumber)`.

- **Hit path:** returns a `palloc`'d copy in `CurrentMemoryContext`; zero I/O.
- **Update point:** `columnar_write_metadata` always refreshes the cache after writing.
- **Eviction:** `columnar_remove_storage` evicts unconditionally (DROP TABLE / TRUNCATE).
- **Cross-backend:** intentionally not shared — matches the no-MVCC stance of the TAM.

### Level 3 — Per-stripe Min/Max Statistics (implemented)

Each stripe gets a companion `stripe_XXXXXX.stats` text file written at flush time by
`columnar_collect_stripe_stats` (called from `columnar_write_stripe`).

**Tracked types:**

| PG type | Arrow format | Stat type | Stored value |
|---|---|---|---|
| INT2/INT4/INT8 | `s`/`i`/`l` | INT | raw integer |
| DATE | `tdD` | INT | days since PG epoch (2000-01-01) |
| TIMESTAMP/TZ | `tsu:...` | INT | µs since PG epoch |
| FLOAT4/FLOAT8 | `f`/`g` | FLOAT | double value |
| BOOL, TEXT, BYTEA, UUID, NUMERIC | other | NONE | not tracked |

**Pruning (`columnar_stripe_should_skip`):** called from `columnar_scan_getnextslot`
when `rs_nkeys > 0`.  Supports all five B-tree strategies (< ≤ = ≥ >).  Returns `true`
(skip stripe) only when it is guaranteed that no row can satisfy the scan key condition.

**Important limitation:** PostgreSQL's sequential scan path passes `nkeys = 0` — WHERE
clause quals are applied by the executor, not pushed down to the TAM.  Pruning fires only
when a caller explicitly sets `rs_nkeys > 0`.  The infrastructure is correct and ready for
Level 4 (custom scan node), which will pass explicit scan keys.

**Backward compatibility:** Stripes written before Level 3 have no `.stats` file.
`columnar_read_stripe_stats` returns NULL; pruning is silently skipped — no errors.

**VACUUM integration:** `columnar_relation_vacuum` removes the `.stats` file for fully-
deleted stripes alongside `.arrow` and `.deleted`.

### Level 3b — Per-stripe Stats In-memory Cache (implemented)

`columnar_read_stripe_stats` previously opened and parsed the `.stats` file from disk on
every call — one `fopen` + parse per stripe per invocation of `columnar_stripe_should_skip`.
For a table with N stripes, every scan with active scan keys incurred N file reads.
Stripe stats are write-once and immutable (stripes are never modified after flush), so
caching them is both safe and highly effective.

**Cache key:** `(dbOid, relNumber, stripe_id)` — the three fields needed to uniquely
identify a stripe's stats file.

**Cache storage:** backend-local `HTAB` (`stats_cache`) in a dedicated
`MemoryContext` (`stats_cache_ctx`) rooted at `TopMemoryContext`. The `cols[]` array
inside each entry is palloc'd in `stats_cache_ctx` so it outlives individual queries.

**Hit path:** returns a `palloc`'d copy of the cached `ColumnarStripeStats` in
`CurrentMemoryContext`; zero file I/O.

**`file_absent` marker:** when `fopen` fails with `ENOENT` (pre-Level-3 stripe or
already-vacuumed stripe), a cache entry is recorded with `file_absent = true`. Subsequent
calls return `NULL` immediately without attempting another `fopen`. This avoids repeated
syscall overhead for old tables that pre-date Level 3.

**`ncols` mismatch:** if a cached entry's `ncols` doesn't match `expected_ncols` (which
can happen after `ALTER TABLE ADD/DROP COLUMN`), the cache returns `NULL` and lets pruning
be silently skipped — no stale stats are used for the schema-mismatched stripe.

**Update points:**
- `columnar_write_stripe_stats` — populates the cache immediately after a successful write.
- `columnar_read_stripe_stats` (cache-miss path) — populates after a successful file read,
  or records `file_absent = true` after a confirmed `ENOENT`.

**Eviction:**
- `columnar_stats_cache_evict_stripe(locator, stripe_id)` — evicts a single stripe entry.
  Declared `extern` in `columnar_storage.h`; called from `columnar_relation_vacuum` after
  the `.stats` file is unlinked for a fully-deleted stripe.
- `columnar_stats_cache_evict_relation(locator)` — evicts all entries for a relation
  (static, called from `columnar_remove_storage` on DROP TABLE / TRUNCATE). Uses a
  collect-then-delete loop since HTAB iteration and deletion cannot be safely interleaved.

**Net effect:** for a table with N stripes, `columnar_stripe_should_skip` goes from
N `fopen` + parse calls per scan to zero file I/O after the first scan warms the cache.

### Level 3c — Delete Bitmap In-memory Cache (implemented)

`columnar_read_delete_bitmap` previously opened and read the `.deleted` file from disk
on every stripe open — one `fopen` + `fread` per stripe for sequential scans, index
fetches, and VACUUM. `columnar_set_deleted_bit` performed a full **read-modify-write**
cycle on the `.deleted` file for every deleted row (one `fopen`+`fread` to load the
current bitmap, then `fopen`+`fwrite` to persist the updated bitmap).

The key difference from the stats cache: delete bitmaps are **mutable** — they are
updated by every `DELETE` and `UPDATE` operation. The cache is therefore kept in sync
after every write, not just after the initial read.

**Cache key:** `(dbOid, relNumber, stripe_id)` — same three-field key as the stats cache.

**Cache storage:** backend-local `HTAB` (`bitmap_cache`) in a dedicated
`MemoryContext` (`bitmap_cache_ctx`) rooted at `TopMemoryContext`. The `bits[]` byte
array inside each entry is palloc'd in `bitmap_cache_ctx` so it outlives individual
queries.

**Hit path (read):** `columnar_read_delete_bitmap` returns a `palloc`'d copy of the
cached bitmap in `CurrentMemoryContext`; zero file I/O.

**`file_absent` marker:** when `fopen` fails with `ENOENT` (no rows have been deleted
from this stripe yet), a cache entry is recorded with `file_absent = true`. Subsequent
calls return `NULL` immediately without attempting another `fopen`. This is particularly
valuable for tables where most stripes are never touched by DELETE.

**Update-after-write discipline:** `columnar_set_deleted_bit` updates the cache
**after** the `fwrite` succeeds. This ensures the cache only ever reflects durably
persisted bitmap state — a failed write leaves the cache unchanged and the on-disk
file is the authoritative source.

**DELETE optimization bonus:** beyond scan read-caching, the bitmap cache also
eliminates the *read* half of `columnar_set_deleted_bit`'s read-modify-write cycle.
For a session that deletes many rows from the same stripe, the first DELETE reads
from disk (cache miss), subsequent DELETEs in that session read from the cache.

**Update points:**
- `columnar_set_deleted_bit` — updates cache after every successful `fwrite`.
- `columnar_read_delete_bitmap` (cache-miss path) — populates after a successful
  `fread`, or records `file_absent = true` after a confirmed `ENOENT`.

**Eviction:**
- `columnar_bitmap_cache_evict_stripe(locator, stripe_id)` — evicts a single stripe
  entry. Declared `extern` in `columnar_storage.h`; called from
  `columnar_relation_vacuum` after the `.deleted` file is unlinked for a
  fully-deleted stripe (alongside `columnar_stats_cache_evict_stripe`).
- `columnar_bitmap_cache_evict_relation(locator)` — evicts all entries for a relation
  (static, called from `columnar_remove_storage` on DROP TABLE / TRUNCATE). Uses the
  same collect-then-delete loop as the stats cache.

**Net effect:**
- Sequential and index scans: zero `.deleted` file I/O after the first scan warms
  the cache for each stripe.
- DELETE-heavy workloads: only the first DELETE per stripe per session hits disk for
  the read; all subsequent DELETEs in the same session skip the read entirely.

### Level 4 — Columnar Buffer Pool (planned)

Custom scan node + shared stripe RecordBatch cache.  Supersedes Level 2 (shared stripe
cache alone). Not yet implemented.

The custom scan node is the key enabler: it will extract WHERE-clause quals at plan time
and pass them as explicit `ScanKeyData` to `columnar_scan_begin`, allowing
`columnar_stripe_should_skip` (Level 3) to prune stripes for all ordinary queries —
not just callers that set `rs_nkeys > 0` manually.

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

## Delete / Update Path

Arrow IPC stripe files are write-once and immutable. Logical deletion uses a **delete
bitmap** companion file rather than modifying the stripe file.

### DELETE

1. `columnar_tuple_delete` decodes the TID to get `(stripe_index, row_offset)`.
2. **Eager flush:** if the TID points to the active write buffer (not yet on disk),
   the buffer is flushed to disk first, converting it to a normal on-disk stripe.
3. Calls `columnar_set_deleted_bit()` — read-modify-write of the
   `stripe_XXXXXX.deleted` file.

### UPDATE

`columnar_tuple_update` decomposes into:
1. **Delete old row:** calls `columnar_tuple_delete(old_tid)`.
2. **Insert new row:** calls `columnar_tuple_insert(new_slot)` — appends to the
   write buffer as a normal insert. The executor receives the new TID and calls
   `ExecInsertIndexTuples` to add the new index entry.

Stale index entries pointing to the old TID are **not** removed immediately. They are
skipped transparently during index scans (bitmap check in `columnar_index_fetch_tuple`).
Index VACUUM cleans them up over time.

### Visibility

The delete bitmap is loaded once per stripe when the stripe is opened:
- **Sequential scans:** `columnar_open_stripe` loads the bitmap into
  `scan->current_delete_bitmap`. `columnar_scan_getnextslot` calls
  `columnar_is_deleted()` on every row; deleted rows are skipped silently.
- **Index scans:** `columnar_index_fetch_tuple` loads the bitmap into
  `ColumnarIndexFetchData.cached_delete_bitmap` when caching a new stripe's RecordBatch.
  The bitmap is checked before returning a row; returns `false` for deleted rows.
- **Zero-row stripes** (fully vacuumed): both seq scan and index fetch skip them
  immediately without opening any file.

---

## Read Path

`columnar_scan_begin` reads the metadata file and caches `StripeMetadata[]`.

`columnar_scan_getnextslot` iterates stripes:
1. **Skips stripes with `row_count == 0`** (fully vacuumed, no file on disk).
2. Opens each remaining stripe with `columnar_open_stripe_stream` and loads the
   corresponding delete bitmap (NULL if no `.deleted` file exists).
3. For **NONE** compression: `ArrowIpcInputStreamInitFile` — reads from FILE* lazily.
4. For **LZ4/ZSTD** compression: reads entire file into memory, decompresses, then
   `ArrowIpcInputStreamInitBuffer` — reads from an in-memory `ArrowBuffer`.
5. Calls `get_schema` immediately; calls `get_next` per-batch in the scan loop.
6. **Skips rows where `columnar_is_deleted(bitmap, row_offset)` is true.**
7. After all on-disk stripes, optionally reads from the active write buffer
   (in-progress rows that have not yet been flushed).

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

No-op. There is no MVCC and no per-tuple visibility information in the index entries
themselves. The delete bitmap is the sole source of truth for live vs. deleted rows.
Index validate scan cannot meaningfully check bitmaps (it operates on TIDs without
having the full scan context), so it is left as a no-op. Deleted-row index entries
are filtered at read time via bitmap checks in `columnar_index_fetch_tuple`.

### `columnar_index_delete_tuples`

Returns `InvalidTransactionId`. Columnar tables do not use PostgreSQL's HOT chain
or dead-tuple tracking. Stale index entries for deleted rows are skipped transparently
during index scans (bitmap check) and are cleaned up by index VACUUM over time.

### Index Limitations

- **Stripe caching is whole-stripe:** the entire 10 000-row RecordBatch is loaded into
  memory for each new stripe touched during an index scan. This is efficient for clustered
  access but can be memory-heavy for wide schemas with scattered lookups.
- **No min/max stripe skipping.** There is no per-stripe statistics metadata, so an
  index scan still has to open every stripe whose rows are referenced by the index.
  (Seq scan is often competitive for low-selectivity queries.)
- **No index-only scans.** The TAM always returns the full tuple; PostgreSQL's
  index-only scan optimisation is not applicable.
- **Stale entries after UPDATE.** After `UPDATE`, the old TID's index entry remains
  until index VACUUM removes it. It is silently skipped during index scans (bitmap
  check returns false). This is correct but may slightly slow index scans on heavily
  updated tables.

---

## VACUUM

`columnar_relation_vacuum` is fully implemented. It is called by PostgreSQL's autovacuum
daemon and by explicit `VACUUM` commands.

### What VACUUM does

1. **Iterates all stripes** in the metadata array.
2. For each stripe, reads its delete bitmap (if it exists) and counts deleted rows.
3. **Fully-deleted stripes** (`deleted_rows == row_count`):
   - Unlinks `stripe_XXXXXX.arrow` from disk.
   - Unlinks `stripe_XXXXXX.deleted` from disk.
   - Sets `stripe_meta[i].row_count = 0` and `stripe_meta[i].file_size = 0` in the
     in-memory metadata array. **The entry is not removed** — doing so would shift
     all subsequent stripe block numbers and invalidate existing index TIDs.
4. **Partially-deleted stripes** (`0 < deleted_rows < row_count`):
   - **No file rewriting.** Rewriting the stripe would shift row offsets for surviving
     rows and invalidate their TIDs in existing indexes. Full compaction (stripe
     rewrite + index rebuild) is deferred to Phase 3.
   - The `.deleted` bitmap file is kept in place.
   - The surviving row count is subtracted from `total_rows`.
5. **Rewrites the metadata file** if any stripes changed.

### After VACUUM

- **Fully-deleted stripes** have `row_count=0` in the metadata array and no files on
  disk. Sequential scans skip them (`row_count == 0` check). Index fetches for their
  TIDs return `false` immediately.
- **Partially-deleted stripes** still have their `.deleted` bitmap on disk. Deleted
  rows are filtered at read time as before.
- `total_rows` in the metadata is updated to reflect the current live row count,
  keeping planner estimates accurate.

### Space reclaim characteristics

| Case | After VACUUM |
|---|---|
| Fully-deleted stripe | `.arrow` + `.deleted` files removed; full space reclaimed |
| Partially-deleted stripe | `.arrow` kept; space proportional to deleted rows **not** reclaimed until Phase 3 |
| Updated rows | Old TID's space not reclaimed (same as partially-deleted); new row appended as normal insert |

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

- **No MVCC.** All rows are always visible; there is no snapshot isolation or
  per-transaction visibility. Concurrent DML from two sessions can see each other's
  changes immediately.
- **DELETE / UPDATE work, but space reclaim is partial.** Deleted rows in
  partially-deleted stripes stay on disk until Phase 3 (full stripe compaction).
  Fully-deleted stripes are reclaimed by VACUUM. Space occupied by old UPDATE
  versions is also only reclaimed when the stripe is fully deleted.
- **Stale index entries after UPDATE.** Old-TID index entries remain until index
  VACUUM removes them. They are silently skipped during index scans (bitmap check).
- **No parallel Seq Scan.** For a `Gather → Parallel Seq Scan` on a single columnar
  table, only the leader process scans the stripes; workers return 0 rows. This means
  single-table parallel scans don't actually parallelise yet.
- **No TOAST.** `columnar_relation_needs_toast_table` returns false.
- **No ANALYZE.** `columnar_scan_analyze_next_block` always returns false (stub).
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

-- Test DELETE: remove a specific row and verify it disappears from scan
DELETE FROM orders_columnar WHERE order_id = 42;
SELECT COUNT(*) FROM orders_columnar WHERE order_id = 42;  -- expect 0

-- Test UPDATE: verify new value visible, old value gone
UPDATE orders_columnar SET amount = 99.99 WHERE order_id = 1;
SELECT amount FROM orders_columnar WHERE order_id = 1;     -- expect 99.99

-- Check delete bitmap files on disk (substitute actual relfilenode)
-- ls -lh /opt/homebrew/var/postgresql@17/columnar/16911/<relfilenode>/*.deleted

-- Inspect metadata file to see stripe row counts and total_rows
-- cat /opt/homebrew/var/postgresql@17/columnar/16911/<relfilenode>/metadata

-- Run VACUUM and check that fully-deleted stripes are removed
VACUUM orders_columnar;
-- After VACUUM: verify live row count is correct
SELECT COUNT(*) FROM orders_columnar;
-- Check disk: fully-deleted stripe files should be gone
-- ls -lh /opt/homebrew/var/postgresql@17/columnar/16911/<relfilenode>/
```
