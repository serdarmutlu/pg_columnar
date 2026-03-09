# pg_columnar

A PostgreSQL extension that adds a `columnar` table access method. Tables created with `USING columnar` store data in Apache Arrow IPC format (columnar layout) instead of PostgreSQL's default heap storage.

Stripe files are standard Arrow IPC and can be read by DuckDB, Pandas, Polars, PyArrow, and any other Arrow-compatible tool.

## Prerequisites

- **PostgreSQL 14, 15, 16, or 17** (with development headers). PostgreSQL 18 support is planned once PG18 reaches general availability.
- **No external Arrow library required** -- [nanoarrow](https://github.com/apache/arrow-nanoarrow) v0.7.0 is vendored in the source tree
- **Optional compression libraries** (detected automatically at build time):
  - [zstd](https://github.com/facebook/zstd) -- best compression ratio
  - [lz4](https://github.com/lz4/lz4) -- fastest compression/decompression

On macOS with Homebrew (replace `17` with your installed major version):

```bash
brew install postgresql@17
brew install zstd lz4    # optional, for compression support
```

On Debian/Ubuntu (replace `17` with your installed major version):

```bash
sudo apt install postgresql-17 postgresql-server-dev-17
sudo apt install libzstd-dev liblz4-dev    # optional, for compression support
```

## Build & Install

```bash
make
make install
```

If `pg_config` is not on your `PATH`, specify it explicitly:

```bash
make PG_CONFIG=/path/to/pg_config
make install PG_CONFIG=/path/to/pg_config
```

## Usage

### Enable the extension

```sql
CREATE EXTENSION pg_columnar;
```

### Create a columnar table

```sql
CREATE TABLE measurements (
    id        int,
    sensor    text,
    value     float8,
    recorded  timestamptz
) USING columnar;
```

### Insert data

```sql
-- Single or batch inserts
INSERT INTO measurements
SELECT g, 'sensor_' || (g % 10), random() * 100, now()
FROM generate_series(1, 100000) g;

-- Bulk load via COPY
COPY measurements FROM '/path/to/data.csv' WITH (FORMAT csv);
```

### Query data

```sql
SELECT sensor, avg(value), count(*)
FROM measurements
GROUP BY sensor
ORDER BY sensor;
```

### Compression

Stripe files can be compressed with ZSTD or LZ4. Compression is controlled per-session via a GUC:

```sql
-- Check current setting
SHOW columnar.compression;    -- default: 'none'

-- Enable ZSTD compression (best ratio)
SET columnar.compression = 'zstd';

-- Enable LZ4 compression (fastest)
SET columnar.compression = 'lz4';

-- Disable compression
SET columnar.compression = 'none';
```

The setting applies to newly written stripes. Existing stripes are not re-compressed. A single table can contain a mix of compressed and uncompressed stripes.

Typical compression ratios (varies with data):

| Compression | Ratio | Notes |
|-------------|-------|-------|
| `none`      | 1.0x  | Default, stripes are standard Arrow IPC |
| `lz4`       | ~1.5x | Fast compression and decompression |
| `zstd`      | ~2.0x | Better ratio, slightly slower |

**Note:** Compressed stripes are not readable by external Arrow tools (PyArrow, DuckDB, etc.) since the entire IPC stream is compressed as a single blob. Uncompressed stripes remain fully compatible.

### Stripe IPC cache

Decompressed stripe bytes are cached per-backend so that repeat scans on the same table
within the same session skip disk I/O and decompression entirely:

```sql
-- Check current setting
SHOW columnar.stripe_cache_size_mb;    -- default: 256

-- Increase the per-backend cache (in MB)
SET columnar.stripe_cache_size_mb = 512;

-- Disable the cache
SET columnar.stripe_cache_size_mb = 0;
```

The GUC controls the maximum number of megabytes of decompressed IPC bytes held in the
backend's memory. When the limit is reached, the least-recently-used stripe is evicted.
The cache is automatically cleared for a table on `DROP TABLE` and `TRUNCATE`.

### DELETE and UPDATE

Standard SQL DELETE and UPDATE are fully supported:

```sql
-- Delete specific rows
DELETE FROM measurements WHERE sensor = 'sensor_5';

-- Conditional delete
DELETE FROM measurements WHERE value < 1.0 AND recorded < now() - interval '30 days';

-- Update a single column
UPDATE measurements SET value = value * 1.05 WHERE sensor = 'sensor_1';

-- Update with a subquery
UPDATE measurements
SET value = sub.avg_val
FROM (SELECT sensor, avg(value) AS avg_val FROM measurements GROUP BY sensor) sub
WHERE measurements.sensor = sub.sensor;
```

Deletes use a **delete bitmap** stored alongside each stripe file (`stripe_XXXXXX.deleted`).
The Arrow IPC stripe file itself is never modified — deleted rows are filtered at read time.
UPDATE is implemented as a delete of the old row plus an insert of the new row.

Stale index entries for updated rows are transparently skipped during index scans
and are cleaned up by `VACUUM`.

### Concurrent write safety

Multiple sessions can safely `INSERT`, `DELETE`, and `UPDATE` the same columnar table
simultaneously. Concurrent writes are serialised using a per-relation advisory lock
that is held for the duration of each metadata-modifying operation (stripe flush,
delete-bitmap update, VACUUM). The lock does not conflict with read-only queries —
`SELECT` and index scans are never blocked while a write is in progress.

### VACUUM

`VACUUM` reclaims disk space from deleted rows:

```sql
VACUUM measurements;
```

Space reclaim behaviour:

| Situation | After VACUUM |
|---|---|
| All rows in a stripe deleted | Stripe file removed from disk; full space reclaimed |
| Some rows in a stripe deleted | Stripe file kept (TIDs must not shift); space reclaimed by `columnar_compact()` |
| Rows updated | Same as partially-deleted; space reclaimed by `columnar_compact()` |

After VACUUM, `SELECT COUNT(*)` and planner estimates reflect only live rows.
Autovacuum runs `VACUUM` automatically in the background.

### Compaction

`columnar_compact()` reclaims space from partially-deleted stripes by rewriting them
without the deleted rows:

```sql
-- Compact a table (rewrites partially-deleted stripes)
SELECT * FROM columnar_compact('measurements');
-- Returns: (stripes_compacted, rows_compacted)
```

Typical workflow — run VACUUM first to remove fully-deleted stripes, then compact to
reclaim space from partially-deleted ones:

```sql
VACUUM measurements;
SELECT * FROM columnar_compact('measurements');
```

Compaction re-inserts surviving rows through the write buffer, producing a new compact
stripe at the end of the table. Indexes are rebuilt automatically for the new rows.
Old TIDs pointing into the original stripe are invalidated (index lookups return no row);
the new rows receive fresh TIDs with new index entries.

### Metadata reconstruction

If the `metadata` file is lost or corrupted, `columnar_rebuild_metadata()` recovers it
by scanning the stripe directory and reading each `.arrow` file:

```sql
SELECT * FROM columnar_rebuild_metadata('measurements');
-- Returns: (stripes_rebuilt, rows_total)
```

The function detects each stripe's compression format from the file header magic bytes,
decompresses it, reads the Arrow IPC stream to recover the row count, and consults the
`.deleted` bitmap files to compute the live row count. It then writes a fresh `metadata`
file and returns the number of stripes recovered and the total live row count.

Stripe files that cannot be read are skipped with a `WARNING`; the remaining stripes
are still recovered. The function is also safe to run on a healthy table — the
reconstructed metadata is equivalent to the original.

Typical use:

```sql
-- After a metadata file is lost
SELECT * FROM columnar_rebuild_metadata('measurements');

-- Optionally run ANALYZE afterwards so the planner has accurate statistics
ANALYZE measurements;
```

### ANALYZE

`ANALYZE` is fully supported and populates column statistics used by the query planner:

```sql
ANALYZE measurements;
```

After `ANALYZE`:
- `pg_class.reltuples` is set to the exact live row count
- `pg_statistic` is populated with per-column statistics (`n_distinct`, most-common
  values, histograms) that the planner uses for cardinality estimation
- Autovacuum runs `ANALYZE` automatically alongside `VACUUM`

### Drop or truncate

```sql
TRUNCATE measurements;
DROP TABLE measurements;
```

## Supported Types

| PostgreSQL     | Arrow Type        |
|----------------|-------------------|
| `bool`         | Bool              |
| `smallint`     | Int16             |
| `integer`      | Int32             |
| `bigint`       | Int64             |
| `real`         | Float32           |
| `double precision` | Float64       |
| `text`         | Utf8              |
| `varchar`      | Utf8              |
| `bytea`        | Binary            |
| `date`         | Date32            |
| `timestamp`    | Timestamp(us)     |
| `timestamptz`  | Timestamp(us, UTC)|
| `uuid`         | FixedSizeBinary(16)|
| `numeric`      | Utf8 (text repr)  |

## Storage Layout

Data is stored under `$PGDATA/columnar/<dbOid>/<relNumber>/`:

```
$PGDATA/columnar/16384/16421/
    metadata                      # stripe index (count, row counts, sizes, compression)
    stripe_000001.arrow           # Arrow IPC stream (one RecordBatch)
    stripe_000001.deleted         # delete bitmap — only present if rows were deleted
    stripe_000001.stats           # per-column min/max statistics for stripe pruning
    stripe_000002.arrow
    stripe_000002.deleted
    stripe_000002.stats
    ...
```

Rows are buffered in memory and flushed to a new stripe file every 10,000 rows or at
transaction commit.

The `.deleted` file is a packed bitset (`ceil(row_count / 8)` bytes). Bit `i` set means
row `i` within that stripe is logically deleted. After VACUUM, fully-deleted stripes have
their `.arrow`, `.deleted`, and `.stats` files removed from disk.

The `.stats` file records the per-column min/max values for the stripe (integers, dates,
timestamps, and floats). It is used by the stripe pruning layer to skip stripes that
cannot possibly match a query's filter conditions.

## Reading Stripes Externally

Uncompressed stripe files are standard Arrow IPC streams and can be read directly:

```python
import pyarrow.ipc

reader = pyarrow.ipc.open_stream("stripe_000001.arrow")
table = reader.read_all()
print(table.to_pandas())
```

Compressed stripes (written with `columnar.compression = 'zstd'` or `'lz4'`) must be decompressed first before passing to an Arrow reader.

## Indexes

Standard B-tree indexes are supported on columnar tables:

```sql
CREATE INDEX ON measurements (sensor);
CREATE INDEX ON measurements (value, sensor);
REINDEX INDEX measurements_sensor_idx;
```

Index scans work correctly, including after DELETE and UPDATE operations. Indexes can be
created before or after data is loaded.

Note: index-only scans are not supported — the access method always fetches the full tuple.

## Performance

### Stripe pruning

Each stripe carries a companion `.stats` file with per-column min/max values for
integer, date, timestamp, and float columns. A custom scan node intercepts queries
at plan time and extracts `col op constant` filter conditions from the `WHERE` clause.
Stripes whose value ranges cannot possibly satisfy the filter are skipped entirely —
no file is opened and no rows are read.

For example, if a table has 100 stripes of 10,000 rows each and a filter matches
only one stripe's range, 99 stripes are skipped with no I/O.

`EXPLAIN ANALYZE` reports the number of stripes skipped:

```sql
EXPLAIN (ANALYZE, COSTS OFF)
SELECT count(*) FROM measurements WHERE id < 50000;
-- Custom Scan (ColumnarScan) on measurements
--   Stripes skipped: 9 of 10
```

### In-memory caches

Four backend-local in-memory caches eliminate repeated file I/O within a session:

| Cache | Key | Eliminates |
|---|---|---|
| Metadata cache | `(dbOid, relNumber)` | One metadata file read per TID during index scans |
| Stats cache | `(dbOid, relNumber, stripe_id)` | One `.stats` file read per stripe per scan |
| Bitmap cache | `(dbOid, relNumber, stripe_id)` | One `.deleted` file read per stripe per scan + read half of DELETE read-modify-write |
| IPC bytes cache | `(dbOid, relNumber, stripe_id)` | All disk I/O and decompression for repeat scans on the same stripes |

All caches are backend-local (not shared across connections) and are automatically
invalidated on `DROP TABLE`, `TRUNCATE`, and `VACUUM`.

The stats cache and bitmap cache store a **"file absent" marker** for stripes that have
no companion file (pre-stats stripes; stripes with no deletions), so repeated scans do
not retry failed `fopen` calls.

The IPC bytes cache stores the decompressed Arrow IPC stream for each stripe (bounded
by `columnar.stripe_cache_size_mb`). On a warm cache, a second scan of the same table
replays the IPC bytes from memory rather than reopening stripe files:

| Compression | Typical speedup (warm vs cold) |
|---|---|
| `none`   | ~4–5× |
| `lz4`    | ~1.5–2× |
| `zstd`   | ~2× |

### Parallel scans

**Parallel Seq Scan** (`Gather → Parallel Seq Scan`) is fully supported. Workers divide
stripes among themselves using a lock-free atomic counter stored in shared memory:

```sql
SET max_parallel_workers_per_gather = 4;
EXPLAIN (ANALYZE, COSTS OFF) SELECT count(*) FROM measurements;
-- Gather (actual rows=1000000)
--   Workers Planned: 4
--   -> Parallel Seq Scan on measurements (actual rows=200000 loops=5)
```

Each worker atomically increments a shared counter to claim the next unprocessed stripe.
Workers skip zero-row stripes (fully vacuumed) and pruned stripes without needing
coordination. For tables with fewer stripes than workers, any remaining workers simply
find no stripes left and return zero rows — the aggregate result is still correct.

**Parallel Append** (used by `UNION ALL` queries and partitioned tables) is also fully
supported. Each worker is assigned a distinct sub-table by the executor and scans it
independently — no stripe sharing or coordination is needed:

```sql
-- All four tables are scanned in parallel; each worker owns its assigned table
SELECT 'heap'     AS t, count(*) FROM measurements_heap
UNION ALL
SELECT 'columnar' AS t, count(*) FROM measurements_columnar
UNION ALL
SELECT 'lz4'      AS t, count(*) FROM measurements_columnar_lz4
UNION ALL
SELECT 'zstd'     AS t, count(*) FROM measurements_columnar_zstd;
```

## Current Limitations

- **No MVCC** -- no snapshot isolation; all rows are always visible to all sessions
- **No WAL logging** -- crash safety is limited to fsync; stripe files and bitmaps
  are written outside PostgreSQL's WAL infrastructure
- **Partial space reclaim after DELETE/UPDATE** -- deleted rows inside a partially-deleted
  stripe occupy disk space until `columnar_compact()` is called; only fully-deleted
  stripes are reclaimed automatically by VACUUM

## License

See [LICENSE](LICENSE) for details.
