# pg_columnar

A PostgreSQL extension that adds a `columnar` table access method. Tables created with `USING columnar` store data in Apache Arrow IPC format (columnar layout) instead of PostgreSQL's default heap storage.

Stripe files are standard Arrow IPC and can be read by DuckDB, Pandas, Polars, PyArrow, and any other Arrow-compatible tool.

## Prerequisites

- **PostgreSQL 17** (with development headers)
- **No external Arrow library required** -- [nanoarrow](https://github.com/apache/arrow-nanoarrow) v0.7.0 is vendored in the source tree
- **Optional compression libraries** (detected automatically at build time):
  - [zstd](https://github.com/facebook/zstd) -- best compression ratio
  - [lz4](https://github.com/lz4/lz4) -- fastest compression/decompression

On macOS with Homebrew:

```bash
brew install postgresql@17
brew install zstd lz4    # optional, for compression support
```

On Debian/Ubuntu:

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
    metadata              # stripe index (count, row counts, sizes)
    stripe_000001.arrow   # Arrow IPC stream
    stripe_000002.arrow
    ...
```

Rows are buffered in memory and flushed to a new stripe file every 10,000 rows or at transaction commit.

## Reading Stripes Externally

Uncompressed stripe files are standard Arrow IPC streams and can be read directly:

```python
import pyarrow.ipc

reader = pyarrow.ipc.open_stream("stripe_000001.arrow")
table = reader.read_all()
print(table.to_pandas())
```

Compressed stripes (written with `columnar.compression = 'zstd'` or `'lz4'`) must be decompressed first before passing to an Arrow reader.

## Current Limitations

This is a Phase 1 implementation:

- **No UPDATE/DELETE** -- these operations raise an error
- **No indexes** -- index creation is not supported
- **No MVCC** -- no snapshot isolation for columnar data
- **No WAL logging** -- crash safety is limited to fsync
- **No parallel scan**
- **No VACUUM** -- runs as a no-op

## License

See [LICENSE](LICENSE) for details.
