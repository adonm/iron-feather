# 5 GB local vs simulated-S3 benchmark

Measured 2026-09-15 with DuckDB 1.5.5 and rclone 1.75.0 on a 6-vCPU
Haswell VM with 11 GiB RAM. The release server used 8 shared connections.

## Dataset

`fixtures/layercake-5gib.duckdb` was materialized from the public Layercake
buildings GeoParquet:

- 14,985,000 real features
- 5,174,734,848 bytes (5.17 GB / 4.82 GiB)
- 14,901,441 polygons and 83,559 multipolygons
- no null geometries, duplicate IDs, or free DuckDB blocks
- unique single-column ART index on `id` and an R-tree on `geom`
- SHA-256 `da5d377e43cd4c82bffae621916f36b9e6cb8bbd1bc8b0ab3a4858d4e2c6f7b9`

The file is ignored by Git because of its size.
External gzip level 1 reduced it to 1.7 GB, but that form is not directly
attachable because whole-file compression removes random range access.

## Simulation

The same inode was presented through rclone's experimental S3 server:

```sh
mkdir -p fixtures/s3root/shards
ln fixtures/layercake-5gib.duckdb fixtures/s3root/shards/layercake-5gib.duckdb
rclone serve s3 fixtures/s3root --addr 127.0.0.1:19000 \
  --read-only --no-modtime --etag-hash ''
```

The remote server attached
`http://127.0.0.1:19000/shards/layercake-5gib.duckdb`. This exercises S3
`HEAD`/range-`GET` semantics, DuckDB `httpfs`, and remote database attachment,
but it is a lower bound: loopback adds no real S3 RTT, TLS, signing, egress,
throttling, or network loss. The local filesystem and OS page cache are also
shared by both paths.

## Important query-plan result

The original wide query used the R-tree but then semi-joined its row IDs to a
wide base-table scan. Remote cold behavior was unacceptable:

This baseline used the immediately preceding 15.75M-row, 5.03 GiB build; the
final optimized shard above is 4.5% smaller by row count.

| Original query | Local | Simulated S3 |
|---|---:|---:|
| First 10-feature bbox | 173 ms | 16,644 ms |
| RSS after first query | 147 MiB | 4,277 MiB |

DuckDB reported 4,058 MiB under `BASE_TABLE` for the remote query.

The implementation now performs late materialization:

1. Use the R-tree and narrow columns to select ordered candidate IDs.
2. Fetch geometry and properties through the single-column ART index.

A direct SQL probe on the same object fell from 16.6 s / 4.3 GiB to 353 ms /
240 MiB. Composite ART indexes cannot service DuckDB index scans, so shard IDs
are now globally unique and indexed alone.

## Optimized service results

All requests succeeded. “Random” uses 64 feature-centered bboxes distributed
across the shard. Cache-off repeated tests still benefit from DuckDB's buffer
cache and request coalescing, but not the response cache.

### Cache disabled

| Workload | Local | Simulated S3 |
|---|---:|---:|
| Server startup | 210 ms | 294 ms |
| First OGC item page | 137 ms | 530 ms |
| Flight-first, 1,000 rows | 237 ms | 255 ms |
| Tile-first, 247 KB MVT | 2,135 ms | 2,088 ms |
| Repeated OGC page | 72 req/s | 69 req/s |
| Repeated Flight 1,000 rows | 48 req/s | 46 req/s |
| Random OGC bboxes | 225 req/s, p99 61 ms | 59 req/s, p99 230 ms |
| Random Flight bboxes | 624 req/s, p99 22 ms | 650 req/s, p99 19 ms |
| RSS after all workloads | 405 MiB | 465 MiB |

Flight random requests returned only a few rows each and ran after index pages
were warm, so their small local/remote difference should not be generalized to
large cold results. The tile is CPU-heavy because it transforms and encodes up
to 5,000 complex polygons; backing storage is not its hot-path bottleneck.

### 256 MiB response cache

| Repeated workload | Local | Simulated S3 |
|---|---:|---:|
| OGC items | 1,435 req/s | 1,385 req/s |
| Flight, 1,000 rows | 966 req/s | 1,007 req/s |
| MVT | 228 req/s, p50 1.6 ms | 204 req/s, p50 1.8 ms |

The MVT totals include the first ~2-second render; subsequent cached requests
are represented by p50. Result caching largely removes backing-store cost for
identical requests, as expected.

## Conclusion

A 5 GB remote DuckDB shard can work after late materialization, but a cold or
spatially scattered OGC workload remains substantially slower even without
real network latency. Five GB should remain an upper bound for direct S3
attachment until the same matrix passes against the deployment's actual S3
region and network.

For low cold p99 and predictable memory, keep immutable shards in S3 but stage
them onto local NVMe before opening. Direct attachment is reasonable when hit
rates are high, several-hundred-millisecond cold OGC latency is acceptable, and
the service has enough memory for persistent R-tree/ART pages.
