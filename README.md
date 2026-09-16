# iron-feather

**Build one DuckDB shard, then serve it through OGC REST and Arrow Flight.**
The default fast path reads a local immutable copy; read-only HTTP/S3 attachment
is available for experiments and lower-local-storage deployments.

```text
OSM Layercake GeoParquet (HTTPS)
  → bbox-pruned import → immutable DuckDB + R-tree + feature-id index
                          └─ shared read-only connection pool
                               ├─ OGC Features / XYZ tiles → cached response bytes
                               └─ Arrow Flight → native DuckDB Arrow batches (uncached)
```

One binary, two commands, one shard. Apache-2.0.

## Quickstart

Install stable Rust with rustup and `just` with `mise install`, then:

```sh
just fixture-osm --limit 20000    # Layercake buildings, central Berlin
just fmt-check check test
just run                        # fixtures/osm.duckdb; HTTP :3000, Flight :50051
```

```sh
curl localhost:3000/collections
curl 'localhost:3000/collections/buildings/items?sources=1&limit=10'
curl 'localhost:3000/collections/buildings/items?bbox=13.395,52.515,13.405,52.525&sources=1'
# Human-readable API documentation: http://localhost:3000/api.html
```

The recipes configure the prebuilt DuckDB library. For direct Cargo commands:

```sh
just setup-duckdb
export DUCKDB_LIB_DIR="$PWD/.deps/duckdb"
export LD_LIBRARY_PATH="$DUCKDB_LIB_DIR"       # macOS: DYLD_LIBRARY_PATH
cargo run --locked -- build \
  --from https://data.openstreetmap.us/layercake/buildings.parquet \
  --collection buildings --bbox=13.35,52.48,13.45,52.55 \
  --out fixtures/berlin.duckdb
cargo run --locked --release -- serve --shard fixtures/berlin.duckdb
```

## Materialization

`build` reads [Layercake](https://layercake.openstreetmap.us/) GeoParquet with
`type`, `id`, `geometry`, and `bbox.{xmin,ymin,xmax,ymax}` columns. It supports
local files and DuckDB HTTP/S3 sources. `--bbox` is required; `--limit` is an
optional development cap. Current flat property columns and older nested
`tags` structs are preserved as JSON. Original geometry is retained; IDs are
`type:id` so an OSM way and relation with the same numeric ID stay distinct.

The resulting schema is:

```text
features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON,
         cx DOUBLE, cy DOUBLE, name VARCHAR)
collections(id VARCHAR)
provenance(source VARCHAR, built_at TIMESTAMPTZ)
```

Geometry is non-null, 2D CRS84 (longitude/latitude). Import uses both Parquet
bbox statistics and exact geometry intersection, then creates an R-tree and a
unique single-column `id` index. IDs must therefore be globally unique within a
shard. `cx`/`cy`/`name` are build-time derivatives (centroid coordinates and
display name) so Flight `x`/`y`/`name` projections avoid per-row geometry and
JSON work; older shards without them keep serving through computed fallbacks.
`--source-id` defaults to `1`. Metadata is discovered from the shard's
actual layers; other materializers can populate this schema with multiple
collections. A versioned `<shard>.manifest.json` (backend, schema version,
source, bbox, rows, layout) is written atomically alongside the shard.

Publication is atomic and refuses to overwrite an existing shard. Build a new
file and restart `serve --shard NEW_FILE` to switch snapshots. Completed shards
can be served without the remote source. The serving host needs the matching
DuckDB `spatial` extension installed; `build` installs it automatically.

```sh
just fixture-nw-europe            # ~10 GB Benelux + N. France buildings
just fixture-nw-europe-hilbert out=fixtures/nw-europe-hilbert.duckdb
just fixture-verify shard=fixtures/nw-europe.duckdb
just workloads                    # saved deterministic request sets
```

`--hilbert` orders heap rows by Hilbert value before indexing, so spatially
close rows share storage blocks at the cost of a one-time sort during build.
`just workloads` writes hot, urban, rural, scattered, broad, empty, deep and
mixed URL sets for `bench-http --workload`; regenerate with `--region` for a
different shard extent.

### Remote attachment

`serve --shard` also accepts an HTTP(S) URL or public/preconfigured `s3://`
location and attaches it read-only through DuckDB `httpfs`. A `.ducklake`
catalog path or URL selects the DuckLake backend instead: same endpoints,
pool and response cache, with single predicate-preserving scans in place of
the narrow-ID two-step fetch (DuckLake has no R-tree or ART indexes). See
[`docs/nw-europe-10gib.md`](docs/nw-europe-10gib.md) for the layout
comparison. For private S3,
prefer a short-lived presigned HTTPS object URL; the binary deliberately does
not accept cloud credentials. Every pooled connection is switched to the
attached catalog before external access is locked down on single-file
backends; DuckLake resolves its Parquet data files at query time, so the
lockdown stays off there and all SQL remains server-generated.

Remote spatial queries use late materialization: the R-tree first returns only
candidate IDs, then geometry/properties are fetched through the ID ART index.
Without this split, a cold 5 GB remote query downloaded/materialized about 4 GB.
See [`docs/s3-benchmark.md`](docs/s3-benchmark.md) for the full local versus
`rclone serve s3` comparison, and [`docs/nw-europe-10gib.md`](docs/nw-europe-10gib.md)
for the 25M-row fixture, workload battery, heap-layout experiment and
per-request S3 accounting. Local NVMe remains the recommended production
path for predictable cold latency.

Layercake data is © [OpenStreetMap contributors](https://www.openstreetmap.org/copyright),
available under the [ODbL](https://opendatacommons.org/licenses/odbl/).

## HTTP API

| Route | Representation |
|---|---|
| `/`, `/conformance` | Landing links and supported conformance classes |
| `/collections`, `/collections/{id}` | Actual collection metadata |
| `/collections/{id}/items` | `application/geo+json` FeatureCollection |
| `/collections/{id}/items/{fid}` | Original geometry and typed properties |
| `/collections/{id}/tiles/{z}/{x}/{y}` | Web Mercator XYZ MVT; 204 if empty |
| `/api`, `/api.html` | OpenAPI 3.0 JSON and HTML documentation |
| `/healthz` | Liveness |

The Features surface implements Core, GeoJSON and OpenAPI 3.0 requirements.
Tiles are an XYZ extension, capped at 5,000 features per tile.

Items support `bbox`, `limit` (1–1,000, default 10), `offset`, `datetime`, and
`sources`. Bboxes support antimeridian crossing, degenerate bounds and 3D
bounds over 2D data. Pages are ordered by feature ID and include `self` and
`next` links. `numberReturned` is included; `numberMatched` is omitted to avoid
an extra count. Layercake edit timestamps are preserved as properties, not
treated as temporal geometry, so static features match every valid datetime.
Unsupported parameters, including `filter` and `properties`, return 400.

### Source selection

Supply `?sources=1,2` or `X-Source-Ids: 1,2`; Flight accepts `sources` in its
ticket or `x-source-ids` metadata. If both are supplied, their **intersection**
is used. Missing both, or an explicitly empty set, returns no features.
These are data filters, not authentication. Cached results include the full
effective source set, and a cache belongs to one immutable shard instance.

## Arrow Flight

`ListFlights`, `GetFlightInfo`, `GetSchema` and `DoGet` are supported. A descriptor
is either a one-component collection path or a command containing the same
JSON used in a `DoGet` ticket:

```json
{"collection":"buildings","bbox":[13.35,52.48,13.45,52.55],"columns":["id","geometry","properties"],"limit":10000,"sources":[1]}
```

Default columns: `id`, `geometry` (WKB), `properties` (JSON string), `source_id`.
Optional projections also include `x`, `y` (centroid coordinates), and `name`.
`offset` defaults to 0; `limit` defaults to 10,000 and is capped at 100,000.
Unknown or duplicate columns are rejected. Empty streams include their schema.

DuckDB produces native Arrow batches, which are encoded directly for Flight without a result cache. A bounded query result is materialized before transmission. The service provides read-only Flight with JSON tickets.

```sh
cargo run --locked --example flight_client
```

## Performance and verification

`--connections` defaults to 8, shared across both protocols. Past the pool,
up to `--max-waiters` (default 128) requests queue for `--max-wait-ms`
(default 250) before the pool fails fast with HTTP **429 + Retry-After: 1** or
Flight **RESOURCE_EXHAUSTED**. `--flight-concurrency` caps concurrent bulk
queries (default: the pool size, i.e. uncapped); lower it to reserve
connections for interactive OGC under bulk load. Heavy HTTP pages
(`limit > 100`, `offset >= 1000`, or broad region slices) share the same bulk
lane. `--threads` sets shared DuckDB threads for the whole process
(default 1; keep at 1 for many small concurrent queries, raise only with
fewer connections for bulk) and `--memory-mb` caps shared DuckDB memory in
MiB (default 4096, sized for the ~10 GB shard urban working set; 0 leaves
DuckDB's unbounded default). Remote shards also enable DuckDB's HTTP
metadata cache, Parquet metadata cache, HTTP connection reuse and
`NO_VALIDATION` for the immutable external-file cache by default; each can
be flipped (`--disable-http-metadata-cache`,
`--disable-parquet-metadata-cache`, `--disable-connection-cache`,
`--enable-cache-validation`, `--enable-parquet-prefetch`). `--query-timeout-ms` interrupts HTTP and Flight
queries past their deadline (default 30000; 0 disables). Queries run on blocking
workers with one bounded lifecycle: cancellation is owned from before
execution through final delivery, the worker clears its interrupt handle
before the connection returns to the pool (late drops cannot cancel the next
query), oversized batches drain instead of spinning, and fetch failures
surface as errors instead of truncated streams. Batches flow through a
per-stream byte budget (`--flight-stream-mb`, default 32) plus a process-wide
budget (`--flight-total-mb`, default 128). Native small pages set
`late_materialization_max_rows=0` to keep the narrow TOP_N on the R-tree path
instead of a 25M-row scan plus rowid semi-join; DuckLake keeps the default so
narrow scans fetch page payloads late. Moka coalesces identical HTTP requests and caches HTTP
bytes, with a `--cache-mb` budget (default 256 MiB). Set `--cache-mb 0` to
measure the uncached path. `/metrics` is `no-store` and reports
`cache_requests` (lookups), `http_requests`, `cache_hits` (fast-path),
`cache_coalesced` (shared waiters), `cache_computes`, `cache_failures`,
`cache_evictions`, plus `duck_external_cache_ranges/bytes` and the effective
`duck_setting_*` storage tuning. For restart-persistent S3 blocks, pass
`--duck-disk-cache-dir` (opt-in `cache_httpfs` on-disk cache, 512 KiB blocks
via `--duck-disk-cache-block-kb`); it serves warm restarts with zero S3 GETs
at ~2x cold-populate read amplification, so it stays off by default. See
[`docs/nw-europe-10gib.md`](docs/nw-europe-10gib.md) for the benchmarked
tradeoffs (`just bench-duck-cache`).
Error responses are always `Cache-Control: no-store` and carry no ETag.

GeoJSON embeds DuckDB's geometry/properties text without a Rust-side
re-parse, equivalent page URLs share one cache entry, and every response
carries an `ETag` with `Cache-Control: public, max-age=60` (`If-None-Match`
returns 304). ETags are computed once per cached body, and gzip variants are
compressed once then served from the same byte budget with their own strong
ETag; MVT tiles are already compact and skip compression.

Use a release server. The compiled HTTP client warms up before its timed
phase, counts successful responses separately from overload/errors, consumes
complete responses, and reports returned data. Prefer `--passes N
--workload FILE` (identical complete passes on every backend) over
`--duration` (different duration-sliced prefixes are not comparable):

```sh
just bench-matrix base=http://127.0.0.1:3000 dir=workloads/nw-europe passes=2
CONC=8 DUR=15 just bench-http -- --warmup-secs 3
CONC=32 DUR=15 just bench-http -- --warmup-secs 3
CONC=8 DUR=15 just bench-http -- --jitter --seed 1
CONC=8 DUR=15 just bench-http -- --jitter --seed 2
CONC=8 DUR=15 just bench-http -- --gzip
CONC=32 DUR=15 just bench-http -- --route tiles
CONC=32 REQ=100 just bench-flight --limit 1000
CONC=8 REQ=100 just bench-flight --jitter --limit 1000
just bench-workloads dir=workloads/nw-europe
```

Use a fresh `--seed` per jitter run: every request in a run is unique and
warmup never overlaps measurement. Keep seeds small (1-3 on the Berlin
fixture): larger seeds drift the bbox out of the data and measure empty
pages instead. Pass `--cache-mb 0` for true miss tests. Defaults match the
Berlin fixture; pass `--bbox` for another shard. These are closed-loop
client measurements; compare successful rps, p50/p99, rejected (429) counts
and wire throughput together. To size capacity, sweep `--connections
4/8/16/32` against rising client concurrency and stop at the last step
holding p99 under 100 ms with under 1% rejected requests.

Pages carry cursor `next` links: `cursor` is an exclusive lower bound on
feature id and takes precedence over `offset`, so deep pages traverse fewer
discarded rows than `OFFSET` (verified 3.5x on full-region pages at offset
50000; DuckDB does not ART-seek the range, the win is the smaller sort
input). Direct `offset` links keep working.

### Replicas

Shards are immutable, so scale past one CPU by running one instance per core
group against the same local shard file. Each replica keeps its own pool and
in-app response cache: budget roughly one `--connections` pool (8 DuckDB
threads at one thread each) plus `--cache-mb` per instance, and confirm with
`bench-http` round-robining across replicas versus one instance:

```sh
BASE=http://127.0.0.1:3010,http://127.0.0.1:3012 CONC=8 DUR=15 just bench-http -- --jitter --seed 2
```

On the 20k Berlin fixture, loopback, two 4-connection/128 MiB replicas
served jittered misses at ~720 rps against ~350 rps for one 8-connection
instance at equal totals.

Reference loopback run on the 5.17 GB / 14.985M-feature Layercake shard:

| Workload | Local | `rclone serve s3` |
|---|---:|---:|
| First OGC page | 137 ms | 530 ms |
| Cached OGC pages | 1,435 req/s | 1,385 req/s |
| Flight-first, 1,000 rows | 237 ms | 255 ms |
| Cached Flight, 1,000 rows (old 2-cache build) | 966 req/s | 1,007 req/s |
| Random OGC bbox p99 | 61 ms | 230 ms |

The S3 simulation is loopback and therefore excludes real network latency.
Full methodology, MVT results, memory, and the query-plan correction are in
[`docs/s3-benchmark.md`](docs/s3-benchmark.md).

These are not universal capacity claims; run the included tools on the target
CPU, storage, shard size and response shape.

`just check test` runs Clippy and real-DuckDB regressions for materialization,
geometry/lineage agreement between protocols, pagination, request validation,
Flight discovery and empty schemas, tile cache isolation and Mercator encoding,
read-only operation, overload and cancellation.
