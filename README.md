# iron-feather

**Build a DuckLake snapshot on S3, then serve it through OGC REST and Arrow Flight.**

```text
OSM Layercake GeoParquet (HTTPS)
  → bbox-pruned import → DuckLake catalog + clustered Parquet (local or S3)
                          └─ shared read-only connection pool
                               ├─ OGC Features / XYZ tiles → response bytes
                               └─ Arrow Flight → native DuckDB Arrow batches
                          └─ Quack bulk listener → pinned read-only snapshot
```

One binary, two commands, one snapshot. Apache-2.0. DuckDB 2.0 nightly
(`v2.0.0-alpha42069`; see `scripts/duckdb_version.py` for the exact pin),
accessed exclusively through the stable v2 C API.

One binary, two commands, one shard. Apache-2.0.

## Quickstart

Install stable Rust with rustup and `just` with `mise install`, then:

```sh
just fixture-osm --limit 20000    # Layercake buildings, central Berlin
just fmt-check check test
just run                        # fixtures/osm.ducklake; HTTP :3000, Flight :50051
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
  --out fixtures/berlin.ducklake --data-dir fixtures/berlin.files
cargo run --locked --release -- serve --shard fixtures/berlin.ducklake
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
         sortkey BIGINT, xmin/ymin/xmax/ymax DOUBLE, cx DOUBLE, cy DOUBLE, name VARCHAR)
collections(id VARCHAR)
```

Geometry is non-null, 2D CRS84 (longitude/latitude). Import uses both Parquet
bbox statistics and exact geometry intersection, then writes ZSTD Parquet
clustered by `--sort` (`grid` cell, `hilbert`, or `none`) with tight per-file
bbox statistics (`xmin/ymin/xmax/ymax` min/max pruning replaces a spatial
index). Measured on the 25M-row NW-Europe shard (25 files): `grid` keeps
the default — city windows prune to 4–5 files vs 7–8 for `hilbert`, a
rural window to 3 vs 4, at 3.0 vs 3.1 GiB total. `cx`/`cy`/`name` are
build-time derivatives (centroid coordinates and
display name) so Flight `x`/`y`/`name` projections avoid per-row geometry and
JSON work. `--source-id` defaults to `1`. Metadata is discovered from the
shard's actual layers. A versioned `<catalog>.manifest.json` (backend,
schema version, source, bbox, rows, layout) is written alongside the catalog.

Publication is atomic and refuses to overwrite an existing catalog: data
files publish first (additive copy only, never deleting files another
snapshot references), then the catalog that references them. Build a new
snapshot and restart `serve --shard NEW_CATALOG` to switch. The serving
host needs the matching DuckDB `spatial`/`ducklake` extensions installed;
`build` installs them automatically. `--data-url` records the
zone-independent data root in the catalog (an `s3://` prefix for lake
publishes; defaults to the local data dir); each reader overrides it with
`serve --data-base` pointing at its zone's Cachey, so relative Parquet
paths resolve AZ-local everywhere.

```sh
just fixture-nw-europe            # ~10 GB Benelux + N. France buildings
just fixture-verify shard=fixtures/nw-europe.ducklake
just workloads                    # saved deterministic request sets
```

`just workloads` writes hot, urban, rural, scattered, broad, empty, deep and
mixed URL sets for `bench-http --workload`; regenerate with `--region` for a
different shard extent.

### Serving from S3

`serve --shard` takes a local `.ducklake` catalog path or the HTTP(S)/`s3://`
URL of a published catalog and attaches it read-only. For private S3,
prefer a short-lived presigned HTTPS object URL; the binary deliberately does
not accept cloud credentials. In production the catalog stores a
zone-independent `s3://` data root and each reader passes `--data-base`
with its zone's Cachey `/fetch/` prefix, so all storage reads are range
GETs through the zone-shared page cache (see
[`docs/cachey-lake.md`](docs/cachey-lake.md)).
Every pooled connection is switched to the attached catalog; DuckLake resolves
its Parquet data files at query time, so all SQL remains server-generated.

Spatial queries prune by file/row-group bbox statistics, then run exact
`ST_Intersects` only on boundary candidates (fully contained bboxes skip it).
Page-first planning converts geometry only for the returned page. See
[`docs/nw-europe-10gib.md`](docs/nw-europe-10gib.md) for the 25M-row fixture,
workload battery and per-request S3 accounting. [`docs/s3-benchmark.md`](docs/s3-benchmark.md)
records the earlier single-file era and is superseded by the DuckLake design.

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
These are data filters, not authentication. Responses include the full
effective source set.

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

## Quack bulk protocol

`serve` also listens for [Quack](https://duckdb.org/docs/current/quack/overview)
(`--quack-listen`, default `127.0.0.1:9494`; `--no-quack` disables it): raw
SQL over HTTP for DuckDB-native bulk consumers, served from a **separate
database** so bulk scans never evict the OGC/Flight working set. That
separate database carries its own engine budgets, so the wide reader
fleet disables it (`quack.enabled=false` in the chart); run bulk on
dedicated pods or locally instead.

The view is narrowed, not full access:

- **Pinned snapshot.** The Quack instance attaches the exact snapshot the
  server resolved at startup (`SNAPSHOT_VERSION`); later publishes stay
  invisible, and the engine rejects writes on pinned attaches.
- **Read-only.** Catalog writes fail engine-side.
- **Token auth.** `--quack-token` / `IRON_FEATHER_QUACK_TOKEN`, else a random
  per-process token printed once at startup and never logged.
- **Localhost bind** unless `--allow-remote-quack` is passed (front remote
  exposure with a TLS-terminating proxy, per upstream guidance).
- **Statement filter.** A guard macro denies control plane
  (`quack_serve`/`quack_stop`), server-global settings, catalog topology
  (`ATTACH`/`DETACH`), file I/O (`COPY`, `read_*`, `st_read`, direct-URL
  `FROM`), extension loading, and secret creation.

Treat the token as privileged: anything else a holder runs is equivalent to
a local DuckDB shell. Clients address `shard.<table>`:

```sql
-- Any DuckDB with the quack extension (CLI shown).
CREATE SECRET (TYPE quack, TOKEN '<token>');
ATTACH 'quack:127.0.0.1:9494' AS r;
SELECT count(*) FROM r.shard.main.features;
-- Or stateless per query (no ATTACH needed):
SELECT * FROM quack_query('quack:127.0.0.1:9494',
  'SELECT id FROM shard.features ORDER BY id LIMIT 10', token => '<token>');
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
DuckDB's unbounded default). Parquet/HTTP block and metadata caching lives
in Cachey, the per-zone page cache readers fetch through, not in DuckDB:
there are no storage-tuning flags by design (see
[`docs/cachey-lake.md`](docs/cachey-lake.md)).
`--query-timeout-ms` interrupts HTTP and Flight
queries past their deadline (default 30000; 0 disables). Queries run on blocking
workers with one bounded lifecycle: cancellation is owned from before
execution through final delivery, the worker clears its interrupt handle
before the connection returns to the pool (late drops cannot cancel the next
query), oversized batches drain instead of spinning, and fetch failures
surface as errors instead of truncated streams. Batches flow through a
per-stream byte budget (`--flight-stream-mb`, default 32) plus a process-wide
budget (`--flight-total-mb`, default 128). Narrow scans fetch page payloads
late via file/row-number instead of reading geom+properties first. `/metrics`
is `no-store` and reports `http_requests` plus `duck_setting_*` engine
budgets (threads, memory). Storage-cache benchmarks now live in Cachey
instead: see [`docs/cachey-lake.md`](docs/cachey-lake.md) for the local rig
(`just cachey-up`, `just lake-publish`, `just lake-serve`).
Error responses are always `Cache-Control: no-store` and carry no ETag.

GeoJSON embeds DuckDB's geometry/properties text without a Rust-side
re-parse, and every response carries an `ETag` with
`Cache-Control: public, max-age=60` (`If-None-Match` returns 304). ETags
are content hashes over the exact bytes, and gzip variants are compressed
per request with their own strong ETag; MVT tiles are already compact and
skip compression. Repeated storage reads are absorbed by the zone-shared
Cachey layer, not by per-pod memory.

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
pages instead. Defaults match the
Berlin fixture; pass `--bbox` for another shard. These are closed-loop
client measurements; compare successful rps, p50/p99, rejected (429) counts
and wire throughput together. To size capacity, sweep `--connections
4/8/16/32` against rising client concurrency and stop at the last step
holding p99 under 100 ms with under 1% rejected requests.

Pages carry cursor `next` links: `cursor` is an exclusive lower bound on
feature id and takes precedence over `offset`, so deep pages traverse fewer
discarded rows than `OFFSET` (verified 3.5x on full-region pages at offset
50000; the win is the smaller sort input). Direct `offset` links keep working.

### Replicas

Snapshots are immutable, so scale past one CPU by running one instance per
core group against the same catalog. Each replica keeps its own pool:
budget roughly one `--connections` pool (8 DuckDB threads at one thread
each) per instance, with repeated storage reads shared zone-wide through
Cachey, and confirm with `bench-http` round-robining across replicas
versus one instance:

```sh
BASE=http://127.0.0.1:3010,http://127.0.0.1:3012 CONC=8 DUR=15 just bench-http -- --jitter --seed 2
```

On the 20k Berlin fixture, loopback, two 4-connection replicas served
jittered misses at ~720 rps against ~350 rps for one 8-connection instance
at equal totals (measured before the app-cache removal; the scaling shape
— replicas add DuckDB throughput, Cachey shares storage reads — still
applies).

Reference loopback run on the 5.17 GB / 14.985M-feature single-file Layercake
shard (DuckDB 1.5.5 era, before the DuckLake-only rewrite; kept for scale
context, not quoted as current capacity):

| Workload | Local | `rclone serve s3` |
|---|---:|---:|
| First OGC page | 137 ms | 530 ms |
| Cached OGC pages | 1,435 req/s | 1,385 req/s |
| Flight-first, 1,000 rows | 237 ms | 255 ms |
| Random OGC bbox p99 | 61 ms | 230 ms |

The S3 simulation is loopback and therefore excludes real network latency.
The DuckLake matrix in [`docs/nw-europe-10gib.md`](docs/nw-europe-10gib.md)
is the current reference; re-run it on the 2.0 nightly before quoting
ratios externally.

Verified on the nightly (`v2.0.0-alpha42069`, 20k Berlin lake, loopback
`rclone serve s3`): local and S3 return identical items and MVT bytes;
miss-path HTTP runs 56 vs 54 rps and 1,000-row Flight runs 40 vs 41 rps
(measured before the app-cache removal, on unique-URL miss traffic).

These are not universal capacity claims; run the included tools on the target
CPU, storage, shard size and response shape.

`just check test` runs Clippy and real-DuckDB regressions for materialization,
geometry/lineage agreement between protocols, pagination, request validation,
Flight discovery and empty schemas, tile coordinate isolation and Mercator
encoding, read-only operation, overload and cancellation.
