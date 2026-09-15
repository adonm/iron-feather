# iron-feather

**Build one DuckDB shard, then serve it through OGC REST and Arrow Flight.**
The default fast path reads a local immutable copy; read-only HTTP/S3 attachment
is available for experiments and lower-local-storage deployments.

```text
OSM Layercake GeoParquet (HTTPS)
  → bbox-pruned import → immutable DuckDB + R-tree + feature-id index
                          └─ shared read-only connection pool
                               ├─ OGC Features / XYZ tiles → cached response bytes
                               └─ Arrow Flight → native DuckDB Arrow batches
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
features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON)
collections(id VARCHAR)
provenance(source VARCHAR, built_at TIMESTAMPTZ)
```

Geometry is non-null, 2D CRS84 (longitude/latitude). Import uses both Parquet
bbox statistics and exact geometry intersection, then creates an R-tree and a
unique single-column `id` index. IDs must therefore be globally unique within a
shard. `--source-id` defaults to `1`. Metadata is discovered from the shard's
actual layers; other materializers can populate this schema with multiple
collections.

Publication is atomic and refuses to overwrite an existing shard. Build a new
file and restart `serve --shard NEW_FILE` to switch snapshots. Completed shards
can be served without the remote source. The serving host needs the matching
DuckDB `spatial` extension installed; `build` installs it automatically.

### Remote attachment

`serve --shard` also accepts an HTTP(S) URL or public/preconfigured `s3://`
location and attaches it read-only through DuckDB `httpfs`. For private S3,
prefer a short-lived presigned HTTPS object URL; the binary deliberately does
not accept cloud credentials. Every pooled connection is switched to the
attached catalog before external access is locked down.

Remote spatial queries use late materialization: the R-tree first returns only
candidate IDs, then geometry/properties are fetched through the ID ART index.
Without this split, a cold 5 GB remote query downloaded/materialized about 4 GB.
See [`docs/s3-benchmark.md`](docs/s3-benchmark.md) for the full local versus
`rclone serve s3` comparison. Local NVMe remains the recommended production
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

DuckDB produces native Arrow batches, which are cached and encoded directly
for Flight. A bounded query result is materialized before transmission. The
service provides read-only Flight with JSON tickets.

```sh
cargo run --locked --example flight_client
```

## Performance and verification

`--connections` defaults to 8, shared across both protocols. A busy pool fails
fast with HTTP **429 + Retry-After: 1** or Flight **RESOURCE_EXHAUSTED**. Queries
run on blocking workers; client cancellation keeps the connection checked out
until that worker finishes. Moka coalesces identical requests and caches HTTP
bytes or Arrow batches, with a total `--cache-mb` budget (default 256 MiB).
Set `--cache-mb 0` to measure the uncached path.

Use a release server. The benchmarks count successful responses separately
from overload/errors, consume complete responses, and report returned data:

```sh
CONC=8 REQ=20 just bench-ogc
CONC=32 REQ=100 just bench-ogc
CONC=8 REQ=100 just bench-ogc --jitter
CONC=32 REQ=100 just bench-ogc --route tiles
CONC=32 REQ=100 just bench-flight --limit 1000
CONC=8 REQ=100 just bench-flight --jitter --limit 1000
```

Defaults match the Berlin fixture. Pass `--bbox` for another shard. These are
closed-loop client measurements; compare successful rps, nonempty results,
payload size and latency together.

Reference loopback run on the 5.17 GB / 14.985M-feature Layercake shard:

| Workload | Local | `rclone serve s3` |
|---|---:|---:|
| First OGC page | 137 ms | 530 ms |
| Cached OGC pages | 1,435 req/s | 1,385 req/s |
| Flight-first, 1,000 rows | 237 ms | 255 ms |
| Cached Flight, 1,000 rows | 966 req/s | 1,007 req/s |
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
