# iron-feather

One shard served as fast as possible: **raw Arrow Flight** straight out of
**DuckDB** over **HTTP Parquet** — no ADBC layer. Beside it, the same lineage
core drives a TiPG-esque **Poem** OGC API (Features + tiles, Martin tile math).
Apache-2.0.

```
PyArrow client --DoGet ticket--> Flight --SQL+lineage+bbox--> DuckDB --range scans--> HTTP Parquet (OSM slice)
```

## Quickstart

```sh
mise install          # just (Rust itself via rustup)
just fmt-check check test
cargo run -- --help   # stub store: synthetic demo data, no backend
curl localhost:3000/collections
curl 'localhost:3000/collections/buildings/items?bbox=-87.35,13.95,-87.05,14.2&sources=1'
curl localhost:3000/api | head -c 400   # OpenAPI contract, TiPG-style
```

## OSM fixtures (one slice feeds both paths)

```sh
just fixture-osm
# fixtures/osm-buildings.parquet  -> Flight --shard-source (file:// or http://)
# fixtures/demo.duckdb            -> Poem  --shard-dir
```

Slices Overture buildings (public HTTPS, central Berlin default) into the fast
schema `id, x, y, source_id, name`. `source_id` is synthetic (author count),
purely so the dev loop exercises lineage filtering end to end.

## Flight fast path (standalone)

```sh
just check-serve test-serve
cargo run --locked --features serve -- \
  --shard-source ./fixtures/osm-buildings.parquet \
  --flight-listen 127.0.0.1:50051 &
cargo run --locked --features serve --example flight_client
```

Ticket JSON over `DoGet`: `{"collection":"buildings","bbox":[..],
"columns":["id","x"],"limit":10000,"sources":[1]}`. Lineage also merges from
`x-source-ids` metadata; empty matches nothing. Sort shard files by `x` so
bbox filters prune via Parquet stats.

## Poem OGC endpoints

| Route | Notes |
|---|---|
| `GET /` | landing, links |
| `GET /conformance` | core, geojson, oas30 |
| `GET /collections`, `/collections/{id}` | `buildings`, `ag_fields` |
| `GET /collections/{id}/items?bbox&limit&offset&datetime&filter&properties` | `numberMatched` omitted by design (OGC permits it; counts cost the most) |
| `GET /collections/{id}/items/{fid}` | single feature |
| `GET /collections/{id}/tiles/{z}/{x}/{y}` | MVT bytes, `204` when empty |
| `GET /api` | OpenAPI JSON contract |
| `GET /healthz` | liveness |

## Lineage (the important part)

`source_ids` come from auth, never from `?filter`. Today that's the dev
stand-in `?sources=1,2` (tiles/Flight: `X-Source-ids` header). Empty matches
nothing. Prod replaces it with a JWT/OIDC Bearer scheme injecting the caller's
policy set. Cache keys bind `serving_version + policy_version + source set`,
so responses are only ever shared between identically-visible callers.

## DuckDB: prebuilt binaries, never `bundled`

The `duckdb` crate builds with `default-features = false`, so the multi-minute
C++ `bundled` compile can never trigger. Linked `libduckdb` comes from:

1. `$DUCKDB_LIB_DIR` (`just setup-duckdb` fetches the release matching
   `Cargo.lock` into `.deps/duckdb`), or
2. the build-script auto-download of the matching prebuilt release.

Override with `DUCKDB_VERSION=x.y.z just setup-duckdb`. Live Poem shards
expect `features(id, layer, source_id, geom, name)` with `LOAD spatial`
(DuckDB >= 1.4).

## Load shedding & benching

Overload sheds, never piles on: identical concurrent requests coalesce onto
one DuckDB query (singleflight), at most 16 DuckDB queries run at once, and
the rest get **429 + `Retry-After: 1`** on tiles (gRPC: `resource_exhausted`).
503 stays reserved for "dependency down". Cache keys bind
`serving_version + policy_version + source set`, so responses are only shared
between identically-visible callers. Turso L2 + moka L1 sit in front; both are
best-effort and can never fail a request.

Scale the bench up gradually — start tiny to profile, then ramp:

```sh
just fixture-osm                                   # one-time OSM slice
cargo run --locked --features serve -- \
  --shard-source ./fixtures/osm-buildings.parquet &  # Flight :50051 + HTTP :3000
CONC=8 REQ=20 just bench-ogc                       # smoke: all 200/204?
CONC=64 REQ=100 just bench-ogc                     # push toward 2k rps OGC
just bench-flight                                  # Flight max-throughput mode
just bench-flight --jitter                         # cold DuckDB path
```

Judge 2k on the *success* rate at 2k offered load — shed 429s are the
system working, and both benches print status breakdowns.

Measured loopback, debug build, 20k-row OSM fixture, one box:

| path | workload | rps | p50 | p99 |
|---|---|---|---|---|
| OGC tiles | hot, 64-way | ~1900 | ~30ms | ~85ms |
| OGC items | hot cache, 64-way | ~1750 | ~31ms | ~93ms |
| OGC items | cold jitter | ~300 | ~236ms | ~281ms |
| Flight | hot cache | ~2550 | ~11ms | ~28ms |
| Flight | cold jitter | ~107 | ~145ms | ~182ms |

Release profile + real shards move all of these; cold numbers size the pool.
