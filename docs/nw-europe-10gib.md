# 10 GB shard: fixture, layout experiment and S3 behavior

> Historical record (DuckDB 1.5.5 era): this doc compares the removed
> single-file backend against DuckLake and motivated the DuckLake-only
> rewrite on the DuckDB 2.0 nightly. `just fixture-ducklake`,
> `just fixture-verify-layouts`, `just bench-layouts` and
> `scripts/build_ducklake.sh` no longer exist — `build` now writes the lake
> layout directly (`--sort grid|hilbert|none --file-mb N --row-group N`).
> The `--duck-disk-cache-dir` experiment below is also gone (`cache_httpfs`
> has no 2.0 build); current tuning is HTTP/Parquet metadata caches plus
> `NO_VALIDATION`. All DuckDB access since runs through the stable v2 C API,
> and Quack serves the pinned snapshot as the bulk protocol (see README).
> Re-run the matrix on the nightly before quoting ratios.

Question: what does serving a ~10 GB shard look like, and does heap layout
matter for direct S3 attachment? All runs below are loopback (no real S3
latency), release builds, 8 shared connections unless stated.

## Fixture

`just fixture-nw-europe` materializes all Layercake buildings intersecting
`west=2, south=48, east=6, north=54` (Benelux, northern France, western
Germany edge: Paris, Brussels, Randstad, Ruhr fringe plus rural land):

- 25,358,254 features, 8.6 GiB file including R-tree and id ART indexes.
- `just fixture-verify` checks counts, collections and indexes.
- `just fixture-nw-europe-hilbert` builds the same rows with
  `ORDER BY ST_Hilbert(geom)` heap clustering (`--hilbert`).

The full candidate region (east to 10) holds 52.7M rows (~18 GB) and was cut
in half to hit the 10 GB target. Region sizing rule of thumb from the earlier
15M-row / 5.17 GB shard: ~345 bytes per indexed row.

## Workloads

`just workloads` writes deterministic URL sets for the shard region
(`workloads/nw-europe/*.txt`, consumed by `bench-http --workload`):

| File | Content |
|---|---|
| hot | 3 dense windows, repeated (hit throughput) |
| urban | 14 cities x 3 sizes x 3 limits (mixed) |
| rural | sparse land points (half the 100 m windows are empty) |
| scatter | 3000 fixed-seed points, mixed sizes/limits (misses) |
| broad | quarter/full-region slices, limits 100/1000 (heavy) |
| empty | North Sea windows (empty-result path, reported separately) |
| deep | full-region offsets 1000/5000/10000 (traversal; pair with `--cache-mb 0`) |
| mixed | long-tail combination of the above |

Window sizes are ~100 m / 1 km / 10 km. `http_bench` reports `nonempty`
alongside rps so empty results cannot masquerade as capacity.

## Local battery (baseline shard, 256 MiB cache, current code)

Sequential battery on local NVMe, one process, 8 conns (cache accumulates;
final state 2155 entries, 257,736,850 weight bytes, 942,424 requests, 940,268
hits, 2156 computes, 0 evictions):

| Workload | Result |
|---|---|
| hot | 27,424 rps, p50 242 us, p99 539 us |
| urban | 2,192 rps, p50 465 us, p99 4.2 ms |
| rural | 11,602 rps, half nonempty |
| scatter | 151 rps, p50 1.2 ms, p99 706 ms |
| broad (concurrency 2) | p50 5.7 s per quarter-region limit=1000 page |
| empty | 36,125 rps, all empty |

Stage profile of a nonempty miss (`RUST_LOG=iron_feather=debug`, 10-feature
page): candidate, payload fetch, then JSON encode (now measured as
`encode_us` over final serialization). Candidate selection dominates;
encoding is small but no longer reported as ~0.

## Detailed S3 A/B (cache disabled, 8 conns, loopback rclone, current code)

Fresh native (`:3000`) and lake (`:3001`) processes, sequential workloads per
process (urban first on cold buffers; later workloads reuse warm buffers).
S3 GETs counted from rclone debug logs (counts exact; bytes are rclone
backend reads, an approximation of wire bytes). Duration-based runs cover
different workload prefixes; per-miss GET/MB divide log totals by completed
requests:

| Workload | single .duckdb | DuckLake |
|---|---|---|
| urban (126 urls, 20 s) | 14 rps, p50 70 ms, p99 2.3 s, 16 GETs / 51 MB per miss | 22 rps, p50 202 ms, p99 1.2 s, 5.3 GETs / 25 MB per miss |
| scatter (3000 urls, 15 s) | 109 rps, p50 3.1 ms, p99 969 ms, 7.2 GETs / 22 MB | 42 rps, p50 128 ms, p99 830 ms, 1.4 GETs / 7.2 MB |
| rural (36 urls, 15 s) | 354 rps, p50 1.6 ms, p99 193 ms, ~0 GETs (buffered) | 36 rps, p50 161 ms, p99 675 ms, 0 GETs (buffered) |
| empty (sea boxes, 10 s) | 8,544 rps, p50 0.86 ms, 0 GETs | 1,110 rps, p50 3.7 ms, p99 28 ms, 0 GETs |
| broad (8 urls, 4 conns, 30 s) | 12 reqs, p50 11.9 s, ~1,090 GETs / ~4.0 GB per miss | 16 reqs, p50 9.3 s, ~0.3 GETs / ~0.9 MB per miss (buffered) |
| deep (offsets, 4 conns, 20 s) | 21 reqs, p50 6.6 s, ~0.4 GETs (buffered) | 14 reqs, p50 3.5 s, 0 GETs (buffered) |
| tiles z14 Brussels (10 s) | 21 rps, p50 390 ms | 24 rps, p50 329 ms |
| Flight limit=100 Brussels (2 conns) | 2 rps, p50 853 ms | 8 rps, p50 250 ms |

RSS after urban (before broad inflated buffers): native ~1.4 GB, lake ~3.0
GB; after the full matrix: native ~5.9 GB, lake ~3.3 GB. With cache disabled,
`/metrics` still counts coalesced waiters as hits (native urban run: 93,105
requests, 35,974 coalesced hits, 57,131 computes).

Read it as a tradeoff, not a sweep: the lake minimizes bytes and round
trips per cold miss, so it wins whenever requests are I/O-bound — large
windows, cold buffers, and especially real S3 round-trip time, which
multiplies the GET-count gap (loopback hides this). The native path wins
tiny-query medians on hot buffers (fewer fixed round trips; R-tree +
late-materialization disabled) and sparse/empty shapes by a wide margin
(rural 354 vs 36 rps, empty 8,544 vs 1,110 rps). Broad pages stay CPU-heavy
either way (~9–12 s); the lake's near-zero broad GETs above are warm
Parquet buffers, not pruning alone.

## Cursor vs offset at scale

`EXPLAIN` on a deep broad page: the offset plan sorts all matches then
discards 50,000 rows; the cursor plan (`id > last_id`) uses TOP_N with 11
rows. Cold-process timing over the full region: **3.9 s offset vs 1.1 s
cursor (3.5x)**. Note the range predicate does **not** produce an ART index
seek (DuckDB documents ART scans for equality/`IN`); the win is the smaller
sort input. `next` links therefore emit cursors; direct `offset` still works.

## Layout experiment: baseline beats Hilbert

Rowid locality for 160k Brussels rows: baseline spans 447k rowids (2.8x the
count), Hilbert spans 18.8M. The parquet insertion order was already
spatially grouped; single-key `ST_Hilbert(geom)` scattered it. File sizes are
indistinguishable (8.6 vs 8.7 GiB), and local scatter throughput is identical
(98 vs 88 rps): heap locality does not matter on local NVMe at this scale.

It matters enormously over S3. Cache disabled, urban workload, loopback
`rclone serve s3` attachment, current code, fresh processes:

| Shard | rps | p50 | p99 | S3 GETs/req | MB/req | Server RSS |
|---|---|---|---|---|---|---|
| baseline | 14 | 70 ms | 2.3 s | 16 | 51 | 1.4 GB |
| hilbert | 6 | 242 ms | 10.8 s | 216 | 622 | 7.6 GB |

Same rows, same requests: the scattered heap needs **13x the range reads and
12x the bytes per miss**, plus 5x the buffer memory. With real S3 round-trip
time multiplying every GET, heap locality is the dominant remote cost after
the unavoidable candidate scan. **Preserve the spatially grouped insertion
order; do not reorder blindly.**

Bulk over S3 is seconds per request either way: Flight limit=100 ran ~850 ms
native vs ~250 ms lake at 2 conns (current code). Stage bulky S3 workloads
onto NVMe; reserve direct attachment for small selective requests with high
hit rates.

## /metrics

`GET /metrics` reports `cache_entries`, `cache_weight_bytes`,
`cache_requests`, `cache_hits` (fast-path + coalesced waiters; hit rate =
`hits / requests`), `cache_computes` (distinct miss executions) and
`cache_evictions`. Sample before/after a run to size `--cache-mb`. With
cache disabled, hits are coalesced waiters only (native urban run above:
93,105 requests, 35,974 coalesced hits, 57,131 computes).

## DuckLake + Parquet layout vs single .duckdb over S3

`just fixture-ducklake` converts the regional fixture into a DuckLake
catalog (5.8 MB) plus 59 ZSTD level-3 Parquet files (2.7 GB total): 128 MiB
target files, 64k row groups, explicit grid-cell sort (`sortkey`, `id`) for
tight per-file bbox statistics, plus numeric `xmin/ymin/xmax/ymax` columns
so min/max statistics prune where no spatial index exists.
`FILE_MB`, `ROW_GROUP` and `SORT` (grid/hilbert/none) tune the layout;
screen ordering first, then 64/128/256 MB files and 16k/64k/128k row groups
on the strongest candidates. `just fixture-verify-layouts` checks identical
rows, geometry bytes and properties. `--shard` accepts the catalog path or
URL; the app detects the `.ducklake` suffix and serves the same endpoints,
encoding, pool and cache through it. DuckLake has no R-tree or ART indexes,
so items, tiles and Flight run single predicate-preserving scans instead of
the narrow-ID two-step fetch. DuckLake scans select the page before
converting geometry (`ST_AsGeoJSON`/`ST_AsWKB`/centroids run over ~1k page
rows, not ~5M scanned rows) and skip exact `ST_Intersects` for bboxes fully
inside the query window.

Urban workload, cache disabled, 8 connections, loopback `rclone serve s3`,
current code, fresh processes:

| Backend | rps | p50 | p99 | S3 GETs/req | MB/req | Server RSS |
|---|---|---|---|---|---|---|
| single .duckdb | 14 | 70 ms | 2.3 s | 16 | 51 | 1.4 GB |
| DuckLake, remote catalog | 22 | 202 ms | 1.2 s | 5.3 | 25 | 3.0 GB |

The lake serves **~1.6x the urban throughput with ~3x fewer GETs and ~2x
fewer bytes per cold miss**: file/row-group pruning beats the 25M-row
candidate path that dominates single-file misses. The median stays slower
(per-query catalog/footer round trips), but the tail is better. Scatter
shows the other side: native 109 rps at p50 3.1 ms vs lake 42 rps at p50
128 ms on warm buffers — tiny queries pay the lake's fixed per-query trips.

Flight limit=100 over S3 runs ~380 ms on the lake. Bulk over S3 remains a
stage-to-NVMe workload either way; reserve direct attachment for small
selective requests with high hit rates. Flight streams Arrow batches through
a 32 MiB byte budget (`FLIGHT_BYTE_BUDGET`) while holding its pool
connection; dropping the response interrupts the DuckDB query via
`interrupt_handle`, and `--query-timeout-ms` (default 30000) bounds runaway
scans. Brussels 100-row pages measured ~850 ms native vs ~410 ms lake, and
~360 ms lake after the follow-ups below (loopback, 2 conns).

## Follow-up improvements (implemented, loopback-measured)

Earlier deltas (old → then-current code) are superseded by the matrix above;
representative before/after from that round:

| Workload | single .duckdb (old → then) | DuckLake (old → then) |
|---|---|---|
| urban | 9 → 13 rps, p50 101 → 67 ms, p99 3.5 → 2.5 s | 25 → 33 rps, p50 190 → 149 ms, p99 1.2 s → 668 ms |
| scatter | 100 → 122 rps | 43–48 → 59 rps, p50 ~110 → 89 ms |
| broad limit=1000 | cold/warm variance dominates | ~13 s → 6.5 s cold, 4.8 s warm |

What changed:

- Page-first DuckLake conversion: `ST_AsGeoJSON`/`ST_AsWKB`/centroids run
  over the selected page (`~11 rows` in `EXPLAIN` vs `~5M` before). Broad
  lake pages roughly halved; the ~13 s figure was conversion overhead, not
  an inherent polygon cost.
- Interior fast path: `(contained OR ST_Intersects)` with antimeridian-safe
  `bbox_overlap`/`bbox_contained`. Counts match the old predicate on city
  and full-region windows; disjoint windows miss on bbox columns alone.
- `late_materialization_max_rows=0` on native only: removes the 25M-row scan
  plus rowid semi-join from small native TOP_N plans, keeping the R-tree
  path. DuckLake keeps the default so narrow scans fetch page payloads late
  via file/row-number instead of reading geom+properties first (verified in
  `EXPLAIN`: default shows the file/row-number semi-join, `SET 0` does not).
- `--threads` (default 1), `--memory-mb` (default 0) and `--query-timeout-ms`
  (default 30000) expose engine budgets alongside `--connections`. Sweep
  connections and threads under a fixed memory/CPU budget; use
  `--flight-concurrency` to shield OGC.
- `build_ducklake.sh` accepts `WEST SOUTH EAST NORTH FILE_MB ROW_GROUP SORT`
  (SORT = grid/hilbert/none). Hilbert now binds via
  `ST_Hilbert(geom, ST_Extent(envelope))` with the region extent instead of
  the previous type-erroring geometry envelope; the old single-argument
  Hilbert result still does not cover bounded Hilbert, so screen orderings
  before sizing.
- Tiles run page-first on DuckLake (select 5k id/geom, then Mercator+clip),
  with an early `SELECT 1 ... LIMIT 1` so empty tiles stay 204 like native.
  Lake tiles measured 24 rps loopback (8 conns, miss path).
- Items SQL now builds on miss (hits pay validation + key + lookup only),
  `/metrics` reports `cache_hits` explicitly (fast-path + coalesced; hit
  rate = `hits / requests`), and `encode_us` measures final JSON
  serialization instead of ~0. Hot loopback: 139,194 requests, 139,191 hits,
  3 computes.

Single-run loopback numbers, not a full matrix: use `just bench-matrix`
(`http_bench --passes N`) for identical complete passes, equal memory/CPU
budgets, alternating backend order, and fresh/warm/cached states before
quoting ratios externally. `scripts/s3_stats.py LOG MARKER [REQUESTS]`
counts exact S3 GETs; byte totals sum requested backend-read lengths
(approximation, not measured response bodies). `/metrics` now separates
fast-path `cache_hits`, `cache_coalesced`, `cache_computes`,
`cache_failures` and `http_requests`; hit rate is
`(hits + coalesced) / requests`.

Rebench after the lifecycle/plan/budget rebuild (loopback rclone, release,
8 conns, `--cache-mb 0`, fixed `--passes`, identical sets):

| Workload | single .duckdb | DuckLake |
|---|---|---|
| urban 2 passes (252 reqs) | 11 rps, p50 75 ms, p99 2.24 s, 18.9 GETs / 58 MB per req | 22 rps, p50 201 ms, p99 1.08 s, 9.8 GETs / 48 MB per req |
| rural 2 passes (72 reqs) | 92 rps, p50 9.5 ms, 2.9 GETs / 9.3 MB | 36 rps, p50 121 ms, 4.2 GETs / 26 MB |
| empty 2 passes (12 reqs) | 1,684 rps, 0 GETs | 249 rps, 0 GETs |
| Flight-100 Brussels (6 reqs, 2 conns) | 2 rps, p50 865 ms | 8 rps, p50 266 ms |

RSS: native 1.4 GB, lake 3.2 GB. Cache-off `/metrics` shows the new
split (336 lookups, 0 fast hits, 2 coalesced, 334 computes): coalesced
waiters no longer inflate hits. Tradeoff holds: lake wins cold-miss
throughput/tail, native wins tiny/sparse/empty medians.

## Design notes (current code)

- One bounded execution lifecycle for HTTP and Flight: cancellation is
  owned from before execution through delivery, the worker clears its
  DuckDB interrupt handle before the connection returns to the pool,
  oversized batches drain instead of spinning, iterator advancement is
  inside the panic boundary, and mid-stream failures surface as errors.
- Normalized request planning (`src/plan.rs`): items/tiles/Flight share
  pagination, cache-key and heaviness rules. Heavy pages
  (`limit > 100`, `offset >= 1000`, broad slices) share the bulk lane with
  Flight; SQL builds on miss for items, single features and tiles.
- Budgets are explicit (`StoreConfig`): shared DuckDB threads/memory,
  pool queue, response-cache bytes, per-stream (`--flight-stream-mb`) and
  process-wide (`--flight-total-mb`) Flight buffers, plus HTTP/Flight
  execution deadlines (`--query-timeout-ms`).
- Immutable builds carry serving work: native and DuckLake builders write
  `cx`/`cy`/`name` derivatives plus a versioned `.manifest.json`
  atomically (DuckLake stages then renames; never deletes a published
  catalog first).

## Warm cache: backend-independent

Fresh servers with `--cache-mb 256`, current code:

| Workload | single .duckdb | lake, remote catalog |
|---|---|---|
| hot (hot.txt, first pass + hits) | 26,227 rps, p50 251 us, 3 entries / 51,754 bytes / 3 computes | 27,418 rps, p50 242 us, same cache state |
| urban (126 urls, first pass + hits) | 3,765 rps, p50 463 us, 126 entries / 36,043,504 bytes / 126 computes | 6,030 rps, p50 458 us, same cache state |

Hot-hit speed is identical by construction (memory-served bytes; 3 computes
each; ~99.999% hits, e.g. 139,191/139,194 on the lake hot run). The urban
gap is entirely first-pass miss cost: the lake fills its 126 entries faster,
then both serve hits at the same rate. Cache state converges bit-for-bit:
126 entries, 36,043,504 weight bytes on every backend.

Verdict: for 10 GB S3 serving, DuckLake with sorted Parquet beats the
single indexed file on throughput, tail latency, bytes per miss and memory
stability. Keep the spatially grouped insertion order (the Hilbert
counter-example above applies here as well: pruning lives or dies by file
statistics), keep `--cache-mb 0` runs in the matrix to watch the miss path,
and re-run `just bench-layouts` against real-region S3 before committing:
loopback excludes round-trip time, which multiplies the GET-count advantage
further.

## DuckDB HTTP/S3 caching: benchmarked target

`just bench-duck-cache` (loopback rclone S3, release, 8 conns, `--cache-mb
0`, urban 126 urls, 1 cold + 1 warm pass per fresh process):

| Case | Cold | Warm (same process) |
|---|---|---|
| lake stock (all caches off, VALIDATE_ALL, unlimited mem) | 19 rps, p50 288 ms, 20.8 GETs/req | 27 rps, p50 140 ms, 0.05 GETs/req |
| lake tuned defaults (4 GiB) | 23 rps, p50 274 ms, 19.9 GETs/req | 25 rps, p50 136 ms, 0.05 GETs/req |
| lake tuned + prefetch-all | 23 rps, p50 273 ms, 19.8 GETs/req | 27 rps, p50 142 ms, 0.02 GETs/req |
| lake tuned 1 GiB (pressure) | 19 rps, p50 306 ms, 39.7 GETs/req | 14 rps, p50 317 ms, 48.6 GETs/req |
| native stock vs tuned | identical (10 vs 9 rps cold, 37.9 GETs/req; 0 GETs warm) | — |

Persistent disk cache (`--duck-disk-cache-dir`, 512 KiB blocks, lake):

| Case | Result |
|---|---|
| populate (empty disk) | 16 rps, p50 288 ms, 43.7 GETs/req, 1.8 GB on disk |
| restart, warm disk | 21 rps, p50 173 ms, **0 GETs** |
| restart, empty disk (control) | 20 rps, p50 295 ms, 44.1 GETs/req |

Findings:

- The **memory budget dominates**: 1 GiB thrashes (2x cold GETs, warm pass
  re-fetches everything: 48.6 GETs/req), while 4 GiB holds the ~1.8 GB urban
  external-file-cache working set with zero warm GETs. Hence the new
  `--memory-mb` default of 4096 (0 = unlimited remains available).
- Built-in caches (HTTP metadata, Parquet metadata, connection reuse,
  NO_VALIDATION) are small but free on loopback (+20% cold throughput,
  −4% GETs) and grow with real S3 round-trip time, where each saved HEAD
  and revalidation matters. They are **on by default**; every flag has an
  opt-out. Prefetch-all shows no urban win and stays off (wide-scan opt-in).
- `cache_httpfs` disk cache is the only restart-persistent layer: warm disk
  serves cold processes with zero S3 traffic at RAM-like latency. Cost: ~2x
  cold-populate GET amplification (block-aligned 512 KiB reads) and an extra
  extension + disk provisioning. It stays **opt-in** (`--duck-disk-cache-dir`)
  for restart-heavy deployments, not the default.
- Bug fixed along the way: `try_clone` does not reliably inherit httpfs
  storage SETs, so the pool now tunes **every** connection (previously only
  the opener was tuned; 7/8 conns silently ran stock). `/metrics` reports
  the effective `duck_setting_*` values plus
  `duck_external_cache_ranges/bytes` so benches can verify tuning stuck.
- Native single-file reads are storage-block traffic, not metadata traffic:
  tuning is neutral there (identical GETs); memory sizing still applies.

Pragmatic final target (now the defaults): built-in caches on,
NO_VALIDATION for immutable URLs, prefetch off, 4 GiB DuckDB memory, no
disk cache unless restarts dominate. Re-run `just bench-duck-cache` against
real S3 before quoting external ratios: loopback hides HEAD/TLS latency,
which only widens the tuned gap.
