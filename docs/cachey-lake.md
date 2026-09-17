# Cachey lake rig: local rehearsal of the AZ-local cache design

All Parquet/HTTP block and metadata caching lives in Cachey, not in DuckDB.
`serve` fetches catalog and Parquet byte ranges over plain HTTP through a
Cachey, which pages a shared 16 MiB-granularity cache in front of S3. The app
keeps no storage-tuning flags by design (see `setup_session`).

## Layout in S3 (`s3://lake`)

```text
s3://lake
├── refs/
│   ├── latest            # object containing "sha_002.ducklake"
│   ├── main              # object containing "sha_002.ducklake"
│   └── tags/<name>       # one tiny object per tag
├── catalogs/
│   ├── sha_001.ducklake  # immutable once published; data paths are Cachey URLs
│   └── sha_002.ducklake
└── data/
    └── ...               # immutable Parquet, shared across snapshots
```

Everything large is immutable; only tiny ref objects are mutable. A catalog
API will own refs in production (versioning TBD); locally
`scripts/lake_ref.sh` plays that role over S3 objects (`get`/`set`/`list`).
Readers resolve a ref once at startup and serve the pinned snapshot:

```sh
serve --shard http://cachey/fetch/lake/catalogs/sha_002.ducklake
```

`Store` resolves `max(snapshot_id)` at startup and re-attaches with
`SNAPSHOT_VERSION`, so even a swapped catalog object cannot move a reader.

`build` bakes the publish location into the catalog: `--data-url` becomes
each data file's location (Cachey `/fetch/` prefix or S3), so readers need
no shared filesystem. The default is the absolute local data dir, which
keeps `just fixture-osm` serving straight off disk.

## Local rig (this repo)

```sh
just cachey-up                   # MinIO S3 + Cachey page cache on loopback
just lake-publish sha_003 --bbox=... --limit ...   # build + upload + refs/latest move
just lake-serve latest --listen 127.0.0.1:3000     # serve refs/latest (positional ref first)
just lake-verify http://127.0.0.1:3000
just cachey-down                 # stop Cachey (add --wipe to drop S3 too)
```

Components (all local, all ephemeral under `/tmp/opencode`):

| Piece | What | Why |
|---|---|---|
| MinIO (single node) | S3-compatible bucket store on `:3900`, plain files under `/tmp/opencode/minio` | Closest widely-run S3 emulation; zero bootstrap (fixed dev creds, buckets autocreate) |
| Cachey (per-zone page cache) | HTTP read-through on `:8088`, 512 MiB memory | The AZ-local cache stand-in; prod points the same image at real S3 |
| toxiproxy (only with `SLOW_S3_MS`) | Adds downstream latency to S3 traffic | Realish miss RTTs for honest cached-vs-uncached comparisons |

All images are pinned by digest (charts values, `scripts/cachey_up.sh`);
refresh deliberately.

`SLOW_S3_MS=300 just cachey-up` routes Cachey's S3 traffic through a
toxiproxy adding ~300 ms downstream latency, so misses cost a realish RTT
while hits stay loopback-fast — the honest way to compare cached vs
uncached paths locally. 300 ms is a sustained worst case, not the average:
same-region S3 Standard GETs typically land 50–200 ms with p99s in the
low hundreds of ms, and cross-region adds another 150–300 ms; only
throttling backoff (seconds, 503s) sits beyond it, and that is a retry
regime rather than a steady benchmark. Measured on a fresh 20k Berlin
snapshot (8 conns, 500 unique jittered misses, single local server):
uncached 9 rps / p50 802 ms / p99 1670 ms vs Cachey-warm 145 rps /
p50 46 ms / p99 124 ms — a ~16x throughput gap at worst-case latency.
Without the proxy the same comparison is 44 vs 87 rps: loopback flatters
the uncached path.

Verified 2026-09-17: ranged GETs serve Parquet through Cachey (miss then
hit per `C0-Status`); a 20k-feature Berlin snapshot serves OGC + MVT straight
through it; a second reader process adds zero S3 downloads
(zone sharing); warm Cachey serves jittered misses at ~2x uncached S3 and
within ~15% of local disk on loopback (see the benchmark section in the
pre-migration spike notes; redo on the 10 GB fixture before quoting).

## kind whole-stack (`charts/`)

`just kind-up` (config in `k8s/kind-2az.yaml`) builds a 2-worker cluster
with real `topology.kubernetes.io/zone` labels and node-local `/nvme`
extraMounts. Two Helm charts manage the stack:

- `charts/cachey` — MinIO (S3 stand-in) + one Cachey Deployment + Service
  per zone with node-local disk caches on `/nvme`.
- `charts/iron-feather` — depends on `cachey`; one read-pool Deployment per
  zone (each pod fetches through its zone's Cachey, pinned to the concrete
  `catalog`), a shared Service, and the optional hourly writer CronJob
  (`writer.enabled=true`) — the single ETL publisher (build to scratch,
  upload data + catalog, move the ref).

```sh
just kind-up                  # cluster (skip if it exists)
just kind-image               # build + kind load iron-feather:kind
just kind-install             # helm upgrade --install (image.tag=kind)
just kind-seed                # Berlin snapshot Job (mirrors the writer)
just kind-bench               # fixed-pass http_bench via port-forward
```

Findings (verified, not assumed):

- DuckDB's httpfs only issues closed single-range GETs, which is exactly
  Cachey's `/fetch` contract — but Cachey rejects bare `HEAD` (400) and
  open-ended ranges (416), so the reader `wait-for-publish` probe sends
  `Range: bytes=0-0`.
- Cachey (AWS SDK) needs no S3-side addressing help: every DuckDB
  connection creates a scoped HTTP secret sending `C0-Config: fps=true`,
  so Cachey fetches path-style against the endpoint host itself — the
  bundled MinIO Service short name, resolved by plain cluster DNS
  (verified: `NoSuchBucket`/`NoSuchKey` surface correctly, virtual-host
  quirks never arise). Real S3 needs nothing either. `rclone serve s3`
  was rejected as the stand-in precisely because it only serves
  path-style to SDKs that cannot be forced there — DuckDB's secret
  mechanism is what makes path-style reliable here.
- Fresh containers have an empty extension dir, so `setup_session`
  INSTALLs every extension on demand (and the image bakes them) instead of
  assuming a warm cache.
- Cachey caches immutable pages keyed by (kind, object, page) with no
  revalidation: safe because snapshots are immutable and data files are
  never rewritten — keep that invariant (same as today's "never delete
  referenced files").

## Benchmark: Berlin 20k, mixed 500 unique (slow S3, 300 ms miss RTT)

Single local server, MinIO backend through the latency proxy, 8 conns,
one pass over the workload per row:

| path | rps | p50 | p99 |
|---|---|---|---|
| uncached (direct range GETs, every miss pays) | 9 | 802 ms | 1670 ms |
| Cachey cold (fresh server + empty Cachey, first pass) | 128 | 46 ms | 364 ms |
| Cachey warm (immediate repeat pass) | 145 | 46 ms | 124 ms |

0 rejected, 0 errors throughout. Notes: the working set (~4 MB) fills
Cachey within the first request wave, so cold-vs-warm differs only in
p99 (single-miss cost) while p50 sits on the DuckDB compute floor;
uncached pays the miss on nearly every request. Wipe Cachey (restart the
container; memory-only in this rig) plus a fresh server for a true cold.

On loopback, fetch cost nearly vanishes and DuckDB compute dominates:
cold Cachey runs ~134 rps / p50 47 ms, warm Cachey ~140-150 rps — the
20k-row scan/sort/render floor, identical on host-local disk (~47 ms
warmed single-request). The old 2,000-4,500 rps figures measured the
removed per-pod app cache on repeated URLs, not storage: with it gone,
per-unique-request work is unchanged by design (it never hit the app
cache), and zone-shared Cachey is what absorbs repeats across pods.

## Production mapping

- One Cachey Deployment + Service per AZ over the same S3 bucket; each
  zone's readers address their zone's Service, so the page cache is shared
  zone-wide with no CSI, FUSE, or sidecars.
- Only Cachey and the writer talk to S3 (writer uploads; Cachey fetches).
  Readers need no cloud credentials at all.
- Publish = build with `--data-url` at the Cachey prefix, upload immutable
  data + catalog, then advance refs through the API. Hourly-or-slower
  cadence means no distributed locking and no
  Nessie/Postgres/registry.duckdb: refs are tiny objects (here) or API
  rows (prod).
- Branching is zero-copy: another ref pointing at an existing catalog.
