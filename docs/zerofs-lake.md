# ZeroFS lake rig: local rehearsal of the AZ-local cache design

All Parquet/HTTP block and metadata caching lives in ZeroFS, not in DuckDB.
`serve` reads plain local paths off a FUSE mount; the mount is backed by an
S3 bucket through a ZeroFS server with its own NVMe cache. The app keeps no
storage-tuning flags by design (see `setup_session`).

## Layout on the mount (`/lake`)

```text
/lake
├── refs/
│   ├── latest            # file containing "sha_002.ducklake"
│   ├── main              # file containing "sha_002.ducklake"
│   └── tags/<name>       # one tiny file per tag
├── catalogs/
│   ├── sha_001.ducklake  # immutable once published
│   └── sha_002.ducklake
└── data/
    └── parquet/...       # immutable, shared across snapshots
```

Everything large is immutable; only tiny ref files are mutable. The catalog
API owns refs in production; locally `scripts/lake_ref.sh` plays that role
(`get`/`set`/`list`). Readers resolve a ref once at startup and attach
pinned:

```sql
ATTACH 'ducklake:/mnt/lake/catalogs/sha_002.ducklake' AS lake (READ_ONLY);
```

`Store` resolves `max(snapshot_id)` at startup and re-attaches with
`SNAPSHOT_VERSION`, so even a swapped catalog file cannot move a reader.

## Local rig (this repo)

```sh
just zerofs-up                  # Garage S3 + Redis + ZeroFS 9P + FUSE mount
just lake-init                  # refs/catalogs/data skeleton on the mount
just lake-publish sha_003 --bbox=... --limit ...   # build + refs/latest move
just lake-serve                 # serve refs/latest (add ref=<tag> for history)
just lake-verify
just zerofs-down                # unmount + stop (add --wipe for full reset)
```

Components (all local, all ephemeral under `/tmp/opencode`):

| Piece | What | Why |
|---|---|---|
| Garage (`dxflrs/garage:v2.1.0`) | S3-compatible bucket store on `:3900` | Small single binary; real buckets/keys. rclone was tried first but ignores `If-None-Match`, which ZeroFS requires for fencing |
| Redis (`redis:8-alpine`) | CAS coordinator (`conditional_put`) | Garage also ignores conditional writes, so ZeroFS enforces the compare-and-swap itself through Redis |
| ZeroFS 2.3.3 (pinned via `scripts/setup_zerofs.py`) | 9P server on `:5564`, 5 GB disk + 0.5 GB memory cache | The AZ-local cache stand-in; `file://` or real S3 swaps in without config-shape changes |
| FUSE mount | `/tmp/opencode/lake-mnt` | `zerofs mount` needs root here (unprivileged FUSE is blocked), so the rig mounts with `sudo --access all` |

Verified 2026-09-17: file written through the mount survives a ZeroFS
server restart (served back from Garage S3); a 20k-feature Berlin snapshot
builds and serves straight off the mount; after publishing a second snapshot
and moving `refs/latest`, the old reader still serves its 15 west-edge
features while the new reader returns 0 for the same query (pinned reads).

## kind whole-stack (`charts/`)

`just kind-up` (config in `k8s/kind-2az.yaml`) builds a 2-worker cluster
with real `topology.kubernetes.io/zone` labels and node-local `/nvme`
extraMounts. Two Helm charts manage the stack:

- `charts/zerofs` — Garage (S3 stand-in) + Redis (CAS fencing) + one
  ZeroFS gateway per zone (writer/leader + standby) with hostPath caches
  on `/nvme`, the upstream CSI driver, and one StorageClass per zone.
- `charts/iron-feather` — depends on `zerofs`; read-pool Deployment
  (spread across zones, ref resolved once per pod, RO mount), Service,
  shared RWX lake PVC, and the optional hourly writer CronJob
  (`writer.enabled=true`) — the single ETL publisher.

```sh
just kind-up                  # cluster (skip if it exists)
just kind-image               # build + kind load iron-feather:kind
just kind-install             # helm upgrade --install (image.tag=kind)
just kind-bootstrap-s3        # Garage layout + bucket + key -> lake-s3 Secret
just kind-seed                # Berlin snapshot Job (mirrors the writer)
just kind-bench               # fixed-pass http_bench via port-forward
```

Topology findings (verified, not assumed):

- Independent leaders over one bucket **fence each other** (victim logs
  `compactor ... Fenced`); standbys **refuse 9P** until failover. So the
  chart runs writer/leader + standby, and every zone's StorageClass points
  at the writer today — the per-zone names are the routing seam for a
  future read-serving replica (values-only switch).
- The vendored CSI manifests drop upstream's livenessprobe sidecar: in this
  environment it never serves `:9808`, so kubelet SIGTERMs the healthy
  plugin into a CrashLoop (node plugins now run 0 restarts). Revisit
  outside kind.
- DaemonSet `OnDelete` is kept from upstream: deleting node-plugin pods is
  the deliberate per-node upgrade path.
- Fresh containers have an empty extension dir, so `setup_session` now
  INSTALLs every extension on demand (and the image bakes them) instead of
  assuming a warm cache.

## Benchmark: kind vs host (Berlin 20k, 500-req mixed, `--cache-mb 0`)

| Setup | Pass 1 (engine cold) | Pass 2 (engine warm) |
|---|---|---|
| kind, CSI mount, 2 pods (port-forward fans out) | 105 rps, p50 63 ms, p99 163 ms | 88 rps, p50 72 ms, p99 182 ms |
| host, direct FUSE mount, 1 process | 126 rps, p50 51 ms, p99 139 ms | 98 rps, p50 66 ms, p99 141 ms |

Same order of magnitude (~15–20% kind overhead: extra 9P hop +
kubelet bind-mount + port-forward; snapshots differ but share
region/shape/row count). Gateway caches were warm both sides (seed/build
traffic); DuckDB buffers cold on pass 1. With the default 256 MiB app
cache the same workload serves ~6k rps from memory either way.

## Production mapping

- One ZeroFS server per AZ replaces this rig's single server; the CSI
  extension routes each pod's mount to its AZ-local instance.
- The single catalog writer uses the ZeroFS RW path; readers mount RO.
- Publish = write immutable catalog + data, then advance refs through the
  API. Hourly-or-slower cadence means no distributed locking and no
  Nessie/Postgres/registry.duckdb: refs are just tiny files (here) or API
  rows (prod).
- Branching is zero-copy: another ref file pointing at an existing catalog.
