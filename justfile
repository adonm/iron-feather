# Rust via rustup, just via mise. DuckDB always links a prebuilt library.
DUCKDB_DIR := justfile_directory() + "/.deps/duckdb"
export DUCKDB_LIB_DIR := DUCKDB_DIR
export LD_LIBRARY_PATH := DUCKDB_DIR
export DYLD_LIBRARY_PATH := DUCKDB_DIR

default: check

setup-duckdb:
    python3 scripts/setup_duckdb.py

check: setup-duckdb
    cargo clippy --locked --all-targets -- -D warnings

test: setup-duckdb
    cargo test --locked --all-targets

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

# --- Fixtures: reproducible Layercake shards (git-ignored, see .gitignore) ---

# Tiny Berlin slice for fast iteration (add --limit 20000).
fixture-osm *args: setup-duckdb
    cargo run --locked -- build --bbox=13.35,52.48,13.45,52.55 {{args}}

# ~3 GB / 25M-row Benelux + northern France buildings for capacity work.
# Takes on the order of ten minutes on a fast link. Builds a DuckLake
# catalog plus Parquet data files directly; --data-url sets the stored
# zone-independent DATA_PATH (an s3:// prefix for lake publishes, local
# data dir by default). Layout tuning via --sort grid|hilbert|none
# --file-mb N --row-group N.
fixture-nw-europe out="fixtures/nw-europe.ducklake" datadir="fixtures/nw-europe.files" *args: setup-duckdb
    cargo run --locked -- build --bbox=2,48,6,54 --out {{quote(out)}} --data-dir {{quote(datadir)}} {{args}}

# Row counts and collections for a built shard (needs setup-duckdb once).
fixture-verify shard="fixtures/nw-europe.ducklake":
    "{{justfile_directory()}}/{{DUCKDB_DIR}}/duckdb" :memory: "LOAD ducklake; ATTACH 'ducklake:{{shard}}' AS s; USE s; SELECT count(*) AS features FROM features; SELECT * FROM collections;"

# Saved deterministic workloads for a completed shard's region.
# Regenerate for another extent: just workloads region="13.35,52.48,13.45,52.55" dir="workloads/osm"
workloads region="2,48,6,54" dir="workloads/nw-europe":
    python3 scripts/make_workload.py --region {{quote(region)}} --out-dir {{quote(dir)}}

# Reproducible benchmark contract: fresh process, warm engine, warm Cachey.
# Fresh: new server. Warm engine: complete workload once, then measure.
# Warm storage: pre-populate Cachey with one pass, then measure. Alternate
# backend order between repeats and record S3 GETs (scripts/s3_stats.py),
# rows, latency and peak RSS.
bench-matrix base="http://127.0.0.1:3000" dir="workloads/nw-europe" passes="2":
    #!/usr/bin/env bash
    set -euo pipefail
    base="{{base}}"
    wdir={{quote(dir)}}
    for file in hot urban rural scatter empty; do
      echo "=== $file ({{passes}} passes) ==="
      cargo run --locked --release --example http_bench -- --base "$base" --concurrency 8 --passes {{passes}} --warmup-secs 2 --workload "$wdir/$file.txt"
    done
    echo "=== broad/deep (1 pass, cold storage) ==="
    cargo run --locked --release --example http_bench -- --base "$base" --concurrency 4 --passes 1 --warmup-secs 0 --workload "$wdir/broad.txt"
    cargo run --locked --release --example http_bench -- --base "$base" --concurrency 4 --passes 1 --warmup-secs 0 --workload "$wdir/deep.txt"

# --- Cachey lake rig -------------------------------------------------------
# Local rehearsal of the AZ-local cache design (see docs/cachey-lake.md):
# MinIO (S3) + Cachey (per-zone page-cache stand-in) on loopback.
# Readers fetch catalog/Parquet byte ranges over HTTP; nothing is mounted.

# Start MinIO + Cachey (idempotent).
cachey-up:
    bash scripts/cachey_up.sh

# Stop Cachey. Pass --wipe to drop containers and cached state.
cachey-down *args:
    bash scripts/cachey_down.sh {{args}}

# Publish a new immutable snapshot, then move a ref at it.
# Build stores a zone-independent DATA_PATH (s3://); readers override it
# per zone via --data-base (see lake-serve). Uploads are additive only:
# data files are never deleted (older catalogs still reference them) and
# catalog keys are never overwritten. Catalog versioning (ref service) is
# TBD; locally scripts/lake_ref.sh plays that role over tiny S3 objects.
# Example: just lake-publish sha_003 --bbox=13.38,52.50,13.42,52.54 --limit 20000
lake-publish sha *args: cachey-up
    #!/usr/bin/env bash
    set -euo pipefail
    source "{{justfile_directory()}}/scripts/dev-s3.env"
    export RCLONE_CONFIG_LAKE_TYPE=s3 RCLONE_CONFIG_LAKE_PROVIDER=Other
    export RCLONE_CONFIG_LAKE_ENDPOINT=http://127.0.0.1:3900
    export RCLONE_CONFIG_LAKE_ACCESS_KEY_ID="$S3_USER"
    export RCLONE_CONFIG_LAKE_SECRET_ACCESS_KEY="$S3_PASS"
    export RCLONE_CONFIG_LAKE_REGION="$S3_REGION" RCLONE_CONFIG_LAKE_FORCE_PATH_STYLE=true
    STAGE=$(mktemp -d /tmp/opencode/publish-XXXXXX)
    trap 'rm -rf "$STAGE"' EXIT
    sha={{quote(sha)}}
    name="$sha.ducklake"
    if rclone lsf "lake:lake/catalogs/" 2>/dev/null | grep -qx "$name"; then
      echo "catalog key $name already published; refusing to overwrite" >&2
      exit 1
    fi
    cargo run --locked -- build --out "$STAGE/$name" --content-address \
      --data-dir "$STAGE/files" --data-url "s3://lake/data/" {{args}}
    # Additive only: copy new files, never delete. Publication order is
    # data first, then the catalog that references them, then the ref move.
    # The serving index travels with the catalog so readers prune to
    # candidate files without touching catalog metadata per query.
    rclone copy "$STAGE/files" lake:lake/data/
    rclone copyto "$STAGE/$name" "lake:lake/catalogs/$name"
    rclone copyto "$STAGE/$name.serving.json" "lake:lake/catalogs/$name.serving.json"
    bash scripts/lake_ref.sh set "$name" latest
    bash scripts/lake_ref.sh list

# Serve the catalog a ref points at (default: latest), resolved once at
# startup; the reader stays pinned to that snapshot across later publishes.
# Args are positional: just lake-serve <tag> --listen ...
# Data reads go through the local Cachey via --data-base; the catalog's
# stored s3:// DATA_PATH is never fetched directly.
lake-serve ref="latest" *args: cachey-up
    #!/usr/bin/env bash
    set -euo pipefail
    catalog=$(bash scripts/lake_ref.sh get {{quote(ref)}})
    [ -n "$catalog" ] || { echo "empty ref {{quote(ref)}}" >&2; exit 1; }
    cargo run --locked --release -- serve --shard "http://127.0.0.1:8088/fetch/lake/catalogs/$catalog" --data-base "http://127.0.0.1:8088/fetch/lake/data/" {{args}}

# --- kind whole-stack ------------------------------------------------------
# 2-AZ kind cluster (nodes carry topology.kubernetes.io/zone; /nvme is the
# node-local NVMe stand-in). sidecars pull from the public registries.

# Create the 2-worker cluster (idempotent-ish; deletes nothing).
kind-up:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! kind get clusters | grep -qx lake; then
      mkdir -p /tmp/opencode/kind-nvme-a /tmp/opencode/kind-nvme-b
      kind create cluster --config k8s/kind-2az.yaml
    fi

kind-down:
    kind delete cluster --name lake

# Build the serve image and load it into kind.
kind-image:
    docker build -t iron-feather:kind .
    kind load docker-image iron-feather:kind --name lake

# Install/upgrade the whole stack (cachey cache layer + read pool).
# image.tag=kind matches `just kind-image`; writer stays off unless asked.
kind-install *args:
    helm dependency update charts/iron-feather >/dev/null
    helm upgrade --install lake charts/iron-feather --namespace lake --create-namespace --set image.tag=kind {{args}}

# The dev lake-s3 Secret ships in the cachey chart (backend.enabled);
# no bootstrap step exists. In prod, supply it out of band.

# Deterministic Berlin workload (seed 7; regenerates byte-identical).
workloads-berlin dir="workloads/berlin":
    python3 scripts/make_berlin_workload.py --out-dir {{quote(dir)}}

# Berlin benchmark against a port-forwarded read pool. Responses always
# execute against the pool; repeated storage reads hit zone-shared Cachey.
kind-bench base="http://127.0.0.1:3000" workload="workloads/berlin/mixed.txt": workloads-berlin
    cargo run --locked --release --example http_bench -- --base {{quote(base)}} --concurrency 8 --passes 1 --warmup-secs 0 --workload {{quote(workload)}}

kind-status:
    kubectl -n lake get pods,deployments,services 2>&1 | head -30

# Seed the lake with the Berlin snapshot (same shape as the writer).
kind-seed:
    kubectl apply -f k8s/seed-job.yaml
    kubectl -n lake wait --for=condition=complete --timeout=1200s job/lake-seed-berlin
    kubectl -n lake logs job/lake-seed-berlin | tail -2

# Real-stack test: MinIO → Cachey → servers over real HTTP range reads.
# Needs docker, rclone, curl, python3 and the prebuilt DuckDB in .deps
# (`just setup-duckdb` runs first).
test-stack: setup-duckdb
    bash tests/stack/cachey_stack.sh

# Smoke-check a running lake server (default: local :3000).
lake-verify base="http://127.0.0.1:3000":
    #!/usr/bin/env bash
    set -euo pipefail
    curl -sf "{{base}}/healthz"
    curl -sf "{{base}}/collections" | head -c 200; echo
    curl -sf "{{base}}/collections/buildings/items?sources=1&limit=1" | head -c 200; echo
    curl -s "{{base}}/metrics" | head -12

run shard="fixtures/osm.ducklake" *args: setup-duckdb
    cargo run --locked --release -- serve --shard {{quote(shard)}} {{args}}

# Full workload battery against a running release server (see `just workloads`).
# Small files (broad/deep/empty) fit in Cachey after warmup: wipe the Cachey
# disk to measure first-render and traversal cost instead.
bench-workloads dir="workloads/nw-europe" *args:
    #!/usr/bin/env bash
    set -euo pipefail
    base="${BASE:-http://127.0.0.1:3000}"
    wdir={{quote(dir)}}
    for file in hot urban rural scatter broad empty deep mixed; do
      echo "=== $file ==="
      cargo run --locked --release --example http_bench -- --base "$base" --concurrency 8 --duration 10 --warmup-secs 2 --workload "$wdir/$file.txt" {{args}}
    done

bench-flight *args: setup-duckdb
    cargo run --locked --release --example flight_bench -- --addr "${ADDR:-http://127.0.0.1:50051}" --concurrency "${CONC:-32}" --requests "${REQ:-100}" {{args}}

bench-http *args: setup-duckdb
    cargo run --locked --release --example http_bench -- --base "${BASE:-http://127.0.0.1:3000}" --concurrency "${CONC:-32}" --duration "${DUR:-15}" {{args}}

bench-ogc *args:
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency "${CONC:-32}" --requests "${REQ:-100}" {{args}}

# Fixed client-side matrix against a running release server: warmup, then
# cached pages, fresh-seed jitter (cache misses), and tiles.
bench-ogc-matrix:
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency 8 --requests 100 --warmup 20
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency 32 --requests 100 --warmup 20
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency 8 --requests 100 --warmup 20 --jitter --seed 1
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency 32 --requests 100 --warmup 20 --route tiles
