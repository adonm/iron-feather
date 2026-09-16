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

# ~9 GB / 25M-row Benelux + northern France buildings for capacity work.
# Takes on the order of ten minutes on a fast link. Builds a DuckLake
# catalog plus Parquet data files directly; --data-url must match the prefix
# the files will be served from (local data dir by default). Layout tuning
# via --sort grid|hilbert|none --file-mb N --row-group N.
fixture-nw-europe out="fixtures/nw-europe.ducklake" datadir="fixtures/nw-europe.files" *args: setup-duckdb
    cargo run --locked -- build --bbox=2,48,6,54 --out {{quote(out)}} --data-dir {{quote(datadir)}} {{args}}

# Row counts and collections for a built shard (needs setup-duckdb once).
fixture-verify shard="fixtures/nw-europe.ducklake":
    "{{justfile_directory()}}/{{DUCKDB_DIR}}/duckdb" :memory: "LOAD ducklake; ATTACH 'ducklake:{{shard}}' AS s; USE s; SELECT count(*) AS features FROM features; SELECT * FROM collections;"

# Saved deterministic workloads for a completed shard's region.
# Regenerate for another extent: just workloads region="13.35,52.48,13.45,52.55" dir="workloads/osm"
workloads region="2,48,6,54" dir="workloads/nw-europe":
    python3 scripts/make_workload.py --region {{quote(region)}} --out-dir {{quote(dir)}}

# Reproducible benchmark contract: fresh process, warm engine, warm cache.
# Fresh: new server with --cache-mb 0. Warm engine: complete workload once
# with --cache-mb 0, then measure. Warm cache: --cache-mb 256, pre-populate
# with --passes, then measure. Alternate backend order between repeats and
# record S3 GETs (scripts/s3_stats.py), rows, latency and peak RSS.
bench-matrix base="http://127.0.0.1:3000" dir="workloads/nw-europe" passes="2":
    #!/usr/bin/env bash
    set -euo pipefail
    base="{{base}}"
    wdir={{quote(dir)}}
    for file in hot urban rural scatter empty; do
      echo "=== $file ({{passes}} passes) ==="
      cargo run --locked --release --example http_bench -- --base "$base" --concurrency 8 --passes {{passes}} --warmup-secs 2 --workload "$wdir/$file.txt"
    done
    echo "=== broad/deep (cache-mb 0, 1 pass) ==="
    cargo run --locked --release --example http_bench -- --base "$base" --concurrency 4 --passes 1 --warmup-secs 0 --workload "$wdir/broad.txt"
    cargo run --locked --release --example http_bench -- --base "$base" --concurrency 4 --passes 1 --warmup-secs 0 --workload "$wdir/deep.txt"

# --- ZeroFS lake rig -------------------------------------------------------
# Local rehearsal of the AZ-local cache design (see docs/zerofs-lake.md):
# Garage (S3) + Redis fencing + ZeroFS 9P + FUSE mount at /tmp/opencode.
# All block/metadata caching lives in ZeroFS; serve reads plain local paths.

# Install the pinned zerofs binary (no package manager).
zerofs-setup:
    python3 scripts/setup_zerofs.py

# Start Garage + Redis + ZeroFS server + FUSE mount (idempotent).
zerofs-up: zerofs-setup
    bash scripts/zerofs_up.sh

# Stop the mount and server. Pass --wipe to drop containers and cached state.
zerofs-down *args:
    bash scripts/zerofs_down.sh {{args}}

# Create the /lake layout (refs, catalogs, data) on the mount.
lake-init: zerofs-up
    #!/usr/bin/env bash
    set -euo pipefail
    MNT=/tmp/opencode/lake-mnt
    mkdir -p "$MNT/refs/tags" "$MNT/catalogs" "$MNT/data/parquet"
    ls "$MNT"

# Publish a new immutable snapshot, then move a ref at it.
# This plays the catalog API locally; in prod only the API mutates refs.
# Example: just lake-publish sha_003 --bbox=13.38,52.50,13.42,52.54 --limit 20000
lake-publish sha *args: zerofs-up
    #!/usr/bin/env bash
    set -euo pipefail
    MNT=/tmp/opencode/lake-mnt
    cargo run --locked -- build --out "$MNT/catalogs/{{quote(sha)}}.ducklake" --data-dir "$MNT/data/parquet" {{args}}
    bash scripts/lake_ref.sh set "{{quote(sha)}}.ducklake" latest
    bash scripts/lake_ref.sh list

# Serve the catalog a ref points at (default: latest), resolved once at
# startup; the reader stays pinned to that snapshot across later publishes.
lake-serve ref="latest" *args: zerofs-up
    #!/usr/bin/env bash
    set -euo pipefail
    MNT=/tmp/opencode/lake-mnt
    catalog=$(bash scripts/lake_ref.sh get {{quote(ref)}})
    cargo run --locked --release -- serve --shard "$MNT/catalogs/$catalog" {{args}}

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
# Small files (broad/deep/empty) fit the cache after warmup: serve with
# --cache-mb 0 to measure first-render and traversal cost instead.
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
