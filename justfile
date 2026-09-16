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

# DuckDB storage-cache A/B: fresh process per case, 1 cold + 1 warm urban
# pass over loopback rclone S3. Compares stock settings against the tuned
# defaults. See docs/nw-europe-10gib.md.
bench-duck-cache shard="http://127.0.0.1:19000/catalog/nw-europe-lake.ducklake":
    #!/usr/bin/env bash
    set -euo pipefail
    bench() {
      echo "=== $1 ==="
      cargo run --locked --release -- serve --shard {{quote(shard)}} --listen 127.0.0.1:3000 --flight-listen 127.0.0.1:50051 --connections 8 --cache-mb 0 ${@:2} &
      SRV=$!
      for i in $(seq 1 30); do curl -s -o /dev/null http://127.0.0.1:3000/healthz && break; sleep 1; done
      curl -s http://127.0.0.1:3000/metrics | grep duck_
      cargo run --locked --release --example http_bench -- --base http://127.0.0.1:3000 --concurrency 8 --passes 1 --warmup-secs 0 --workload workloads/nw-europe/urban.txt
      cargo run --locked --release --example http_bench -- --base http://127.0.0.1:3000 --concurrency 8 --passes 1 --warmup-secs 0 --workload workloads/nw-europe/urban.txt
      curl -s http://127.0.0.1:3000/metrics | grep -E "duck_external|http_requests"
      kill $SRV; wait $SRV 2>/dev/null || true
    }
    bench "stock (all caches off)" --disable-http-metadata-cache --disable-parquet-metadata-cache --enable-cache-validation --memory-mb 0
    bench "tuned defaults (4 GiB)" --memory-mb 4096

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
