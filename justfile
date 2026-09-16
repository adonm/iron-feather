# Rust via rustup, just via mise. DuckDB always links a prebuilt library.
DUCKDB_DIR := justfile_directory() + "/.deps/duckdb"
export DUCKDB_LIB_DIR := DUCKDB_DIR
export LD_LIBRARY_PATH := DUCKDB_DIR
export DYLD_LIBRARY_PATH := DUCKDB_DIR

default: check

setup-duckdb:
    #!/usr/bin/env bash
    set -euo pipefail
    VER=$(python3 scripts/duckdb_version.py)
    case "$(uname -m)" in
      x86_64) ARCH=amd64 ;;
      aarch64|arm64) ARCH=arm64 ;;
      *) echo "unsupported architecture" >&2; exit 1 ;;
    esac
    case "$(uname -s)" in
      Linux) ASSET="libduckdb-linux-$ARCH.zip" ;;
      Darwin) ASSET="libduckdb-osx-universal.zip" ;;
      *) echo "unsupported OS" >&2; exit 1 ;;
    esac
    mkdir -p "{{DUCKDB_DIR}}"
    if [ "$(cat '{{DUCKDB_DIR}}/version' 2>/dev/null || true)" = "$VER" ]; then exit 0; fi
    ZIP=$(mktemp "{{DUCKDB_DIR}}/.download.XXXXXX")
    trap 'rm -f "$ZIP"' EXIT
    curl -sSLf "https://github.com/duckdb/duckdb/releases/download/v$VER/$ASSET" -o "$ZIP"
    python3 -c "import sys,zipfile; zipfile.ZipFile(sys.argv[1]).extractall(sys.argv[2])" "$ZIP" "{{DUCKDB_DIR}}"
    echo "$VER" > "{{DUCKDB_DIR}}/version"

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
# Takes on the order of ten minutes on a fast link.
fixture-nw-europe out="fixtures/nw-europe.duckdb" *args: setup-duckdb
    cargo run --locked -- build --bbox=2,48,6,54 --out {{quote(out)}} {{args}}

# Same rows, Hilbert-clustered heap, for the layout experiment.
fixture-nw-europe-hilbert out="fixtures/nw-europe-hilbert.duckdb" *args: setup-duckdb
    cargo run --locked -- build --bbox=2,48,6,54 --hilbert --out {{quote(out)}} {{args}}

# Row counts, collections and indexes for a built shard.
# Needs the DuckDB CLI: uncomment `duckdb` under [tools] in mise.toml, then `mise install`.
fixture-verify shard="fixtures/nw-europe.duckdb":
    #!/usr/bin/env bash
    set -euo pipefail
    command -v duckdb >/dev/null || { echo "duckdb CLI not found (see recipe comment)" >&2; exit 1; }
    duckdb -readonly {{quote(shard)}} "SELECT count(*) AS features FROM features; SELECT * FROM collections; SELECT index_name, table_name FROM duckdb_indexes();"

# Saved deterministic workloads for a completed shard's region.
# Regenerate for another extent: just workloads region="13.35,52.48,13.45,52.55" dir="workloads/osm"
workloads region="2,48,6,54" dir="workloads/nw-europe":
    python3 scripts/make_workload.py --region {{quote(region)}} --out-dir {{quote(dir)}}

# DuckLake copy of an existing shard: catalog plus tuned Parquet layout.
# DATA_URL must match the prefix the files will be served from (see bench-layouts).
# Extra args append positionally: WEST SOUTH EAST NORTH FILE_MB ROW_GROUP SORT
# (SORT = grid|hilbert|none). Env FILE_MB/ROW_GROUP/SORT also work.
fixture-ducklake src="fixtures/nw-europe.duckdb" catalog="fixtures/nw-europe-lake.ducklake" data_url="http://127.0.0.1:19000/dl/" datadir="fixtures/dl-build" *args: setup-duckdb
    bash scripts/build_ducklake.sh {{quote(src)}} {{quote(catalog)}} {{quote(data_url)}} {{quote(datadir)}} {{args}}

# Row counts and content fingerprints must match across layouts.
fixture-verify-layouts src="fixtures/nw-europe.duckdb" catalog="fixtures/nw-europe-lake.ducklake" datadir="fixtures/dl-build": setup-duckdb
    #!/usr/bin/env bash
    set -euo pipefail
    fingerprint() {
      duckdb :memory: "LOAD spatial; LOAD ducklake; $1 SELECT count(*) AS n, min(id) AS first, max(id) AS last, sum(octet_length(ST_AsWKB(geom))) AS geom_bytes, sum(length(properties::VARCHAR)) AS props_chars FROM features;"
    }
    native=$(fingerprint "ATTACH '{{src}}' AS s (READ_ONLY); USE s;")
    lake=$(fingerprint "ATTACH 'ducklake:{{catalog}}' AS s (DATA_PATH '{{datadir}}/', OVERRIDE_DATA_PATH true); USE s;")
    echo "--- native ---"; echo "$native"
    echo "--- lake ---"; echo "$lake"
    test "$native" = "$lake" && echo LAYOUTS_MATCH || { echo LAYOUTS_DIFFER; exit 1; }

# Urban A/B across two running servers (native vs lake); prints /metrics deltas.
# Fixed passes cover identical request sets on both backends; duration mode
# is kept for quick smoke checks only.
bench-layouts base_a="http://127.0.0.1:3000" base_b="http://127.0.0.1:3001" dir="workloads/nw-europe" *args:
    #!/usr/bin/env bash
    set -euo pipefail
    for base in "{{base_a}}" "{{base_b}}"; do
      echo "=== $base before ==="; curl -s "$base/metrics"
      echo "=== $base urban (2 fixed passes) ==="
      cargo run --locked --release --example http_bench -- --base "$base" --concurrency 8 --passes 2 --warmup-secs 0 --workload "{{quote(dir)}}/urban.txt" {{args}}
      echo "=== $base after ==="; curl -s "$base/metrics"
    done

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

run shard="fixtures/osm.duckdb" *args: setup-duckdb
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
