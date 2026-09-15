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

# One materialized Layercake shard; additional build options pass through.
fixture-osm *args: setup-duckdb
    cargo run --locked -- build --bbox=13.35,52.48,13.45,52.55 {{args}}

run shard="fixtures/osm.duckdb" *args: setup-duckdb
    cargo run --locked --release -- serve --shard {{quote(shard)}} {{args}}

bench-flight *args: setup-duckdb
    cargo run --locked --release --example flight_bench -- --addr "${ADDR:-http://127.0.0.1:50051}" --concurrency "${CONC:-32}" --requests "${REQ:-100}" {{args}}

bench-ogc *args:
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency "${CONC:-32}" --requests "${REQ:-100}" {{args}}
