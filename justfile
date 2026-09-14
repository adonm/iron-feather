# iron-feather recipes. Toolchain via mise (`mise install`); Rust via rustup.
# DuckDB links PREBUILT libduckdb — never the `bundled` C++ build.

DUCKDB_DIR := justfile_directory() + "/.deps/duckdb"

default: check

# Fetch the prebuilt libduckdb matching Cargo.lock into .deps/duckdb.
# Honors DUCKDB_VERSION=x.y.z to override the lockfile-derived version.
setup-duckdb:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ -n "${DUCKDB_VERSION:-}" ]; then VER="$DUCKDB_VERSION"; else VER=$(python3 scripts/duckdb_version.py); fi
    case "$(uname -m)" in
      x86_64) ARCH=amd64 ;;
      aarch64|arm64) ARCH=arm64 ;;
      *) echo "unsupported arch: $(uname -m)" >&2; exit 1 ;;
    esac
    case "$(uname -s)" in
      Linux) ASSET="libduckdb-linux-$ARCH.zip" ;;
      Darwin) ASSET="libduckdb-osx-universal.zip" ;;
      *) echo "unsupported os" >&2; exit 1 ;;
    esac
    mkdir -p "{{DUCKDB_DIR}}"
    if [ -f "{{DUCKDB_DIR}}/libduckdb.so" ] || [ -f "{{DUCKDB_DIR}}/libduckdb.dylib" ]; then
      echo "prebuilt libduckdb already present in {{DUCKDB_DIR}}"
      exit 0
    fi
    URL="https://github.com/duckdb/duckdb/releases/download/v$VER/$ASSET"
    echo "fetching $URL"
    curl -sSLf "$URL" -o /tmp/iron-feather-libduckdb.zip
    python3 -c "import zipfile; zipfile.ZipFile('/tmp/iron-feather-libduckdb.zip').extractall('{{DUCKDB_DIR}}')"
    ls "{{DUCKDB_DIR}}"

# Slice OSM buildings (Overture, plain HTTPS) into fixtures/ for local dev:
# fixtures/osm-buildings.parquet (Flight over file://) + fixtures/demo.duckdb (Poem --shard-dir).
fixture-osm: setup-duckdb
    DUCKDB_LIB_DIR="{{DUCKDB_DIR}}" LD_LIBRARY_PATH="{{DUCKDB_DIR}}" cargo run --locked --features serve --bin iron-feather-fixture

# Fast path: API + stub store only, no heavy backends.
check:
    cargo check --locked

check-serve: setup-duckdb
    DUCKDB_LIB_DIR="{{DUCKDB_DIR}}" cargo check --locked --features serve
    DUCKDB_LIB_DIR="{{DUCKDB_DIR}}" cargo check --locked --features serve --examples

test:
    cargo test --locked

test-serve: setup-duckdb
    DUCKDB_LIB_DIR="{{DUCKDB_DIR}}" LD_LIBRARY_PATH="{{DUCKDB_DIR}}" cargo test --locked --features serve

fmt:
    cargo fmt --all

fmt-check:
    cargo fmt --all --check

run:
    cargo run --locked

run-serve: setup-duckdb
    DUCKDB_LIB_DIR="{{DUCKDB_DIR}}" LD_LIBRARY_PATH="{{DUCKDB_DIR}}" cargo run --locked --features serve -- --help

clean:
    cargo clean
    rm -rf .deps

# Hammer a running Flight server. Start the server first (see README),
# then e.g. ADDR=http://127.0.0.1:50051 CONC=64 REQ=200 just bench-flight.
# Extra args pass through: just bench-flight -- --jitter --limit 5000
bench-flight *args: setup-duckdb
    DUCKDB_LIB_DIR="{{DUCKDB_DIR}}" LD_LIBRARY_PATH="{{DUCKDB_DIR}}" cargo run --locked --features serve --example flight_bench -- --addr "${ADDR:-http://127.0.0.1:50051}" --concurrency "${CONC:-32}" --requests "${REQ:-100}" {{args}}

# Hammer the running Poem OGC HTTP endpoint (2k rps target). Server first.
bench-ogc:
    python3 scripts/ogc_bench.py --base "${BASE:-http://127.0.0.1:3000}" --concurrency "${CONC:-64}" --requests "${REQ:-100}"
