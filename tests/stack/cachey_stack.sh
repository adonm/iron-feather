#!/usr/bin/env bash
# Real-stack test: a synthetic 262,144-feature catalog served from MinIO
# through Cachey by real server processes. No mocks, no timing asserts:
# every check is exact (counts, byte equality, status codes, Cachey
# download counters). Needs: docker, rclone, curl, python3, cargo and
# .deps/duckdb — run via `just test-stack`.
set -euo pipefail

# Let rust-toolchain.toml choose the compiler: an ambient RUSTUP_TOOLCHAIN
# override would silently win over the pin.
unset RUSTUP_TOOLCHAIN

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="$(mktemp -d /tmp/opencode/stack-XXXXXX)"
MINIO_PORT=3901
CACHEY_PORT=8089
BUCKET=big
N=262144
GRID=512
BBOX="2,48,6,54"
# Pinned images (keep in sync with charts/cachey/values.yaml).
MINIO_IMAGE="quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e"
CACHEY_IMAGE="ghcr.io/s2-streamstore/cachey@sha256:beab43f996c0d183d4c3adb68257d342ef87d700befe5b30c196f8a1524b96ca"
S3_USER=testkey
S3_PASS=testsecret12345678
SERVERS=""

export DUCKDB_LIB_DIR="${DUCKDB_LIB_DIR:-$ROOT/.deps/duckdb}"
export LD_LIBRARY_PATH="$DUCKDB_LIB_DIR"
export RCLONE_CONFIG_STACK_TYPE=s3 RCLONE_CONFIG_STACK_PROVIDER=Minio
export RCLONE_CONFIG_STACK_ENDPOINT="http://127.0.0.1:$MINIO_PORT"
export RCLONE_CONFIG_STACK_ACCESS_KEY_ID="$S3_USER"
export RCLONE_CONFIG_STACK_SECRET_ACCESS_KEY="$S3_PASS"
export RCLONE_CONFIG_STACK_REGION=us-east-1
export RCLONE_CONFIG_STACK_FORCE_PATH_STYLE=true

cleanup() {
  rc=$?
  for pid in $SERVERS; do kill "$pid" 2>/dev/null || true; done
  if [ $rc -ne 0 ]; then
    echo "--- serve logs on failure:"; tail -n 20 "$WORK"/serve-*.log 2>/dev/null || true
  fi
  docker rm -f stack-minio stack-cachey >/dev/null 2>&1 || true
  rm -rf "$WORK"
  exit "$rc"
}
trap cleanup EXIT

step() { echo "=== $*"; }
wait_for() {
  for _ in $(seq 1 60); do
    curl -sf "$1" >/dev/null 2>&1 && return 0
    sleep 2
  done
  echo "timeout waiting for $1" >&2
  return 1
}

step "start MinIO + Cachey (isolated ports $MINIO_PORT/$CACHEY_PORT)"
mkdir -p "$WORK/minio"
docker run -d --name stack-minio -p "127.0.0.1:$MINIO_PORT:9000" \
  --user "$(id -u):$(id -g)" \
  -v "$WORK/minio:/data" -e MINIO_ROOT_USER="$S3_USER" -e MINIO_ROOT_PASSWORD="$S3_PASS" \
  "$MINIO_IMAGE" server /data >/dev/null
wait_for "http://127.0.0.1:$MINIO_PORT/minio/health/live"
rclone mkdir "stack:$BUCKET" 2>/dev/null || true
docker run -d --name stack-cachey --network host \
  -e AWS_ACCESS_KEY_ID="$S3_USER" -e AWS_SECRET_ACCESS_KEY="$S3_PASS" \
  -e AWS_REGION=us-east-1 -e AWS_ENDPOINT_URL_S3="http://127.0.0.1:$MINIO_PORT" \
  "$CACHEY_IMAGE" --memory 256MiB --port "$CACHEY_PORT" >/dev/null
wait_for "http://127.0.0.1:$CACHEY_PORT/stats"
CACHEY="http://127.0.0.1:$CACHEY_PORT"

step "generate deterministic $N-row source ($GRID x $GRID grid)"
"$ROOT/.deps/duckdb/duckdb" :memory: <<SQL
INSTALL spatial;
LOAD spatial;
COPY (
  SELECT 'way' AS type, i AS id,
    ST_AsWKB(ST_Point(2 + (i % $GRID) * (4.0 / $((GRID - 1))), 48 + (i // $GRID) * (6.0 / $((GRID - 1))))) AS geometry,
    {'xmin': 2 + (i % $GRID) * (4.0 / $((GRID - 1))), 'ymin': 48 + (i // $GRID) * (6.0 / $((GRID - 1))),
     'xmax': 2 + (i % $GRID) * (4.0 / $((GRID - 1))), 'ymax': 48 + (i // $GRID) * (6.0 / $((GRID - 1)))} AS bbox,
    'pt_' || i::VARCHAR AS name, i % 7 AS height
  FROM range($N) t(i)
) TO '$WORK/source.parquet' (FORMAT PARQUET);
SQL
test -s "$WORK/source.parquet"

step "build v1 catalog with Cachey data URLs"
cargo run --locked --manifest-path "$ROOT/Cargo.toml" -- build \
  --from "$WORK/source.parquet" --collection buildings --bbox="$BBOX" \
  --out "$WORK/v1.ducklake" --data-dir "$WORK/files" \
  --data-url "$CACHEY/fetch/$BUCKET/data/" --file-mb 8 --source-id 1
rclone sync "$WORK/files" "stack:$BUCKET/data/"
rclone copyto "$WORK/v1.ducklake" "stack:$BUCKET/catalogs/v1.ducklake"
printf '%s' v1.ducklake | rclone rcat "stack:$BUCKET/refs/latest"
python3 - "$WORK" <<'EOF'
import sys
work = sys.argv[1]
files = __import__("subprocess").check_output(
    ["rclone", "lsf", "-R", "stack:big/data/"], text=True).split()
assert len(files) > 1, f"expected multiple data files, got {files}"
print("data files:", len(files))
EOF

step "serve reader A"
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" -- serve \
  --shard "$CACHEY/fetch/$BUCKET/catalogs/v1.ducklake" \
  --listen 127.0.0.1:3211 --flight-listen 127.0.0.1:5211 --no-quack \
  >"$WORK/serve-a.log" 2>&1 &
SERVERS="$SERVERS $!"
wait_for "http://127.0.0.1:3211/healthz"

step "reader A: discovery, full walks, bbox ground truth, tiles"
python3 - http://127.0.0.1:3211 "$WORK" <<'EOF'
import json, sys, urllib.request
from fractions import Fraction
base, work = sys.argv[1], sys.argv[2]

def get(path, base=base):
    with urllib.request.urlopen(base + path, timeout=120) as r:
        assert r.status == 200, (path, r.status)
        return r.status, r.headers, json.load(r)

def walk(first):
    ids, url = [], first
    while url:
        _, _, page = get(url)
        ids += [f["id"] for f in page["features"]]
        nxt = [l["href"] for l in page["links"] if l["rel"] == "next"]
        url = nxt[0] if nxt else None
    return ids

_, _, cols = get("/collections")
assert [c["id"] for c in cols["collections"]] == ["buildings"], cols

cursor = walk("/collections/buildings/items?sources=1&limit=1000")
assert len(cursor) == 262144, len(cursor)
assert len(set(cursor)) == 262144, "duplicate ids in cursor walk"
with open(f"{work}/walk-a.txt", "w") as f:
    f.write("\n".join(cursor))

offset, start, seen = [], 0, None
while True:
    _, _, page = get(f"/collections/buildings/items?sources=1&limit=1000&offset={start}")
    batch = [f["id"] for f in page["features"]]
    if not batch:
        break
    offset += batch
    start += 1000
assert offset == cursor, "offset walk disagrees with cursor walk"

# Exact ground truth from grid math (box edges avoid gridlines).
xs = [k for k in range(512) if Fraction(5, 2) <= 2 + Fraction(4 * k, 511) <= Fraction(7, 2)]
ys = [m for m in range(512) if Fraction(50, 1) <= 48 + Fraction(6 * m, 511) <= Fraction(52, 1)]
want = len(xs) * len(ys)
assert want > 10000, want
boxed = walk("/collections/buildings/items?sources=1&limit=1000&bbox=2.5,50,3.5,52")
assert len(boxed) == want, (len(boxed), want)

import math
def tile(lon, lat, z):
    n = 2 ** z
    x = int((lon + 180) / 360 * n)
    lr = math.radians(lat)
    y = int((1 - math.log(math.tan(lr) + 1 / math.cos(lr)) / math.pi) / 2 * n)
    return z, x, y
z, x, y = tile(3.0, 51.0, 8)
with urllib.request.urlopen(
    f"{base}/collections/buildings/tiles/{z}/{x}/{y}?sources=1", timeout=120
) as r:
    assert r.status == 200, r.status
    assert r.headers.get_content_type() == "application/vnd.mapbox-vector-tile"
    assert int(r.headers.get("Content-Length", 0)) > 0
z, x, y = tile(-100.0, 40.0, 8)
with urllib.request.urlopen(
    f"{base}/collections/buildings/tiles/{z}/{x}/{y}?sources=1", timeout=120
) as r:
    assert r.status == 204, r.status
    assert r.read() == b""
print("reader A checks passed")
EOF

step "reader A: Flight agrees (1000 rows)"
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" --example flight_client -- \
  --addr http://127.0.0.1:5211 | tee "$WORK/flight.out"
grep -q "streamed 1000 rows" "$WORK/flight.out"

step "serve reader B; sharing means a repeat walk adds zero S3 downloads"
python3 - "http://127.0.0.1:$CACHEY_PORT" "$WORK" <<'EOF'
import re, sys, urllib.request
txt = urllib.request.urlopen(sys.argv[1] + "/metrics", timeout=30).read().decode()
def total(kind):
    return sum(
        float(m.group(2))
        for line in txt.splitlines()
        if (m := re.match(r'^[^{\s]+\{([^}]*)\}\s+([0-9.eE+-]+)', line))
        and f'type="{kind}"' in m.group(1)
    )
assert total("download") > 0, "no downloads observed; metrics shape changed?"
with open(sys.argv[2] + "/cachey-before.txt", "w") as f:
    f.write(f"{total('download')}\n")
print("downloads so far:", total("download"))
print("--- full metrics for the record:")
for line in txt.splitlines():
    if "cachey_page_request_total" in line or "cachey_fetch_request_total" in line:
        print("   ", line)
EOF
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" -- serve \
  --shard "$CACHEY/fetch/$BUCKET/catalogs/v1.ducklake" \
  --listen 127.0.0.1:3212 --flight-listen 127.0.0.1:5212 --no-quack \
  >"$WORK/serve-b.log" 2>&1 &
SERVERS="$SERVERS $!"
wait_for "http://127.0.0.1:3212/healthz"
python3 - http://127.0.0.1:3212 "$WORK" "http://127.0.0.1:$CACHEY_PORT" <<'EOF'
import json
import re
import sys
import urllib.error
import urllib.request
base, work, cachey = sys.argv[1], sys.argv[2], sys.argv[3]
def walk(first):
    ids, url = [], first
    while url:
        with urllib.request.urlopen(base + url, timeout=120) as r:
            page = json.load(r)
        ids += [f["id"] for f in page["features"]]
        nxt = [l["href"] for l in page["links"] if l["rel"] == "next"]
        url = nxt[0] if nxt else None
    return ids
ids = walk("/collections/buildings/items?sources=1&limit=1000")
assert len(ids) == 262144, len(ids)
assert open(f"{work}/walk-a.txt").read().split("\n") == ids, "reader B disagrees with A"
def snapshot(tag):
    txt = urllib.request.urlopen(cachey + "/metrics", timeout=30).read().decode()
    def total(kind):
        return sum(
            float(m.group(2))
            for line in txt.splitlines()
            if (m := re.match(r'^[^{\s]+\{([^}]*)\}\s+([0-9.eE+-]+)', line))
            and f'type="{kind}"' in m.group(1)
        )
    print(tag, "downloads:", total("download"), "hits:",
          total("cache_hit") + total("hit") + total("coalesced"))
    return total("download")
d_walk1 = snapshot("after B walk 1:")
# Steady state is deterministic: an identical second walk must add nothing.
ids2 = walk("/collections/buildings/items?sources=1&limit=1000")
assert ids2 == ids
d_walk2 = snapshot("after B walk 2:")
assert d_walk2 == d_walk1, (d_walk1, d_walk2)
before = float(open(f"{work}/cachey-before.txt").read())
print(f"reader B: first walk added {d_walk1 - before} downloads (cold-process reads,",
      "typically past-EOF prefetch probes), second walk added 0: sharing holds")
txt = urllib.request.urlopen(cachey + "/metrics", timeout=30).read().decode()
for line in txt.splitlines():
    if "cachey_page_request_total" in line or "cachey_fetch_request_total" in line:
        print("   ", line)
EOF

step "publish v2 (small box) and prove reader A is pinned"
cargo run --locked --manifest-path "$ROOT/Cargo.toml" -- build \
  --from "$WORK/source.parquet" --collection buildings --bbox="2.0,48.0,2.1,48.1" \
  --out "$WORK/v2.ducklake" --data-dir "$WORK/files2" \
  --data-url "$CACHEY/fetch/$BUCKET/data/" --file-mb 8 --source-id 1
rclone sync "$WORK/files2" "stack:$BUCKET/data/"
rclone copyto "$WORK/v2.ducklake" "stack:$BUCKET/catalogs/v2.ducklake"
printf '%s' v2.ducklake | rclone rcat "stack:$BUCKET/refs/latest"
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" -- serve \
  --shard "$CACHEY/fetch/$BUCKET/catalogs/v2.ducklake" \
  --listen 127.0.0.1:3213 --flight-listen 127.0.0.1:5213 --no-quack \
  >"$WORK/serve-c.log" 2>&1 &
SERVERS="$SERVERS $!"
wait_for "http://127.0.0.1:3213/healthz"
python3 - <<'EOF'
import json
import sys
import urllib.error
import urllib.request
from fractions import Fraction
def status(base, path):
    try:
        urllib.request.urlopen(base + path, timeout=60)
        return 200
    except urllib.error.HTTPError as e:
        return e.code
probe = "/collections/buildings/items/way:200000?sources=1"
assert status("http://127.0.0.1:3211", probe) == 200, "reader A moved off v1"
assert status("http://127.0.0.1:3213", probe) == 404, "reader C sees v1 rows"
assert status("http://127.0.0.1:3213", "/collections/buildings/items/way:0?sources=1") == 200
# v2 holds exactly the small-box grid points.
ks = [k for k in range(512) if Fraction(2, 1) <= 2 + Fraction(4 * k, 511) <= Fraction(21, 10)]
ms = [m for m in range(512) if Fraction(48, 1) <= 48 + Fraction(6 * m, 511) <= Fraction(481, 10)]
want = len(ks) * len(ms)
assert 0 < want < 262144, want
ids, url = [], "/collections/buildings/items?sources=1&limit=1000"
while url:
    with urllib.request.urlopen("http://127.0.0.1:3213" + url, timeout=120) as r:
        page = json.load(r)
    ids += [f["id"] for f in page["features"]]
    nxt = [l["href"] for l in page["links"] if l["rel"] == "next"]
    url = nxt[0] if nxt else None
assert len(ids) == want, (len(ids), want)
print("pinning holds: A serves v1, C serves", want, "v2 rows")
EOF

echo "ALL STACK CHECKS PASSED"
