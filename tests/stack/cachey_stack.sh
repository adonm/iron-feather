#!/usr/bin/env bash
# Real-stack test: a synthetic 262,144-feature catalog served from MinIO
# through Cachey by real server processes. No mocks, no timing asserts:
# every check is exact (counts, byte equality, status codes, Cachey
# download counters). Needs: docker, rclone, curl, python3, cargo and
# .deps/duckdb — run via `just test-stack`.
#
# Layout under test: the published catalog stores a zone-independent
# `s3://` DATA_PATH; every reader overrides it per zone via --data-base,
# so relative Parquet paths resolve through that zone's Cachey only.
set -euo pipefail

# Let rust-toolchain.toml choose the compiler: an ambient RUSTUP_TOOLCHAIN
# override would silently win over the pin.
unset RUSTUP_TOOLCHAIN

ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
WORK="$(mktemp -d /tmp/opencode/stack-XXXXXX)"
MINIO_PORT=3901
CACHEY_A_PORT=8089
CACHEY_B_PORT=8090
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
  docker rm -f stack-minio stack-cachey-a stack-cachey-b >/dev/null 2>&1 || true
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
# usage: cachey_total <base> <type>; sums cachey_page_request_total{type=...}
cachey_total() {
  curl -sf "$1/metrics" | python3 -c "
import re, sys
want = \"$2\"
total = 0.0
for line in sys.stdin:
    m = re.match(r'^[^{\s]+\{([^}]*)\}\s+([0-9.eE+-]+)', line)
    if m and f'type=\"{want}\"' in m.group(1):
        total += float(m.group(2))
print(int(total))
"
}
# Successful S3 page fetches (excludes failed probes such as catalog WAL or
# past-EOF 416/404s, which still count as download *attempts*): successes
# that were not cache hits.
cachey_successful_downloads() {
  local base="$1"
  local success hits
  success=$(cachey_total "$base" success)
  hits=$(cachey_total "$base" cache_hit)
  echo $((success - hits))
}

step "start MinIO + two Cacheys (zones A :$CACHEY_A_PORT, B :$CACHEY_B_PORT)"
mkdir -p "$WORK/minio"
docker run -d --name stack-minio -p "127.0.0.1:$MINIO_PORT:9000" \
  --user "$(id -u):$(id -g)" \
  -v "$WORK/minio:/data" -e MINIO_ROOT_USER="$S3_USER" -e MINIO_ROOT_PASSWORD="$S3_PASS" \
  "$MINIO_IMAGE" server /data >/dev/null
wait_for "http://127.0.0.1:$MINIO_PORT/minio/health/live"
rclone mkdir "stack:$BUCKET" 2>/dev/null || true
for zone in a b; do
  port_var="CACHEY_$(echo "$zone" | tr '[:lower:]' '[:upper:]')_PORT"
  port="${!port_var}"
  docker run -d --name "stack-cachey-$zone" --network host \
    -e AWS_ACCESS_KEY_ID="$S3_USER" -e AWS_SECRET_ACCESS_KEY="$S3_PASS" \
    -e AWS_REGION=us-east-1 -e AWS_ENDPOINT_URL_S3="http://127.0.0.1:$MINIO_PORT" \
    "$CACHEY_IMAGE" --memory 256MiB --port "$port" >/dev/null
  wait_for "http://127.0.0.1:$port/stats"
done
CACHEY_A="http://127.0.0.1:$CACHEY_A_PORT"
CACHEY_B="http://127.0.0.1:$CACHEY_B_PORT"

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

step "build v1 catalog with zone-independent s3:// DATA_PATH"
cargo run --locked --manifest-path "$ROOT/Cargo.toml" -- build \
  --from "$WORK/source.parquet" --collection buildings --bbox="$BBOX" \
  --out "$WORK/v1.ducklake" --data-dir "$WORK/files" \
  --data-url "s3://$BUCKET/data/" --file-mb 8 --source-id 1
# Additive publish only: data first, then the catalog, then the ref.
# Catalog keys are never overwritten; data files are never deleted.
if rclone lsf "stack:$BUCKET/catalogs/" 2>/dev/null | grep -qx v1.ducklake; then
  echo "catalog key v1.ducklake already published" >&2
  exit 1
fi
rclone copy "$WORK/files" "stack:$BUCKET/data/"
rclone copyto "$WORK/v1.ducklake" "stack:$BUCKET/catalogs/v1.ducklake"
printf '%s' v1.ducklake | rclone rcat "stack:$BUCKET/refs/latest"
python3 - <<'EOF'
import json, subprocess
out = subprocess.check_output(
    ["rclone", "size", "stack:big/data/", "--json"], text=True)
size = json.loads(out)
nfiles, nbytes = size["count"], size["bytes"]
print(f"data files: {nfiles}, bytes: {nbytes}")
assert nfiles > 1, "expected multiple data files"
assert nbytes > 1_000_000, f"expected >1MB of data, got {nbytes}"
EOF

step "serve reader A (zone A Cachey for catalog + data)"
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" -- serve \
  --shard "$CACHEY_A/fetch/$BUCKET/catalogs/v1.ducklake" \
  --data-base "$CACHEY_A/fetch/$BUCKET/data/" \
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

# First page bodies for the Flight cross-protocol check below.
_, _, first = get("/collections/buildings/items?sources=1&limit=1000")
with open(f"{work}/page-a.json", "w") as f:
    json.dump(first["features"], f)

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

step "reader A: Flight agrees on ids, geometry and properties (1000 rows)"
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" --example flight_client -- \
  --addr http://127.0.0.1:5211 --limit 1000 --out "$WORK/flight.tsv" | tee "$WORK/flight.out"
grep -q "streamed 1000 rows" "$WORK/flight.out"
python3 - "$WORK" <<'EOF'
import json, struct, sys
work = sys.argv[1]
ogc = json.load(open(f"{work}/page-a.json"))
flight = [line.split("\t") for line in open(f"{work}/flight.tsv").read().splitlines()]
assert len(flight) == 1000, len(flight)
assert len(ogc) == 1000, len(ogc)
for (fid, wkb_hex, props), feat in zip(flight, ogc):
    assert fid == feat["id"], (fid, feat["id"])
    # Flight properties are the same JSON string OGC parses into an object.
    assert json.loads(props) == feat["properties"], fid
    # Synthetic geometries are all WKB points; compare against GeoJSON.
    wkb = bytes.fromhex(wkb_hex)
    assert wkb[0] == 1, "expected little-endian WKB"
    assert struct.unpack("<I", wkb[1:5])[0] == 1, "expected Point"
    lon, lat = struct.unpack("<2d", wkb[5:21])
    gx, gy = feat["geometry"]["coordinates"]
    assert feat["geometry"]["type"] == "Point"
    assert abs(lon - gx) < 1e-6 and abs(lat - gy) < 1e-6, (fid, lon, lat, gx, gy)
print("flight content matches OGC on 1000 rows")
EOF

step "fresh reader B on the SAME Cachey: hits, zero new successful downloads"
dl_before=$(cachey_successful_downloads "$CACHEY_A")
hits_before=$(cachey_total "$CACHEY_A" cache_hit)
[ "$dl_before" -gt 0 ] || { echo "no downloads observed; metrics shape changed?" >&2; exit 1; }
echo "cachey A before B: successful_downloads=$dl_before hits=$hits_before"
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" -- serve \
  --shard "$CACHEY_A/fetch/$BUCKET/catalogs/v1.ducklake" \
  --data-base "$CACHEY_A/fetch/$BUCKET/data/" \
  --listen 127.0.0.1:3212 --flight-listen 127.0.0.1:5212 --no-quack \
  >"$WORK/serve-b.log" 2>&1 &
SERVERS="$SERVERS $!"
wait_for "http://127.0.0.1:3212/healthz"
python3 - http://127.0.0.1:3212 "$WORK" <<'EOF'
import json, sys, urllib.request
base, work = sys.argv[1], sys.argv[2]
ids, url = [], "/collections/buildings/items?sources=1&limit=1000"
while url:
    with urllib.request.urlopen(base + url, timeout=120) as r:
        page = json.load(r)
    ids += [f["id"] for f in page["features"]]
    nxt = [l["href"] for l in page["links"] if l["rel"] == "next"]
    url = nxt[0] if nxt else None
assert len(ids) == 262144, len(ids)
assert open(f"{work}/walk-a.txt").read().split("\n") == ids, "reader B disagrees with A"
print("reader B walk matches A")
EOF
dl_after=$(cachey_successful_downloads "$CACHEY_A")
hits_after=$(cachey_total "$CACHEY_A" cache_hit)
echo "cachey A after B: successful_downloads=$dl_after hits=$hits_after"
[ "$hits_after" -gt "$hits_before" ] || { echo "no new cache hits from B" >&2; exit 1; }
[ "$dl_after" -eq "$dl_before" ] || { echo "B added $((dl_after - dl_before)) successful downloads; sharing broken" >&2; exit 1; }
echo "sharing holds: B hit every page, successful S3 downloads unchanged at $dl_before"

step "zone B reader warms independently and never touches zone A data"
dl_a_before=$(cachey_successful_downloads "$CACHEY_A")
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" -- serve \
  --shard "$CACHEY_B/fetch/$BUCKET/catalogs/v1.ducklake" \
  --data-base "$CACHEY_B/fetch/$BUCKET/data/" \
  --listen 127.0.0.1:3213 --flight-listen 127.0.0.1:5213 --no-quack \
  >"$WORK/serve-c.log" 2>&1 &
SERVERS="$SERVERS $!"
wait_for "http://127.0.0.1:3213/healthz"
python3 - http://127.0.0.1:3213 "$WORK" <<'EOF'
import json, sys, urllib.request
base, work = sys.argv[1], sys.argv[2]
ids, url = [], "/collections/buildings/items?sources=1&limit=1000"
while url:
    with urllib.request.urlopen(base + url, timeout=120) as r:
        page = json.load(r)
    ids += [f["id"] for f in page["features"]]
    nxt = [l["href"] for l in page["links"] if l["rel"] == "next"]
    url = nxt[0] if nxt else None
assert len(ids) == 262144, len(ids)
assert open(f"{work}/walk-a.txt").read().split("\n") == ids, "zone B disagrees with A"
print("zone B walk matches A")
EOF
dl_a_after=$(cachey_successful_downloads "$CACHEY_A")
dl_b=$(cachey_successful_downloads "$CACHEY_B")
hits_b=$(cachey_total "$CACHEY_B" cache_hit)
echo "zone A successful downloads: $dl_a_before -> $dl_a_after; zone B successful=$dl_b hits=$hits_b"
[ "$dl_a_after" -eq "$dl_a_before" ] || { echo "zone B read through zone A" >&2; exit 1; }
[ "$dl_b" -gt 0 ] || { echo "zone B fetched nothing from S3?" >&2; exit 1; }
echo "zone locality holds"

step "publish v2 (small box) additively and prove reader A is pinned"
cargo run --locked --manifest-path "$ROOT/Cargo.toml" -- build \
  --from "$WORK/source.parquet" --collection buildings --bbox="2.0,48.0,2.1,48.1" \
  --out "$WORK/v2.ducklake" --data-dir "$WORK/files2" \
  --data-url "s3://$BUCKET/data/" --file-mb 8 --source-id 1
if rclone lsf "stack:$BUCKET/catalogs/" 2>/dev/null | grep -qx v2.ducklake; then
  echo "catalog key v2.ducklake already published" >&2
  exit 1
fi
rclone copy "$WORK/files2" "stack:$BUCKET/data/"
rclone copyto "$WORK/v2.ducklake" "stack:$BUCKET/catalogs/v2.ducklake"
printf '%s' v2.ducklake | rclone rcat "stack:$BUCKET/refs/latest"
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" -- serve \
  --shard "$CACHEY_A/fetch/$BUCKET/catalogs/v2.ducklake" \
  --data-base "$CACHEY_A/fetch/$BUCKET/data/" \
  --listen 127.0.0.1:3214 --flight-listen 127.0.0.1:5214 --no-quack \
  >"$WORK/serve-v2.log" 2>&1 &
SERVERS="$SERVERS $!"
wait_for "http://127.0.0.1:3214/healthz"
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
assert status("http://127.0.0.1:3214", probe) == 404, "reader v2 sees v1 rows"
assert status("http://127.0.0.1:3214", "/collections/buildings/items/way:0?sources=1") == 200
# v2 holds exactly the small-box grid points.
ks = [k for k in range(512) if Fraction(2, 1) <= 2 + Fraction(4 * k, 511) <= Fraction(21, 10)]
ms = [m for m in range(512) if Fraction(48, 1) <= 48 + Fraction(6 * m, 511) <= Fraction(481, 10)]
want = len(ks) * len(ms)
assert 0 < want < 262144, want
ids, url = [], "/collections/buildings/items?sources=1&limit=1000"
while url:
    with urllib.request.urlopen("http://127.0.0.1:3214" + url, timeout=120) as r:
        page = json.load(r)
    ids += [f["id"] for f in page["features"]]
    nxt = [l["href"] for l in page["links"] if l["rel"] == "next"]
    url = nxt[0] if nxt else None
assert len(ids) == want, (len(ids), want)
print("pinning holds: A serves v1, v2 serves", want, "rows")
EOF

step "old snapshot survives publish with cold caches (v1 data not deleted)"
docker rm -f stack-cachey-a >/dev/null 2>&1
docker run -d --name stack-cachey-a --network host \
  -e AWS_ACCESS_KEY_ID="$S3_USER" -e AWS_SECRET_ACCESS_KEY="$S3_PASS" \
  -e AWS_REGION=us-east-1 -e AWS_ENDPOINT_URL_S3="http://127.0.0.1:$MINIO_PORT" \
  "$CACHEY_IMAGE" --memory 256MiB --port "$CACHEY_A_PORT" >/dev/null
wait_for "http://127.0.0.1:$CACHEY_A_PORT/stats"
cargo run --locked --release --manifest-path "$ROOT/Cargo.toml" -- serve \
  --shard "$CACHEY_A/fetch/$BUCKET/catalogs/v1.ducklake" \
  --data-base "$CACHEY_A/fetch/$BUCKET/data/" \
  --listen 127.0.0.1:3215 --flight-listen 127.0.0.1:5215 --no-quack \
  >"$WORK/serve-d.log" 2>&1 &
SERVERS="$SERVERS $!"
wait_for "http://127.0.0.1:3215/healthz"
python3 - http://127.0.0.1:3215 "$WORK" <<'EOF'
import json, sys, urllib.request
base, work = sys.argv[1], sys.argv[2]
with urllib.request.urlopen(
    base + "/collections/buildings/items?sources=1&limit=1000", timeout=120
) as r:
    page = json.load(r)
ids = [f["id"] for f in page["features"]]
want = open(f"{work}/walk-a.txt").read().split("\n")[:1000]
assert ids == want, "cold v1 reader disagrees on first page"
print("cold v1 first page matches")
EOF
dl_cold=$(cachey_successful_downloads "$CACHEY_A")
[ "$dl_cold" -gt 0 ] || { echo "cold cache fetched nothing from S3?" >&2; exit 1; }
echo "cold v1 served from S3 after v2 publish (successful downloads=$dl_cold)"

echo "ALL STACK CHECKS PASSED"
