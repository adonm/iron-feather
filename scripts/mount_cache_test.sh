#!/usr/bin/env bash
# Validates how mountpoint S3 caches, against real SeaweedFS over real FUSE.
# No mocks: uploads fixture Parquet to S3, mounts with a fresh disk cache,
# then cold / warm / cache-dropped DuckDB scans asserting:
#   1. identical results on every pass,
#   2. warm pass issues zero new S3 GetObject requests (disk served it),
#   3. wiping the cache resumes S3 GETs (warm hits came from disk).
# Timings and cache occupancy print for the record (loopback: behavior, not
# capacity). Usage: mount_cache_test.sh [nfiles]
#
# NOTE: host FUSE mounts require privileges this dev host does not grant, so
# the mount + queries run in a throwaway --privileged testbox container
# (same mount-s3 1.24.0 binary, same SeaweedFS over loopback). The S3,
# FUSE and cache behavior under test is identical.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
NFILES="${1:-8}"
BOX=mp-testbox
MNT=/mnt/t
CACHE=/cache
PREFIX=cachetest
# shellcheck disable=SC1091
source "$ROOT/scripts/dev-s3.env"

command -v mount-s3 >/dev/null || { echo "mount-s3 not found in PATH" >&2; exit 1; }
[ -x "$ROOT/.deps/duckdb/duckdb" ] || { echo "duckdb CLI missing" >&2; exit 1; }

echo "== seaweed =="
bash "$ROOT/scripts/seaweed_up.sh" >/dev/null

export RCLONE_CONFIG_LAKE_TYPE=s3 RCLONE_CONFIG_LAKE_PROVIDER=Other
export RCLONE_CONFIG_LAKE_ENDPOINT=http://127.0.0.1:8333
export RCLONE_CONFIG_LAKE_ACCESS_KEY_ID="$S3_USER"
export RCLONE_CONFIG_LAKE_SECRET_ACCESS_KEY="$S3_PASS"
export RCLONE_CONFIG_LAKE_REGION="$S3_REGION" RCLONE_CONFIG_LAKE_FORCE_PATH_STYLE=true

echo "== upload $NFILES parquet files =="
STAGE=/tmp/opencode/cachetest-stage
rm -rf "$STAGE" && mkdir -p "$STAGE"
# Data files only: DuckLake also writes tiny internal change-tracking files
# (_ducklake_internal_* columns, different schema) that must not be globbed
# with the feature files.
find "$ROOT/fixtures/nw-europe.files" -name "*.parquet" -size +1M | sort | head -"$NFILES" \
  | while read -r f; do cp "$f" "$STAGE/"; done
[ "$(find "$STAGE" -type f | wc -l)" -eq "$NFILES" ] || { echo "short upload set" >&2; exit 1; }
rclone purge "lake:lake/$PREFIX/" >/dev/null 2>&1 || true
rclone copy "$STAGE" lake:lake/$PREFIX/ >/dev/null
rm -rf "$STAGE"

echo "== testbox =="
if ! docker ps --format '{{.Names}}' | grep -qx "$BOX"; then
  docker rm -f "$BOX" >/dev/null 2>&1 || true
  docker run -d --name "$BOX" --privileged --network host \
    -v "$HOME/.local/bin:/mpbin:ro" -v "$ROOT/.deps:/deps:ro" \
    fedora:44 sleep infinity >/dev/null
fi
docker exec "$BOX" sh -c 'rpm -q fuse3 >/dev/null 2>&1 || dnf install -y -q fuse3' >/dev/null
docker exec "$BOX" mkdir -p "$MNT" "$CACHE" /logs

xbox() { docker exec "$BOX" "$@"; }

mount_fresh() { # unmount, wipe cache+logs, mount with metrics
  xbox sh -c "mountpoint -q $MNT 2>/dev/null && (fusermount3 -u $MNT 2>/dev/null || umount $MNT) || true"
  xbox sh -c "rm -rf ${CACHE:?}/* /logs/*"
  xbox env AWS_ACCESS_KEY_ID="$S3_USER" AWS_SECRET_ACCESS_KEY="$S3_PASS" \
    /mpbin/mount-s3 lake "$MNT" --endpoint-url http://127.0.0.1:8333 \
    --region us-east-1 --prefix "$PREFIX/" --cache "$CACHE" \
    --max-cache-size 8192 --log-metrics --log-directory /logs >/dev/null
  for _ in $(seq 1 30); do
    xbox mountpoint -q "$MNT" 2>/dev/null && return 0
    sleep 1
  done
  echo "mount failed" >&2
  xbox sh -c 'cat /logs/*.log 2>/dev/null' | tail -5 >&2 || true
  exit 1
}

workload() { # one OGC-like scan pass over the mount; prints ms + result md5
  local start end out digest
  start=$(date +%s%3N)
  out=$(xbox /deps/duckdb/duckdb :memory: -csv -noheader -c "
    SELECT id, cx, cy, name FROM read_parquet('$MNT/*.parquet')
      WHERE xmax >= 4.3 AND xmin <= 4.5 AND ymax >= 51.9 AND ymin <= 52.1 AND source_id IN (1)
      ORDER BY id LIMIT 100;
    SELECT id, properties::VARCHAR FROM read_parquet('$MNT/*.parquet')
      WHERE xmax >= 2 AND xmin <= 6 AND ymax >= 48 AND ymin <= 54 AND source_id IN (1)
      ORDER BY id LIMIT 1000;" 2>&1) || { echo "WORKLOAD_ERROR: $out" >&2; return 1; }
  case "$out" in
    *[Ee]rror*) echo "WORKLOAD_ERROR: $out" >&2; return 1 ;;
  esac
  digest=$(printf '%s' "$out" | md5sum | cut -d' ' -f1)
  end=$(date +%s%3N)
  echo "$((end - start)) $digest"
}

s3_gets() { # max S3 GetObject count seen in the current mount log (cumulative)
  sleep 7 # metrics flush every 5s
  xbox sh -c 'cat /logs/*.log 2>/dev/null' | python3 -c "
import re, sys
best = 0
for line in sys.stdin:
    for m in re.finditer(r's3\.request_count\[s3_request=GetObject\]:\s*(\d+)', line):
        best = max(best, int(m.group(1)))
print(best)"
}

cache_bytes() { xbox du -sb "$CACHE" | cut -f1; }

echo "== cold pass (fresh cache) =="
mount_fresh
C0=$(s3_gets)
read -r COLD_MS COLD_DIGEST <<<"$(workload)"
C1=$(s3_gets)
COLD_GETS=$((C1 - C0))
COLD_CACHE=$(cache_bytes)
echo "cold: ${COLD_MS}ms new_gets=$COLD_GETS cache_bytes=$COLD_CACHE digest=$COLD_DIGEST"

echo "== warm pass (same mount, same cache) =="
W0=$(s3_gets)
read -r WARM_MS WARM_DIGEST <<<"$(workload)"
W1=$(s3_gets)
WARM_GETS=$((W1 - W0))
WARM_CACHE=$(cache_bytes)
echo "warm: ${WARM_MS}ms new_gets=$WARM_GETS cache_bytes=$WARM_CACHE digest=$WARM_DIGEST"

fail=0
[ "$COLD_GETS" -gt 0 ] || { echo "FAIL: cold pass issued no S3 GETs (metrics parse broken?)" >&2; fail=1; }
[ "$COLD_DIGEST" = "$WARM_DIGEST" ] || { echo "FAIL: digest changed cold=$COLD_DIGEST warm=$WARM_DIGEST" >&2; fail=1; }
[ "$WARM_GETS" -eq 0 ] || { echo "FAIL: warm pass issued $WARM_GETS new S3 GetObjects" >&2; fail=1; }
[ "$WARM_CACHE" -ge "$COLD_CACHE" ] || { echo "FAIL: cache shrank?! cold=$COLD_CACHE warm=$WARM_CACHE" >&2; fail=1; }

echo "== cache-drop pass (wipe cache, remount) =="
mount_fresh
D0=$(s3_gets)
read -r DROP_MS DROP_DIGEST <<<"$(workload)"
D1=$(s3_gets)
DROP_GETS=$((D1 - D0))
echo "dropped: ${DROP_MS}ms new_gets=$DROP_GETS digest=$DROP_DIGEST"
[ "$DROP_DIGEST" = "$COLD_DIGEST" ] || { echo "FAIL: digest changed after wipe" >&2; fail=1; }
[ "$DROP_GETS" -gt 0 ] || { echo "FAIL: no S3 GETs after cache wipe" >&2; fail=1; }

xbox sh -c "fusermount3 -u $MNT 2>/dev/null || umount $MNT 2>/dev/null || true"
if [ "$fail" -eq 0 ]; then
  echo "PASS: cold fetched $COLD_GETS objects in ${COLD_MS}ms; warm served from disk with zero new S3 GETs in ${WARM_MS}ms; wipe resumed S3 reads"
else
  echo "FAIL" >&2; exit 1
fi
