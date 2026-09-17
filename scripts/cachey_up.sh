#!/usr/bin/env bash
# Local Cachey lake rig: MinIO (S3) + Cachey (AZ-local page cache stand-in).
# Idempotent: safe to re-run. All rig state lives under /tmp/opencode
# (ephemeral, never committed). Readers fetch Parquet/catalog byte ranges
# over HTTP; nothing is mounted.
set -euo pipefail

RIG=/tmp/opencode
CACHEY_PORT="${CACHEY_PORT:-8088}"
CACHEY_MEMORY="${CACHEY_MEMORY:-512MiB}"
CACHEY_IMAGE="${CACHEY_IMAGE:-ghcr.io/s2-streamstore/cachey@sha256:beab43f996c0d183d4c3adb68257d342ef87d700befe5b30c196f8a1524b96ca}"
MINIO_IMAGE="${MINIO_IMAGE:-quay.io/minio/minio@sha256:14cea493d9a34af32f524e538b8346cf79f3321eff8e708c1e2960462bd8936e}"
TOXY_IMAGE="${TOXY_IMAGE:-shopify/toxiproxy@sha256:a6b080af39986b863a1f7c5a3b9bacf2afeb48abab8f0eb7e243f8f7ad38c645}"
# Optional realish-S3 simulation: route Cachey's S3 traffic through a
# toxiproxy adding this many ms downstream latency (jitter ~1/6).
# Unset/0 keeps the direct loopback path.
SLOW_S3_MS="${SLOW_S3_MS:-0}"
# Dev-only S3 credentials (loopback rig; kind uses the same pair via Secret).
# shellcheck disable=SC1091
source "$(dirname "$0")/dev-s3.env"

# --- MinIO (S3-compatible bucket store; plain files under $RIG/minio) --------
# Retired containers from the Garage era are removed (their data dir stays
# on disk); the new store starts empty, so re-publish after switching.
for old in iron-garage iron-redis; do
  if docker ps --format '{{.Names}}' | grep -qx "$old"; then
    echo "retiring $old (zerofs-era; data dir kept under $RIG)"
    docker rm -f "$old" >/dev/null
  fi
done
if ! docker ps --format '{{.Names}}' | grep -qx iron-minio; then
  mkdir -p "$RIG/minio"
  docker rm -f iron-minio >/dev/null 2>&1 || true
  docker run -d --name iron-minio \
    -p 127.0.0.1:3900:9000 \
    -v "$RIG/minio:/data" \
    -e MINIO_ROOT_USER="$S3_USER" -e MINIO_ROOT_PASSWORD="$S3_PASS" \
    "$MINIO_IMAGE" server /data >/dev/null
  for _ in $(seq 1 30); do
    curl -sf http://127.0.0.1:3900/minio/health/live >/dev/null 2>&1 && break
    sleep 1
  done
fi
export AWS_ACCESS_KEY_ID="$S3_USER" AWS_SECRET_ACCESS_KEY="$S3_PASS"

# --- Optional slow-S3 simulation -------------------------------------------
# toxiproxy adds SLOW_S3_MS downstream latency to Garage traffic so Cachey
# misses cost a realish RTT while hits stay loopback-fast.
S3_ENDPOINT=http://127.0.0.1:3900
if [ "$SLOW_S3_MS" -gt 0 ] 2>/dev/null; then
  if ! docker ps --format '{{.Names}}' | grep -qx iron-toxy; then
    docker rm -f iron-toxy >/dev/null 2>&1 || true
    docker run -d --name iron-toxy --network host "$TOXY_IMAGE" >/dev/null
    for _ in $(seq 1 30); do
      curl -sf http://127.0.0.1:8474/version >/dev/null 2>&1 && break
      sleep 1
    done
  fi
  curl -sf -X POST http://127.0.0.1:8474/proxies -H 'Content-Type: application/json' \
    -d '{"name":"garage","listen":"127.0.0.1:3903","upstream":"127.0.0.1:3900"}' >/dev/null 2>&1 || true
  curl -sf -X DELETE http://127.0.0.1:8474/proxies/garage/toxics/slow >/dev/null 2>&1 || true
  curl -sf -X POST http://127.0.0.1:8474/proxies/garage/toxics -H 'Content-Type: application/json' \
    -d "{\"name\":\"slow\",\"type\":\"latency\",\"stream\":\"downstream\",\"toxicity\":1.0,\"attributes\":{\"latency\":$SLOW_S3_MS,\"jitter\":$((SLOW_S3_MS / 6 + 1))}}" >/dev/null
  S3_ENDPOINT=http://127.0.0.1:3903
  echo "slow S3: +${SLOW_S3_MS}ms via toxiproxy :3903"
fi

# --- Cachey (the AZ-local cache stand-in; plain HTTP, no mount) --------------
# Recreate when the upstream endpoint or credentials changed.
if docker ps --format '{{.Names}}' | grep -qx iron-cachey; then
  prev="$(cat "$RIG/cachey-endpoint-used" 2>/dev/null || true)"
  if [ "$prev" != "$S3_ENDPOINT $S3_USER" ]; then
    docker rm -f iron-cachey >/dev/null
  fi
fi
if ! docker ps --format '{{.Names}}' | grep -qx iron-cachey; then
  docker rm -f iron-cachey >/dev/null 2>&1 || true
  docker run -d --name iron-cachey --network host \
    -e AWS_ACCESS_KEY_ID="$S3_USER" \
    -e AWS_SECRET_ACCESS_KEY="$S3_PASS" \
    -e AWS_REGION=us-east-1 \
    -e AWS_ENDPOINT_URL_S3="$S3_ENDPOINT" \
    "$CACHEY_IMAGE" --memory "$CACHEY_MEMORY" --port "$CACHEY_PORT" >/dev/null
  printf '%s %s' "$S3_ENDPOINT" "$S3_USER" > "$RIG/cachey-endpoint-used"
  for _ in $(seq 1 30); do
    curl -sf "http://127.0.0.1:$CACHEY_PORT/stats" >/dev/null 2>&1 && break
    sleep 1
  done
fi
echo "s3 at http://127.0.0.1:3900, cachey at http://127.0.0.1:$CACHEY_PORT"
