#!/usr/bin/env bash
# Local ZeroFS lake rig: Garage (S3) + Redis fencing + ZeroFS 9P + FUSE mount.
# Idempotent: safe to re-run; `zerofs_down.sh` tears it down.
# All rig state lives under /tmp/opencode (ephemeral, never committed).
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RIG=/tmp/opencode
export ZEROFS_PASSWORD="${ZEROFS_PASSWORD:-local-rig-password-not-a-secret}"

python3 "$ROOT/scripts/setup_zerofs.py"

# --- Garage (small S3 server with real buckets/keys) -------------------------
if ! docker ps --format '{{.Names}}' | grep -qx iron-garage; then
  mkdir -p "$RIG/garage/data" "$RIG/garage/meta"
  if [ ! -f "$RIG/garage/garage.toml" ]; then
    SECRET=$(head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n')
    cat > "$RIG/garage/garage.toml" <<EOF
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 1
consistency_mode = "consistent"
rpc_bind_addr = "[::]:3901"
rpc_public_addr = "127.0.0.1:3901"
rpc_secret = "$SECRET"
[s3_api]
s3_region = "us-east-1"
api_bind_addr = "[::]:3900"
root_domain = ".s3.garage.localhost"
[k8s_discovery]
skip = true
EOF
  fi
  docker run -d --name iron-garage \
    -p 127.0.0.1:3900:3900 \
    -v "$RIG/garage/garage.toml:/etc/garage.toml" \
    -v "$RIG/garage/data:/var/lib/garage/data" \
    -v "$RIG/garage/meta:/var/lib/garage/meta" \
    dxflrs/garage:v2.1.0 >/dev/null
  for _ in $(seq 1 30); do
    docker exec iron-garage /garage status >/dev/null 2>&1 && break
    sleep 1
  done
fi
NODE=$(docker exec iron-garage /garage status 2>/dev/null | awk '/NO ROLE ASSIGNED|HEALTHY/{found=1} found && /^[0-9a-f]{16} /{print $1; exit}')
if docker exec iron-garage /garage status 2>/dev/null | grep -q "NO ROLE ASSIGNED"; then
  ID=$(docker exec iron-garage /garage status 2>/dev/null | awk '$2 ~ /^[0-9a-f]+$/ && $0 ~ /NO ROLE/ {print $1; exit}')
  [ -z "$ID" ] && ID=$(docker exec iron-garage /garage status 2>/dev/null | grep -oE '^[0-9a-f]{16}' | head -1)
  docker exec iron-garage /garage layout assign -z dc1 -c 10G "$ID" >/dev/null
  VER=$(docker exec iron-garage /garage layout show 2>/dev/null | grep -oE 'version [0-9]+' | head -1 | awk '{print $2}')
  docker exec iron-garage /garage layout apply --version "$((VER + 1))" >/dev/null
fi
if ! docker exec iron-garage /garage bucket list 2>/dev/null | grep -q lake; then
  docker exec iron-garage /garage bucket create lake >/dev/null
fi
if [ ! -f "$RIG/garage/rig-key.env" ]; then
  OUT=$(docker exec iron-garage /garage key create rig-key 2>/dev/null)
  AK=$(echo "$OUT" | awk '/Key ID:/{print $3}')
  SK=$(echo "$OUT" | awk '/Secret key:/{print $3}')
  docker exec iron-garage /garage bucket allow lake --read --write --key rig-key >/dev/null
  printf 'AWS_ACCESS_KEY_ID=%s\nAWS_SECRET_ACCESS_KEY=%s\n' "$AK" "$SK" > "$RIG/garage/rig-key.env"
fi
# shellcheck disable=SC1091
source "$RIG/garage/rig-key.env"

# --- Redis (CAS coordinator: Garage ignores If-None-Match) -------------------
if ! docker ps --format '{{.Names}}' | grep -qx iron-redis; then
  docker run -d --name iron-redis -p 127.0.0.1:6379:6379 redis:8-alpine >/dev/null
  sleep 3
fi

# --- ZeroFS server ------------------------------------------------------------
mkdir -p "$RIG/zerofs-cache"
cat > "$RIG/zerofs-rig.toml" <<EOF
[cache]
dir = "$RIG/zerofs-cache"
disk_size_gb = 5.0
memory_size_gb = 0.5
[storage]
url = "s3://lake/zerofs"
encryption_password = "${ZEROFS_PASSWORD}"
[servers.ninep]
addresses = ["127.0.0.1:5564"]
unix_socket = "$RIG/zerofs.9p.sock"
[aws]
access_key_id = "$AWS_ACCESS_KEY_ID"
secret_access_key = "$AWS_SECRET_ACCESS_KEY"
endpoint = "http://127.0.0.1:3900"
default_region = "us-east-1"
allow_http = "true"
conditional_put = "redis://127.0.0.1:6379"
[telemetry]
enabled = false
EOF
if ! kill -0 "$(cat "$RIG/zerofs.pid" 2>/dev/null)" 2>/dev/null; then
  rm -f "$RIG/zerofs.9p.sock"
  ("$ROOT/.deps/zerofs/zerofs" run --config "$RIG/zerofs-rig.toml" > "$RIG/zerofs.log" 2>&1 & echo $! > "$RIG/zerofs.pid")
  for _ in $(seq 1 30); do
    [ -S "$RIG/zerofs.9p.sock" ] && break
    sleep 1
  done
fi

# --- FUSE mount ----------------------------------------------------------------
# Unprivileged FUSE is blocked in containers, so the mount runs under sudo
# with --access all (owner + everyone). Mount state is detected through
# /proc/mounts, which plain `mountpoint -q` misses for FUSE-as-root.
mkdir -p "$RIG/lake-mnt"
if ! grep -q " $RIG/lake-mnt " /proc/mounts 2>/dev/null; then
  (sudo "$ROOT/.deps/zerofs/zerofs" mount --access all 127.0.0.1:5564 "$RIG/lake-mnt" > "$RIG/zerofs-mount.log" 2>&1 & echo $! > "$RIG/zerofs-mount.pid")
  for _ in $(seq 1 15); do
    grep -q " $RIG/lake-mnt " /proc/mounts 2>/dev/null && break
    sleep 1
  done
  grep -q " $RIG/lake-mnt " /proc/mounts || { echo "FUSE mount failed"; tail -5 "$RIG/zerofs-mount.log"; exit 1; }
fi
ls "$RIG/lake-mnt" >/dev/null
echo "lake mounted at $RIG/lake-mnt"
