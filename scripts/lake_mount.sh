#!/usr/bin/env bash
# Mount the lake bucket locally via mount-s3 (same client as the
# mountpoint-S3-CSI driver runs in-cluster). Usage: lake_mount.sh [mountpoint].
# Writes go direct to S3; this mount is for reads only.
#
# Local disk cache (the point of this rig): MOUNT_CACHE_DIR enables
# mount-s3's disk cache, MOUNT_CACHE_SIZE_MB bounds it. Extra mount-s3
# flags pass through via MOUNT_EXTRA_ARGS.
set -euo pipefail
MNT="${1:-/tmp/opencode/mnt/lake}"
CACHE_DIR="${MOUNT_CACHE_DIR:-/tmp/opencode/mount-cache}"
# shellcheck disable=SC1091
source "$(dirname "$0")/dev-s3.env"
if mountpoint -q "$MNT" 2>/dev/null; then
  echo "already mounted: $MNT"
  exit 0
fi
if ! command -v mount-s3 >/dev/null; then
  echo "mount-s3 not found (needed for the local CSI-equivalent mount)." >&2
  echo "Build: git clone --branch <tag> https://github.com/awslabs/mountpoint-s3" >&2
  echo "  cargo install --locked --path mountpoint-s3 --root ~/.local" >&2
  exit 1
fi
mkdir -p "$MNT" "$CACHE_DIR"
# shellcheck disable=SC2086
AWS_ACCESS_KEY_ID="$S3_USER" AWS_SECRET_ACCESS_KEY="$S3_PASS" \
  mount-s3 lake "$MNT" \
  --endpoint-url http://127.0.0.1:8333 \
  --region us-east-1 \
  --cache "$CACHE_DIR" \
  ${MOUNT_CACHE_SIZE_MB:+--max-cache-size $MOUNT_CACHE_SIZE_MB} \
  ${MOUNT_EXTRA_ARGS:-}
echo "mounted lake at $MNT (disk cache: $CACHE_DIR)"
