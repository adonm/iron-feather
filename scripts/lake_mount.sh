#!/usr/bin/env bash
# Mount the lake bucket locally via mount-s3 (same semantics as the
# mountpoint-S3-CSI driver in-cluster). Usage: lake_mount.sh [mountpoint].
# Writes go direct to S3; this mount is for reads only.
set -euo pipefail
MNT="${1:-/tmp/opencode/mnt/lake}"
# shellcheck disable=SC1091
source "$(dirname "$0")/dev-s3.env"
if mountpoint -q "$MNT" 2>/dev/null; then
  echo "already mounted: $MNT"
  exit 0
fi
command -v mount-s3 >/dev/null || {
  echo "mount-s3 not found (needed for the local CSI-equivalent mount)." >&2
  echo "Install: https://github.com/awslabs/mountpoint-s3/releases" >&2
  exit 1
}
mkdir -p "$MNT"
AWS_ACCESS_KEY_ID="$S3_USER" AWS_SECRET_ACCESS_KEY="$S3_PASS" \
  mount-s3 lake "$MNT" \
  --endpoint-url http://127.0.0.1:8333 \
  --region us-east-1 \
  --allow-other
echo "mounted lake at $MNT"
