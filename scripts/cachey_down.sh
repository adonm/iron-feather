#!/usr/bin/env bash
# Stop the Cachey rig. Plain stop keeps S3 + cached state in place so a later
# `cachey-up` resumes; --wipe drops containers and all rig state.
set -euo pipefail

RIG=/tmp/opencode
WIPE=false
[ "${1:-}" = "--wipe" ] && WIPE=true

docker rm -f iron-cachey >/dev/null 2>&1 || echo "cachey not running"
if $WIPE; then
  docker rm -f iron-minio iron-toxy >/dev/null 2>&1 || true
  rm -rf "$RIG/minio" "$RIG/publish-"* "$RIG/cachey-endpoint-used"
  echo "wiped minio + cachey state"
fi
