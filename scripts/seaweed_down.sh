#!/usr/bin/env bash
# Stop the SeaweedFS rig. Pass --wipe to drop containers and stored state.
set -euo pipefail
docker rm -f lake-seaweed >/dev/null 2>&1 || true
if [ "${1:-}" = "--wipe" ]; then
  rm -rf /tmp/opencode/seaweed
fi
echo "seaweed rig stopped"
