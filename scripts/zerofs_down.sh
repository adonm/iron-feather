#!/usr/bin/env bash
# Tear down the local ZeroFS lake rig. Leaves Garage data and the ZeroFS
# cache in place so a later `zerofs-up` resumes the same filesystem;
# pass --wipe to also drop containers and cached state.
set -euo pipefail

RIG=/tmp/opencode
WIPE="${1:-}"

if grep -q " $RIG/lake-mnt " /proc/mounts 2>/dev/null; then
  sudo umount "$RIG/lake-mnt"
  echo "unmounted $RIG/lake-mnt"
fi
if kill -0 "$(cat "$RIG/zerofs.pid" 2>/dev/null)" 2>/dev/null; then
  kill "$(cat "$RIG/zerofs.pid")"
  echo "stopped zerofs server"
fi
sudo pkill -f "zerofs mount" 2>/dev/null || true

if [ "$WIPE" = "--wipe" ]; then
  docker rm -f iron-garage iron-redis >/dev/null 2>&1 || true
  rm -rf "$RIG/zerofs-cache" "$RIG/garage" "$RIG/zerofs-rig.toml" "$RIG/*.pid" "$RIG/*.sock"
  echo "wiped rig containers and cache"
fi
