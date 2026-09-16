#!/usr/bin/env bash
# Minimal refs "API" for the local lake rig. In production the catalog API
# owns latest/main/tag -> catalog-hash mapping and readers never mutate
# refs directly; here this script plays that role so the flow is explicit.
#
#   lake_ref.sh get [ref]        # print catalog filename for a ref (default: latest)
#   lake_ref.sh set <catalog> [ref]  # point a ref at an immutable catalog snapshot
#   lake_ref.sh list             # show all refs
set -euo pipefail

MNT="${LAKE_MNT:-/tmp/opencode/lake-mnt}"
cmd="${1:-list}"
ref="${3:-latest}"

case "$cmd" in
get)
  name="${2:-latest}"
  if [ "$name" = latest ] || [ "$name" = main ]; then
    cat "$MNT/refs/$name"
  else
    cat "$MNT/refs/tags/$name"
  fi
  ;;
set)
  catalog="${2:?usage: lake_ref.sh set <catalog.ducklake> [ref]}"
  if [ ! -f "$MNT/catalogs/$catalog" ]; then
    echo "no such published catalog: $catalog" >&2
    exit 1
  fi
  if [ "$ref" = latest ] || [ "$ref" = main ]; then
    printf '%s' "$catalog" > "$MNT/refs/$ref"
  else
    mkdir -p "$MNT/refs/tags"
    printf '%s' "$catalog" > "$MNT/refs/tags/$ref"
  fi
  echo "$ref -> $catalog"
  ;;
list)
  echo "== branches =="
  for f in "$MNT"/refs/latest "$MNT"/refs/main; do
    [ -f "$f" ] && echo "$(basename "$f") -> $(cat "$f")"
  done
  echo "== tags =="
  for f in "$MNT"/refs/tags/*; do
    [ -f "$f" ] && echo "$(basename "$f") -> $(cat "$f")"
  done
  ;;
*)
  echo "usage: lake_ref.sh {get [ref]|set <catalog> [ref]|list}" >&2
  exit 1
  ;;
esac
