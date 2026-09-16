#!/usr/bin/env bash
# Build a DuckLake copy of an existing iron-feather .duckdb shard.
#
# Usage: build_ducklake.sh SRC_DB CATALOG DATA_URL LOCAL_DATA_DIR [WEST SOUTH [EAST NORTH [FILE_MB ROW_GROUP SORT]]]
#   SRC_DB         local .duckdb shard to copy rows from
#   CATALOG        output .ducklake catalog path (local file)
#   DATA_URL       http(s) base URL the Parquet files will be served from
#   LOCAL_DATA_DIR local directory for writing Parquet during the build
#   WEST SOUTH     region origin for the grid-cell sort key (default 2 48)
#   EAST NORTH     region extent for the Hilbert envelope (default 6 54)
#   FILE_MB        target Parquet file size in MiB (default 128)
#   ROW_GROUP      Parquet row-group size in rows (default 65536)
#   SORT           grid (default), hilbert, or none
#
# Layout defaults: ZSTD level 3, 128 MiB target files, 64k row groups,
# explicit grid-cell sort (sortkey, id) for tight per-file bbox statistics.
# The stored DATA_PATH is the serve URL; writes go to LOCAL_DATA_DIR via
# override, so sync LOCAL_DATA_DIR to the served prefix afterwards.
# Screen FILE_MB in {64,128,256} and ROW_GROUP in {16384,65536,131072};
# start with SORT, then tune sizes on the strongest candidates.
#
# Publication is atomic: the catalog builds to a staging path and renames
# into place, and a versioned manifest lands alongside it. Never deletes a
# published catalog before its replacement is complete.
set -euo pipefail

SRC_DB="$1"
CATALOG="$2"
DATA_URL="$3"
LOCAL_DATA_DIR="$4"
WEST="${5:-2}"
SOUTH="${6:-48}"
EAST="${7:-6}"
NORTH="${8:-54}"
FILE_MB="${9:-${FILE_MB:-128}}"
ROW_GROUP="${10:-${ROW_GROUP:-65536}}"
SORT="${11:-${SORT:-grid}}"

mkdir -p "$LOCAL_DATA_DIR"
STAGING_DIR="$(dirname "$CATALOG")/.iron-feather-$(basename "$CATALOG").$$"
mkdir -p "$STAGING_DIR"
STAGING_CATALOG="$STAGING_DIR/catalog.ducklake"
STAGING_DATA="$STAGING_DIR/files"
mkdir -p "$STAGING_DATA"
cleanup() { rm -rf "$STAGING_DIR"; }
trap cleanup EXIT
export DUCKDB_LIB_DIR="${DUCKDB_LIB_DIR:-$PWD/.deps/duckdb}"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-$DUCKDB_LIB_DIR}"

case "$SORT" in
  grid)
    SORT_DDL="ALTER TABLE features SET SORTED BY (sortkey ASC, id ASC);"
    SORT_SELECT="((ST_XMin(geom) - $WEST) * 100)::BIGINT * 1000 + ((ST_YMin(geom) - $SOUTH) * 100)::BIGINT,"
    ;;
  hilbert)
    # Regional envelope keeps Hilbert values normalized to the shard extent;
    # ST_Hilbert needs a BOX_2D bound (ST_Extent), not a geometry envelope.
    SORT_DDL="ALTER TABLE features SET SORTED BY (sortkey ASC, id ASC);"
    SORT_SELECT="ST_Hilbert(geom, ST_Extent(ST_MakeEnvelope($WEST, $SOUTH, $EAST, $NORTH))),"
    ;;
  none)
    SORT_DDL="-- no sorted-by: preserve source insertion order"
    SORT_SELECT="0,"
    ;;
  *)
    echo "SORT must be grid, hilbert, or none" >&2; exit 1 ;;
esac

duckdb :memory: <<SQL
LOAD spatial;
LOAD ducklake;
LOAD httpfs;
ATTACH 'ducklake:$STAGING_CATALOG' AS lake (DATA_PATH '$DATA_URL');
DETACH lake;
ATTACH 'ducklake:$STAGING_CATALOG' AS lake (DATA_PATH '$STAGING_DATA/', OVERRIDE_DATA_PATH true);
USE lake;
CALL lake.set_option('target_file_size', '${FILE_MB}MB');
CALL lake.set_option('parquet_row_group_size', $ROW_GROUP);
CALL lake.set_option('parquet_compression', 'zstd');
CALL lake.set_option('parquet_compression_level', 3);
CREATE TABLE features(
  id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, properties JSON,
  sortkey UBIGINT, xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE,
  cx DOUBLE, cy DOUBLE, name VARCHAR
);
CREATE TABLE collections(id VARCHAR);
$SORT_DDL
ATTACH '$SRC_DB' AS src (READ_ONLY);
INSERT INTO features
  SELECT id, layer, source_id, geom, properties::JSON,
    $SORT_SELECT
    ST_XMin(geom), ST_YMin(geom), ST_XMax(geom), ST_YMax(geom),
    COALESCE(cx, ST_X(ST_Centroid(geom))), COALESCE(cy, ST_Y(ST_Centroid(geom))),
    COALESCE(name, coalesce(json_extract_string(properties::VARCHAR, '\$.name'), json_extract_string(properties::VARCHAR, '\$.tags.name')))
  FROM src.features;
INSERT INTO collections SELECT DISTINCT layer AS id FROM src.features ORDER BY id;
CALL ducklake_flush_inlined_data('lake');
SELECT count(*) AS features FROM features;
SQL
# Atomic publish: data files first, then the catalog that references them.
mkdir -p "$LOCAL_DATA_DIR"
# Sync staging Parquet into the build dir without deleting published files
# another backend may still reference.
cp -n "$STAGING_DATA"/*.parquet "$LOCAL_DATA_DIR"/ 2>/dev/null || cp "$STAGING_DATA"/*.parquet "$LOCAL_DATA_DIR"/
mv -f "$STAGING_CATALOG" "$CATALOG"
trap - EXIT
rm -rf "$STAGING_DIR"
ROWS=$(duckdb :memory: "LOAD ducklake; ATTACH 'ducklake:$CATALOG' AS l (DATA_PATH '$LOCAL_DATA_DIR/', OVERRIDE_DATA_PATH true); USE l; SELECT count(*) FROM features;" -csv -noheader 2>/dev/null | tail -1)
python3 - "$CATALOG" "$ROWS" <<PY
import json, sys, time
catalog, rows = sys.argv[1], sys.argv[2]
manifest = {
  "version": 1,
  "backend": "lake",
  "schema_version": 2,
  "source": "$SRC_DB",
  "bbox": [$WEST, $SOUTH, $EAST, $NORTH],
  "rows": int(rows or 0),
  "built_at": str(int(time.time())),
  "layout": {"file_mb": $FILE_MB, "row_group": $ROW_GROUP, "sort": "$SORT"},
}
open(catalog + ".manifest.json", "w").write(json.dumps(manifest, indent=2))
PY
echo "catalog: $CATALOG sort=$SORT file_mb=$FILE_MB row_group=$ROW_GROUP"
find "$LOCAL_DATA_DIR" -name '*.parquet' | wc -l
du -sh "$LOCAL_DATA_DIR"
