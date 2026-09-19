// Package materialize ports src/materialize.rs: build + index SQL shapes.
//
// Writes go direct to S3 (no mount, no Cachey): build stages locally, then
// publishes data files + catalog + sidecars with additive-only semantics.
package materialize

import (
	"fmt"
	"strings"

	"github.com/adonm/lakewing/internal/filter"
)

type SortOrder string

const (
	SortGrid    SortOrder = "grid"
	SortHilbert SortOrder = "hilbert"
	SortNone    SortOrder = "none"
)

// BuildSpec mirrors materialize::Build flags.
type BuildSpec struct {
	From       string
	Collection string
	BBox       [4]float64
	Out        string
	DataDir    string
	DataURL    string
	Limit      *int64
	FileMB     int
	RowGroup   int
	Sort       SortOrder
	SourceID   int64
}

// InsertSQL mirrors the INSERT INTO features SELECT in materialize.rs.
func InsertSQL(spec BuildSpec) string {
	bbox := spec.BBox
	prefilter := fmt.Sprintf("xmin <= %v AND xmax >= %v AND ymin <= %v AND ymax >= %v",
		bbox[2], bbox[0], bbox[3], bbox[1])
	spatial := filter.SpatialPredicate(bbox)
	sortkey := "0"
	switch spec.Sort {
	case SortGrid:
		sortkey = "grid_sortkey(ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry)))"
	case SortHilbert:
		sortkey = "hilbert_sortkey(ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry)))"
	}
	limit := ""
	if spec.Limit != nil {
		limit = fmt.Sprintf(" LIMIT %d", *spec.Limit)
	}
	_ = prefilter
	return fmt.Sprintf(
		"INSERT INTO features SELECT type || ':' || id, %s, %d, geometry, "+
			"to_json(struct_pack(type, id)), %s, "+
			"ST_XMin(geometry), ST_YMin(geometry), ST_XMax(geometry), ST_YMax(geometry), "+
			"ST_X(ST_Centroid(geometry)), ST_Y(ST_Centroid(geometry)), NULL "+
			"FROM read_parquet(%s) WHERE %s AND (%s)%s",
		filter.Quote(spec.Collection), spec.SourceID, sortkey,
		filter.Quote(spec.From), prefilter, spatial, limit)
}

// SchemaDDL mirrors the features/collections schema.
func SchemaDDL(sort SortOrder) string {
	sorted := ""
	if sort == SortGrid || sort == SortHilbert {
		sorted = "SET SORTED BY (sortkey, id); "
	}
	return "CREATE TABLE features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY, " +
		"properties JSON, sortkey BIGINT, xmin DOUBLE, ymin DOUBLE, xmax DOUBLE, ymax DOUBLE, " +
		"cx DOUBLE, cy DOUBLE, name VARCHAR); " +
		"CREATE TABLE collections(id VARCHAR); " + sorted
}

// AttachDataPath returns the DATA_PATH clause for a build staging area.
func AttachDataPath(staging, dataURL string) string {
	base := staging
	if strings.HasPrefix(dataURL, "s3://") {
		base = dataURL
	} else if dataURL != "" {
		base = dataURL
	}
	return "DATA_PATH " + filter.Quote(base)
}
