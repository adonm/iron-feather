// Package filter ports src/filter.rs: shared request validation and SQL
// literals. No caller-supplied SQL; every value is quoted or numeric.
package filter

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Quote renders a SQL string literal.
func Quote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// CollectionID mirrors filter::collection_id: 1..128 ASCII alnum, _ or -.
func CollectionID(value string) bool {
	if value == "" || len(value) > 128 {
		return false
	}
	for i := 0; i < len(value); i++ {
		c := value[i]
		if c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '-' {
			continue
		}
		return false
	}
	return true
}

// BBox validates a CRS84 bbox: 4 numbers, or 6 with ordered heights
// (heights are ignored for 2D data). West > east crosses the antimeridian.
func BBox(values []float64) ([4]float64, error) {
	var b [4]float64
	switch len(values) {
	case 4:
		copy(b[:], values)
	case 6:
		if !(values[2] <= values[5]) {
			return b, fmt.Errorf("bbox requires 4 or 6 ordered numbers")
		}
		b = [4]float64{values[0], values[1], values[3], values[4]}
	default:
		return b, fmt.Errorf("bbox requires 4 or 6 ordered numbers")
	}
	for _, v := range values {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return b, fmt.Errorf("bbox must be finite CRS84 bounds with south <= north")
		}
	}
	if b[0] < -180 || b[0] > 180 || b[2] < -180 || b[2] > 180 ||
		b[1] < -90 || b[1] > 90 || b[3] < -90 || b[3] > 90 || b[1] > b[3] {
		return b, fmt.Errorf("bbox must be finite CRS84 bounds with south <= north")
	}
	return b, nil
}

// ParseBBox parses a comma-separated bbox parameter.
func ParseBBox(raw string) ([4]float64, error) {
	parts := strings.Split(raw, ",")
	values := make([]float64, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseFloat(strings.TrimSpace(p), 64)
		if err != nil {
			return [4]float64{}, fmt.Errorf("bbox must contain numbers")
		}
		values = append(values, v)
	}
	return BBox(values)
}

// SpatialPredicate mirrors filter::spatial_predicate.
func SpatialPredicate(b [4]float64) string {
	envelope := func(w, e float64) string {
		return fmt.Sprintf("ST_Intersects(geom, ST_MakeEnvelope(%v, %v, %v, %v))", w, b[1], e, b[3])
	}
	if b[0] > b[2] {
		return fmt.Sprintf("(%s OR %s)", envelope(b[0], 180.0), envelope(-180.0, b[2]))
	}
	return envelope(b[0], b[2])
}

// ParseSources parses a comma-separated integer source list.
func ParseSources(raw string) ([]int64, error) {
	if raw == "" {
		return []int64{}, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int64, 0, len(parts))
	for _, p := range parts {
		v, err := strconv.ParseInt(strings.TrimSpace(p), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("sources must be integers")
		}
		out = append(out, v)
	}
	return out, nil
}

// Sources intersects the ?sources set with the X-Source-Ids header set.
// A request may narrow the header set, never broaden it.
func Sources(requested, header []int64, hasRequested, hasHeader bool) []int64 {
	var ids []int64
	switch {
	case hasRequested && hasHeader:
		allowed := map[int64]bool{}
		for _, id := range header {
			allowed[id] = true
		}
		for _, id := range requested {
			if allowed[id] {
				ids = append(ids, id)
			}
		}
	case hasRequested:
		ids = append([]int64{}, requested...)
	case hasHeader:
		ids = append([]int64{}, header...)
	default:
		return []int64{}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	out := ids[:0]
	var prev int64
	for i, id := range ids {
		if i == 0 || id != prev {
			out = append(out, id)
			prev = id
		}
	}
	return out
}

// Predicate builds the WHERE fragment for a collection + bounds + sources.
// An empty source set matches nothing (FALSE).
func Predicate(collection string, bounds *[4]float64, sources []int64) string {
	lineage := "FALSE"
	if len(sources) > 0 {
		strs := make([]string, len(sources))
		for i, s := range sources {
			strs[i] = strconv.FormatInt(s, 10)
		}
		lineage = "source_id IN (" + strings.Join(strs, ",") + ")"
	}
	sql := "layer = " + Quote(collection) + " AND " + lineage
	if bounds != nil {
		sql += " AND " + SpatialPredicate(*bounds)
	}
	return sql
}

// BBoxOverlap is the cheap min/max pruning half over explicit bbox columns.
func BBoxOverlap(b [4]float64) string {
	w, s, e, n := b[0], b[1], b[2], b[3]
	lat := fmt.Sprintf("ymax >= %v AND ymin <= %v", s, n)
	if w <= e {
		return fmt.Sprintf("xmax >= %v AND xmin <= %v AND %s", w, e, lat)
	}
	return fmt.Sprintf("(xmax >= %v OR xmin <= %v) AND %s", w, e, lat)
}

// BBoxContained is the interior fast path: bboxes fully inside the query
// window skip exact ST_Intersects work.
func BBoxContained(b [4]float64) string {
	w, s, e, n := b[0], b[1], b[2], b[3]
	lat := fmt.Sprintf("ymin >= %v AND ymax <= %v", s, n)
	if w <= e {
		return fmt.Sprintf("xmin >= %v AND xmax <= %v AND %s", w, e, lat)
	}
	return fmt.Sprintf("(xmin >= %v OR xmax <= %v) AND %s", w, e, lat)
}

// ValidateDatetime mirrors filter::datetime: Layercake edit timestamps are
// preserved as properties, so static features match every valid datetime.
// This only validates RFC3339 shape / interval ordering.
func ValidateDatetime(raw string) error {
	parse := func(s string) (time.Time, error) {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			return time.Time{}, fmt.Errorf("invalid RFC3339 datetime")
		}
		return t, nil
	}
	if strings.Contains(raw, "/") {
		parts := strings.SplitN(raw, "/", 2)
		var start, end *time.Time
		if parts[0] != ".." && parts[0] != "" {
			t, err := parse(parts[0])
			if err != nil {
				return err
			}
			start = &t
		}
		if parts[1] != ".." && parts[1] != "" {
			t, err := parse(parts[1])
			if err != nil {
				return err
			}
			end = &t
		}
		if start == nil && end == nil {
			return fmt.Errorf("datetime requires a nonempty, ordered interval")
		}
		if start != nil && end != nil && start.After(*end) {
			return fmt.Errorf("datetime requires a nonempty, ordered interval")
		}
		return nil
	}
	_, err := parse(raw)
	return err
}
