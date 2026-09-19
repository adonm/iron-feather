// Package flight ports src/flight.rs: read-only Arrow Flight over the shard.
//
// Tickets mirror ShardTicket{collection,bbox,columns,limit,offset,sources}.
// Columns: id, geometry (WKB), properties (JSON), source_id, x, y, name.
package flight

import (
	"fmt"
	"strings"

	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/store"
)

var allowedColumns = map[string]string{
	"id":         "page.id",
	"geometry":   "ST_AsWKB(page.geom) AS geometry",
	"properties": "page.properties::VARCHAR AS properties",
	"source_id":  "page.source_id",
	"x":          "page.cx AS x",
	"y":          "page.cy AS y",
	"name":       "page.name",
}

// Ticket mirrors flight::ShardTicket. BBox stays a raw number list so 3D
// bounds validate like the OGC path instead of failing JSON shape.
type Ticket struct {
	Collection string    `json:"collection"`
	BBox       []float64 `json:"bbox"`
	Columns    []string  `json:"columns"`
	Limit      *int      `json:"limit"`
	Offset     *int      `json:"offset"`
	Sources    []int64   `json:"sources"`
}

// Validate mirrors flight validation: nil columns take defaults, explicit
// empty is rejected like unknown/duplicates; limit defaults to 10k, max 100k.
func (t Ticket) Validate() ([]string, int, int, error) {
	cols := t.Columns
	if cols == nil {
		cols = []string{"id", "geometry", "properties", "source_id"}
	}
	if len(cols) == 0 {
		return nil, 0, 0, fmt.Errorf("columns must not be empty")
	}
	seen := map[string]bool{}
	for _, c := range cols {
		if _, ok := allowedColumns[c]; !ok {
			return nil, 0, 0, fmt.Errorf("unknown column %q", c)
		}
		if seen[c] {
			return nil, 0, 0, fmt.Errorf("duplicate column %q", c)
		}
		seen[c] = true
	}
	limit := 10000
	if t.Limit != nil {
		limit = *t.Limit
	}
	if limit < 0 || limit > 100000 {
		return nil, 0, 0, fmt.Errorf("limit must be 0..100000")
	}
	offset := 0
	if t.Offset != nil {
		offset = *t.Offset
	}
	if offset < 0 {
		return nil, 0, 0, fmt.Errorf("offset must be >= 0")
	}
	return cols, limit, offset, nil
}

// SQL builds the bulk SELECT over the frozen serving source.
func (t Ticket) SQL(st *store.Store, sources []int64) (string, error) {
	cols, limit, offset, err := t.Validate()
	if err != nil {
		return "", err
	}
	var bounds *[4]float64
	if t.BBox != nil {
		b, err := filter.BBox(t.BBox)
		if err != nil {
			return "", err
		}
		bounds = &b
	}
	proj := make([]string, len(cols))
	for i, c := range cols {
		proj[i] = allowedColumns[c]
	}
	from := st.ReadSource(bounds)
	fetch := store.Predicate(t.Collection, bounds, sources)
	inner := fmt.Sprintf("SELECT id, geom, properties, source_id, cx, cy, name FROM %s WHERE %s ORDER BY id LIMIT %d OFFSET %d",
		from, fetch, limit, offset)
	return fmt.Sprintf("SELECT %s FROM (%s) AS page ORDER BY id", strings.Join(proj, ", "), inner), nil
}
