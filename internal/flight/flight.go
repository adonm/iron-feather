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
	"id":         "id",
	"geometry":   "ST_AsWKB(geom) AS geometry",
	"properties": "properties::VARCHAR AS properties",
	"source_id":  "source_id",
	"x":          "cx AS x",
	"y":          "cy AS y",
	"name":       "name",
}

// Ticket mirrors flight::ShardTicket.
type Ticket struct {
	Collection string      `json:"collection"`
	BBox       *[4]float64 `json:"bbox"`
	Columns    []string    `json:"columns"`
	Limit      *int        `json:"limit"`
	Offset     *int        `json:"offset"`
	Sources    []int64     `json:"sources"`
}

// Validate mirrors flight validation: unknown/dup columns rejected,
// limit default 10k capped at 100k.
func (t Ticket) Validate() ([]string, int, int, error) {
	cols := t.Columns
	if len(cols) == 0 {
		cols = []string{"id", "geometry", "properties", "source_id"}
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
	proj := make([]string, len(cols))
	for i, c := range cols {
		proj[i] = allowedColumns[c]
	}
	from := st.ReadSource(t.BBox)
	fetch := store.Predicate(t.Collection, t.BBox, sources)
	inner := fmt.Sprintf("SELECT id, geom, properties, source_id, cx, cy, name FROM %s WHERE %s ORDER BY id LIMIT %d OFFSET %d",
		from, fetch, limit, offset)
	// Rewrite centroid/name exprs to the page alias, mirroring Rust.
	rewritten := make([]string, len(proj))
	for i, p := range proj {
		r := strings.ReplaceAll(p, "cx AS x", "page.cx AS x")
		r = strings.ReplaceAll(r, "cy AS y", "page.cy AS y")
		r = strings.ReplaceAll(r, "ST_AsWKB(geom) AS geometry", "ST_AsWKB(page.geom) AS geometry")
		r = strings.ReplaceAll(r, "properties::VARCHAR AS properties", "page.properties::VARCHAR AS properties")
		r = strings.ReplaceAll(r, "source_id", "page.source_id")
		r = strings.ReplaceAll(r, "page.page.", "page.")
		rewritten[i] = r
	}
	_ = filter.Quote
	return fmt.Sprintf("SELECT %s FROM (%s) AS page ORDER BY id", strings.Join(rewritten, ", "), inner), nil
}
