// Package ogc ports src/api.rs: OGC API Features over the shared shard,
// now via Huma v2 (OpenAPI 3.1 generated from Go structs).
package ogc

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/plan"
	"github.com/adonm/lakewing/internal/store"
	"github.com/danielgtaylor/huma/v2"
)

const GeoJSON = "application/geo+json"

var Conformance = []string{
	"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
	"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
	"http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
}

// Register wires all OGC routes onto the Huma API.
func Register(api huma.API, st *store.Store) {
	huma.Register(api, huma.Operation{
		OperationID: "landing", Method: http.MethodGet, Path: "/",
		Summary: "Landing page",
	}, func(ctx context.Context, in *struct{}) (*struct {
		Body struct {
			Title string `json:"title"`
			Links []Link `json:"links"`
		}
	}, error) {
		out := &struct {
			Body struct {
				Title string `json:"title"`
				Links []Link `json:"links"`
			}
		}{}
		out.Body.Title = "lakewing"
		out.Body.Links = []Link{{Href: "/", Rel: "self"}, {Href: "/conformance", Rel: "conformance"}, {Href: "/collections", Rel: "data"}}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "conformance", Method: http.MethodGet, Path: "/conformance",
	}, func(ctx context.Context, in *struct{}) (*struct {
		Body struct {
			ConformsTo []string `json:"conformsTo"`
		}
	}, error) {
		out := &struct {
			Body struct {
				ConformsTo []string `json:"conformsTo"`
			}
		}{}
		out.Body.ConformsTo = Conformance
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "health", Method: http.MethodGet, Path: "/healthz",
	}, func(ctx context.Context, in *struct{}) (*struct {
		Body struct {
			Status string `json:"status"`
		}
	}, error) {
		out := &struct {
			Body struct {
				Status string `json:"status"`
			}
		}{}
		out.Body.Status = "ok"
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "collections", Method: http.MethodGet, Path: "/collections",
	}, func(ctx context.Context, in *struct{}) (*struct {
		Body Collections
	}, error) {
		out := &struct{ Body Collections }{}
		for _, c := range st.Collections {
			out.Body.Collections = append(out.Body.Collections, CollectionMeta{ID: c, Title: c})
		}
		out.Body.Links = []Link{{Href: "/collections", Rel: "self"}}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "collection", Method: http.MethodGet, Path: "/collections/{collection}",
	}, func(ctx context.Context, in *CollectionInput) (*struct {
		Body CollectionMeta
	}, error) {
		if err := st.Collection(in.Collection); err != nil {
			return nil, toHumaErr(err)
		}
		out := &struct{ Body CollectionMeta }{}
		out.Body = CollectionMeta{ID: in.Collection, Title: in.Collection}
		return out, nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "items", Method: http.MethodGet, Path: "/collections/{collection}/items",
	}, func(ctx context.Context, in *ItemsInput) (*RawJSONResponse, error) {
		return itemsHandler(ctx, st, in, nil)
	})

	huma.Register(api, huma.Operation{
		OperationID: "item", Method: http.MethodGet, Path: "/collections/{collection}/items/{featureId}",
	}, func(ctx context.Context, in *ItemInput) (*RawJSONResponse, error) {
		if err := st.Collection(in.Collection); err != nil {
			return nil, toHumaErr(err)
		}
		sources, err := sourceIDs(in.Sources, in.SourceHeader)
		if err != nil {
			return nil, huma.Error400BadRequest(err.Error())
		}
		if len(sources) == 0 {
			return nil, huma.Error404NotFound("not found")
		}
		from := st.ReadSource(nil)
		fetch := store.Predicate(in.Collection, nil, sources)
		query := fmt.Sprintf("SELECT ST_AsGeoJSON(geom), properties::VARCHAR FROM %s WHERE id=%s AND %s LIMIT 1",
			from, filter.Quote(in.FeatureID), fetch)
		var geomJSON, props string
		qerr := st.QueryRow(ctx, false, func(ctx context.Context, db *sql.DB) error {
			return db.QueryRowContext(ctx, query).Scan(&geomJSON, &props)
		})
		if qerr != nil {
			if qerr == sql.ErrNoRows {
				return nil, huma.Error404NotFound("not found")
			}
			return nil, toHumaErr(qerr)
		}
		body := renderFeature(in.Collection, in.FeatureID, geomJSON, props)
		return rawJSON(body, GeoJSON), nil
	})

	huma.Register(api, huma.Operation{
		OperationID: "metrics", Method: http.MethodGet, Path: "/metrics",
	}, func(ctx context.Context, in *struct{}) (*struct {
		Body string
	}, error) {
		out := &struct{ Body string }{}
		out.Body = st.Metrics()
		return out, nil
	})
}

type Link struct {
	Href string `json:"href"`
	Rel  string `json:"rel"`
}

type Collections struct {
	Collections []CollectionMeta `json:"collections"`
	Links       []Link           `json:"links"`
}

type CollectionMeta struct {
	ID    string `json:"id"`
	Title string `json:"title"`
}

type CollectionInput struct {
	Collection string `path:"collection"`
}

type ItemsInput struct {
	Collection   string `path:"collection"`
	BBox         string `query:"bbox"`
	Limit        int    `query:"limit"`
	Offset       int    `query:"offset"`
	Cursor       string `query:"cursor"`
	Datetime     string `query:"datetime"`
	Sources      string `query:"sources"`
	SourceHeader string `header:"X-Source-Ids"`
}

type ItemInput struct {
	Collection   string `path:"collection"`
	FeatureID    string `path:"featureId"`
	Sources      string `query:"sources"`
	SourceHeader string `header:"X-Source-Ids"`
}

// RawJSONResponse carries pre-rendered bytes with content type.
type RawJSONResponse struct {
	ContentType string
	Body        []byte
}

type featureRow struct{ id, geom, props string }

func rawJSON(body []byte, ct string) *RawJSONResponse {
	return &RawJSONResponse{ContentType: ct, Body: body}
}

func sourceIDs(query, header string) ([]int64, error) {
	var q, h []int64
	hasQ, hasH := query != "", header != ""
	var err error
	if hasQ {
		if q, err = filter.ParseSources(query); err != nil {
			return nil, err
		}
	}
	if hasH {
		if h, err = filter.ParseSources(header); err != nil {
			return nil, err
		}
	}
	return filter.Sources(q, h, hasQ, hasH), nil
}

func toHumaErr(err error) error {
	if se, ok := err.(*store.StoreError); ok {
		switch se.Kind {
		case "invalid":
			return huma.Error400BadRequest(se.Message)
		case "notfound":
			return huma.Error404NotFound(se.Message)
		case "overloaded":
			return huma.Error429TooManyRequests("overloaded")
		}
		return huma.Error500InternalServerError("shard query failed")
	}
	if err == sql.ErrNoRows {
		return huma.Error404NotFound("not found")
	}
	return huma.Error500InternalServerError("shard query failed")
}

func itemsHandler(ctx context.Context, st *store.Store, in *ItemsInput, _ any) (*RawJSONResponse, error) {
	if err := st.Collection(in.Collection); err != nil {
		return nil, toHumaErr(err)
	}
	limit := in.Limit
	if in.Limit == 0 {
		limit = 10
	}
	if limit < 1 || limit > 1000 {
		return nil, huma.Error400BadRequest("limit must be 1..1000")
	}
	if in.Offset < 0 {
		return nil, huma.Error400BadRequest("offset must be >= 0")
	}
	var bounds *[4]float64
	if in.BBox != "" {
		b, err := filter.ParseBBox(in.BBox)
		if err != nil {
			return nil, huma.Error400BadRequest(err.Error())
		}
		bounds = &b
	}
	if in.Datetime != "" {
		if err := filter.ValidateDatetime(in.Datetime); err != nil {
			return nil, huma.Error400BadRequest(err.Error())
		}
	}
	sources, err := sourceIDs(in.Sources, in.SourceHeader)
	if err != nil {
		return nil, huma.Error400BadRequest(err.Error())
	}
	var pagination plan.Pagination
	if in.Cursor != "" {
		c := in.Cursor
		pagination = plan.Pagination{Cursor: &c}
	} else {
		pagination = plan.Pagination{Offset: uint32(in.Offset)}
	}
	var dt *string
	if in.Datetime != "" {
		dt = &in.Datetime
	}
	req := plan.ItemsRequest{
		Collection: in.Collection, Sources: sources, Bounds: bounds,
		Limit: uint32(limit), Pagination: pagination, Datetime: dt,
	}
	from := st.ReadSource(bounds)
	fetch := store.Predicate(in.Collection, bounds, sources)
	pageWhere, pageTail := plan.PageParts(fetch, uint32(limit), pagination)
	query := plan.ItemsSQL(req, pageWhere+" "+pageTail, from)
	var rows []featureRow
	heavy := plan.IsHeavy(uint32(limit), pagination, bounds)
	qerr := st.QueryRow(ctx, heavy, func(ctx context.Context, db *sql.DB) error {
		r, err := db.QueryContext(ctx, query)
		if err != nil {
			return err
		}
		defer r.Close()
		for r.Next() {
			var x featureRow
			if err := r.Scan(&x.id, &x.geom, &x.props); err != nil {
				return err
			}
			rows = append(rows, x)
		}
		return r.Err()
	})
	if qerr != nil {
		return nil, toHumaErr(qerr)
	}
	var next *string
	if len(rows) > limit {
		rows = rows[:limit]
		qs := req.CanonicalQS()
		// Advance cursor/offset for the next link.
		_ = qs
		last := rows[len(rows)-1].id
		var href string
		if pagination.Cursor != nil {
			nr := req
			nr.Pagination = plan.Pagination{Cursor: &last}
			href = nr.Href()
		} else {
			nr := req
			nr.Pagination = plan.Pagination{Offset: uint32(in.Offset) + uint32(limit)}
			href = nr.Href()
		}
		next = &href
	}
	body := renderCollection(req, rowsToFeatures(in.Collection, rows), next)
	_ = strconv.Itoa(limit)
	return rawJSON(body, GeoJSON), nil
}

func rowsToFeatures(collection string, rows []featureRow) []json.RawMessage {
	out := make([]json.RawMessage, 0, len(rows))
	for _, r := range rows {
		out = append(out, renderFeature(collection, r.id, r.geom, r.props))
	}
	return out
}

func renderFeature(collection, id, geomJSON, props string) json.RawMessage {
	var geom, p json.RawMessage
	_ = json.Unmarshal([]byte(geomJSON), &geom)
	if geom == nil {
		geom = json.RawMessage("null")
	}
	if props == "" {
		p = json.RawMessage("{}")
	} else {
		p = json.RawMessage(props)
	}
	feat := map[string]any{
		"type": "Feature", "id": id,
		"geometry": geom, "properties": p,
		"collection": collection,
	}
	b, _ := json.Marshal(feat)
	return b
}

func renderCollection(req plan.ItemsRequest, feats []json.RawMessage, next *string) []byte {
	links := []Link{{Href: req.Href(), Rel: "self"}}
	if next != nil {
		links = append(links, Link{Href: *next, Rel: "next"})
	}
	var buf bytes.Buffer
	buf.WriteString(`{"type":"FeatureCollection","features":[`)
	for i, f := range feats {
		if i > 0 {
			buf.WriteByte(',')
		}
		buf.Write(f)
	}
	buf.WriteString(`],"links":`)
	lb, _ := json.Marshal(links)
	buf.Write(lb)
	fmt.Fprintf(&buf, `,"numberReturned":%d}`, len(feats))
	return buf.Bytes()
}

// Ensure imports used.
var _ = strings.Contains
