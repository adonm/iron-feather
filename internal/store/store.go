// Package store ports src/store.rs + src/db.rs: the shared DuckDB pool over
// a DuckLake snapshot mounted via mountpoint S3 CSI.
//
// Storage model (lakewing): reads resolve to local CSI mount paths
// (e.g. /mnt/lake/catalogs/<sha>.ducklake + /mnt/lake/data/...). There is
// no Cachey HTTP layer, no per-origin secrets, no --data-base sharding.
// Writes (build/index publish) go direct to S3 over the S3 API.
package store

import (
	"bytes"
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/adonm/lakewing/internal/filter"
	"github.com/adonm/lakewing/internal/index"
)

// StoreError mirrors store::Error.
type StoreError struct {
	Kind    string // invalid|notfound|overloaded|backend
	Message string
}

func (e *StoreError) Error() string { return e.Message }

func Invalid(msg string) *StoreError  { return &StoreError{Kind: "invalid", Message: msg} }
func NotFound(msg string) *StoreError { return &StoreError{Kind: "notfound", Message: msg} }
func Overloaded() *StoreError         { return &StoreError{Kind: "overloaded", Message: "server overloaded"} }
func Backend(msg string) *StoreError  { return &StoreError{Kind: "backend", Message: msg} }

// Config mirrors StoreConfig minus the Cachey fields: mount paths only.
type Config struct {
	Location     string // mount path to the .ducklake catalog
	DataRoot     string // mount path to the data root (DATA_PATH override)
	IndexJSON    *string
	Connections  int
	MaxWaiters   int
	MaxWait      time.Duration
	BulkLimit    int
	Threads      int64
	MemoryMB     uint64
	QueryTimeout time.Duration
}

// ResolvedIndex is a trusted serving index over mount-relative file paths.
type ResolvedIndex struct {
	Index index.ServingIndex
	Base  string // mount data root used to join relative paths
}

// Store is the shared read pool.
type Store struct {
	Collections  []string
	Snapshot     int64
	fallbackFrom string
	index        *ResolvedIndex

	db           *sql.DB
	sem          chan struct{}
	bulkSem      chan struct{}
	queued       atomic.Int64
	maxWaiters   int64
	maxWait      time.Duration
	queryTimeout time.Duration

	httpRequests atomic.Uint64
	tuning       [][2]string
}

// CachedBody mirrors store::CachedBody: strong ETag over exact bytes.
type CachedBody struct {
	Bytes []byte
	ETag  string
}

func WithBytes(b []byte) CachedBody {
	h := fnv.New64a()
	h.Write(b)
	return CachedBody{Bytes: b, ETag: fmt.Sprintf(`"%x-%d"`, h.Sum64(), len(b))}
}

// GzipBody compresses on a worker (callers run this off the pool).
func GzipBody(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(b); err != nil {
		return nil, err
	}
	if err := w.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// catalogURL mirrors store::catalog_url.
func catalogURL(location string) string {
	return "ducklake:" + strings.TrimRight(location, "/")
}

func attachOptions(snapshot *int64, dataPathOverride *string) string {
	opts := []string{"READ_ONLY"}
	if dataPathOverride != nil {
		base := *dataPathOverride
		if !strings.HasSuffix(base, "/") {
			base += "/"
		}
		opts = append(opts, "DATA_PATH "+filter.Quote(base), "OVERRIDE_DATA_PATH true")
	}
	if snapshot != nil {
		opts = append(opts, fmt.Sprintf("SNAPSHOT_VERSION %d", *snapshot))
	}
	return strings.Join(opts, ", ")
}

// Open mounts the shard at a pinned snapshot with a fixed-size pool.
// The duckdb-go driver name ("duckdb") is registered by the connector
// package; callers must blank-import it (see cmd/lakewing).
func Open(ctx context.Context, cfg Config) (*Store, error) {
	if cfg.Connections < 1 {
		return nil, Invalid("connections must be >= 1")
	}
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, Backend("open duckdb: " + err.Error())
	}
	db.SetMaxOpenConns(cfg.Connections)
	db.SetMaxIdleConns(cfg.Connections)
	db.SetConnMaxIdleTime(0)

	boot := func(q string) error {
		ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
		defer cancel()
		_, err := db.ExecContext(ctx, q)
		return err
	}
	// Extensions are baked into the image; autoinstall off by policy.
	for _, q := range []string{
		"LOAD spatial", "LOAD ducklake",
		"SET autoinstall_extension=false", "SET autoload_known_extensions=false",
		"SET parquet_metadata_cache=true",
	} {
		if err := boot(q); err != nil {
			db.Close()
			return nil, Backend("boot " + q + ": " + err.Error())
		}
	}
	if cfg.Threads >= 0 {
		if err := boot(fmt.Sprintf("SET threads=%d", cfg.Threads)); err != nil {
			db.Close()
			return nil, Backend(err.Error())
		}
	}
	if cfg.MemoryMB > 0 {
		if err := boot(fmt.Sprintf("SET memory_limit='%dMB'", cfg.MemoryMB)); err != nil {
			db.Close()
			return nil, Backend(err.Error())
		}
	}
	var dataOverride *string
	if cfg.DataRoot != "" {
		dataOverride = &cfg.DataRoot
	}
	// Attach latest, pin max(snapshot_id), re-attach pinned.
	if err := boot(fmt.Sprintf("ATTACH %s AS shard (%s)",
		filter.Quote(catalogURL(cfg.Location)), attachOptions(nil, dataOverride))); err != nil {
		db.Close()
		return nil, Backend("attach: " + err.Error())
	}
	var snapshot int64
	if err := db.QueryRowContext(ctx, "SELECT max(snapshot_id) FROM ducklake_snapshots('shard')").Scan(&snapshot); err != nil {
		db.Close()
		return nil, Backend("pin snapshot: " + err.Error())
	}
	if err := boot("DETACH shard"); err != nil {
		db.Close()
		return nil, Backend(err.Error())
	}
	if err := boot(fmt.Sprintf("ATTACH %s AS shard (%s)",
		filter.Quote(catalogURL(cfg.Location)), attachOptions(&snapshot, dataOverride))); err != nil {
		db.Close()
		return nil, Backend("attach pinned: " + err.Error())
	}
	if err := boot("USE shard"); err != nil {
		db.Close()
		return nil, Backend(err.Error())
	}

	s := &Store{
		Snapshot:     snapshot,
		db:           db,
		sem:          make(chan struct{}, cfg.Connections),
		maxWaiters:   int64(cfg.MaxWaiters),
		maxWait:      cfg.MaxWait,
		queryTimeout: cfg.QueryTimeout,
	}
	for i := 0; i < cfg.Connections; i++ {
		s.sem <- struct{}{}
	}
	if cfg.BulkLimit > 0 && cfg.BulkLimit < cfg.Connections {
		s.bulkSem = make(chan struct{}, cfg.BulkLimit)
		for i := 0; i < cfg.BulkLimit; i++ {
			s.bulkSem <- struct{}{}
		}
	}

	rows, err := db.QueryContext(ctx, "SELECT id FROM collections ORDER BY id")
	if err != nil {
		db.Close()
		return nil, Backend("collections: " + err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			db.Close()
			return nil, Backend(err.Error())
		}
		s.Collections = append(s.Collections, id)
	}
	if err := rows.Err(); err != nil {
		db.Close()
		return nil, Backend(err.Error())
	}

	// Frozen fallback: exact file list live at the pinned snapshot.
	files, err := s.resolveFiles(ctx, dataOverride)
	if err != nil {
		// Fail closed to the catalog table, as in Rust.
		s.fallbackFrom = "features"
	} else if len(files) == 0 {
		s.fallbackFrom = "features"
	} else {
		quoted := make([]string, len(files))
		for i, f := range files {
			quoted[i] = filter.Quote(f)
		}
		s.fallbackFrom = "read_parquet([" + strings.Join(quoted, ",") + "])"
	}

	// Serving index sidecar: mount-local read only.
	idxJSON := cfg.IndexJSON
	if idxJSON == nil {
		if raw, err := os.ReadFile(index.IndexPathFor(cfg.Location)); err == nil {
			str := string(raw)
			idxJSON = &str
		}
	}
	if idxJSON != nil {
		var doc index.ServingIndex
		if err := json.Unmarshal([]byte(*idxJSON), &doc); err == nil && s.checkIndex(doc, files) {
			base := cfg.DataRoot
			s.index = &ResolvedIndex{Index: doc, Base: base}
		}
	}

	for _, key := range []string{"threads", "memory_limit"} {
		var name, val string
		if err := db.QueryRowContext(ctx, "SELECT name, value FROM duckdb_settings() WHERE name='"+key+"'").Scan(&name, &val); err == nil {
			s.tuning = append(s.tuning, [2]string{name, val})
		}
	}
	return s, nil
}

func (s *Store) resolveFiles(ctx context.Context, dataOverride *string) ([]string, error) {
	// Footer min/max over the catalog table; falls back on any doubt.
	rows, err := s.db.QueryContext(ctx, "SELECT file_name FROM ducklake_files('shard') ORDER BY file_name")
	if err != nil {
		// Older catalogs: derive from parquet metadata via the table.
		return nil, err
	}
	defer rows.Close()
	var rels []string
	for rows.Next() {
		var f string
		if err := rows.Scan(&f); err != nil {
			return nil, err
		}
		rels = append(rels, f)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	base := ""
	if dataOverride != nil {
		base = strings.TrimRight(*dataOverride, "/") + "/"
	}
	out := make([]string, 0, len(rels))
	for _, r := range rels {
		r = strings.TrimLeft(r, "/")
		out = append(out, base+r)
	}
	sort.Strings(out)
	return out, nil
}

func (s *Store) checkIndex(doc index.ServingIndex, files []string) bool {
	if doc.Version != index.Version || doc.DuckLakeCommit != s.Snapshot {
		return false
	}
	if len(doc.Files) != len(files) {
		return false
	}
	// Exact file-order match required, else fall back.
	for i := range doc.Files {
		rel := strings.TrimLeft(doc.Files[i].Path, "/")
		if !strings.HasSuffix(files[i], rel) {
			return false
		}
	}
	return true
}

// Collection validates a collection id.
func (s *Store) Collection(id string) error {
	if !filter.CollectionID(id) {
		return Invalid("unknown collection")
	}
	for _, c := range s.Collections {
		if c == id {
			return nil
		}
	}
	return NotFound("unknown collection")
}

// ReadSource prunes to candidate mount files via the serving index.
func (s *Store) ReadSource(bounds *[4]float64) string {
	if s.index == nil {
		return s.fallbackFrom
	}
	if bounds == nil {
		return s.fallbackFrom
	}
	picks := index.PruneFiles(&s.index.Index, bounds)
	if len(picks) == 0 {
		// Footer-only probe: first file with FALSE still prunes row groups.
		first := strings.TrimLeft(s.index.Index.Files[0].Path, "/")
		base := strings.TrimRight(s.index.Base, "/")
		if base != "" {
			first = base + "/" + first
		}
		return "read_parquet([" + filter.Quote(first) + "]) WHERE FALSE"
	}
	if len(picks) == len(s.index.Index.Files) {
		return s.fallbackFrom
	}
	urls := make([]string, len(picks))
	base := strings.TrimRight(s.index.Base, "/")
	for i, p := range picks {
		rel := strings.TrimLeft(s.index.Index.Files[p].Path, "/")
		u := rel
		if base != "" {
			u = base + "/" + rel
		}
		urls[i] = filter.Quote(u)
	}
	return "read_parquet([" + strings.Join(urls, ",") + "])"
}

// Predicate builds the WHERE fragment with bbox-range pruning + exact
// ST_Intersects on boundary candidates, mirroring store::predicate.
func Predicate(collection string, bounds *[4]float64, sources []int64) string {
	base := filter.Predicate(collection, bounds, sources)
	if bounds == nil {
		return base
	}
	// Cheap range pruning AND (contained OR exact), as in Rust.
	return fmt.Sprintf("%s AND %s AND ((%s) OR (%s))",
		base, filter.BBoxOverlap(*bounds),
		filter.BBoxContained(*bounds), filter.SpatialPredicate(*bounds))
}

func (s *Store) acquire(ctx context.Context, bulk bool) (release func(), err error) {
	if bulk && s.bulkSem != nil {
		select {
		case <-s.bulkSem:
			defer func() {
				if err != nil {
					s.bulkSem <- struct{}{}
				}
			}()
		case <-ctx.Done():
			return nil, Overloaded()
		}
	}
	q := s.queued.Add(1)
	defer func() {
		if err != nil {
			s.queued.Add(-1)
		}
	}()
	if q > s.maxWaiters {
		return nil, Overloaded()
	}
	ctx, cancel := context.WithTimeout(ctx, s.maxWait)
	defer cancel()
	select {
	case <-s.sem:
		s.queued.Add(-1)
		bulkRelease := func() {}
		if bulk && s.bulkSem != nil {
			bulkRelease = func() { s.bulkSem <- struct{}{} }
		}
		return func() {
			s.sem <- struct{}{}
			bulkRelease()
		}, nil
	case <-ctx.Done():
		return nil, Overloaded()
	}
}

// QueryRow runs fn with one pooled connection and a deadline.
func (s *Store) QueryRow(ctx context.Context, bulk bool, fn func(ctx context.Context, db *sql.DB) error) error {
	release, err := s.acquire(ctx, bulk)
	if err != nil {
		return err
	}
	defer release()
	if s.queryTimeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, s.queryTimeout)
		defer cancel()
	}
	return fn(ctx, s.db)
}

// Metrics mirrors /metrics without consuming a pool connection.
func (s *Store) Metrics() string {
	var b strings.Builder
	fmt.Fprintf(&b, "http_requests %d\n", s.httpRequests.Load())
	for _, kv := range s.tuning {
		fmt.Fprintf(&b, "duck_setting_%s %s\n", kv[0], kv[1])
	}
	return b.String()
}

func (s *Store) CountRequest() { s.httpRequests.Add(1) }

// Close drains the pool.
func (s *Store) Close() error { return s.db.Close() }
