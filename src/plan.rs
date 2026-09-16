//! Normalized requests and backend-specific query planning.
//!
//! HTTP and Flight adapters validate first, then build one of these plans.
//! Cache keys derive from the normalized request so equivalent spellings
//! share an entry. SQL construction happens on a cache miss (or for
//! uncached Flight) inside the database worker.

use crate::filter;

/// Cursor pages skip leading rows through the id ordering; offset pages
/// traverse and discard them. Both stay supported.
#[derive(Debug, Clone)]
pub enum Pagination {
    Cursor(String),
    Offset(u32),
}

/// A validated items page: everything needed to plan, key and render it.
#[derive(Debug, Clone)]
pub struct ItemsRequest {
    pub collection: String,
    pub sources: Vec<i64>,
    pub bounds: Option<[f64; 4]>,
    pub limit: u32,
    pub pagination: Pagination,
    pub datetime: Option<String>,
}

impl ItemsRequest {
    pub fn canonical_query(&self) -> (u32, Option<String>) {
        match &self.pagination {
            Pagination::Cursor(cursor) => (0, Some(cursor.clone())),
            Pagination::Offset(offset) => (*offset, None),
        }
    }

    /// Canonical query string: equivalent requests share one cache entry.
    pub fn canonical_qs(&self) -> String {
        let (offset, cursor) = self.canonical_query();
        let bbox = self
            .bounds
            .map(|b| format!("{},{},{},{}", b[0], b[1], b[2], b[3]));
        let sources = self
            .sources
            .iter()
            .map(i64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        // Manual encoding keeps key generation independent of serde ordering.
        let mut parts = vec![format!("limit={}", self.limit), format!("offset={offset}")];
        if let Some(cursor) = cursor {
            parts.push(format!(
                "cursor={}",
                percent_encoding::utf8_percent_encode(&cursor, percent_encoding::NON_ALPHANUMERIC)
            ));
        }
        if let Some(bbox) = bbox {
            parts.push(format!(
                "bbox={}",
                percent_encoding::utf8_percent_encode(&bbox, percent_encoding::NON_ALPHANUMERIC)
                    .to_string()
                    .replace("%2C", ",")
                    .replace("%2E", ".")
                    .replace("%2D", "-")
            ));
        }
        if let Some(datetime) = &self.datetime {
            parts.push(format!("datetime={datetime}"));
        }
        parts.push(format!("sources={}", sources.replace(',', "%2C")));
        // Sort for stability across construction sites.
        parts.sort();
        parts.join("&")
    }

    pub fn cache_key(&self) -> String {
        format!("items:{}:{}", self.collection, self.canonical_qs())
    }

    pub fn href(&self) -> String {
        format!(
            "/collections/{}/items?{}",
            self.collection,
            self.canonical_qs()
        )
    }
}

/// Heavy pages hold a pool connection and DuckDB memory long enough to
/// starve interactive traffic, so they share the bulk admission lane with
/// Flight instead of the plain interactive queue.
pub fn is_heavy(limit: u32, pagination: &Pagination, bounds: Option<[f64; 4]>) -> bool {
    if limit > 100 {
        return true;
    }
    match pagination {
        Pagination::Offset(offset) if *offset >= 1000 => return true,
        Pagination::Cursor(_) => {}
        Pagination::Offset(_) => {}
    }
    if let Some([w, s, e, n]) = bounds {
        // Broad quarter/full-region slices: area in degrees is a cheap proxy.
        // City windows (even 2x2deg test boxes) stay interactive; region
        // quarters (~6deg²) and full extents go bulk.
        let width = if e >= w {
            e - w
        } else {
            (180.0 - w) + (e + 180.0)
        };
        let area = width * (n - s);
        if area >= 6.0 {
            return true;
        }
    } else if limit >= 100 {
        // Unbounded scans with a large page are bulk work.
        return true;
    }
    false
}

/// Native candidate selection: narrow id scan through the R-tree path.
pub fn native_candidate_sql(
    collection: &str,
    bounds: Option<[f64; 4]>,
    sources: &[i64],
    limit: u32,
    pagination: &Pagination,
) -> String {
    let base = filter::predicate(collection, bounds, sources);
    let (filter, tail) = page_parts(&base, limit, pagination);
    format!("SELECT id FROM features WHERE {filter} {tail}")
}

/// Native payload fetch through the single-column id index.
pub fn native_payload_sql(ids: &[String]) -> String {
    if ids.is_empty() {
        return "SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM features WHERE FALSE"
            .to_string();
    }
    format!(
        "SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM features WHERE id IN ({}) ORDER BY id",
        ids.iter()
            .map(|id| filter::quote(id))
            .collect::<Vec<_>>()
            .join(",")
    )
}

/// DuckLake page query: one predicate-preserving scan, page first and
/// conversion second so expensive projections run over page rows only.
#[allow(dead_code)]
pub fn lake_page_sql(
    collection: &str,
    bounds: Option<[f64; 4]>,
    sources: &[i64],
    projection: &str,
    limit: u32,
    pagination: &Pagination,
    lake_predicate: &str,
) -> String {
    let _ = (collection, bounds, sources);
    let (filter, tail) = page_parts(lake_predicate, limit, pagination);
    format!(
        "SELECT {projection} FROM (SELECT id, geom, properties, source_id, cx, cy, name FROM features WHERE {filter} {tail}) AS page ORDER BY page.id"
    )
}

/// Items page SQL for the lake backend (GeoJSON conversion outside the page).
pub fn lake_items_sql(req: &ItemsRequest, page_where: &str) -> String {
    let _ = req;
    format!(
        "SELECT id, ST_AsGeoJSON(geom), properties::VARCHAR FROM \
         (SELECT id, geom, properties FROM features WHERE {page_where}) AS page \
         ORDER BY id"
    )
}

fn page_parts(base: &str, limit: u32, pagination: &Pagination) -> (String, String) {
    match pagination {
        Pagination::Cursor(cursor) => (
            format!("{} AND id > {}", base, filter::quote(cursor)),
            format!("ORDER BY id LIMIT {}", limit + 1),
        ),
        Pagination::Offset(offset) => (
            base.to_string(),
            format!("ORDER BY id LIMIT {} OFFSET {}", limit + 1, offset),
        ),
    }
}

/// Tile cache key: coordinate plus effective source set.
pub fn tile_key(z: u8, x: u32, y: u32, collection: &str, sources: &[i64]) -> String {
    format!("tile:{z}:{x}:{y}:{collection}:{sources:?}")
}

/// Single-feature cache key.
pub fn item_key(collection: &str, id: &str, sources: &[i64]) -> String {
    format!("item:{collection}:{id}:{sources:?}")
}
