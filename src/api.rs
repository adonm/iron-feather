//! OGC API Features Core / GeoJSON / OpenAPI 3.0 over the shared shard.

use crate::{
    filter,
    plan::{self, ItemsRequest, Pagination},
    store::{CachedBody, Error, QueryFn, Store},
};
use bytes::Bytes;
use poem::{
    get, handler,
    http::{header, StatusCode},
    web::{Data, Path, Query},
    EndpointExt, Request, Response, Route,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, value::RawValue, Value};
use std::sync::Arc;

pub const GEOJSON: &str = "application/geo+json";
const CACHE_CONTROL: &str = "public, max-age=60";

pub const CONFORMANCE: [&str; 3] = [
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
];

pub fn routes(store: Arc<Store>) -> impl poem::Endpoint {
    // Gzip variants are cached per representation (see body_response); MVT
    // tiles are compact protobuf and served identity-only.
    Route::new()
        .at("/", get(landing))
        .at("/conformance", get(conformance))
        .at("/api", get(spec))
        .at("/api.html", get(documentation))
        .at("/healthz", get(health))
        .at("/collections", get(collections))
        .at("/collections/:collection", get(collection_metadata))
        .at("/collections/:collection/items", get(items))
        .at("/collections/:collection/items/:id", get(item))
        .at(
            "/collections/:collection/tiles/:z/:x/:y",
            get(crate::tiles::tile),
        )
        .at("/metrics", get(metrics))
        .data(store)
}

impl poem::error::ResponseError for Error {
    fn status(&self) -> StatusCode {
        match self {
            Self::Invalid(_) => StatusCode::BAD_REQUEST,
            Self::NotFound(_) => StatusCode::NOT_FOUND,
            Self::Overloaded => StatusCode::TOO_MANY_REQUESTS,
            Self::Backend(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
    fn as_response(&self) -> Response {
        let description = if matches!(self, Self::Backend(_)) {
            tracing::error!(error = %self, "shard query failed");
            "shard query failed".into()
        } else {
            self.to_string()
        };
        let mut response = Response::builder()
            .content_type("application/json")
            .header(header::VARY, "Accept, Accept-Encoding, X-Source-Ids")
            // Errors describe this attempt, not the shard: never cacheable.
            .header(header::CACHE_CONTROL, "no-store")
            .body(Bytes::from(
                json!({"code": self.status().as_u16(), "description": description}).to_string(),
            ));
        response.set_status(self.status());
        if matches!(self, Self::Overloaded) {
            response
                .headers_mut()
                .insert(header::RETRY_AFTER, "1".parse().unwrap());
        }
        response
    }
}

pub fn response(bytes: Bytes, content_type: &str) -> Response {
    let body = CachedBody::with_bytes(bytes);
    identity_response(&body, content_type)
}

fn identity_response(body: &CachedBody, content_type: &str) -> Response {
    Response::builder()
        .content_type(content_type)
        .header(header::VARY, "Accept, Accept-Encoding, X-Source-Ids")
        .header(header::ETAG, body.etag.clone())
        .header(header::CACHE_CONTROL, CACHE_CONTROL)
        .body(body.bytes.clone())
}

fn gzip_response(body: &CachedBody, content_type: &str) -> Response {
    Response::builder()
        .content_type(content_type)
        .header(header::CONTENT_ENCODING, "gzip")
        .header(header::VARY, "Accept, Accept-Encoding, X-Source-Ids")
        .header(header::ETAG, body.etag.clone())
        .header(header::CACHE_CONTROL, CACHE_CONTROL)
        .body(body.bytes.clone())
}

fn not_modified(etag: &str) -> Response {
    Response::builder()
        .status(StatusCode::NOT_MODIFIED)
        .header(header::VARY, "Accept, Accept-Encoding, X-Source-Ids")
        .header(header::ETAG, etag.to_owned())
        .header(header::CACHE_CONTROL, CACHE_CONTROL)
        .finish()
}

fn etag_matches(header: &str, etag: &str) -> bool {
    let wanted = etag.trim_matches('"');
    header.split(',').any(|candidate| {
        let candidate = candidate.trim();
        if candidate == "*" {
            return true;
        }
        candidate
            .strip_prefix("W/")
            .unwrap_or(candidate)
            .trim_matches('"')
            == wanted
    })
}

/// Skip the body when the client already holds these exact bytes. Each
/// representation carries its own strong validator, so identity and gzip
/// variants never share an ETag.
pub fn conditional_response(req: &Request, body: &CachedBody, content_type: &str) -> Response {
    if if_none_match(req, &body.etag) {
        not_modified(&body.etag)
    } else {
        identity_response(body, content_type)
    }
}

/// Serve a cached body, negotiating a stored gzip variant when the client
/// accepts it. The encoding is checked first: a surviving gzip entry serves
/// without touching the identity bytes or the database. The compressed
/// variant shares the same byte budget; `compressed` does no database work
/// and consumes no pool connection.
pub async fn body_response(
    req: &Request,
    store: &Store,
    key: String,
    heavy: bool,
    query: QueryFn,
    content_type: &str,
) -> Result<Response, Error> {
    store.note_http();
    if wants_gzip(req) {
        let gzip_key = format!("{key}:gzip");
        if let Some(hit) = store.get(&gzip_key).await {
            return Ok(if if_none_match(req, &hit.etag) {
                not_modified(&hit.etag)
            } else {
                gzip_response(&hit, content_type)
            });
        }
    }
    let body = store.bytes(key.clone(), heavy, query).await?;
    if !wants_gzip(req) {
        return Ok(conditional_response(req, &body, content_type));
    }
    let gzip_key = format!("{key}:gzip");
    let raw = body.bytes.clone();
    let gzipped = store.compressed(gzip_key, raw).await?;
    if if_none_match(req, &gzipped.etag) {
        Ok(not_modified(&gzipped.etag))
    } else {
        Ok(gzip_response(&gzipped, content_type))
    }
}

fn if_none_match(req: &Request, etag: &str) -> bool {
    req.headers()
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| etag_matches(value, etag))
}

fn wants_gzip(req: &Request) -> bool {
    let Some(header) = req
        .headers()
        .get(header::ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    header.split(',').any(|range| {
        let mut parts = range.trim().split(';');
        if !parts
            .next()
            .unwrap_or_default()
            .trim()
            .eq_ignore_ascii_case("gzip")
        {
            return false;
        }
        let quality = parts
            .find_map(|parameter| parameter.trim().strip_prefix("q="))
            .map(|quality| quality.parse::<f32>().unwrap_or(0.0))
            .unwrap_or(1.0);
        quality > 0.0
    })
}
fn json_response(value: Value) -> Response {
    response(Bytes::from(value.to_string()), "application/json")
}
fn link(href: &str, rel: &str, kind: &str) -> Value {
    json!({"href": href, "rel": rel, "type": kind})
}
fn metadata(id: &str) -> Value {
    json!({"id": id, "title": id, "itemType": "feature", "links": [
        link(&format!("/collections/{id}"), "self", "application/json"),
        link(&format!("/collections/{id}/items"), "items", GEOJSON)
    ]})
}

#[handler]
fn landing(Query(_): Query<NoQuery>) -> Response {
    json_response(
        json!({"title": "Iron Feather", "description": "OGC Features and Arrow Flight over a DuckDB shard", "links": [
            link("/", "self", "application/json"), link("/api", "service-desc", "application/vnd.oai.openapi+json;version=3.0"),
            link("/api.html", "service-doc", "text/html"), link("/conformance", "conformance", "application/json"), link("/collections", "data", "application/json")
        ]}),
    )
}
#[handler]
fn conformance(Query(_): Query<NoQuery>) -> Response {
    json_response(json!({"conformsTo": CONFORMANCE}))
}
#[handler]
fn health(Query(_): Query<NoQuery>) -> &'static str {
    "ok"
}
/// Point-in-time pool cache counters for capacity experiments. Plain text
/// with `no-store`: hit rate over lookups is `(hits + coalesced) /
/// requests`; `http_requests` counts HTTP responses attempted (one request
/// can cause up to two lookups: gzip then identity); `failures` counts miss
/// executions that errored and were not cached. `duck_*` lines report the
/// DuckDB external-file-cache occupancy (ranges/bytes) best-effort.
#[handler]
async fn metrics(Query(_): Query<NoQuery>, Data(store): Data<&Arc<Store>>) -> Response {
    let stats = store.cache_stats();
    let duck = store.duck_cache_stats().await;
    let tuning = store.duck_tuning().await;
    let mut out = format!(
        "cache_entries {}\ncache_weight_bytes {}\ncache_requests {}\nhttp_requests {}\ncache_hits {}\ncache_coalesced {}\ncache_computes {}\ncache_failures {}\ncache_evictions {}\nduck_external_cache_ranges {}\nduck_external_cache_bytes {}\n",
        stats.entries,
        stats.weight_bytes,
        stats.requests,
        stats.http_requests,
        stats.hits,
        stats.coalesced,
        stats.computes,
        stats.failures,
        stats.evictions,
        duck.ranges,
        duck.bytes,
    );
    for (key, value) in tuning {
        out.push_str(&format!("duck_setting_{key} {value}\n"));
    }
    Response::builder()
        .content_type("text/plain; charset=utf-8")
        .header(header::CACHE_CONTROL, "no-store")
        .header(header::VARY, "Accept, Accept-Encoding, X-Source-Ids")
        .body(Bytes::from(out.into_bytes()))
}
#[handler]
fn spec(Query(_): Query<NoQuery>) -> Response {
    response(
        Bytes::from_static(include_bytes!("../docs/openapi.json")),
        "application/vnd.oai.openapi+json;version=3.0",
    )
}
#[handler]
fn documentation(Query(_): Query<NoQuery>) -> Response {
    response(
        Bytes::from_static(include_bytes!("../docs/api.html")),
        "text/html",
    )
}
#[handler]
fn collections(Query(_): Query<NoQuery>, Data(store): Data<&Arc<Store>>) -> Response {
    json_response(
        json!({"collections": store.collections.iter().map(|id| metadata(id)).collect::<Vec<_>>(),
        "links": [link("/collections", "self", "application/json")]}),
    )
}
#[handler]
fn collection_metadata(
    Path(id): Path<String>,
    Query(_): Query<NoQuery>,
    Data(store): Data<&Arc<Store>>,
) -> Result<Response, Error> {
    store.collection(&id)?;
    Ok(json_response(metadata(&id)))
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ItemsQuery {
    bbox: Option<String>,
    #[serde(default = "default_limit")]
    limit: u32,
    #[serde(default)]
    offset: u32,
    /// Exclusive lower bound on feature id. Takes precedence over `offset`
    /// and is what `next` links emit, so deep pages skip leading rows
    /// through the id index instead of traversing them with `OFFSET`.
    cursor: Option<String>,
    datetime: Option<String>,
    sources: Option<String>,
}
fn default_limit() -> u32 {
    10
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct NoQuery {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceQuery {
    pub sources: Option<String>,
}

pub fn source_ids(req: &Request, requested: Option<&str>) -> Result<Vec<i64>, Error> {
    let header = req
        .headers()
        .get("x-source-ids")
        .map(|v| v.to_str().map_err(|e| Error::Invalid(e.to_string())))
        .transpose()?;
    Ok(filter::sources(
        requested
            .map(filter::parse_sources)
            .transpose()
            .map_err(Error::Invalid)?,
        header
            .map(filter::parse_sources)
            .transpose()
            .map_err(Error::Invalid)?,
    ))
}

/// Respect the most specific Accept range (including q=0 exclusions).
fn geojson_type(req: &Request) -> Result<&'static str, poem::Error> {
    let Some(accept) = req.headers().get(header::ACCEPT) else {
        return Ok(GEOJSON);
    };
    let accept = accept.to_str().unwrap_or_default();
    let mut matched = (-1, 0.0);
    for range in accept.split(',') {
        let mut parts = range.trim().split(';');
        let media = parts.next().unwrap_or_default().trim();
        let specificity = if media == GEOJSON {
            2
        } else if media == "application/*" {
            1
        } else if media == "*/*" {
            0
        } else {
            continue;
        };
        let q = parts
            .find_map(|p| p.trim().strip_prefix("q="))
            .map_or(1.0, |q| q.parse::<f32>().unwrap_or(0.0));
        if specificity > matched.0 {
            matched = (specificity, q);
        }
    }
    if matched.1 > 0.0 && matched.1 <= 1.0 {
        Ok(GEOJSON)
    } else {
        Err(poem::Error::from_status(StatusCode::NOT_ACCEPTABLE))
    }
}

/// Geometry and properties arrive as JSON text; embedding them through
/// [`RawValue`] avoids parsing large coordinate arrays into a DOM only to
/// serialize them again.
const FEATURE_COLUMNS: &str = "id, ST_AsGeoJSON(geom), properties::VARCHAR";

#[derive(Serialize)]
struct Feature {
    #[serde(rename = "type")]
    kind: &'static str,
    id: String,
    geometry: Box<RawValue>,
    properties: Box<RawValue>,
    links: Value,
}

#[derive(Serialize)]
struct FeatureCollection {
    #[serde(rename = "type")]
    kind: &'static str,
    #[serde(rename = "numberReturned")]
    number_returned: usize,
    features: Vec<Feature>,
    links: Vec<Value>,
}

fn feature_links(id: &str, collection: &str, sources: &[i64]) -> Value {
    let sources = sources
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let id = percent_encoding::utf8_percent_encode(id, percent_encoding::NON_ALPHANUMERIC);
    let href = format!("/collections/{collection}/items/{id}?sources={sources}");
    json!([
        link(&href, "self", GEOJSON),
        link(
            &format!("/collections/{collection}"),
            "collection",
            "application/json"
        )
    ])
}

fn feature(
    id: String,
    geometry: String,
    properties: String,
    links: Value,
) -> Result<Feature, Error> {
    Ok(Feature {
        kind: "Feature",
        id,
        geometry: RawValue::from_string(geometry)?,
        properties: RawValue::from_string(properties)?,
        links,
    })
}

fn candidate_ids(conn: &duckdb::Connection, sql: &str) -> Result<Vec<String>, Error> {
    Ok(conn
        .prepare(sql)?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?)
}

fn fetch_rows(
    conn: &duckdb::Connection,
    sql: &str,
) -> Result<Vec<(String, String, String)>, Error> {
    let mut stmt = conn.prepare(sql)?;
    let rows = stmt.query_map([], |r| {
        Ok((
            r.get::<_, String>(0)?,
            r.get::<_, Option<String>>(1)?
                .unwrap_or_else(|| "null".into()),
            r.get::<_, Option<String>>(2)?
                .unwrap_or_else(|| "null".into()),
        ))
    })?;
    rows.collect::<Result<Vec<_>, _>>().map_err(Error::from)
}

fn rows_to_features(
    rows: Vec<(String, String, String)>,
    collection: &str,
    sources: &[i64],
) -> Result<Vec<Feature>, Error> {
    rows.into_iter()
        .map(|(id, geometry, properties)| {
            let links = feature_links(&id, collection, sources);
            feature(id, geometry, properties, links)
        })
        .collect()
}

fn features(
    conn: &duckdb::Connection,
    ids: &[String],
    collection: &str,
    sources: &[i64],
) -> Result<Vec<Feature>, Error> {
    if ids.is_empty() {
        return Ok(vec![]);
    }
    let sql = format!(
        "SELECT {FEATURE_COLUMNS} FROM features WHERE id IN ({}) ORDER BY id",
        ids.iter()
            .map(|id| filter::quote(id))
            .collect::<Vec<_>>()
            .join(",")
    );
    rows_to_features(fetch_rows(conn, &sql)?, collection, sources)
}

/// Normalized page inputs live in [`plan::ItemsRequest`]: equivalent
/// requests share one cache entry however the client ordered its query
/// string.

#[handler]
async fn items(
    Path(collection): Path<String>,
    Query(query): Query<ItemsQuery>,
    req: &Request,
    Data(store): Data<&Arc<Store>>,
) -> poem::Result<Response> {
    store.collection(&collection)?;
    let kind = geojson_type(req)?;
    if !(1..=1000).contains(&query.limit) {
        return Err(Error::Invalid("limit must be between 1 and 1000".into()).into());
    }
    let bounds = query
        .bbox
        .as_deref()
        .map(filter::parse_bbox)
        .transpose()
        .map_err(Error::Invalid)?;
    if let Some(raw) = &query.datetime {
        filter::datetime(raw).map_err(Error::Invalid)?;
    }
    let sources = source_ids(req, query.sources.as_deref())?;
    // Normalized request: equivalent spellings share one cache entry and
    // one body. A cursor wins over any offset.
    let pagination = match query.cursor.clone() {
        Some(cursor) => Pagination::Cursor(cursor),
        None => Pagination::Offset(query.offset),
    };
    let normalized = ItemsRequest {
        collection: collection.clone(),
        sources: sources.clone(),
        bounds,
        limit: query.limit,
        pagination: pagination.clone(),
        datetime: query.datetime.clone(),
    };
    let href = normalized.href();
    let key = normalized.cache_key();
    let heavy = plan::is_heavy(query.limit, &pagination, bounds);
    let started = std::time::Instant::now();
    // SQL construction happens on miss inside the closure: hits pay for
    // validation, canonical-key creation, lookup and response assembly only.
    // A cursor bounds the id range, so DuckDB starts at the page instead of
    // discarding `offset` leading rows. Offset stays for direct links.
    let lake = store.is_lake();
    let limit = query.limit;
    let query: QueryFn = Box::new(move |conn: &duckdb::Connection| {
        let selected = std::time::Instant::now();
        let req_owned = normalized.clone();
        let pagination = req_owned.pagination.clone();
        let base_predicate = if lake {
            Store::lake_predicate(&req_owned.collection, req_owned.bounds, &req_owned.sources)
        } else {
            filter::predicate(&req_owned.collection, req_owned.bounds, &req_owned.sources)
        };
        let (page_filter, page_tail) = match &pagination {
            Pagination::Cursor(cursor) => (
                format!("{} AND id > {}", base_predicate, filter::quote(cursor)),
                format!("ORDER BY id LIMIT {}", limit + 1),
            ),
            Pagination::Offset(offset) => (
                base_predicate,
                format!("ORDER BY id LIMIT {} OFFSET {}", limit + 1, offset),
            ),
        };
        // DuckLake has no ART index for the two-step fetch, so the page
        // payload comes from the same predicate-preserving scan. Page first,
        // convert second: the inner scan selects raw columns for the page,
        // the outer converts only those rows to GeoJSON.
        let lake_sql = plan::lake_items_sql(&req_owned, &format!("{page_filter} {page_tail}"));
        let candidate_sql = plan::native_candidate_sql(
            &req_owned.collection,
            req_owned.bounds,
            &req_owned.sources,
            limit,
            &pagination,
        );
        let (page, has_next, last_id): (Vec<Feature>, bool, Option<String>) = if lake {
            let rows = fetch_rows(conn, &lake_sql)?;
            let has_next = rows.len() > limit as usize;
            let mut rows = rows;
            rows.truncate(limit as usize);
            let last_id = rows.last().map(|row| row.0.clone());
            let fetched = std::time::Instant::now();
            let page = rows_to_features(rows, &req_owned.collection, &req_owned.sources)?;
            let assembled = std::time::Instant::now();
            tracing::debug!(
                candidate_us = fetched.duration_since(selected).as_micros(),
                fetch_us = assembled.duration_since(fetched).as_micros(),
                encode_us = 0,
                "ogc items render"
            );
            (page, has_next, last_id)
        } else {
            let mut ids = candidate_ids(conn, &candidate_sql)?;
            let has_next = ids.len() > limit as usize;
            ids.truncate(limit as usize);
            let last_id = ids.last().cloned();
            let fetched = std::time::Instant::now();
            let page = features(conn, &ids, &req_owned.collection, &req_owned.sources)?;
            let assembled = std::time::Instant::now();
            tracing::debug!(
                candidate_us = fetched.duration_since(selected).as_micros(),
                fetch_us = assembled.duration_since(fetched).as_micros(),
                encode_us = 0,
                "ogc items render"
            );
            (page, has_next, last_id)
        };
        let number_returned = page.len();
        let mut links = vec![
            link(&href, "self", GEOJSON),
            link(
                &format!("/collections/{}", req_owned.collection),
                "collection",
                "application/json",
            ),
        ];
        if has_next {
            // The next page resumes after the last id on this page: the
            // database traverses fewer discarded rows than with OFFSET.
            if let Some(last) = last_id.as_ref() {
                let next = ItemsRequest {
                    pagination: Pagination::Cursor(last.clone()),
                    ..req_owned.clone()
                };
                links.push(link(
                    &format!(
                        "/collections/{}/items?{}",
                        req_owned.collection,
                        next.canonical_qs()
                    ),
                    "next",
                    GEOJSON,
                ));
            }
        }
        let rendered = FeatureCollection {
            kind: "FeatureCollection",
            number_returned,
            features: page,
            links,
        };
        let encode_started = std::time::Instant::now();
        let bytes = Bytes::from(serde_json::to_vec(&rendered)?);
        tracing::debug!(
            encode_us = encode_started.elapsed().as_micros(),
            "ogc items encode"
        );
        Ok(bytes)
    });
    let response = body_response(req, store, key, heavy, query, kind).await;
    tracing::debug!(total_ms = started.elapsed().as_millis(), "ogc items serve");
    response.map_err(|e| e.into())
}

#[handler]
async fn item(
    Path((collection, id)): Path<(String, String)>,
    Query(query): Query<SourceQuery>,
    req: &Request,
    Data(store): Data<&Arc<Store>>,
) -> poem::Result<Response> {
    store.collection(&collection)?;
    let kind = geojson_type(req)?;
    let sources = source_ids(req, query.sources.as_deref())?;
    // Key and SQL both derive from the normalized inputs; SQL builds on
    // miss so hits pay validation + lookup only.
    let key = plan::item_key(&collection, &id, &sources);
    let query: QueryFn = Box::new(move |conn: &duckdb::Connection| {
        let sql = format!(
            "SELECT {FEATURE_COLUMNS}, layer, source_id FROM features WHERE id = {} LIMIT 1",
            filter::quote(&id)
        );
        let (raw_id, geometry, properties, actual_collection, source_id): (
            String,
            Option<String>,
            Option<String>,
            String,
            i64,
        ) = match conn.query_row(&sql, [], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
            ))
        }) {
            Ok(row) => row,
            Err(duckdb::Error::QueryReturnedNoRows) => return Err(Error::NotFound(id)),
            Err(error) => return Err(error.into()),
        };
        if actual_collection != collection || !sources.contains(&source_id) {
            return Err(Error::NotFound(id));
        }
        let links = feature_links(&raw_id, &collection, &sources);
        let single = feature(
            raw_id,
            geometry.unwrap_or_else(|| "null".into()),
            properties.unwrap_or_else(|| "null".into()),
            links,
        )?;
        Ok(Bytes::from(serde_json::to_vec(&single)?))
    });
    body_response(req, store, key, false, query, kind)
        .await
        .map_err(|e| e.into())
}
