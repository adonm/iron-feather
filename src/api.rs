//! OGC API Features Core / GeoJSON / OpenAPI 3.0 over the shared shard.

use crate::{
    filter,
    store::{Error, Store},
};
use bytes::Bytes;
use poem::{
    get, handler,
    http::{header, StatusCode},
    web::{Data, Path, Query},
    EndpointExt, Request, Response, Route,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

pub const GEOJSON: &str = "application/geo+json";
pub const CONFORMANCE: [&str; 3] = [
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
];

pub fn routes(store: Arc<Store>) -> impl poem::Endpoint {
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
        let mut response =
            json_response(json!({"code": self.status().as_u16(), "description": description}));
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
    Response::builder()
        .content_type(content_type)
        .header(header::VARY, "Accept, X-Source-Ids")
        .body(bytes)
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

const FEATURE_SQL: &str = "SELECT json_object('type', 'Feature', 'id', id, 'geometry', ST_AsGeoJSON(geom)::JSON, 'properties', properties::JSON) FROM features WHERE";

fn add_links(mut feature: Value, collection: &str, sources: &[i64]) -> Value {
    let sources = sources
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(",");
    let id = percent_encoding::utf8_percent_encode(
        feature["id"].as_str().unwrap(),
        percent_encoding::NON_ALPHANUMERIC,
    );
    let href = format!("/collections/{collection}/items/{id}?sources={sources}");
    feature["links"] = json!([
        link(&href, "self", GEOJSON),
        link(
            &format!("/collections/{collection}"),
            "collection",
            "application/json"
        )
    ]);
    feature
}

fn candidate_ids(conn: &duckdb::Connection, sql: &str) -> Result<Vec<String>, Error> {
    Ok(conn
        .prepare(sql)?
        .query_map([], |row| row.get(0))?
        .collect::<Result<Vec<_>, _>>()?)
}

fn features(
    conn: &duckdb::Connection,
    ids: &[String],
    collection: &str,
    sources: &[i64],
) -> Result<Vec<Value>, Error> {
    if ids.is_empty() {
        return Ok(vec![]);
    }
    let sql = format!(
        "{FEATURE_SQL} id IN ({}) ORDER BY id",
        ids.iter()
            .map(|id| filter::quote(id))
            .collect::<Vec<_>>()
            .join(",")
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |r| r.get::<_, String>(0))?;
    rows.map(|row| Ok(add_links(serde_json::from_str(&row?)?, collection, sources)))
        .collect()
}

#[handler]
async fn items(
    Path(collection): Path<String>,
    Query(mut query): Query<ItemsQuery>,
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
    let candidate_sql = format!(
        "SELECT id FROM features WHERE {} ORDER BY id LIMIT {} OFFSET {}",
        filter::predicate(&collection, bounds, &sources),
        query.limit + 1,
        query.offset
    );
    let href = req.uri().to_string();
    let key = format!("items:{href}:{sources:?}");
    let bytes = store.bytes(key, move |conn| {
        let mut ids = candidate_ids(conn, &candidate_sql)?;
        let has_next = ids.len() > query.limit as usize;
        ids.truncate(query.limit as usize);
        let page = features(conn, &ids, &collection, &sources)?;
        let mut links = vec![link(&href, "self", GEOJSON), link(&format!("/collections/{collection}"), "collection", "application/json")];
        if has_next {
            if let Some(offset) = query.offset.checked_add(query.limit) {
                query.offset = offset;
                let next = format!("/collections/{collection}/items?{}", serde_urlencoded::to_string(query).map_err(|e| Error::Backend(e.to_string()))?);
                links.push(link(&next, "next", GEOJSON));
            }
        }
        Ok(Bytes::from(serde_json::to_vec(&json!({"type": "FeatureCollection", "numberReturned": page.len(), "features": page, "links": links}))?))
    }).await?;
    Ok(response(bytes, kind))
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
    let sql = format!(
        "SELECT json_object('type', 'Feature', 'id', id, 'geometry', ST_AsGeoJSON(geom)::JSON, \
         'properties', properties::JSON), layer, source_id FROM features WHERE id = {} LIMIT 1",
        filter::quote(&id)
    );
    let key = format!("item:{collection}:{id}:{sources:?}");
    let bytes = store
        .bytes(key, move |conn| {
            let (raw, actual_collection, source_id): (String, String, i64) =
                match conn.query_row(&sql, [], |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?))) {
                    Ok(row) => row,
                    Err(duckdb::Error::QueryReturnedNoRows) => return Err(Error::NotFound(id)),
                    Err(error) => return Err(error.into()),
                };
            if actual_collection != collection || !sources.contains(&source_id) {
                return Err(Error::NotFound(id));
            }
            let feature = add_links(serde_json::from_str(&raw)?, &collection, &sources);
            Ok(Bytes::from(serde_json::to_vec(&feature)?))
        })
        .await?;
    Ok(response(bytes, kind))
}
