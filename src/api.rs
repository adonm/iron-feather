//! OGC API Features surface: landing, conformance, collections, items.
//!
//! The OpenAPI contract is derived by `poem-openapi` and served at `/api`
//! (TiPG-style). Tiles live on plain-Poem routes in [`crate::tiles`] so the
//! MVT content type is set exactly.

use crate::{
    filter,
    store::{ItemQuery, Store, StoreError},
};
use poem_openapi::{
    param::{Path, Query},
    payload::Json,
    ApiResponse, Object, OpenApi,
};
use std::{collections::BTreeMap, sync::Arc};

pub const CONFORMANCE_CLASSES: [&str; 3] = [
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/core",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/geojson",
    "http://www.opengis.net/spec/ogcapi-features-1/1.0/conf/oas30",
];

#[derive(Object, Clone, Debug)]
pub struct Link {
    pub href: String,
    pub rel: String,
    #[oai(rename = "type")]
    pub media_type: Option<String>,
    pub title: Option<String>,
}

impl Link {
    fn new(href: &str, rel: &str, media_type: Option<&str>, title: Option<&str>) -> Self {
        Self {
            href: href.to_string(),
            rel: rel.to_string(),
            media_type: media_type.map(str::to_string),
            title: title.map(str::to_string),
        }
    }
}

#[derive(Object, Clone, Debug)]
pub struct Landing {
    pub title: String,
    pub description: String,
    pub links: Vec<Link>,
}

#[derive(Object, Clone, Debug)]
pub struct Conformance {
    #[oai(rename = "conformsTo")]
    pub conforms_to: Vec<String>,
}

#[derive(Object, Clone, Debug)]
pub struct CollectionMeta {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    #[oai(rename = "itemType")]
    pub item_type: String,
    pub links: Vec<Link>,
}

#[derive(Object, Clone, Debug)]
pub struct Collections {
    pub collections: Vec<CollectionMeta>,
    pub links: Vec<Link>,
}

#[derive(Object, Clone, Debug)]
pub struct Geometry {
    #[oai(rename = "type")]
    pub kind: String,
    pub coordinates: Vec<f64>,
}

#[derive(Object, Clone, Debug)]
pub struct Feature {
    #[oai(rename = "type")]
    pub kind: String,
    pub id: String,
    pub geometry: Option<Geometry>,
    pub properties: BTreeMap<String, String>,
}

#[derive(Object, Clone, Debug)]
pub struct FeatureCollection {
    #[oai(rename = "type")]
    pub kind: String,
    pub features: Vec<Feature>,
    pub links: Vec<Link>,
    // NOTE: `numberMatched` deliberately omitted. OGC Core permits omission
    // and exact total counts are the most expensive part of a page.
    #[oai(rename = "numberReturned")]
    pub number_returned: i64,
}

#[derive(Object, Clone, Debug)]
pub struct ErrorBody {
    pub code: String,
    pub description: String,
}

impl ErrorBody {
    fn new(code: &str, description: impl Into<String>) -> Self {
        Self {
            code: code.to_string(),
            description: description.into(),
        }
    }
}

#[derive(ApiResponse)]
pub enum CollectionResponse {
    #[oai(status = 200)]
    Ok(Json<CollectionMeta>),
    #[oai(status = 404)]
    NotFound(Json<ErrorBody>),
    #[oai(status = 500)]
    Internal(Json<ErrorBody>),
}

#[derive(ApiResponse)]
pub enum ItemsResponse {
    #[oai(status = 200)]
    Ok(Json<FeatureCollection>),
    #[oai(status = 400)]
    BadRequest(Json<ErrorBody>),
    #[oai(status = 404)]
    NotFound(Json<ErrorBody>),
    #[oai(status = 500)]
    Internal(Json<ErrorBody>),
}

#[derive(ApiResponse)]
pub enum ItemResponse {
    #[oai(status = 200)]
    Ok(Json<Feature>),
    #[oai(status = 404)]
    NotFound(Json<ErrorBody>),
    #[oai(status = 500)]
    Internal(Json<ErrorBody>),
}

#[derive(Clone)]
pub struct Api {
    pub store: Arc<Store>,
    pub serving_version: String,
}

fn backend_error(e: StoreError) -> (String, String) {
    match e {
        StoreError::NotFound(id) => ("not-found".to_string(), format!("unknown collection: {id}")),
        // No Retry-After on JSON errors (ApiResponse can't set headers);
        // clients must back off on code == "overloaded". Tiles use 503.
        StoreError::Overloaded => (
            "overloaded".to_string(),
            "server shedding load; retry".to_string(),
        ),
        StoreError::Backend(msg) => ("backend".to_string(), msg),
    }
}

#[OpenApi]
impl Api {
    #[oai(path = "/", method = "get")]
    async fn landing(&self) -> Json<Landing> {
        Json(Landing {
            title: "Iron Feather".to_string(),
            description: "TiPG-esque OGC Features + tiles over DuckDB shards.".to_string(),
            links: vec![
                Link::new("/", "self", Some("application/json"), Some("landing")),
                Link::new(
                    "/api",
                    "service-desc",
                    Some("application/json"),
                    Some("OpenAPI contract"),
                ),
                Link::new(
                    "/conformance",
                    "conformance",
                    Some("application/json"),
                    Some("conformance"),
                ),
                Link::new(
                    "/collections",
                    "data",
                    Some("application/json"),
                    Some("collections"),
                ),
            ],
        })
    }

    #[oai(path = "/conformance", method = "get")]
    async fn conformance(&self) -> Json<Conformance> {
        Json(Conformance {
            conforms_to: CONFORMANCE_CLASSES.iter().map(|s| s.to_string()).collect(),
        })
    }

    #[oai(path = "/collections", method = "get")]
    async fn collections(&self) -> Json<Collections> {
        let collections = self.store.collections().await.unwrap_or_default();
        Json(Collections {
            collections,
            links: vec![Link::new(
                "/collections",
                "self",
                Some("application/json"),
                None,
            )],
        })
    }

    #[oai(path = "/collections/:collection_id", method = "get")]
    async fn collection(&self, collection_id: Path<String>) -> CollectionResponse {
        match self.store.collection(&collection_id.0).await {
            Ok(meta) => CollectionResponse::Ok(Json(meta)),
            Err(StoreError::NotFound(id)) => CollectionResponse::NotFound(Json(ErrorBody::new(
                "not-found",
                format!("unknown collection: {id}"),
            ))),
            Err(e) => {
                let (code, description) = backend_error(e);
                CollectionResponse::Internal(Json(ErrorBody::new(&code, description)))
            }
        }
    }

    #[oai(path = "/collections/:collection_id/items", method = "get")]
    #[allow(clippy::too_many_arguments)]
    async fn items(
        &self,
        collection_id: Path<String>,
        bbox: Query<Option<String>>,
        limit: Query<Option<u32>>,
        offset: Query<Option<u32>>,
        datetime: Query<Option<String>>,
        filter: Query<Option<String>>,
        properties: Query<Option<String>>,
        sources: Query<Option<String>>,
    ) -> ItemsResponse {
        let bbox = match bbox.0.as_deref().map(filter::parse_bbox).transpose() {
            Ok(b) => b,
            Err(e) => {
                return ItemsResponse::BadRequest(Json(ErrorBody::new("bad-bbox", e)));
            }
        };
        let query = ItemQuery {
            bbox,
            limit: limit.0.unwrap_or(10).min(1000),
            offset: offset.0.unwrap_or(0),
            datetime: datetime.0,
            filter: filter.0,
            properties: properties.0,
            // DEV STAND-IN: comma-separated source ids. Absent/empty matches
            // nothing (secure default). Prod replaces this with a JWT/OIDC
            // Bearer securityScheme injecting the caller's lineage set.
            source_ids: filter::parse_source_list(sources.0.as_deref()),
        };
        match self.store.items(&collection_id.0, &query).await {
            Ok(fc) => ItemsResponse::Ok(Json(fc)),
            Err(StoreError::NotFound(id)) => ItemsResponse::NotFound(Json(ErrorBody::new(
                "not-found",
                format!("unknown collection: {id}"),
            ))),
            Err(e) => {
                let (code, description) = backend_error(e);
                ItemsResponse::Internal(Json(ErrorBody::new(&code, description)))
            }
        }
    }

    #[oai(path = "/collections/:collection_id/items/:feature_id", method = "get")]
    async fn item(
        &self,
        collection_id: Path<String>,
        feature_id: Path<String>,
        sources: Query<Option<String>>,
    ) -> ItemResponse {
        let source_ids = filter::parse_source_list(sources.0.as_deref());
        match self
            .store
            .item(&collection_id.0, &feature_id.0, &source_ids)
            .await
        {
            Ok(feature) => ItemResponse::Ok(Json(feature)),
            Err(StoreError::NotFound(id)) => ItemResponse::NotFound(Json(ErrorBody::new(
                "not-found",
                format!("not found: {id}"),
            ))),
            Err(e) => {
                let (code, description) = backend_error(e);
                ItemResponse::Internal(Json(ErrorBody::new(&code, description)))
            }
        }
    }
}
