//! Storage backends behind one enum.
//!
//! `Store` is an enum (not `dyn`) because async fns are not object-safe.
//! `StubStore` serves synthetic demo data with no backend; the live
//! DuckDB+Turso implementation lives in [`crate::serve`] (`--features serve`).

use super::api::{CollectionMeta, Feature, FeatureCollection, Geometry, Link};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("overloaded: shedding load")]
    Overloaded,
    #[error("backend error: {0}")]
    Backend(String),
}

#[derive(Debug, Clone, Default)]
pub struct ItemQuery {
    pub bbox: Option<[f64; 4]>,
    pub limit: u32,
    pub offset: u32,
    pub datetime: Option<String>,
    pub filter: Option<String>,
    pub properties: Option<String>,
    /// Allowlisted source ids, from auth. Never from `filter`.
    pub source_ids: Vec<i64>,
}

pub enum Store {
    Stub(StubStore),
    #[cfg(feature = "serve")]
    Serve(super::serve::ServeStore),
}

impl Store {
    pub async fn collections(&self) -> Result<Vec<CollectionMeta>, StoreError> {
        match self {
            Store::Stub(store) => Ok(store.collections()),
            #[cfg(feature = "serve")]
            Store::Serve(store) => store.collections().await,
        }
    }

    pub async fn collection(&self, id: &str) -> Result<CollectionMeta, StoreError> {
        match self {
            Store::Stub(store) => store.collection(id),
            #[cfg(feature = "serve")]
            Store::Serve(store) => store.collection(id).await,
        }
    }

    pub async fn items(
        &self,
        collection: &str,
        query: &ItemQuery,
    ) -> Result<FeatureCollection, StoreError> {
        match self {
            Store::Stub(store) => store.items(collection, query),
            #[cfg(feature = "serve")]
            Store::Serve(store) => store.items(collection, query).await,
        }
    }

    pub async fn item(
        &self,
        collection: &str,
        id: &str,
        source_ids: &[i64],
    ) -> Result<Feature, StoreError> {
        match self {
            Store::Stub(store) => store.item(collection, id, source_ids),
            #[cfg(feature = "serve")]
            Store::Serve(store) => store.item(collection, id, source_ids).await,
        }
    }

    pub async fn tile(
        &self,
        collection: &str,
        bbox: [f64; 4],
        zoom: u8,
        source_ids: &[i64],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        match self {
            Store::Stub(store) => store.tile(collection, bbox, zoom, source_ids),
            #[cfg(feature = "serve")]
            Store::Serve(store) => store.tile(collection, bbox, zoom, source_ids).await,
        }
    }
}

/// Collections served by every backend in v0.
pub(crate) fn builtin_collections() -> Vec<CollectionMeta> {
    vec![
        CollectionMeta {
            id: "buildings".to_string(),
            title: "Buildings".to_string(),
            description: Some("Buildings and building parts.".to_string()),
            item_type: "feature".to_string(),
            links: vec![],
        },
        CollectionMeta {
            id: "ag_fields".to_string(),
            title: "Agricultural fields".to_string(),
            description: Some("Fields and subfields.".to_string()),
            item_type: "feature".to_string(),
            links: vec![],
        },
    ]
}

/// In-memory demo backend. Synthetic points only, so lineage has nothing to
/// leak; the live backend enforces it in SQL.
#[derive(Debug, Clone, Copy)]
pub struct StubStore;

impl StubStore {
    fn collections(&self) -> Vec<CollectionMeta> {
        builtin_collections()
    }

    fn collection(&self, id: &str) -> Result<CollectionMeta, StoreError> {
        self.collections()
            .into_iter()
            .find(|c| c.id == id)
            .ok_or_else(|| StoreError::NotFound(id.to_string()))
    }

    fn demo_features(collection: &str) -> Option<Vec<Feature>> {
        let point = |id: &str, lon: f64, lat: f64, props: &[(&str, &str)]| Feature {
            kind: "Feature".to_string(),
            id: id.to_string(),
            geometry: Some(Geometry {
                kind: "Point".to_string(),
                coordinates: vec![lon, lat],
            }),
            properties: props
                .iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
        };
        match collection {
            "buildings" => Some(vec![
                point(
                    "b1",
                    13.40,
                    52.52,
                    &[("use", "residential"), ("height_m", "21")],
                ),
                point(
                    "b2",
                    13.41,
                    52.53,
                    &[("use", "commercial"), ("height_m", "34")],
                ),
            ]),
            "ag_fields" => Some(vec![
                point("f1", 11.10, 49.40, &[("crop", "wheat")]),
                point("f2", 11.12, 49.41, &[("crop", "barley")]),
            ]),
            _ => None,
        }
    }

    fn items(&self, collection: &str, query: &ItemQuery) -> Result<FeatureCollection, StoreError> {
        let mut features = Self::demo_features(collection)
            .ok_or_else(|| StoreError::NotFound(collection.to_string()))?;
        // Stub honors bbox coarsely (point-in-box) so pagination demos behave.
        if let Some(b) = query.bbox {
            features.retain(|f| {
                f.geometry.as_ref().is_some_and(|g| {
                    g.coordinates.len() == 2
                        && g.coordinates[0] >= b[0]
                        && g.coordinates[0] <= b[2]
                        && g.coordinates[1] >= b[1]
                        && g.coordinates[1] <= b[3]
                })
            });
        }
        let start = (query.offset as usize).min(features.len());
        let end = start
            .saturating_add(query.limit as usize)
            .min(features.len());
        let page = features[start..end].to_vec();
        let number_returned = page.len() as i64;
        Ok(FeatureCollection {
            kind: "FeatureCollection".to_string(),
            features: page,
            links: vec![Link {
                href: String::new(),
                rel: String::new(),
                media_type: None,
                title: None,
            }],
            number_returned,
        })
    }

    fn item(&self, collection: &str, id: &str, _source_ids: &[i64]) -> Result<Feature, StoreError> {
        Self::demo_features(collection)
            .ok_or_else(|| StoreError::NotFound(collection.to_string()))?
            .into_iter()
            .find(|f| f.id == id)
            .ok_or_else(|| StoreError::NotFound(id.to_string()))
    }

    fn tile(
        &self,
        collection: &str,
        _bbox: [f64; 4],
        _zoom: u8,
        _source_ids: &[i64],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Self::demo_features(collection)
            .ok_or_else(|| StoreError::NotFound(collection.to_string()))?;
        // No tile bytes in stub mode: 204. Live tiles come from DuckDB ST_AsMVT.
        Ok(None)
    }
}
