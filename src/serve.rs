//! Live backends behind `--features serve`.
//!
//! * DuckDB links PREBUILT libduckdb (see `just setup-duckdb`; the crate's
//!   `bundled` C++ build is deliberately off). One versioned shard file is
//!   opened here; sharded routing across many files graduates next.
//! * Reads run on a small connection pool (8): DuckDB parallelizes across
//!   connections, while one shared connection would serialize everything.
//! * Turso/libSQL is the fragment L2 in front of DuckDB; moka L1s sit above
//!   it for tiles, item pages, and single features. Everything sits behind
//!   [`crate::guard::RequestGuard`], which coalesces identical concurrent
//!   requests and sheds overflow as 429/overloaded instead of piling onto
//!   the worker.
//!
//! Expected shard schema (built by the materialization pipeline, see README):
//! `features(id VARCHAR, layer VARCHAR, source_id BIGINT, geom GEOMETRY,
//! name VARCHAR)`.

use super::{
    guard::{GuardError, RequestGuard},
    store::{builtin_collections, ItemQuery, StoreError},
};
use crate::{
    api::{CollectionMeta, Feature, FeatureCollection, Geometry},
    filter::{lineage_predicate, visibility_fingerprint},
};
use moka::future::Cache;
use std::{
    path::{Path, PathBuf},
    sync::Mutex,
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

const POOL_SIZE: usize = 8;
const CHECKOUT_TIMEOUT: Duration = Duration::from_secs(2);

/// Fixed-size pool of read connections. Checkout holds a semaphore permit;
/// dropping the guard returns the connection. Saturation sheds as
/// [`GuardError::Overloaded`] instead of queueing without bound.
struct DuckPool {
    slots: std::sync::Arc<Semaphore>,
    conns: Mutex<Vec<duckdb::Connection>>,
}

struct Checkout<'a> {
    conn: Option<duckdb::Connection>,
    pool: &'a DuckPool,
    _permit: OwnedSemaphorePermit,
}

impl Drop for Checkout<'_> {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.pool.conns.lock().unwrap().push(conn);
        }
    }
}

impl DuckPool {
    fn open_all(path: &Path, size: usize) -> Result<Self, String> {
        let mut conns = Vec::with_capacity(size);
        for _ in 0..size {
            let conn = duckdb::Connection::open(path).map_err(|e| format!("duckdb open: {e}"))?;
            conn.execute_batch("LOAD spatial")
                .map_err(|e| format!("LOAD spatial (needs DuckDB >= 1.4 spatial): {e}"))?;
            conns.push(conn);
        }
        Ok(Self {
            slots: std::sync::Arc::new(Semaphore::new(size)),
            conns: Mutex::new(conns),
        })
    }

    async fn checkout(&self) -> Result<Checkout<'_>, GuardError> {
        let permit = tokio::time::timeout(CHECKOUT_TIMEOUT, self.slots.clone().acquire_owned())
            .await
            .map_err(|_| GuardError::Overloaded)?
            .map_err(|_| GuardError::Backend("pool closed".to_string()))?;
        let conn = self
            .conns
            .lock()
            .unwrap()
            .pop()
            .ok_or_else(|| GuardError::Backend("pool empty".to_string()))?;
        Ok(Checkout {
            conn: Some(conn),
            pool: self,
            _permit: permit,
        })
    }
}

fn guard_overloaded(error: GuardError) -> StoreError {
    match error {
        GuardError::Overloaded => StoreError::Overloaded,
        GuardError::Missing => StoreError::Backend("unexpected cache miss".to_string()),
        GuardError::Backend(message) => StoreError::Backend(message),
    }
}

/// Guard constructors. Generic so each cache gets its own value type; the
/// pool (not the guard) is what normally sheds, so limits stay generous.
fn new_guard<T: Send + Sync + 'static>() -> RequestGuard<T> {
    RequestGuard::new(128, Duration::from_secs(10), Duration::from_secs(30))
}

pub struct ServeStore {
    pool: DuckPool,
    turso: libsql::Connection,
    frag: Cache<String, Vec<u8>>,
    items_cache: Cache<String, FeatureCollection>,
    item_cache: Cache<String, Feature>,
    serving_version: String,
    /// Singleflight + overflow shed for tiles (DuckDB bound lives in the pool).
    tile_guard: RequestGuard<Option<Vec<u8>>>,
    items_guard: RequestGuard<FeatureCollection>,
    item_guard: RequestGuard<Feature>,
}

impl ServeStore {
    pub async fn open(shard: &Path) -> Result<Self, String> {
        let shard_file = pick_shard(shard)?;
        let pool = DuckPool::open_all(&shard_file, POOL_SIZE)?;

        let turso_path = shard_file.with_extension("frag.db");
        let db = libsql::Builder::new_local(turso_path.to_string_lossy().into_owned())
            .build()
            .await
            .map_err(|e| format!("turso open: {e}"))?;
        let turso = db.connect().map_err(|e| format!("turso connect: {e}"))?;
        turso
            .execute(
                "CREATE TABLE IF NOT EXISTS frag (k TEXT PRIMARY KEY, v BLOB)",
                (),
            )
            .await
            .map_err(|e| format!("turso ddl: {e}"))?;
        let mut rows = turso
            .query("SELECT k FROM frag LIMIT 1", ())
            .await
            .map_err(|e| format!("turso probe: {e}"))?;
        let proof_of_life = rows
            .next()
            .await
            .map_err(|e| format!("turso rows: {e}"))?
            .is_some();
        tracing::info!(
            cached_fragment_seen = proof_of_life,
            "turso fragment cache ready"
        );

        let serving_version = std::fs::read_to_string(shard_file.with_extension("version"))
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|_| "v0".to_string());

        Ok(Self {
            pool,
            turso,
            frag: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(300))
                .build(),
            items_cache: Cache::builder()
                .max_capacity(5_000)
                .time_to_live(Duration::from_secs(120))
                .build(),
            item_cache: Cache::builder()
                .max_capacity(10_000)
                .time_to_live(Duration::from_secs(300))
                .build(),
            serving_version,
            tile_guard: new_guard(),
            items_guard: new_guard(),
            item_guard: new_guard(),
        })
    }

    fn check_collection(collection: &str) -> Result<(), StoreError> {
        if builtin_collections().iter().any(|c| c.id == collection) {
            Ok(())
        } else {
            Err(StoreError::NotFound(collection.to_string()))
        }
    }

    /// Feature ids are interpolated into SQL, so they pass a strict charset
    /// gate first. (Graduate to bound params with the Flight endpoint.)
    fn safe_id(id: &str) -> Result<&str, StoreError> {
        if !id.is_empty()
            && id.len() <= 128
            && id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "-_.:".contains(c))
        {
            Ok(id)
        } else {
            Err(StoreError::Backend("bad feature id".to_string()))
        }
    }

    fn visibility(&self, source_ids: &[i64]) -> String {
        visibility_fingerprint(&self.serving_version, "dev", source_ids)
    }

    pub async fn collections(&self) -> Result<Vec<CollectionMeta>, StoreError> {
        Ok(builtin_collections())
    }

    pub async fn collection(&self, id: &str) -> Result<CollectionMeta, StoreError> {
        Self::check_collection(id)?;
        self.collections()
            .await?
            .into_iter()
            .find(|c| c.id == id)
            .ok_or_else(|| StoreError::NotFound(id.to_string()))
    }

    pub async fn items(
        &self,
        collection: &str,
        query: &ItemQuery,
    ) -> Result<FeatureCollection, StoreError> {
        Self::check_collection(collection)?;
        let key = format!(
            "items:{collection}:{:?}:{}:{}:{:?}:{:?}:{}",
            query.bbox,
            query.limit.min(1000),
            query.offset,
            query.datetime,
            query.filter,
            self.visibility(&query.source_ids),
        );
        if let Some(cached) = self.items_cache.get(&key).await {
            return Ok(cached);
        }
        let pred = lineage_predicate(&query.source_ids);
        let bbox_sql = query
            .bbox
            .map(|b| {
                // Inlined numerics => planner-known constants => R-tree eligible.
                format!(
                    " AND ST_Intersects(geom, ST_MakeEnvelope({}, {}, {}, {}))",
                    b[0], b[1], b[2], b[3]
                )
            })
            .unwrap_or_default();
        let sql = format!(
            "SELECT id, ST_X(ST_Centroid(geom)) AS cx, ST_Y(ST_Centroid(geom)) AS cy, \
             CAST(name AS VARCHAR) AS nm FROM features \
             WHERE layer = '{collection}' AND {pred}{bbox_sql} \
             LIMIT {} OFFSET {}",
            query.limit.min(1000),
            query.offset,
        );
        let shared = self
            .items_guard
            .run(key.clone(), || async {
                let checked = self.pool.checkout().await?;
                tokio::task::block_in_place(|| {
                    let conn = checked.conn.as_ref().unwrap();
                    let mut stmt = conn
                        .prepare(&sql)
                        .map_err(|e| GuardError::Backend(e.to_string()))?;
                    let rows = stmt
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, f64>(1)?,
                                row.get::<_, f64>(2)?,
                                row.get::<_, Option<String>>(3)?,
                            ))
                        })
                        .map_err(|e| GuardError::Backend(e.to_string()))?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| GuardError::Backend(e.to_string()))?;
                    let features = rows
                        .into_iter()
                        .map(|(id, lon, lat, name)| {
                            let mut properties = std::collections::BTreeMap::new();
                            properties.insert("name".to_string(), name.unwrap_or_default());
                            Feature {
                                kind: "Feature".to_string(),
                                id,
                                geometry: Some(Geometry {
                                    kind: "Point".to_string(),
                                    coordinates: vec![lon, lat],
                                }),
                                properties,
                            }
                        })
                        .collect::<Vec<_>>();
                    let number_returned = features.len() as i64;
                    Ok(FeatureCollection {
                        kind: "FeatureCollection".to_string(),
                        features,
                        links: vec![],
                        number_returned,
                    })
                })
            })
            .await
            .map_err(guard_overloaded)?;
        let page = (*shared).clone();
        self.items_cache.insert(key, page.clone()).await;
        Ok(page)
    }

    pub async fn item(
        &self,
        collection: &str,
        id: &str,
        source_ids: &[i64],
    ) -> Result<Feature, StoreError> {
        Self::check_collection(collection)?;
        let id = Self::safe_id(id)?;
        let key = format!("item:{collection}:{id}:{}", self.visibility(source_ids));
        if let Some(cached) = self.item_cache.get(&key).await {
            return Ok(cached);
        }
        let pred = lineage_predicate(source_ids);
        let sql = format!(
            "SELECT id, ST_X(ST_Centroid(geom)) AS cx, ST_Y(ST_Centroid(geom)) AS cy, \
             CAST(name AS VARCHAR) AS nm FROM features \
             WHERE layer = '{collection}' AND id = '{id}' AND {pred} LIMIT 1",
        );
        let shared = self
            .item_guard
            .run(key.clone(), || async {
                let checked = self.pool.checkout().await?;
                tokio::task::block_in_place(|| {
                    let conn = checked.conn.as_ref().unwrap();
                    let mut stmt = conn
                        .prepare(&sql)
                        .map_err(|e| GuardError::Backend(e.to_string()))?;
                    let mut rows = stmt
                        .query_map([], |row| {
                            Ok((
                                row.get::<_, String>(0)?,
                                row.get::<_, f64>(1)?,
                                row.get::<_, f64>(2)?,
                                row.get::<_, Option<String>>(3)?,
                            ))
                        })
                        .map_err(|e| GuardError::Backend(e.to_string()))?
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(|e| GuardError::Backend(e.to_string()))?;
                    rows.pop()
                        .map(|(id, lon, lat, name)| {
                            let mut properties = std::collections::BTreeMap::new();
                            properties.insert("name".to_string(), name.unwrap_or_default());
                            Feature {
                                kind: "Feature".to_string(),
                                id,
                                geometry: Some(Geometry {
                                    kind: "Point".to_string(),
                                    coordinates: vec![lon, lat],
                                }),
                                properties,
                            }
                        })
                        .ok_or(GuardError::Missing)
                })
            })
            .await
            .map_err(|error| match error {
                GuardError::Missing => StoreError::NotFound(id.to_string()),
                error => guard_overloaded(error),
            })?;
        let feature = (*shared).clone();
        self.item_cache.insert(key, feature.clone()).await;
        Ok(feature)
    }

    pub async fn tile(
        &self,
        collection: &str,
        bbox: [f64; 4],
        zoom: u8,
        source_ids: &[i64],
    ) -> Result<Option<Vec<u8>>, StoreError> {
        Self::check_collection(collection)?;
        // Shared only between identical effective visibility. Policy
        // version is a request header today, JWT claim next ("dev" meanwhile).
        let key = format!(
            "tile:{collection}:{zoom}:{}",
            visibility_fingerprint(&self.serving_version, "dev", source_ids)
        );
        // L1: in-process moka. L2: Turso, best effort: a cache fault must
        // never fail the request.
        if let Some(bytes) = self.frag.get(&key).await {
            return Ok(Some(bytes));
        }
        if let Some(bytes) = self.frag_get(&key).await {
            self.frag.insert(key, bytes.clone()).await;
            return Ok(Some(bytes));
        }
        let pred = lineage_predicate(source_ids);
        let sql = format!(
            "SELECT ST_AsMVT({{'geom': m, 'id': id}}, '{collection}') FROM (SELECT id, \
             ST_AsMVTGeom(geom, ST_MakeEnvelope({}, {}, {}, {})::BOX_2D, 4096, 64, true) AS m \
             FROM features WHERE layer = '{collection}' AND {pred} \
             AND ST_Intersects(geom, ST_MakeEnvelope({}, {}, {}, {})) LIMIT 5000) t",
            bbox[0], bbox[1], bbox[2], bbox[3], bbox[0], bbox[1], bbox[2], bbox[3],
        );
        // Guarded: identical concurrent tiles coalesce onto one DuckDB query;
        // the pool bounds concurrency; saturation sheds as Overloaded.
        let shared = self
            .tile_guard
            .run(key.clone(), || async {
                let checked = self.pool.checkout().await?;
                tokio::task::block_in_place(|| {
                    let conn = checked.conn.as_ref().unwrap();
                    let mut stmt = conn
                        .prepare(&sql)
                        .map_err(|e| GuardError::Backend(e.to_string()))?;
                    let mut rows = stmt
                        .query_map([], |row| {
                            let tile: Vec<u8> = row.get(0)?;
                            Ok(tile)
                        })
                        .map_err(|e| GuardError::Backend(e.to_string()))?;
                    match rows.next() {
                        Some(Ok(tile)) => Ok(Some(tile)),
                        Some(Err(e)) => Err(GuardError::Backend(e.to_string())),
                        None => Ok(None),
                    }
                })
            })
            .await
            .map_err(guard_overloaded)?;
        if let Some(bytes) = shared.as_deref() {
            self.frag.insert(key.clone(), bytes.to_vec()).await;
            self.frag_put(&key, bytes).await;
        }
        Ok(shared.as_deref().map(|bytes| bytes.to_vec()))
    }

    /// Turso L2 read. Returns None on miss AND on error (logged): a cache
    /// fault must never fail the request.
    async fn frag_get(&self, key: &str) -> Option<Vec<u8>> {
        let mut rows = match self
            .turso
            .query("SELECT v FROM frag WHERE k = ?1", libsql::params![key])
            .await
        {
            Ok(rows) => rows,
            Err(error) => {
                tracing::warn!(%error, "turso L2 get failed");
                return None;
            }
        };
        match rows.next().await {
            Ok(Some(row)) => match row.get::<Vec<u8>>(0) {
                Ok(bytes) => Some(bytes),
                Err(error) => {
                    tracing::warn!(%error, "turso L2 decode failed");
                    None
                }
            },
            Ok(None) => None,
            Err(error) => {
                tracing::warn!(%error, "turso L2 rows failed");
                None
            }
        }
    }

    /// Turso L2 write-through. Failures are logged, never fatal.
    async fn frag_put(&self, key: &str, value: &[u8]) {
        if let Err(error) = self
            .turso
            .execute(
                "INSERT OR REPLACE INTO frag (k, v) VALUES (?1, ?2)",
                libsql::params![key.to_string(), value.to_vec()],
            )
            .await
        {
            tracing::warn!(%error, "turso L2 put failed");
        }
    }
}

/// `--shard-dir` may be a file or a directory; directories resolve to the
/// first `*.duckdb` inside (single-file scaffold; manifest routing next).
fn pick_shard(dir: &Path) -> Result<PathBuf, String> {
    if dir.is_file() {
        return Ok(dir.to_path_buf());
    }
    std::fs::read_dir(dir)
        .map_err(|e| format!("read {}: {e}", dir.display()))?
        .filter_map(|entry| entry.ok().map(|e| e.path()))
        .find(|path| path.extension().is_some_and(|ext| ext == "duckdb"))
        .ok_or_else(|| format!("no *.duckdb shard in {}", dir.display()))
}
