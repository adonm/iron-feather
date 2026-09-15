//! One immutable shard, one fail-fast pool, byte-bounded in-process caches.
//! Moka owns both caching and cancellation-safe request coalescing.

use bytes::Bytes;
use duckdb::{
    arrow::{datatypes::SchemaRef, record_batch::RecordBatch},
    AccessMode, Config, Connection,
};
use moka::future::Cache;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};

#[derive(Debug, Clone, thiserror::Error)]
pub enum Error {
    #[error("not found: {0}")]
    NotFound(String),
    #[error("{0}")]
    Invalid(String),
    #[error("server busy; retry")]
    Overloaded,
    #[error("{0}")]
    Backend(String),
}

impl From<duckdb::Error> for Error {
    fn from(e: duckdb::Error) -> Self {
        Self::Backend(e.to_string())
    }
}
impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Backend(e.to_string())
    }
}

pub struct ArrowResult {
    pub schema: SchemaRef,
    pub batches: Vec<RecordBatch>,
}

pub struct Store {
    pub collections: Vec<String>,
    pool: Arc<Mutex<Vec<Connection>>>,
    http: Cache<String, Bytes>,
    arrow: Cache<String, Arc<ArrowResult>>,
}

// The worker owns the checkout: disconnecting a client cannot return a
// connection while its blocking DuckDB query is still running.
struct Checkout {
    conn: Option<Connection>,
    pool: Arc<Mutex<Vec<Connection>>>,
}
impl Drop for Checkout {
    fn drop(&mut self) {
        self.pool.lock().unwrap().push(self.conn.take().unwrap());
    }
}

impl Store {
    pub fn open(location: &str, connections: usize, cache_bytes: u64) -> Result<Self, Error> {
        if connections == 0 {
            return Err(Error::Invalid("connections must be positive".into()));
        }
        let remote = ["http://", "https://", "s3://"]
            .iter()
            .any(|scheme| location.starts_with(scheme));
        let config = Config::default().threads(1)?;
        let conn = if remote {
            Connection::open_in_memory_with_flags(config)?
        } else {
            Connection::open_with_flags(
                Path::new(location),
                config.access_mode(AccessMode::ReadOnly)?,
            )?
        };
        conn.execute_batch(
            "SET autoinstall_known_extensions=false; SET autoload_known_extensions=false; LOAD spatial;",
        )?;
        if remote {
            conn.execute_batch(&format!(
                "LOAD httpfs; ATTACH {} AS shard (READ_ONLY); USE shard;",
                crate::filter::quote(location)
            ))?;
        }
        conn.execute_batch("SET enable_external_access=false;")?;
        let _ = conn.prepare(
            "SELECT id, layer, source_id, ST_AsWKB(geom), properties::JSON FROM features LIMIT 0",
        )?
        .query([])?;
        let collections = conn
            .prepare("SELECT id FROM collections ORDER BY id")?
            .query_map([], |r| r.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if collections
            .iter()
            .any(|id| !crate::filter::collection_id(id))
        {
            return Err(Error::Invalid(
                "shard contains an invalid collection id".into(),
            ));
        }
        let mut pool = Vec::with_capacity(connections);
        for _ in 1..connections {
            let clone = conn.try_clone()?;
            if remote {
                clone.execute_batch("USE shard;")?;
            }
            clone.execute_batch("SET enable_external_access=false;")?;
            pool.push(clone);
        }
        pool.push(conn);
        Ok(Self {
            collections,
            pool: Arc::new(Mutex::new(pool)),
            http: Cache::builder()
                .max_capacity(cache_bytes / 2)
                .weigher(|key: &String, value: &Bytes| {
                    (key.len() + value.len()).min(u32::MAX as usize) as u32
                })
                .build(),
            arrow: Cache::builder()
                .max_capacity(cache_bytes / 2)
                .weigher(|key: &String, value: &Arc<ArrowResult>| {
                    (key.len()
                        + value
                            .batches
                            .iter()
                            .map(RecordBatch::get_array_memory_size)
                            .sum::<usize>()
                        + 1024)
                        .min(u32::MAX as usize) as u32
                })
                .build(),
        })
    }

    pub fn collection(&self, id: &str) -> Result<(), Error> {
        if self.collections.iter().any(|c| c == id) {
            Ok(())
        } else {
            Err(Error::NotFound(id.into()))
        }
    }

    pub async fn run<T, F>(&self, query: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&Connection) -> Result<T, Error> + Send + 'static,
    {
        let conn = self.pool.lock().unwrap().pop().ok_or(Error::Overloaded)?;
        let checkout = Checkout {
            conn: Some(conn),
            pool: self.pool.clone(),
        };
        tokio::task::spawn_blocking(move || query(checkout.conn.as_ref().unwrap()))
            .await
            .map_err(|e| Error::Backend(e.to_string()))?
    }

    /// Key must include every input used to produce the encoded HTTP body.
    pub async fn bytes<F>(&self, key: String, query: F) -> Result<Bytes, Error>
    where
        F: FnOnce(&Connection) -> Result<Bytes, Error> + Send + 'static,
    {
        self.http
            .try_get_with(key, self.run(query))
            .await
            .map_err(|e| (*e).clone())
    }

    /// Fetch candidate IDs using narrow columns, then payloads through the
    /// single-column ART index. This avoids a wide base-table scan, which is
    /// especially important for remotely attached database files.
    pub async fn arrow(
        &self,
        candidate_sql: Option<String>,
        projection: String,
    ) -> Result<Arc<ArrowResult>, Error> {
        let key = format!("arrow:{projection}:{candidate_sql:?}");
        self.arrow
            .try_get_with(
                key,
                self.run(move |conn| {
                    let ids = match candidate_sql {
                        Some(sql) => conn
                            .prepare(&sql)?
                            .query_map([], |row| row.get::<_, String>(0))?
                            .collect::<Result<Vec<_>, _>>()?,
                        None => vec![],
                    };
                    let predicate = if ids.is_empty() {
                        "FALSE".to_string()
                    } else {
                        format!(
                            "id IN ({})",
                            ids.iter()
                                .map(|id| crate::filter::quote(id))
                                .collect::<Vec<_>>()
                                .join(",")
                        )
                    };
                    let sql =
                        format!("SELECT {projection} FROM features WHERE {predicate} ORDER BY id");
                    let mut stmt = conn.prepare(&sql)?;
                    let result = stmt.query_arrow([])?;
                    Ok(Arc::new(ArrowResult {
                        schema: result.get_schema(),
                        batches: result.collect(),
                    }))
                }),
            )
            .await
            .map_err(|e| (*e).clone())
    }
}
