//! Overload protection in front of DuckDB: request coalescing
//! (singleflight) + bounded concurrency (semaphore) + wait timeouts.
//! Shared by the Poem and Flight paths.
//!
//! Caches live outside this guard (moka L1 + Turso L2 for tiles, moka
//! batches for Flight). The guard only decides *who runs* DuckDB and sheds
//! the rest with [`GuardError::Overloaded`], never *what* is cached.

use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, Semaphore};

#[derive(Debug, Clone)]
pub enum GuardError {
    /// Shed load: queue full, follower wait timed out, or DuckDB saturated.
    /// Callers map this to 429 + Retry-After (HTTP) or
    /// `resource_exhausted` (Flight).
    Overloaded,
    /// Cacheable miss (row absent). Shared briefly like any other result;
    /// callers map it to 404 / NOT_FOUND.
    Missing,
    Backend(String),
}

/// Coalesces concurrent identical requests onto one computation and caps
/// simultaneous DuckDB work. Cloneable; share one instance per backend.
#[derive(Clone)]
pub struct RequestGuard<T> {
    inflight: Arc<Mutex<HashMap<String, Vec<oneshot::Sender<Shared<T>>>>>>,
    sem: Arc<Semaphore>,
    /// How long followers wait for the leader's result.
    wait_timeout: Duration,
    /// How long the leader waits for a DuckDB slot.
    queue_timeout: Duration,
}

type Shared<T> = Result<Arc<T>, GuardError>;

impl<T: Send + Sync + 'static> RequestGuard<T> {
    pub fn new(max_inflight: usize, wait_timeout: Duration, queue_timeout: Duration) -> Self {
        Self {
            inflight: Arc::new(Mutex::new(HashMap::new())),
            sem: Arc::new(Semaphore::new(max_inflight)),
            wait_timeout,
            queue_timeout,
        }
    }

    pub async fn run<K, F, Fut>(&self, key: K, compute: F) -> Shared<T>
    where
        K: Into<String>,
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, GuardError>>,
    {
        let key = key.into();
        let waiter = {
            let mut inflight = self.inflight.lock().unwrap();
            match inflight.get_mut(&key) {
                Some(waiters) => {
                    let (tx, rx) = oneshot::channel();
                    waiters.push(tx);
                    Some(rx)
                }
                None => {
                    inflight.insert(key.clone(), Vec::new());
                    None
                }
            }
        };
        if let Some(rx) = waiter {
            // Follower: share the leader's result, give up fast when slow.
            return match tokio::time::timeout(self.wait_timeout, rx).await {
                Ok(Ok(shared)) => shared,
                Ok(Err(_)) => Err(GuardError::Backend("leader gone".to_string())),
                Err(_) => Err(GuardError::Overloaded),
            };
        }
        // Leader: take a DuckDB slot or shed immediately.
        let _permit = match tokio::time::timeout(self.queue_timeout, self.sem.acquire()).await {
            Ok(Ok(permit)) => permit,
            _ => {
                self.inflight.lock().unwrap().remove(&key);
                return Err(GuardError::Overloaded);
            }
        };
        let result = compute().await.map(Arc::new);
        let waiters = self
            .inflight
            .lock()
            .unwrap()
            .remove(&key)
            .unwrap_or_default();
        for tx in waiters {
            let _ = tx.send(result.clone());
        }
        result
    }
}
