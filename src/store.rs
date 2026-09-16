//! One immutable snapshot, one bounded-queue pool, one byte-bounded response cache.
//! Moka owns caching and cancellation-safe request coalescing for HTTP.
//!
//! All DuckDB access goes through the stable v2 C API, see [`crate::db`].

use crate::{
    db::{self, NeoConnection},
    filter,
};
use arrow::{
    datatypes::SchemaRef,
    record_batch::{RecordBatch, RecordBatchReader},
};
use bytes::Bytes;
use duckdb_neo::Parameters;
use moka::future::Cache;
use std::{
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
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

impl From<serde_json::Error> for Error {
    fn from(e: serde_json::Error) -> Self {
        Self::Backend(e.to_string())
    }
}

/// Storage behind the pool: a DuckLake catalog (small `.ducklake` database
/// plus Parquet data files) on local disk or S3. The HTTP surface is
/// identical across locations; only the catalog URL changes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ShardManifest {
    pub version: u32,
    pub backend: String,
    pub schema_version: u32,
    pub source: String,
    pub bbox: [f64; 4],
    pub rows: i64,
    pub built_at: String,
    pub layout: Option<LakeLayout>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct LakeLayout {
    pub file_mb: u64,
    pub row_group: u64,
    pub sort: String,
}

/// Process-wide budgets. `Store::open` keeps its old positional arguments
/// as a thin wrapper; new code should build this struct so budgets stay
/// explicit at the call site.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    pub location: String,
    pub connections: usize,
    pub cache_bytes: u64,
    pub max_waiters: usize,
    pub max_wait: Duration,
    pub bulk_limit: usize,
    /// Shared DuckDB threads for the whole process (not per connection).
    pub threads: i64,
    /// Shared DuckDB memory budget in MiB. 0 leaves DuckDB's default
    /// (unbounded); 4096 holds the ~10 GB shard urban working set.
    pub memory_mb: u64,
    pub query_timeout: Duration,
    /// Per-stream Flight buffer in bytes.
    pub flight_stream_bytes: usize,
    /// Process-wide Flight buffer in bytes across all streams.
    pub flight_total_bytes: usize,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            location: String::new(),
            connections: 8,
            cache_bytes: 256 * 1024 * 1024,
            max_waiters: 128,
            max_wait: Duration::from_millis(250),
            bulk_limit: 8,
            threads: 1,
            memory_mb: 4096,
            query_timeout: Duration::from_millis(30_000),
            flight_stream_bytes: FLIGHT_BYTE_BUDGET,
            flight_total_bytes: FLIGHT_TOTAL_BUDGET,
        }
    }
}

pub struct ArrowResult {
    pub schema: SchemaRef,
    #[allow(dead_code)]
    pub batches: Vec<RecordBatch>,
}

/// Incremental Flight payload: schema up front, then a byte-budgeted batch
/// channel fed by the blocking DuckDB worker holding its pool connection.
/// The guard owns the shared query state: premature drop marks cancellation
/// and interrupts the query, aborts the deadline task, and never touches a
/// reused connection because the worker clears the handle before returning
/// it to the pool.
pub struct FlightBatches {
    pub schema: SchemaRef,
    pub batches: tokio::sync::mpsc::Receiver<Result<RecordBatch, Error>>,
    pub guard: StreamGuard,
    pub buffered: Arc<AtomicUsize>,
    pub global_buffered: Arc<AtomicUsize>,
}

/// Raw v2 connection handle: a pointer, always safe to interrupt from any
/// thread while another thread steps the query's result.
///
/// The handle is `Send`/`Sync` by the engine's contract (`duckdb_v2_connection_interrupt`
/// documents cross-thread use); it is only ever dereferenced for interrupt,
/// never for query execution.
#[derive(Clone, Copy)]
struct RawHandle(libduckdb_sys::v2::duckdb_v2_connection_handle);
// Safe: interrupt is documented safe from any thread, including while
// another thread steps the query's result; a no-op when idle.
unsafe impl Send for RawHandle {}
unsafe impl Sync for RawHandle {}

/// Shared lifecycle for one DuckDB query. The worker records the connection
/// handle on start and clears it on completion before the connection goes
/// back to the pool; guards and timeout tasks only interrupt while a handle
/// is present, so a late drop cannot cancel the next query on that
/// connection.
pub struct QueryState {
    interrupt: Mutex<Option<RawHandle>>,
    cancelled: AtomicBool,
    completed: AtomicBool,
}

impl QueryState {
    fn new() -> Self {
        Self {
            interrupt: Mutex::new(None),
            cancelled: AtomicBool::new(false),
            completed: AtomicBool::new(false),
        }
    }

    fn set_interrupt(&self, conn: &NeoConnection) {
        *self.interrupt.lock().unwrap() = Some(RawHandle(**conn));
    }

    fn finish(&self) {
        self.completed.store(true, Ordering::Release);
        *self.interrupt.lock().unwrap() = None;
    }

    fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
        if let Some(handle) = *self.interrupt.lock().unwrap() {
            // Safe: the handle is cleared before its connection returns to
            // the pool, and interrupt is a no-op on idle connections.
            unsafe { db::interrupt_handle(handle.0) };
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    fn is_completed(&self) -> bool {
        self.completed.load(Ordering::Acquire)
    }
}

/// Cancels an in-flight DuckDB query unless it already finished.
pub struct StreamGuard {
    state: Arc<QueryState>,
    timeout: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for StreamGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.timeout.take() {
            handle.abort();
        }
        if !self.state.is_completed() {
            self.state.cancel();
        }
    }
}

struct RunGuard {
    state: Arc<QueryState>,
    timeout: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for RunGuard {
    fn drop(&mut self) {
        if let Some(handle) = self.timeout.take() {
            handle.abort();
        }
        if !self.state.is_completed() {
            self.state.cancel();
        }
    }
}

/// Byte budget for buffered Flight batches: bounds peak memory by payload
/// bytes instead of batch count (16 large polygon batches can dwarf 16 id
/// batches). Senders reserve before send; the consumer releases on receive.
/// Oversized single batches are allowed once the buffer drains, so progress
/// is guaranteed.
pub const FLIGHT_BYTE_BUDGET: usize = 32 * 1024 * 1024;
/// Process-wide cap across all concurrent Flight streams.
pub const FLIGHT_TOTAL_BUDGET: usize = 128 * 1024 * 1024;

fn reserve_flight_budget(
    per_stream: &AtomicUsize,
    global: &AtomicUsize,
    state: &QueryState,
    closed: impl Fn() -> bool,
    size: usize,
    per_stream_budget: usize,
    total_budget: usize,
) -> bool {
    // Oversized batches cannot satisfy the normal bound; let one through
    // once both buffers drain instead of spinning forever.
    let oversized = size > per_stream_budget || size > total_budget;
    loop {
        if closed() || state.is_cancelled() {
            return false;
        }
        let cur_stream = per_stream.load(Ordering::Acquire);
        let cur_total = global.load(Ordering::Acquire);
        let fits = if oversized {
            cur_stream == 0 && cur_total == 0
        } else {
            cur_stream + size <= per_stream_budget && cur_total + size <= total_budget
        };
        if fits {
            // Reserve both counters; roll back the first if the second races.
            match per_stream.compare_exchange_weak(
                cur_stream,
                cur_stream + size,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => match global.compare_exchange_weak(
                    cur_total,
                    cur_total + size,
                    Ordering::AcqRel,
                    Ordering::Acquire,
                ) {
                    Ok(_) => return true,
                    Err(_) => {
                        per_stream.fetch_sub(size, Ordering::AcqRel);
                        continue;
                    }
                },
                Err(_) => continue,
            }
        } else {
            std::thread::sleep(Duration::from_millis(1));
        }
    }
}

/// A response query deferred past content negotiation: encoding-first
/// lookup needs the key before deciding whether to run the database work.
pub type QueryFn = Box<dyn FnOnce(&NeoConnection) -> Result<Bytes, Error> + Send + 'static>;

/// Bulk versus interactive admission. Bulk holds a bulk semaphore slot for
/// its whole execution so heavy scans and Flight cannot starve small pages;
/// interactive shares only the pool queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkClass {
    Interactive,
    Bulk,
}

/// Counters for the HTTP response cache. `requests` counts cache lookups
/// (one HTTP request can cause up to two: gzip then identity). `http_requests`
/// counts HTTP responses attempted. `hits` counts fast-path hits,
/// `coalesced` counts waiters that shared another caller's computation,
/// `computes` counts distinct miss executions, `failures` counts miss
/// executions that returned an error. Hit rate over lookups is
/// `(hits + coalesced) / requests`.
#[derive(Debug, Default)]
pub struct CacheStats {
    pub requests: AtomicU64,
    pub http_requests: AtomicU64,
    pub hits: AtomicU64,
    pub coalesced: AtomicU64,
    pub computes: AtomicU64,
    pub failures: AtomicU64,
    pub evictions: AtomicU64,
}

#[derive(Debug, Clone, Copy, serde::Serialize)]
pub struct CacheSnapshot {
    pub entries: u64,
    pub weight_bytes: u64,
    pub requests: u64,
    pub http_requests: u64,
    pub hits: u64,
    pub coalesced: u64,
    pub computes: u64,
    pub failures: u64,
    pub evictions: u64,
}

pub struct Store {
    pub collections: Vec<String>,
    /// Pinned DuckLake snapshot this instance serves. Resolved at startup;
    /// both the serving pool and Quack attach at exactly this version.
    pub snapshot: i64,
    pool: Arc<Mutex<Vec<NeoConnection>>>,
    semaphore: Arc<tokio::sync::Semaphore>,
    /// Present only when bulk is capped below the pool size; `None` leaves
    /// bulk sharing the pool queue exactly like interactive traffic.
    bulk: Option<Arc<tokio::sync::Semaphore>>,
    queued: AtomicUsize,
    max_waiters: usize,
    max_wait: Duration,
    query_timeout: Duration,
    flight_stream_budget: usize,
    flight_total_budget: usize,
    flight_used: Arc<AtomicUsize>,
    #[allow(dead_code)]
    manifest: Option<ShardManifest>,
    http: Cache<String, CachedBody>,
    stats: Arc<CacheStats>,
}

/// A cached HTTP representation with its validator, computed once on miss.
/// Cloning is cheap: the body is reference-counted and the tag is small.
#[derive(Debug, Clone)]
pub struct CachedBody {
    pub bytes: Bytes,
    pub etag: String,
}

impl CachedBody {
    /// A strong validator over the exact bytes: the shard is immutable, so
    /// equal bytes mean an equal representation.
    pub fn with_bytes(bytes: Bytes) -> Self {
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;
        let mut hash = FNV_OFFSET;
        for byte in bytes.iter() {
            hash ^= u64::from(*byte);
            hash = hash.wrapping_mul(FNV_PRIME);
        }
        let etag = format!("\"{hash:x}-{}\"", bytes.len());
        Self { bytes, etag }
    }
}

// The worker owns the checkout: disconnecting a client cannot return a
// connection while its blocking DuckDB query is still running. The semaphore
// permit is held alongside the connection, so one waiter advances per return
// in FIFO order.
struct Checkout {
    conn: Option<NeoConnection>,
    pool: Arc<Mutex<Vec<NeoConnection>>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}
impl Drop for Checkout {
    fn drop(&mut self) {
        self.pool.lock().unwrap().push(self.conn.take().unwrap());
        // The permit drops after the connection is back: the next waiter is
        // guaranteed a connection when it wakes.
    }
}
// Dropping the run future while queued (client abort, timeout) releases the
// slot instead of stranding it.
struct QueueGuard<'a> {
    queued: &'a AtomicUsize,
}
impl Drop for QueueGuard<'_> {
    fn drop(&mut self) {
        self.queued.fetch_sub(1, Ordering::Release);
    }
}

fn manifest_path_for(location: &str) -> Option<String> {
    if location.starts_with("http://")
        || location.starts_with("https://")
        || location.starts_with("s3://")
    {
        return None;
    }
    Some(format!("{location}.manifest.json"))
}

/// Attach path for a catalog location. Shared by the serving pool and the
/// Quack instance so both pin the same snapshot.
pub fn catalog_url(location: &str) -> String {
    format!("ducklake:{}", location.trim_end_matches('/'))
}

/// Full per-connection setup: extensions, then the catalog attach pinned to
/// `snapshot`. Every pooled connection runs the session part; the ATTACH
/// itself is database-level and done once (see [`Store::open_config`]).
/// Storage caching (Parquet/HTTP block and metadata caches) lives in the
/// ZeroFS layer below the mount, not in DuckDB: no cache tuning here.
fn setup_session(conn: &NeoConnection, _cfg: &StoreConfig, _remote: bool) -> Result<(), Error> {
    // Extensions load first; ducklake installs on demand once, then loads
    // offline like spatial does.
    if db::execute_all(conn, &["LOAD ducklake"]).is_err() {
        db::execute_all(conn, &["INSTALL ducklake", "LOAD ducklake"])?;
    }
    db::execute_all(
        conn,
        &[
            "SET autoinstall_known_extensions=false",
            "SET autoload_known_extensions=false",
            "LOAD spatial",
            "LOAD httpfs",
        ],
    )?;
    Ok(())
}

impl Store {
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub fn open(
        location: &str,
        connections: usize,
        cache_bytes: u64,
        max_waiters: usize,
        max_wait: Duration,
        bulk_limit: usize,
        threads: i64,
        memory_mb: u64,
        query_timeout: Duration,
    ) -> Result<Self, Error> {
        Self::open_config(StoreConfig {
            location: location.to_string(),
            connections,
            cache_bytes,
            max_waiters,
            max_wait,
            bulk_limit,
            threads,
            memory_mb,
            query_timeout,
            ..StoreConfig::default()
        })
    }

    pub fn open_config(cfg: StoreConfig) -> Result<Self, Error> {
        if cfg.connections == 0 {
            return Err(Error::Invalid("connections must be positive".into()));
        }
        if cfg.threads <= 0 {
            return Err(Error::Invalid("threads must be positive".into()));
        }
        if cfg.bulk_limit == 0 || cfg.bulk_limit > cfg.connections {
            return Err(Error::Invalid(
                "bulk limit must be between 1 and connections".into(),
            ));
        }
        // Only a cap below the pool size changes behavior; at full size bulk
        // shares the queue like everything else.
        let bulk = (cfg.bulk_limit < cfg.connections)
            .then(|| Arc::new(tokio::sync::Semaphore::new(cfg.bulk_limit)));
        let stats = Arc::new(CacheStats::default());
        let evictions = stats.clone();
        let remote = ["http://", "https://", "s3://"]
            .iter()
            .any(|scheme| cfg.location.starts_with(scheme));
        let db = db::open_memory()?;
        // Process-wide budgets, set once on the shared database.
        let threads = duckdb_neo::connection_options::ConfigOptionValue::new(
            "threads",
            &cfg.threads.to_string(),
        )?;
        db.set_option(&threads)?;
        if cfg.memory_mb > 0 {
            let memory = duckdb_neo::connection_options::ConfigOptionValue::new(
                "memory_limit",
                &format!("{}MiB", cfg.memory_mb),
            )?;
            db.set_option(&memory)?;
        }
        let catalog = catalog_url(&cfg.location);
        // Open one connection per pool slot. The ATTACH is database-level:
        // the opener attaches latest to resolve the snapshot, then detaches
        // and re-attaches pinned; every connection only switches into it.
        // Sessions do not inherit storage SETs, so each runs full setup.
        let opener = db.connect()?;
        setup_session(&opener, &cfg, remote)?;
        db::execute_all(
            &opener,
            &[
                &format!(
                    "ATTACH {} AS shard (READ_ONLY)",
                    crate::filter::quote(&catalog)
                ),
                "USE shard",
            ],
        )?;
        let snapshot = db::int_one(&opener, "SELECT max(snapshot_id) FROM snapshots()")?;
        // Frozen view: even a swapped catalog file cannot move the reader.
        // Writes are rejected on pinned attaches by the engine itself.
        db::execute_all(
            &opener,
            &[
                "USE memory",
                "DETACH shard",
                &format!(
                    "ATTACH {} AS shard (READ_ONLY, SNAPSHOT_VERSION {})",
                    crate::filter::quote(&catalog),
                    snapshot
                ),
                "USE shard",
            ],
        )?;
        let mut pool = Vec::with_capacity(cfg.connections);
        for _ in 0..cfg.connections {
            let conn = db.connect()?;
            setup_session(&conn, &cfg, remote)?;
            db::execute_all(&conn, &["USE shard"])?;
            pool.push(conn);
        }
        drop(opener);
        let probe = pool.first().ok_or(Error::Overloaded)?;
        // Build-time derivatives (cx/cy/name) are required: the builder
        // always writes them so Flight and tiles avoid per-row geometry/JSON
        // work.
        db::text_table(probe, "SELECT cx, cy, name FROM features LIMIT 0").map_err(|_| {
            Error::Invalid("shard is missing derived cx/cy/name columns; rebuild".into())
        })?;
        let collections = db::strings_col(probe, "SELECT id FROM collections ORDER BY id")?;
        if collections
            .iter()
            .any(|id| !crate::filter::collection_id(id))
        {
            return Err(Error::Invalid(
                "shard contains an invalid collection id".into(),
            ));
        }
        let manifest = manifest_path_for(&cfg.location)
            .and_then(|path| std::fs::read_to_string(path).ok())
            .and_then(|text| serde_json::from_str::<ShardManifest>(&text).ok());
        Ok(Self {
            collections,
            snapshot,
            pool: Arc::new(Mutex::new(pool)),
            semaphore: Arc::new(tokio::sync::Semaphore::new(cfg.connections)),
            bulk,
            queued: AtomicUsize::new(0),
            max_waiters: cfg.max_waiters,
            max_wait: cfg.max_wait,
            query_timeout: cfg.query_timeout,
            flight_stream_budget: cfg.flight_stream_bytes,
            flight_total_budget: cfg.flight_total_bytes,
            flight_used: Arc::new(AtomicUsize::new(0)),
            manifest,
            http: Cache::builder()
                .max_capacity(cfg.cache_bytes)
                .weigher(|key: &String, value: &CachedBody| {
                    (key.len() + value.bytes.len() + value.etag.len()).min(u32::MAX as usize) as u32
                })
                .eviction_listener(move |_, _, _| {
                    evictions.evictions.fetch_add(1, Ordering::Relaxed);
                })
                .build(),
            stats,
        })
    }

    /// Point-in-time cache counters for capacity experiments.
    pub fn cache_stats(&self) -> CacheSnapshot {
        CacheSnapshot {
            entries: self.http.entry_count(),
            weight_bytes: self.http.weighted_size(),
            requests: self.stats.requests.load(Ordering::Relaxed),
            http_requests: self.stats.http_requests.load(Ordering::Relaxed),
            hits: self.stats.hits.load(Ordering::Relaxed),
            coalesced: self.stats.coalesced.load(Ordering::Relaxed),
            computes: self.stats.computes.load(Ordering::Relaxed),
            failures: self.stats.failures.load(Ordering::Relaxed),
            evictions: self.stats.evictions.load(Ordering::Relaxed),
        }
    }

    /// Storage-cache tuning actually in effect. All block/metadata caching
    /// lives in the ZeroFS layer below the mount; DuckDB only reports its
    /// engine budgets here so bench logs stay comparable.
    pub async fn duck_tuning(&self) -> Vec<(String, String)> {
        self.run(|conn| {
            let mut out = Vec::new();
            for key in ["threads", "memory_limit"] {
                let rows = db::text_table(
                    conn,
                    &format!("SELECT value FROM duckdb_settings() WHERE name='{key}'"),
                )?;
                if let Some(value) = rows
                    .into_iter()
                    .next()
                    .and_then(|mut row| row.pop().flatten())
                {
                    out.push((key.to_string(), value));
                }
            }
            Ok(out)
        })
        .await
        .unwrap_or_default()
    }

    pub fn collection(&self, id: &str) -> Result<(), Error> {
        if self.collections.iter().any(|c| c == id) {
            Ok(())
        } else {
            Err(Error::NotFound(id.into()))
        }
    }

    #[allow(dead_code)]
    pub fn manifest(&self) -> Option<&ShardManifest> {
        self.manifest.as_ref()
    }

    /// Precomputed centroid/name fragments: the builder always writes the
    /// derived cx/cy/name columns, so Flight serves them without per-row
    /// geometry/JSON work.
    pub fn derived_or(&self, column: &str) -> &'static str {
        match column {
            "x" => "cx AS x",
            "y" => "cy AS y",
            "name" => "name AS name",
            _ => "",
        }
    }

    /// Shared lineage filter plus explicit bbox-column overlap for Parquet
    /// statistics pruning, plus an interior fast path. Fully contained
    /// bboxes skip exact geometry work; boundary candidates still go through
    /// `ST_Intersects`, which decides every row.
    pub fn predicate(collection: &str, bounds: Option<[f64; 4]>, sources: &[i64]) -> String {
        let base = filter::predicate(collection, None, sources);
        match bounds {
            None => base,
            Some(b) => {
                let overlap = filter::bbox_range(b);
                let contained = filter::bbox_contained(b);
                let spatial = filter::spatial_predicate(b);
                format!("{base} AND {overlap} AND ({contained} OR {spatial})")
            }
        }
    }

    fn acquire_bulk(&self) -> Result<Option<tokio::sync::OwnedSemaphorePermit>, Error> {
        match &self.bulk {
            Some(semaphore) => Ok(Some(
                semaphore
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| Error::Overloaded)?,
            )),
            None => Ok(None),
        }
    }

    fn timeout_task(&self, state: Arc<QueryState>) -> Option<tokio::task::JoinHandle<()>> {
        if self.query_timeout.is_zero() {
            return None;
        }
        let deadline = self.query_timeout;
        Some(tokio::spawn(async move {
            tokio::time::sleep(deadline).await;
            if !state.is_completed() {
                state.cancel();
            }
        }))
    }

    #[allow(dead_code)]
    pub async fn run<T, F>(&self, query: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&NeoConnection) -> Result<T, Error> + Send + 'static,
    {
        self.run_class(WorkClass::Interactive, query).await
    }

    #[allow(dead_code)]
    pub async fn run_bulk<T, F>(&self, query: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&NeoConnection) -> Result<T, Error> + Send + 'static,
    {
        self.run_class(WorkClass::Bulk, query).await
    }

    async fn run_class<T, F>(&self, class: WorkClass, query: F) -> Result<T, Error>
    where
        T: Send + 'static,
        F: FnOnce(&NeoConnection) -> Result<T, Error> + Send + 'static,
    {
        let bulk = match class {
            WorkClass::Bulk => self.acquire_bulk()?,
            WorkClass::Interactive => None,
        };
        // The permit travels with the connection into the blocking worker,
        // so each return advances exactly one waiter.
        let checkout = self.checkout().await?;
        let state = Arc::new(QueryState::new());
        state.set_interrupt(checkout.conn.as_ref().unwrap());
        let timeout = self.timeout_task(state.clone());
        let guard = RunGuard {
            state: state.clone(),
            timeout,
        };
        let state_worker = state.clone();
        let result = tokio::task::spawn_blocking(move || {
            let _guard = guard;
            let _checkout = checkout;
            let _bulk = bulk;
            let conn = _checkout.conn.as_ref().unwrap();
            let out = query(conn);
            // Clear the handle before the connection returns to the pool so
            // a late timeout or disconnect cannot cancel the next query.
            state_worker.finish();
            out
        })
        .await
        .map_err(|e| Error::Backend(e.to_string()))?;
        result
    }

    /// One HTTP response attempt (used for http_requests, distinct from
    /// per-representation cache lookups).
    pub fn note_http(&self) {
        self.stats.http_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// A cached entry without computing anything. Lets callers check one
    /// representation (for example gzip) before paying for another.
    /// Counts as a lookup, and as a fast-path hit when present.
    pub async fn get(&self, key: &str) -> Option<CachedBody> {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        let hit = self.http.get(key).await;
        if hit.is_some() {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    async fn cached_compute<F, Fut>(&self, key: String, compute: F) -> Result<CachedBody, Error>
    where
        F: FnOnce() -> Fut + Send,
        Fut: std::future::Future<Output = Result<Bytes, Error>> + Send,
    {
        self.stats.requests.fetch_add(1, Ordering::Relaxed);
        if let Some(hit) = self.http.get(&key).await {
            self.stats.hits.fetch_add(1, Ordering::Relaxed);
            return Ok(hit);
        }
        let stats = self.stats.clone();
        let computed = Arc::new(AtomicU64::new(0));
        let computed_flag = computed.clone();
        let result = self
            .http
            .try_get_with(key, async move {
                stats.computes.fetch_add(1, Ordering::Relaxed);
                computed_flag.store(1, Ordering::Relaxed);
                match compute().await {
                    Ok(bytes) => Ok(CachedBody::with_bytes(bytes)),
                    Err(e) => {
                        stats.failures.fetch_add(1, Ordering::Relaxed);
                        Err(e)
                    }
                }
            })
            .await
            .map_err(|e: Arc<Error>| (*e).clone());
        let result = result?;
        if computed.load(Ordering::Relaxed) == 0 {
            // Another caller computed while we waited, or the entry landed
            // between the fast-path check and `try_get_with`.
            self.stats.coalesced.fetch_add(1, Ordering::Relaxed);
        }
        Ok(result)
    }

    /// Key must include every input used to produce the encoded HTTP body.
    /// The validator is computed once on miss and travels with the bytes, so
    /// hits and revalidations never rehash the body. Fast-path hits and
    /// coalesced waiters are counted separately; only the single computing
    /// caller counts as a compute. Failures are counted and never cached.
    /// Heavy pages share the bulk lane with Flight.
    pub async fn bytes<F>(&self, key: String, heavy: bool, query: F) -> Result<CachedBody, Error>
    where
        F: FnOnce(&NeoConnection) -> Result<Bytes, Error> + Send + 'static,
    {
        let class = if heavy {
            WorkClass::Bulk
        } else {
            WorkClass::Interactive
        };
        self.cached_compute(
            key,
            move || async move { self.run_class(class, query).await },
        )
        .await
    }

    /// Backwards-compatible interactive-only entry used by existing callers.
    #[allow(dead_code)]
    pub async fn bytes_interactive<F>(&self, key: String, query: F) -> Result<CachedBody, Error>
    where
        F: FnOnce(&NeoConnection) -> Result<Bytes, Error> + Send + 'static,
    {
        self.bytes(key, false, query).await
    }

    /// A stored gzip variant of an already-cached body, sharing the same
    /// byte budget. Pure CPU work: no pool connection is consumed, and
    /// concurrent misses for one body compress it once.
    pub async fn compressed(&self, key: String, raw: Bytes) -> Result<CachedBody, Error> {
        self.cached_compute(key, move || async move {
            tokio::task::spawn_blocking(move || gzip_bytes(&raw))
                .await
                .map_err(|e| Error::Backend(e.to_string()))?
        })
        .await
    }

    /// Single predicate-preserving Arrow query (see [`Store::predicate`]);
    /// limit/offset page the scan. Page first, convert second: geometry and
    /// JSON projections run only over the selected page instead of every
    /// scanned row.
    /// Flight results are never cached: every request runs its query.
    /// When bulk is capped, extra bulk queries fail fast instead of occupying
    /// shared queue slots, reserving the rest of the pool for interactive use.
    pub async fn arrow(
        &self,
        predicate: String,
        projection: String,
        limit: u32,
        offset: u32,
    ) -> Result<ArrowResult, Error> {
        let bulk = self.acquire_bulk()?;
        let projection = Self::rewrite_projection(&projection);
        self.run_class(WorkClass::Interactive, move |conn| {
            let _held = bulk;
            let sql = format!(
                "SELECT {projection} FROM (SELECT id, geom, properties, source_id, cx, cy, name FROM features \
                 WHERE {predicate} ORDER BY id LIMIT {limit} OFFSET {offset}) AS page ORDER BY page.id"
            );
            let mut result = conn.query(sql.as_str(), Parameters::None)?;
            let reader = db::arrow_stream(&mut result)?;
            let schema = reader.schema();
            let mut batches = Vec::new();
            for batch in reader {
                batches.push(batch.map_err(|e| Error::Backend(e.to_string()))?);
            }
            Ok(ArrowResult { schema, batches })
        })
        .await
    }

    /// One bounded streaming lifecycle. The worker owns
    /// its checkout and bulk permit until it exits; the guard owns
    /// cancellation from before execution through final delivery.
    async fn stream_sql(&self, sql: String) -> Result<FlightBatches, Error> {
        let bulk = self.acquire_bulk()?;
        let checkout = self.checkout().await?;
        let state = Arc::new(QueryState::new());
        state.set_interrupt(checkout.conn.as_ref().unwrap());
        let timeout = self.timeout_task(state.clone());
        let (schema_tx, schema_rx) = tokio::sync::oneshot::channel::<Result<SchemaRef, Error>>();
        // Count-based bound is a backstop; the byte budgets do the real work.
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Result<RecordBatch, Error>>(16);
        let per_stream = Arc::new(AtomicUsize::new(0));
        let per_stream_send = per_stream.clone();
        let global_send = self.flight_used.clone();
        let state_worker = state.clone();
        let per_budget = self.flight_stream_budget;
        let total_budget = self.flight_total_budget;
        tokio::task::spawn_blocking(move || {
            let _checkout = checkout;
            let _bulk = bulk;
            let conn = _checkout.conn.as_ref().unwrap();
            let mut result = match conn.query(sql.as_str(), Parameters::None) {
                Ok(result) => result,
                Err(e) => {
                    let _ = schema_tx.send(Err(e.into()));
                    state_worker.finish();
                    return;
                }
            };
            let reader = match db::arrow_stream(&mut result) {
                Ok(reader) => reader,
                Err(e) => {
                    let _ = schema_tx.send(Err(e));
                    state_worker.finish();
                    return;
                }
            };
            let schema = reader.schema();
            if schema_tx.send(Ok(schema)).is_err() {
                state_worker.finish();
                return;
            }
            for batch in reader {
                if state_worker.is_cancelled() || batch_tx.is_closed() {
                    break;
                }
                let batch = match batch {
                    Err(_) => {
                        let _ = batch_tx.blocking_send(Err(Error::Backend(
                            "shard streaming query failed".into(),
                        )));
                        break;
                    }
                    Ok(batch) => batch,
                };
                let size = batch.get_array_memory_size();
                if !reserve_flight_budget(
                    &per_stream_send,
                    &global_send,
                    &state_worker,
                    || batch_tx.is_closed(),
                    size,
                    per_budget,
                    total_budget,
                ) {
                    break;
                }
                if batch_tx.blocking_send(Ok(batch)).is_err() {
                    per_stream_send.fetch_sub(size, Ordering::AcqRel);
                    global_send.fetch_sub(size, Ordering::AcqRel);
                    break;
                }
            }
            // Clear before the connection returns to the pool.
            state_worker.finish();
        });
        let schema = schema_rx
            .await
            .map_err(|_| Error::Backend("shard streaming query failed".into()))??;
        Ok(FlightBatches {
            schema,
            batches: batch_rx,
            guard: StreamGuard { state, timeout },
            buffered: per_stream,
            global_buffered: self.flight_used.clone(),
        })
    }

    /// Incremental Flight payloads: the blocking worker streams Arrow batches
    /// into byte-budgeted channels while holding its pool connection, so
    /// large results do not buffer fully before the first batch. Dropping the
    /// response guard interrupts the query; a query-timeout task interrupts
    /// runaway scans. Normal completion clears the handle before the
    /// connection is reused. One predicate-preserving scan streams page
    /// rows; expensive projections run only over the page.
    pub async fn arrow_stream(
        &self,
        predicate: String,
        projection: String,
        limit: u32,
        offset: u32,
    ) -> Result<FlightBatches, Error> {
        // Rewrite the projection to use build-time derivatives.
        let projection = Self::rewrite_projection(&projection);
        let sql = format!(
            "SELECT {projection} FROM (SELECT id, geom, properties, source_id, cx, cy, name FROM features \
             WHERE {predicate} ORDER BY id LIMIT {limit} OFFSET {offset}) AS page ORDER BY page.id"
        );
        self.stream_sql(sql).await
    }

    fn rewrite_projection(projection: &str) -> String {
        projection
            .replace(
                "ST_X(ST_Centroid(geom)) AS x",
                "page.cx AS x",
            )
            .replace(
                "ST_Y(ST_Centroid(geom)) AS y",
                "page.cy AS y",
            )
            .replace(
                "coalesce(json_extract_string(properties, '$.name'), json_extract_string(properties, '$.tags.name')) AS name",
                "page.name AS name",
            )
    }

    /// Pool checkout shared by one-shot and streaming queries: bounded FIFO
    /// queue with a single deadline, permit held alongside the connection.
    async fn checkout(&self) -> Result<Checkout, Error> {
        let entered = std::time::Instant::now();
        let (permit, queued) = match self.semaphore.clone().try_acquire_owned() {
            Ok(permit) => (permit, false),
            Err(_) => {
                if self.queued.fetch_add(1, Ordering::AcqRel) >= self.max_waiters {
                    self.queued.fetch_sub(1, Ordering::Release);
                    return Err(Error::Overloaded);
                }
                let _guard = QueueGuard {
                    queued: &self.queued,
                };
                match tokio::time::timeout(self.max_wait, self.semaphore.clone().acquire_owned())
                    .await
                {
                    Ok(Ok(permit)) => (permit, true),
                    _ => return Err(Error::Overloaded),
                }
            }
        };
        tracing::debug!(
            queue_wait_us = entered.elapsed().as_micros(),
            queued,
            "pool checkout"
        );
        let conn = self.pool.lock().unwrap().pop().ok_or(Error::Overloaded)?;
        Ok(Checkout {
            conn: Some(conn),
            pool: self.pool.clone(),
            _permit: permit,
        })
    }
}

fn gzip_bytes(raw: &[u8]) -> Result<Bytes, Error> {
    let mut encoder = flate2::write::GzEncoder::new(
        Vec::with_capacity(raw.len() / 2),
        flate2::Compression::fast(),
    );
    encoder
        .write_all(raw)
        .map_err(|e| Error::Backend(e.to_string()))?;
    encoder
        .finish()
        .map(Bytes::from)
        .map_err(|e| Error::Backend(e.to_string()))
}
