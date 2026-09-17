//! One immutable snapshot and one bounded-queue pool. Responses are
//! computed per request; repeated storage reads are absorbed by the
//! zone-shared Cachey layer below.
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
use std::{
    collections::HashMap,
    io::Write,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
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
    /// Zone-local bases for Parquet reads (e.g. this zone's Cachey
    /// `/fetch/<bucket>/data/`, one entry per cache shard). Every `ATTACH`
    /// uses the first base as `DATA_PATH` override so relative data file
    /// paths resolve through this zone even though the published catalog
    /// stores a zone-independent `DATA_PATH`; per-file serving URLs spread
    /// across all bases by filename hash for stable cache ownership.
    /// Empty uses the stored path (local fixtures, direct-S3 baselines).
    pub data_bases: Vec<String>,
    /// Direct-S3 credentials for baselines that read `s3://` paths without
    /// Cachey (all three must be set). Prod Cachey mode needs no S3
    /// credentials: only Cachey talks to S3.
    pub s3_endpoint: Option<String>,
    pub s3_key_id: Option<String>,
    pub s3_secret: Option<String>,
    /// Publish-time serving index document (see [`crate::index`]), fetched
    /// by the caller (local file read or HTTP fetch) and validated here
    /// against the pinned snapshot. `None` serves without pruning.
    pub index_json: Option<String>,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            location: String::new(),
            connections: 8,
            max_waiters: 128,
            max_wait: Duration::from_millis(250),
            bulk_limit: 8,
            threads: 1,
            memory_mb: 4096,
            query_timeout: Duration::from_millis(30_000),
            flight_stream_bytes: FLIGHT_BYTE_BUDGET,
            flight_total_bytes: FLIGHT_TOTAL_BUDGET,
            data_bases: Vec::new(),
            s3_endpoint: None,
            s3_key_id: None,
            s3_secret: None,
            index_json: None,
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
/// it to the pool. Each batch carries a budget permit that releases on
/// drop, so batches queued but never consumed (disconnect) cannot strand
/// budget.
pub struct FlightBatches {
    pub schema: SchemaRef,
    pub batches: tokio::sync::mpsc::Receiver<Result<BudgetedBatch, Error>>,
    pub guard: StreamGuard,
}

/// One Arrow batch plus the byte-budget reservation backing it. Dropping
/// releases both the per-stream and process-wide reservations and wakes
/// one blocked producer.
pub struct BudgetedBatch {
    pub batch: RecordBatch,
    _permit: BudgetPermit,
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
/// batches). Producers reserve before send; each reservation lives in the
/// queued batch and releases on drop, so disconnects cannot strand budget.
/// Waiting producers sleep on a Condvar (notified on every release) with a
/// short timeout to recheck cancellation. Oversized single batches are
/// allowed once the buffer drains, so progress is guaranteed.
pub const FLIGHT_BYTE_BUDGET: usize = 32 * 1024 * 1024;
/// Process-wide cap across all concurrent Flight streams.
pub const FLIGHT_TOTAL_BUDGET: usize = 128 * 1024 * 1024;

/// Process-wide byte budget shared by all Flight streams. The Condvar is
/// the single wake-up channel for every blocked producer: any release
/// (per-stream or global) notifies it.
struct SharedBudget {
    used: Mutex<usize>,
    cvar: std::sync::Condvar,
    cap: usize,
}

/// Per-stream budget. No Condvar of its own: waiters sleep on the shared
/// budget's Condvar, which every release notifies.
struct StreamBudget {
    used: Mutex<usize>,
    cap: usize,
}

struct BudgetPermit {
    global: Arc<SharedBudget>,
    per: Arc<StreamBudget>,
    size: usize,
}

impl Drop for BudgetPermit {
    fn drop(&mut self) {
        // Lock order is global-then-per everywhere (see acquire); the
        // critical sections only touch counters.
        let mut global = self.global.used.lock().unwrap();
        let mut per = self.per.used.lock().unwrap();
        *global = global.saturating_sub(self.size);
        *per = per.saturating_sub(self.size);
        drop(per);
        drop(global);
        self.global.cvar.notify_one();
    }
}

/// Reserve `size` bytes on both budgets, waiting (cancellation-aware) for
/// room. Returns `None` when the consumer disconnected or the query was
/// cancelled. Lock order is global-then-per, matching [`BudgetPermit`].
fn acquire_flight_budget(
    global: &Arc<SharedBudget>,
    per: &Arc<StreamBudget>,
    state: &QueryState,
    closed: impl Fn() -> bool,
    size: usize,
) -> Option<BudgetPermit> {
    // Oversized batches cannot satisfy the normal bound; let one through
    // once both buffers drain instead of spinning forever.
    let oversized = size > per.cap || size > global.cap;
    let mut global_used = global.used.lock().unwrap();
    loop {
        if closed() || state.is_cancelled() {
            return None;
        }
        {
            let mut per_used = per.used.lock().unwrap();
            let fits = if oversized {
                *global_used == 0 && *per_used == 0
            } else {
                *global_used + size <= global.cap && *per_used + size <= per.cap
            };
            if fits {
                *global_used += size;
                *per_used += size;
                return Some(BudgetPermit {
                    global: Arc::clone(global),
                    per: Arc::clone(per),
                    size,
                });
            }
        }
        // Sleep until a release notifies us; the timeout rechecks
        // cancellation/disconnect promptly.
        let (guard, _) = global
            .cvar
            .wait_timeout(global_used, Duration::from_millis(50))
            .unwrap();
        global_used = guard;
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

pub struct Store {
    pub collections: Vec<String>,
    /// Pinned DuckLake snapshot this instance serves. Resolved at startup.
    pub snapshot: i64,
    /// FROM clause fallback for every serving read: the frozen all-files
    /// list when the snapshot's guards pass, else the catalog table
    /// (`features`). Used whenever no serving index applies (missing or
    /// untrusted index, unbounded request shape the index cannot prune).
    fallback_from: String,
    /// Publish-time serving index with per-file absolute URLs (shard
    /// assigned). `None` serves everything through `fallback_from`.
    index: Option<ResolvedIndex>,
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
    flight_global: Arc<SharedBudget>,
    #[allow(dead_code)]
    manifest: Option<ShardManifest>,
    http_requests: AtomicUsize,
    /// Engine budgets resolved once at startup (`duckdb_settings()` values).
    /// `/metrics` serves these without consuming a pool connection.
    tuning: Vec<(String, String)>,
    /// Arrow schemas per Flight projection, resolved once via a `FALSE`
    /// probe and reused. Projections depend only on requested columns, so
    /// discovery never needs a pool connection after warmup.
    flight_schemas: Mutex<HashMap<String, SchemaRef>>,
}

/// An HTTP representation with its validator. Cloning is cheap: the body
/// is reference-counted and the tag is small.
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

/// `ATTACH ... AS shard (...)` options. The data-path override (when
/// set) redirects relative data file reads to this zone's Cachey while
/// the catalog's stored `DATA_PATH` stays zone-independent.
pub(crate) fn attach_options(snapshot: Option<i64>, data_path_override: Option<&str>) -> String {
    let mut opts = vec!["READ_ONLY".to_string()];
    if let Some(base) = data_path_override {
        let base = if base.ends_with('/') {
            base.to_string()
        } else {
            format!("{base}/")
        };
        opts.push(format!("DATA_PATH {}", crate::filter::quote(&base)));
        opts.push("OVERRIDE_DATA_PATH true".to_string());
    }
    if let Some(snapshot) = snapshot {
        opts.push(format!("SNAPSHOT_VERSION {snapshot}"));
    }
    opts.join(", ")
}

/// Attach path for a catalog location.
pub fn catalog_url(location: &str) -> String {
    format!("ducklake:{}", location.trim_end_matches('/'))
}

/// DuckLake metadata schema for the serving attach (the alias is always
/// `shard` on serving connections, so the suffix is fixed).
const META: &str = "__ducklake_metadata_shard";

/// Builder-schema columns every serving query projects. A catalog whose
/// features table differs (older/newer builder) stays on catalog reads.
const EXPECTED_FEATURE_SCHEMA: [(&str, &str); 13] = [
    ("id", "VARCHAR"),
    ("layer", "VARCHAR"),
    ("source_id", "BIGINT"),
    ("geom", "GEOMETRY"),
    ("properties", "JSON"),
    ("sortkey", "BIGINT"),
    ("xmin", "DOUBLE"),
    ("ymin", "DOUBLE"),
    ("xmax", "DOUBLE"),
    ("ymax", "DOUBLE"),
    ("cx", "DOUBLE"),
    ("cy", "DOUBLE"),
    ("name", "VARCHAR"),
];

/// Files live at a snapshot, before URL resolution: the DATA base plus
/// ordered DATA-relative paths (schema/table prefix included).
pub(crate) struct ResolvedFiles {
    pub base: String,
    pub relpaths: Vec<String>,
}

/// FNV-1a 64-bit: stable across processes, no BuildHasherDoS random state.
fn fnv1a64(s: &str) -> u64 {
    const OFFSET: u64 = 0xcbf29ce484222325;
    const PRIME: u64 = 0x100000001b3;
    let mut h = OFFSET;
    for b in s.bytes() {
        h ^= u64::from(b);
        h = h.wrapping_mul(PRIME);
    }
    h
}

/// Shard-owning base for one file. Empty bases mean "stored path": every
/// file resolves against the catalog's own DATA base (local fixtures,
/// direct-S3 baselines). Otherwise the filename hash picks a base, so each
/// object has exactly one owning cache shard across restarts and scales.
fn shard_base<'a>(bases: &'a [String], stored_base: &'a str, relpath: &str) -> &'a str {
    if bases.is_empty() {
        stored_base
    } else {
        let i = (fnv1a64(relpath) % bases.len() as u64) as usize;
        bases[i].as_str()
    }
}

fn slash(s: &str) -> String {
    if s.ends_with('/') {
        s.to_string()
    } else {
        format!("{s}/")
    }
}

/// Full read URLs for resolved files, with per-file shard assignment.
/// Absolute paths (unverified legacy layouts) pass through unassigned,
/// exactly as before sharding existed.
pub(crate) fn file_urls(bases: &[String], stored_base: &str, relpaths: &[String]) -> Vec<String> {
    relpaths
        .iter()
        .map(|rel| {
            if rel.contains("://") || rel.starts_with('/') {
                rel.clone()
            } else {
                format!("{}{}", slash(shard_base(bases, stored_base, rel)), rel)
            }
        })
        .collect()
}

/// Ordered live files plus DATA base; all serving-source guards live here.
/// Both the catalog-table fallback and the serving-index path build on it.
pub(crate) fn resolve_files(conn: &NeoConnection, snapshot: i64) -> Result<ResolvedFiles, String> {
    let tid = db::int_one(
        conn,
        &format!("SELECT table_id FROM {META}.ducklake_table WHERE table_name='features'"),
    )
    .map_err(|e| format!("features table id: {e}"))?;
    let cols = db::text_table(
        conn,
        "SELECT column_name, column_type FROM (DESCRIBE shard.features)",
    )
    .map_err(|e| format!("features schema: {e}"))?;
    let got: Vec<(String, String)> = cols
        .into_iter()
        .map(|mut row| {
            let mut cells = row.drain(..);
            (
                cells.next().flatten().unwrap_or_default(),
                cells.next().flatten().unwrap_or_default(),
            )
        })
        .collect();
    let want: Vec<(String, String)> = EXPECTED_FEATURE_SCHEMA
        .iter()
        .map(|(name, ty)| (name.to_string(), ty.to_string()))
        .collect();
    if got != want {
        return Err("features schema differs from builder schema".into());
    }
    let files = db::text_table(
        conn,
        &format!(
            "SELECT path, path_is_relative::VARCHAR FROM {META}.ducklake_data_file \
             WHERE table_id={tid} AND begin_snapshot<={snapshot} \
             AND (end_snapshot IS NULL OR end_snapshot>{snapshot}) ORDER BY file_order"
        ),
    )
    .map_err(|e| format!("file list: {e}"))?;
    if files.is_empty() {
        return Err("no live files at snapshot".into());
    }
    // Table files nest under <base>/<schema.path>/<table.path>/ (e.g.
    // `s3://lake/data/main/features/<file>`); resolve both segments at the
    // pinned snapshot. Absolute segments are layouts we have not verified:
    // fail closed.
    let table = db::text_table(
        conn,
        &format!(
            "SELECT schema_id::VARCHAR, path, path_is_relative::VARCHAR FROM {META}.ducklake_table \
             WHERE table_id={tid} AND begin_snapshot<={snapshot} \
             AND (end_snapshot IS NULL OR end_snapshot>{snapshot})"
        ),
    )
    .map_err(|e| format!("table entry: {e}"))?;
    if table.len() != 1 || table[0].len() != 3 {
        return Err("table entry is not unique at snapshot".into());
    }
    let schema_id: i64 = table[0][0]
        .as_deref()
        .unwrap_or_default()
        .parse()
        .map_err(|_| "table schema id is not an integer".to_string())?;
    let table_path = table[0][1].clone().unwrap_or_default();
    let table_relative = table[0][2].as_deref().unwrap_or_default();
    let schema = db::text_table(
        conn,
        &format!(
            "SELECT path, path_is_relative::VARCHAR FROM {META}.ducklake_schema \
             WHERE schema_id={schema_id} AND begin_snapshot<={snapshot} \
             AND (end_snapshot IS NULL OR end_snapshot>{snapshot})"
        ),
    )
    .map_err(|e| format!("schema entry: {e}"))?;
    if schema.len() != 1 || schema[0].len() != 2 {
        return Err("schema entry is not unique at snapshot".into());
    }
    let schema_path = schema[0][0].clone().unwrap_or_default();
    let schema_relative = schema[0][1].as_deref().unwrap_or_default();
    for segment in [&schema_path, &table_path] {
        if segment.contains("://") || segment.starts_with('/') {
            return Err("absolute schema/table segment".into());
        }
    }
    if !table_relative.eq_ignore_ascii_case("true") || !schema_relative.eq_ignore_ascii_case("true")
    {
        return Err("non-relative schema/table segment".into());
    }
    let slash = |s: &str| {
        if s.ends_with('/') {
            s.to_string()
        } else {
            format!("{s}/")
        }
    };
    let prefix = format!("{}{}", slash(&schema_path), slash(&table_path));
    let deletes = db::int_one(
        conn,
        &format!(
            "SELECT count(*) FROM {META}.ducklake_delete_file WHERE table_id={tid} \
             AND begin_snapshot<={snapshot} AND (end_snapshot IS NULL OR end_snapshot>{snapshot})"
        ),
    )
    .map_err(|e| format!("delete files: {e}"))?;
    if deletes > 0 {
        return Err(format!("{deletes} live delete files"));
    }
    // The inlined-delete table exists only once a delete was ever written
    // for the table; absence is clean. Any other error fails closed.
    match db::int_one(
        conn,
        &format!(
            "SELECT count(*) FROM {META}.ducklake_inlined_delete_{tid} WHERE begin_snapshot<={snapshot}"
        ),
    ) {
        Ok(n) if n > 0 => return Err(format!("{n} inlined deletes")),
        Ok(_) => {}
        Err(e) if e.to_string().contains("does not exist") => {}
        Err(e) => return Err(format!("inlined deletes: {e}")),
    }
    let inlined = db::int_one(
        conn,
        &format!("SELECT count(*) FROM {META}.ducklake_inlined_data_tables WHERE table_id={tid}"),
    )
    .map_err(|e| format!("inlined data: {e}"))?;
    if inlined > 0 {
        return Err("inlined data present".into());
    }
    // The stored base travels with the file list; callers pick the
    // effective base per file (override shards or stored path).
    let stored = {
        let rows = db::text_table(conn, "SELECT data_path FROM ducklake_settings('shard')")
            .map_err(|e| format!("catalog data path: {e}"))?;
        rows.into_iter()
            .next()
            .and_then(|mut row| row.pop().flatten())
            .filter(|base| !base.is_empty())
            .ok_or_else(|| "catalog data path is empty".to_string())?
    };
    let mut relpaths = Vec::with_capacity(files.len());
    for mut file in files {
        let mut cells = file.drain(..);
        let path = cells.next().flatten().unwrap_or_default();
        let relative = cells
            .next()
            .flatten()
            .is_some_and(|flag| flag.eq_ignore_ascii_case("true"));
        relpaths.push(if relative {
            format!("{prefix}{path}")
        } else {
            path
        });
    }
    Ok(ResolvedFiles {
        base: stored,
        relpaths,
    })
}

/// Serving index with per-file absolute URLs, resolved once at open.
struct ResolvedIndex {
    index: crate::index::ServingIndex,
    urls: Vec<String>,
}

/// Validate a candidate index document: version, pinned-commit match, and
/// exact file-set agreement with freshly resolved catalog state. Anything
/// else ignores the index (fail closed to the fallback).
fn check_index(
    doc: &str,
    snapshot: i64,
    relpaths: &[String],
) -> Result<crate::index::ServingIndex, String> {
    let index: crate::index::ServingIndex =
        serde_json::from_str(doc).map_err(|e| format!("serving index parse: {e}"))?;
    if index.version != crate::index::INDEX_VERSION {
        return Err(format!("serving index version {}", index.version));
    }
    if index.ducklake_commit != snapshot {
        return Err(format!(
            "serving index commit {} != pinned snapshot {snapshot}",
            index.ducklake_commit
        ));
    }
    let indexed: Vec<String> = index.files.iter().map(|f| f.path.clone()).collect();
    if indexed != relpaths {
        return Err("serving index file set disagrees with catalog".into());
    }
    Ok(index)
}

/// Resolve both serving sources at open: the all-files fallback plus the
/// pruned index when the published document checks out.
fn resolve_serving(
    opener: &NeoConnection,
    cfg: &StoreConfig,
    snapshot: i64,
) -> (String, Option<ResolvedIndex>) {
    let resolved = match resolve_files(opener, snapshot) {
        Ok(resolved) => resolved,
        Err(reason) => {
            tracing::warn!(reason, "serving reads from catalog table");
            return ("features".to_string(), None);
        }
    };
    let all_urls = file_urls(&cfg.data_bases, &resolved.base, &resolved.relpaths);
    let fallback_from = format!(
        "read_parquet([{}])",
        all_urls
            .iter()
            .map(|u| crate::filter::quote(u))
            .collect::<Vec<_>>()
            .join(",")
    );
    tracing::info!("serving reads from frozen file list");
    let index = match cfg.index_json.as_deref() {
        None => {
            tracing::info!("no serving index published; no per-request pruning");
            None
        }
        Some(doc) => match check_index(doc, snapshot, &resolved.relpaths) {
            Ok(index) => {
                tracing::info!(
                    files = resolved.relpaths.len(),
                    partitions = index.partitions.len(),
                    "serving index active"
                );
                Some(ResolvedIndex {
                    index,
                    urls: all_urls,
                })
            }
            Err(reason) => {
                tracing::warn!(reason, "ignoring serving index");
                None
            }
        },
    };
    (fallback_from, index)
}

/// Origin (scheme + authority) of an http(s) location, with an optional
/// `ducklake:` catalog prefix stripped. Used to scope the Cachey
/// request-config secret below.
pub(crate) fn http_origin(location: &str) -> Option<String> {
    let location = location.strip_prefix("ducklake:").unwrap_or(location);
    for scheme in ["http://", "https://"] {
        if let Some(rest) = location.strip_prefix(scheme) {
            let authority = rest.split('/').next().unwrap_or("");
            if !authority.is_empty() {
                return Some(format!("{scheme}{authority}"));
            }
        }
    }
    None
}

/// `CREATE OR REPLACE SECRET` attaching `C0-Config: fps=true` to requests
/// under the catalog's origin, so Cachey fetches path-style from any
/// S3-compatible backend. `None` for local catalogs (no HTTP involved).
/// Data files live under the same Cachey base in every layout we publish,
/// so the catalog origin covers them; when a data-path override points at
/// a different origin, the caller adds a second secret for it.
pub(crate) fn cachey_secret_sql(location: &str) -> Option<String> {
    http_origin(location).map(|origin| {
        format!(
            "CREATE OR REPLACE SECRET iron_feather_cachey (TYPE http, SCOPE {}, EXTRA_HTTP_HEADERS MAP {{'C0-Config': 'fps=true'}})",
            crate::filter::quote(&origin)
        )
    })
}

/// `CREATE SECRET` for direct-S3 reads (baselines only): explicit key pair
/// plus endpoint, so DuckDB signs `s3://` requests itself instead of going
/// through Cachey. The endpoint accepts `host:port` or a full URL; the
/// scheme decides `USE_SSL`. Path-style addressing works against MinIO
/// and real S3 alike.
pub(crate) fn s3_secret_sql(endpoint: &str, key_id: &str, secret: &str) -> String {
    let (host, use_ssl) = if let Some(rest) = endpoint.strip_prefix("https://") {
        (rest.trim_end_matches('/'), true)
    } else if let Some(rest) = endpoint.strip_prefix("http://") {
        (rest.trim_end_matches('/'), false)
    } else {
        (endpoint.trim_end_matches('/'), false)
    };
    format!(
        "CREATE OR REPLACE SECRET iron_feather_s3 (TYPE s3, PROVIDER config, KEY_ID {}, SECRET {}, REGION 'us-east-1', ENDPOINT {}, USE_SSL {}, URL_STYLE 'path')",
        crate::filter::quote(key_id),
        crate::filter::quote(secret),
        crate::filter::quote(host),
        use_ssl,
    )
}

/// Late materialization rewrites small-LIMIT scans into a row-id-driven
/// double read: measured ~2x bytes and latency on small OGC pages at the
/// default 50-row threshold (our limit=10 pages fetch 11 rows and flip;
/// limit=100 pages never do). Serving scans are Top-N over clustered
/// Parquet where the straight scan wins, so disable it. Best-effort: this
/// is a DEBUG setting and a future engine may rename it; failure only
/// costs the optimization, never correctness or startup.
pub(crate) fn disable_late_materialization(conn: &NeoConnection) {
    if let Err(e) = db::execute_all(conn, &["SET late_materialization_max_rows=0"]) {
        tracing::warn!(error = %e, "late materialization tuning not applied");
    }
}

/// Full per-connection setup: extensions, then the catalog attach pinned to
/// `snapshot`. Every pooled connection runs the session part; the ATTACH
/// itself is database-level and done once (see [`Store::open_config`]).
/// Parquet/HTTP block and metadata caching lives in the Cachey layer the
/// catalog URLs point at, not in DuckDB: no cache tuning here.
fn setup_session(conn: &NeoConnection, cfg: &StoreConfig, _remote: bool) -> Result<(), Error> {
    // Every extension installs on demand (fresh containers have an empty
    // extension dir), then the lockdown below freezes further installs.
    for ext in ["ducklake", "spatial", "httpfs"] {
        if db::execute_all(conn, &[&format!("LOAD {ext}")]).is_err() {
            db::execute_all(conn, &[&format!("INSTALL {ext}"), &format!("LOAD {ext}")])?;
        }
    }
    if let Some(secret) = cachey_secret_sql(&cfg.location) {
        db::execute_all(conn, &[secret.as_str()])?;
    }
    // The data-path overrides usually share the catalog's origin (same
    // zone Cachey); each distinct origin gets its own secret so Parquet
    // fetches carry the same request config whichever shard owns them.
    // A BTreeSet keeps secret creation deterministic across restarts.
    let mut origins = std::collections::BTreeSet::new();
    for base in &cfg.data_bases {
        if let Some(origin) = http_origin(base) {
            if Some(&origin) != http_origin(&cfg.location).as_ref() {
                origins.insert(origin);
            }
        }
    }
    for (i, origin) in origins.into_iter().enumerate() {
        let secret = format!(
            "CREATE OR REPLACE SECRET iron_feather_cachey_data_{i} (TYPE http, SCOPE {}, EXTRA_HTTP_HEADERS MAP {{'C0-Config': 'fps=true'}})",
            crate::filter::quote(&origin)
        );
        db::execute_all(conn, &[secret.as_str()])?;
    }
    // Direct-S3 baselines only: prod Cachey mode sets no S3 credentials.
    if let (Some(endpoint), Some(key_id), Some(secret)) = (
        cfg.s3_endpoint.as_deref(),
        cfg.s3_key_id.as_deref(),
        cfg.s3_secret.as_deref(),
    ) {
        let sql = s3_secret_sql(endpoint, key_id, secret);
        db::execute_all(conn, &[sql.as_str()])?;
    }
    db::execute_all(
        conn,
        &[
            "SET autoinstall_known_extensions=false",
            "SET autoload_known_extensions=false",
            // Immutable snapshots: Parquet files and catalog objects are
            // never modified in place, so metadata parsing and cache
            // revalidation are pure overhead. Cache footers/HTTP metadata
            // and skip validation (which would otherwise re-HEAD Cachey
            // per query). Byte caching still lives in Cachey below.
            "SET parquet_metadata_cache=true",
            "SET enable_http_metadata_cache=true",
            "SET validate_external_file_cache='NO_VALIDATION'",
        ],
    )?;
    disable_late_materialization(conn);
    Ok(())
}

impl Store {
    #[allow(clippy::too_many_arguments)]
    #[allow(dead_code)]
    pub fn open(
        location: &str,
        connections: usize,
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
        let s3_parts = [
            cfg.s3_endpoint.is_some(),
            cfg.s3_key_id.is_some(),
            cfg.s3_secret.is_some(),
        ];
        if s3_parts.iter().any(|p| *p) && s3_parts.iter().any(|p| !*p) {
            return Err(Error::Invalid(
                "--s3-endpoint, --s3-key-id and --s3-secret must be set together".into(),
            ));
        }
        // Only a cap below the pool size changes behavior; at full size bulk
        // shares the queue like everything else.
        let bulk = (cfg.bulk_limit < cfg.connections)
            .then(|| Arc::new(tokio::sync::Semaphore::new(cfg.bulk_limit)));
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
        // The catalog attach keeps a single DATA_PATH override (first
        // base): per-file shard URLs carry their own bases, and the rare
        // catalog-table fallback reads through this one shard.
        let data_override = cfg.data_bases.first().map(String::as_str);
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
                    "ATTACH {} AS shard ({})",
                    crate::filter::quote(&catalog),
                    attach_options(None, data_override),
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
                    "ATTACH {} AS shard ({})",
                    crate::filter::quote(&catalog),
                    attach_options(Some(snapshot), data_override),
                ),
                "USE shard",
            ],
        )?;
        // Freeze the serving file list at the pinned snapshot while the
        // opener still holds the pinned attach. Falls back to the catalog
        // table on any doubt (see resolve_serving).
        let (fallback_from, index) = resolve_serving(&opener, &cfg, snapshot);
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
        // Engine budgets are fixed at open; resolve once so /metrics never
        // borrows a pool connection.
        let mut tuning = Vec::new();
        for key in ["threads", "memory_limit"] {
            let rows = db::text_table(
                probe,
                &format!("SELECT value FROM duckdb_settings() WHERE name='{key}'"),
            )?;
            if let Some(value) = rows
                .into_iter()
                .next()
                .and_then(|mut row| row.pop().flatten())
            {
                tuning.push((key.to_string(), value));
            }
        }
        Ok(Self {
            collections,
            snapshot,
            fallback_from,
            index,
            pool: Arc::new(Mutex::new(pool)),
            semaphore: Arc::new(tokio::sync::Semaphore::new(cfg.connections)),
            bulk,
            queued: AtomicUsize::new(0),
            max_waiters: cfg.max_waiters,
            max_wait: cfg.max_wait,
            query_timeout: cfg.query_timeout,
            flight_stream_budget: cfg.flight_stream_bytes,
            flight_global: Arc::new(SharedBudget {
                used: Mutex::new(0),
                cvar: std::sync::Condvar::new(),
                cap: cfg.flight_total_bytes,
            }),
            manifest,
            http_requests: AtomicUsize::new(0),
            tuning,
            flight_schemas: Mutex::new(HashMap::new()),
        })
    }

    /// HTTP responses served. Storage reads are not counted here; Cachey
    /// reports its own page/download counters.
    pub fn http_requests(&self) -> usize {
        self.http_requests.load(Ordering::Relaxed)
    }

    /// Currently reserved Flight buffer bytes process-wide. Exposed for
    /// tests to prove disconnects cannot strand budget.
    #[allow(dead_code)]
    pub fn flight_used_bytes(&self) -> usize {
        *self.flight_global.used.lock().unwrap()
    }

    /// Storage-cache tuning actually in effect. All block/metadata caching
    /// lives in the Cachey layer the catalog URLs point at; DuckDB only reports its
    /// engine budgets here so bench logs stay comparable.
    pub fn duck_tuning(&self) -> Vec<(String, String)> {
        self.tuning.clone()
    }

    pub fn collection(&self, id: &str) -> Result<(), Error> {
        if self.collections.iter().any(|c| c == id) {
            Ok(())
        } else {
            Err(Error::NotFound(id.into()))
        }
    }

    /// FROM clause for a request: pruned candidate files from the serving
    /// index, or the frozen all-files fallback when no index applies.
    /// Empty selections read one footer through a FALSE guard (matches
    /// nothing, opens nothing else).
    pub fn read_source(&self, bounds: Option<[f64; 4]>) -> String {
        let Some(resolved) = self.index.as_ref() else {
            return self.fallback_from.clone();
        };
        let mut idxs = crate::index::prune_files(&resolved.index, bounds);
        idxs.retain(|&i| i < resolved.urls.len());
        if idxs.is_empty() {
            // Footer-only probe of one file; matches nothing, opens nothing
            // else. (Index file sets are never empty: generation and the
            // open-time agreement check both require it.)
            return match resolved.urls.first() {
                Some(first) => format!(
                    "(SELECT * FROM read_parquet([{}]) WHERE FALSE) AS _empty",
                    crate::filter::quote(first)
                ),
                None => self.fallback_from.clone(),
            };
        }
        let urls: Vec<String> = idxs
            .iter()
            .map(|&i| crate::filter::quote(&resolved.urls[i]))
            .collect();
        format!("read_parquet([{}])", urls.join(","))
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

    /// One HTTP response served.
    pub fn note_http(&self) {
        self.http_requests.fetch_add(1, Ordering::Relaxed);
    }

    /// Run one HTTP response query on the pool and wrap the bytes with a
    /// validator. Every request executes: identical concurrent requests each
    /// run their own query (bounded by pool admission), and the zone-shared
    /// Cachey layer absorbs repeated storage reads. Heavy pages share the
    /// bulk lane with Flight.
    pub async fn run_bytes<F>(&self, heavy: bool, query: F) -> Result<CachedBody, Error>
    where
        F: FnOnce(&NeoConnection) -> Result<Bytes, Error> + Send + 'static,
    {
        let class = if heavy {
            WorkClass::Bulk
        } else {
            WorkClass::Interactive
        };
        let bytes = self.run_class(class, query).await?;
        Ok(CachedBody::with_bytes(bytes))
    }

    /// Arrow schema for a Flight projection, resolved once and reused.
    /// Schemas depend only on requested columns (never the predicate), so
    /// discovery pays one `FALSE` probe per unique projection; the pool
    /// stays free afterwards.
    pub async fn flight_schema(&self, projection: &str) -> Result<SchemaRef, Error> {
        let projection = Self::rewrite_projection(projection);
        if let Some(hit) = self.flight_schemas.lock().unwrap().get(&projection) {
            return Ok(hit.clone());
        }
        // The FALSE probe matches nothing; pruning is irrelevant, so it
        // reads the fallback source directly.
        let schema = self
            .arrow(
                "FALSE".into(),
                projection.clone(),
                self.fallback_from.clone(),
                1,
                0,
            )
            .await?
            .schema;
        self.flight_schemas
            .lock()
            .unwrap()
            .insert(projection, schema.clone());
        Ok(schema)
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
        from: String,
        limit: u32,
        offset: u32,
    ) -> Result<ArrowResult, Error> {
        let bulk = self.acquire_bulk()?;
        let projection = Self::rewrite_projection(&projection);
        self.run_class(WorkClass::Interactive, move |conn| {
            let _held = bulk;
            let sql = format!(
                "SELECT {projection} FROM (SELECT id, geom, properties, source_id, cx, cy, name FROM {from} \
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
    /// cancellation from before execution through final delivery,
    /// including while waiting for the schema.
    async fn stream_sql(&self, sql: String) -> Result<FlightBatches, Error> {
        let bulk = self.acquire_bulk()?;
        let checkout = self.checkout().await?;
        let state = Arc::new(QueryState::new());
        state.set_interrupt(checkout.conn.as_ref().unwrap());
        let timeout = self.timeout_task(state.clone());
        // Own cancellation before the schema wait: dropping this future
        // aborts the deadline task and interrupts the worker instead of
        // leaking a detached timeout.
        let guard = StreamGuard {
            state: state.clone(),
            timeout,
        };
        let (schema_tx, schema_rx) = tokio::sync::oneshot::channel::<Result<SchemaRef, Error>>();
        // Count-based bound is a backstop; the byte budgets do the real work.
        let (batch_tx, batch_rx) = tokio::sync::mpsc::channel::<Result<BudgetedBatch, Error>>(16);
        let per_stream = Arc::new(StreamBudget {
            used: Mutex::new(0),
            cap: self.flight_stream_budget,
        });
        let global = Arc::clone(&self.flight_global);
        let state_worker = state.clone();
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
                let Some(permit) = acquire_flight_budget(
                    &global,
                    &per_stream,
                    &state_worker,
                    || batch_tx.is_closed(),
                    size,
                ) else {
                    break;
                };
                // The permit travels with the batch: it releases when the
                // consumer drops it, or when a queued batch is dropped
                // after disconnect. Send failure drops the permit inline.
                if batch_tx
                    .blocking_send(Ok(BudgetedBatch {
                        batch,
                        _permit: permit,
                    }))
                    .is_err()
                {
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
            guard,
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
        from: String,
        limit: u32,
        offset: u32,
    ) -> Result<FlightBatches, Error> {
        // Rewrite the projection to use build-time derivatives.
        let projection = Self::rewrite_projection(&projection);
        let sql = format!(
            "SELECT {projection} FROM (SELECT id, geom, properties, source_id, cx, cy, name FROM {from} \
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

/// Compress one response body off the async runtime. No pooling, no
/// caching: every gzip request pays CPU once.
pub(crate) async fn gzip_body(raw: Bytes) -> Result<CachedBody, Error> {
    tokio::task::spawn_blocking(move || gzip_bytes(&raw).map(CachedBody::with_bytes))
        .await
        .map_err(|e| Error::Backend(e.to_string()))?
}

#[cfg(test)]
mod secret_tests {
    use super::{attach_options, cachey_secret_sql, http_origin, s3_secret_sql};

    #[test]
    fn origin_scopes_to_scheme_and_authority() {
        assert_eq!(
            http_origin("http://127.0.0.1:8088/fetch/lake/c/x.ducklake"),
            Some("http://127.0.0.1:8088".into())
        );
        assert_eq!(
            http_origin("ducklake:https://cachey-az-a/fetch/lake/c/x.ducklake"),
            Some("https://cachey-az-a".into())
        );
        assert_eq!(http_origin("s3://lake/c/x.ducklake"), None);
        assert_eq!(http_origin("/mnt/lake/c/x.ducklake"), None);
        assert_eq!(http_origin("http:///no-authority"), None);
    }

    #[test]
    fn secret_attaches_path_style_header_to_the_origin() {
        let sql = cachey_secret_sql("http://lake-cachey-az-a/fetch/lake/c/x.ducklake").unwrap();
        assert!(sql.starts_with("CREATE OR REPLACE SECRET iron_feather_cachey"));
        assert!(sql.contains("SCOPE 'http://lake-cachey-az-a'"));
        assert!(sql.contains("'C0-Config': 'fps=true'"));
        assert_eq!(cachey_secret_sql("fixtures/osm.ducklake"), None);
    }

    #[test]
    fn attach_options_cover_read_only_pin_and_zone_override() {
        assert_eq!(attach_options(None, None), "READ_ONLY");
        assert_eq!(
            attach_options(Some(6), None),
            "READ_ONLY, SNAPSHOT_VERSION 6"
        );
        assert_eq!(
            attach_options(None, Some("http://cachey/fetch/lake/data")),
            "READ_ONLY, DATA_PATH 'http://cachey/fetch/lake/data/', OVERRIDE_DATA_PATH true"
        );
        assert_eq!(
            attach_options(Some(6), Some("http://cachey/fetch/lake/data/")),
            "READ_ONLY, DATA_PATH 'http://cachey/fetch/lake/data/', OVERRIDE_DATA_PATH true, SNAPSHOT_VERSION 6"
        );
    }

    #[test]
    fn s3_secret_normalizes_endpoint_and_ssl() {
        let sql = s3_secret_sql("http://127.0.0.1:3900/", "ak", "sk");
        assert!(sql.contains("ENDPOINT '127.0.0.1:3900'"));
        assert!(sql.contains("USE_SSL false"));
        assert!(sql.contains("URL_STYLE 'path'"));
        let sql = s3_secret_sql("https://s3.us-east-1.amazonaws.com", "ak", "sk");
        assert!(sql.contains("ENDPOINT 's3.us-east-1.amazonaws.com'"));
        assert!(sql.contains("USE_SSL true"));
        let sql = s3_secret_sql("127.0.0.1:3903", "ak", "sk");
        assert!(sql.contains("ENDPOINT '127.0.0.1:3903'"));
        assert!(sql.contains("USE_SSL false"));
    }
}
