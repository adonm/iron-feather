//! Bulk protocol: a Quack listener over a locked-down, snapshot-pinned view.
//!
//! The Quack instance is a **separate database** from the serving pool, so
//! bulk scans cannot evict the OGC/Flight working set. What a client can
//! reach is narrowed in depth:
//!
//! 1. **Snapshot pin (robust).** The catalog attaches at the exact snapshot
//!    the server resolved at startup. New publishes are invisible; the
//!    engine additionally rejects writes on pinned attaches.
//! 2. **Read-only attach (robust).** `INSERT`/`UPDATE`/DDL against the
//!    catalog fail in the engine, verified by regression tests.
//! 3. **Token auth (robust).** A client secret (or explicit `TOKEN`) is
//!    required on every connection; the token comes from `--quack-token`
//!    or is randomly generated per process and printed once at startup.
//! 4. **Localhost bind (robust).** Non-local binds require an explicit flag;
//!    TLS stays a reverse-proxy concern, following upstream.
//! 5. **Statement filter (defense in depth).** A guard macro rejects control
//!    plane (`quack_serve`/`quack_stop`, server-global settings),
//!    catalog topology (`ATTACH`/`DETACH`), file I/O (`COPY`, `read_*`,
//!    `st_read`, direct-URL `FROM`), extension loading, and secret creation.
//!    Matching is statement-initial (after `^`/`;`, comments and whitespace)
//!    so data literals cannot trip it, except function-call shapes
//!    (`quack_serve(`, `read_*(`, ...) which never appear in real queries.
//!    Unknown statements fail closed only where the engine already denies
//!    them; anything else a token holder runs is equivalent to handing them
//!    a local DuckDB shell, so treat the token as privileged.
//!
//! Parquet-backed tables cannot serve behind `enable_external_access=false`
//! (verified: the quack execution context fails to resolve data files), so
//! the lockdown stays off and file access is filtered by the guard instead.

use crate::{
    db::{self, NeoConnection, NeoDatabase},
    store::{
        attach_options, cachey_secret_sql, cachey_secret_sql_named, disable_late_materialization,
        http_origin, Error, StoreConfig,
    },
};
use duckdb_neo::Parameters;
use std::net::SocketAddr;

/// Statement filter for the Quack authorization hook. Statement-initial
/// keywords (after start/`;`, comments, whitespace) cover smuggled second
/// statements; function-call shapes cover `SELECT quack_serve(...)` style
/// wrapping. Data literals cannot trip the initial-keyword arm; the call
/// shapes never appear in real feature queries.
const GUARD_MACRO: &str = "CREATE MACRO memory.main.quack_guard(sid, query) AS (
  NOT regexp_matches(query, '(?i)(^|;)\\s*(/\\*.*?\\*/\\s*|--[^\\n]*\\n\\s*)*(copy|attach|detach|install|load|set|reset|create\\s+secret|call\\s+quack_(serve|stop))\\b')
  AND NOT regexp_matches(query, '(?i)quack_(serve|stop)\\s*\\(|read_[a-z_]+\\s*\\(|st_read\\s*\\(|from\\s+''(https?|s3)://'))";

pub struct QuackServer {
    #[allow(dead_code)]
    db: NeoDatabase,
    // Held for the server's lifetime: quack_serve runs against this
    // connection's session.
    #[allow(dead_code)]
    conn: NeoConnection,
    uri: String,
    port: u16,
}

impl QuackServer {
    /// Start serving `catalog` pinned at `snapshot`. `listen` decides the
    /// bind address; non-local binds require `allow_remote`.
    pub fn start(
        cfg: &StoreConfig,
        remote: bool,
        catalog: &str,
        snapshot: i64,
        listen: SocketAddr,
        token: Option<String>,
        allow_remote: bool,
    ) -> Result<(Self, String), Error> {
        if !allow_remote && !is_local(listen) {
            return Err(Error::Invalid(
                "quack binds localhost only without --allow-remote-quack".into(),
            ));
        }
        let db = db::open_memory()?;
        apply_budgets(&db, cfg)?;
        let conn = db.connect()?;
        setup_quack_session(&conn, cfg, remote, catalog, snapshot)?;
        let uri = quack_uri(listen);
        let token = match token {
            Some(token) if !token.is_empty() => token,
            _ => db::strings_col(&conn, "SELECT uuid()::VARCHAR")?
                .pop()
                .ok_or(Error::Backend("token generation failed".into()))?,
        };
        let serve = format!(
            "CALL quack_serve({}, token => {})",
            crate::filter::quote(&uri),
            crate::filter::quote(&token)
        );
        conn.execute(serve.as_str(), Parameters::None)?;
        Ok((
            Self {
                db,
                conn,
                uri: uri.clone(),
                port: listen.port(),
            },
            token,
        ))
    }

    pub fn uri(&self) -> &str {
        &self.uri
    }

    pub fn port(&self) -> u16 {
        self.port
    }

    /// Polite shutdown; process exit also terminates the listener.
    pub fn stop(&self) -> Result<(), Error> {
        let stop = format!("CALL quack_stop({})", crate::filter::quote(&self.uri));
        self.conn.execute(stop.as_str(), Parameters::None)?;
        Ok(())
    }
}

fn is_local(addr: SocketAddr) -> bool {
    match addr.ip() {
        std::net::IpAddr::V4(ip) => ip.is_loopback(),
        std::net::IpAddr::V6(ip) => ip.is_loopback(),
    }
}

fn quack_uri(listen: SocketAddr) -> String {
    match listen.ip() {
        std::net::IpAddr::V4(ip) => format!("quack:{ip}:{}", listen.port()),
        std::net::IpAddr::V6(ip) => format!("quack:[{ip}]:{}", listen.port()),
    }
}

fn apply_budgets(db: &NeoDatabase, cfg: &StoreConfig) -> Result<(), Error> {
    // The bulk instance shares the operator's budgets: predictable memory
    // beats squeezing the latency pool.
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
    Ok(())
}

fn setup_quack_session(
    conn: &NeoConnection,
    cfg: &StoreConfig,
    remote: bool,
    catalog: &str,
    snapshot: i64,
) -> Result<(), Error> {
    for ext in ["quack", "ducklake", "spatial", "httpfs"] {
        if db::execute_all(conn, &[&format!("LOAD {ext}")]).is_err() {
            db::execute_all(conn, &[&format!("INSTALL {ext}"), &format!("LOAD {ext}")])?;
        }
    }
    db::execute_all(
        conn,
        &[
            "SET autoinstall_known_extensions=false",
            "SET autoload_known_extensions=false",
            // Same immutable-snapshot rationale as the serving pool (see
            // setup_session): cache metadata, skip revalidation.
            "SET parquet_metadata_cache=true",
            "SET enable_http_metadata_cache=true",
            "SET validate_external_file_cache='NO_VALIDATION'",
        ],
    )?;
    disable_late_materialization(conn);
    // Same Cachey request-config header as the serving pool: the guard
    // installed below rejects secret creation, so this runs before it.
    if let Some(secret) = cachey_secret_sql(catalog) {
        db::execute_all(conn, &[secret.as_str()])?;
    }
    if let Some(base) = cfg.data_path_override.as_deref() {
        if http_origin(base) != http_origin(catalog) {
            if let Some(secret) = cachey_secret_sql_named("iron_feather_cachey_data", base) {
                db::execute_all(conn, &[secret.as_str()])?;
            }
        }
    }
    let _ = remote;
    db::execute_all(
        conn,
        &[
            &format!(
                "ATTACH {} AS shard ({})",
                crate::filter::quote(catalog),
                attach_options(Some(snapshot), cfg.data_path_override.as_deref()),
            ),
            // The guard lives in memory: the lake catalog is read-only and
            // would reject the CREATE.
            GUARD_MACRO,
            "USE shard",
            // No external-access lockdown: Parquet-backed tables cannot
            // serve behind it (the quack execution context fails to resolve
            // data files), so file access is filtered by the guard instead.
            "SET GLOBAL quack_authorization_function = 'memory.main.quack_guard'",
        ],
    )?;
    Ok(())
}
