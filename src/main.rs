mod api;
mod db;
mod filter;
mod flight;
mod index;
mod materialize;
mod plan;
mod store;
#[cfg(test)]
mod tests;
mod tiles;

use clap::{Parser, Subcommand};
use std::{net::SocketAddr, sync::Arc};

#[derive(Parser)]
#[command(
    version,
    about = "Build a DuckLake shard on S3; serve OGC Features, MVT and Arrow Flight"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Build(materialize::Build),
    /// Compile a serving index for an existing catalog (repacks, or
    /// catalogs built before index generation existed). The catalog must
    /// be reachable with local data access for footer statistics.
    Index(materialize::Index),
    Serve {
        #[arg(long, env = "IRON_FEATHER_SHARD")]
        /// Local .ducklake catalog path, or http(s)/s3 URL of the catalog.
        shard: String,
        #[arg(long, env = "IRON_FEATHER_DATA_BASE", action = clap::ArgAction::Append)]
        /// Zone-local bases for Parquet reads (repeat for each cache
        /// shard, e.g. this zone's Cachey `/fetch/<bucket>/data/` URLs).
        /// Overrides the catalog's stored zone-independent `DATA_PATH` so
        /// relative data paths stay AZ-local, and spreads files across
        /// shards by filename hash for stable cache ownership. Omit for
        /// local fixtures and direct-S3 baselines.
        data_base: Vec<String>,
        #[arg(long, env = "IRON_FEATHER_S3_ENDPOINT")]
        /// Direct-S3 baseline endpoint (`host:port` or URL, e.g. MinIO).
        /// Requires `--s3-key-id` and `--s3-secret` together; creates a
        /// DuckDB S3 secret so `s3://` paths are read without Cachey.
        /// Never set in prod Cachey mode (readers need no S3 credentials).
        s3_endpoint: Option<String>,
        #[arg(long, env = "IRON_FEATHER_S3_KEY_ID", hide_env_values = true)]
        /// Direct-S3 baseline access key id. See `--s3-endpoint`.
        s3_key_id: Option<String>,
        #[arg(long, env = "IRON_FEATHER_S3_SECRET", hide_env_values = true)]
        /// Direct-S3 baseline secret access key. See `--s3-endpoint`.
        s3_secret: Option<String>,
        #[arg(long, default_value = "0.0.0.0:3000")]
        listen: SocketAddr,
        #[arg(long, default_value = "127.0.0.1:50051")]
        flight_listen: SocketAddr,
        #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(u16).range(1..))]
        connections: u16,
        /// Requests beyond the pool queue for a connection up to this long.
        #[arg(long, default_value_t = 250)]
        max_wait_ms: u64,
        /// Concurrent requests that may queue before failing fast.
        #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u16).range(0..))]
        max_waiters: u16,
        /// Concurrent Flight queries before bulk work fails fast. Defaults to
        /// the pool size, which leaves bulk sharing the queue; lower it to
        /// reserve connections for interactive OGC.
        #[arg(long, default_value_t = 0, value_parser = clap::value_parser!(u16).range(0..))]
        flight_concurrency: u16,
        /// Shared DuckDB threads for the whole process. Keep at 1 for many
        /// small concurrent queries; raise only with fewer connections for
        /// bulk.
        #[arg(long, default_value_t = 1)]
        threads: i64,
        /// Shared DuckDB memory budget in MiB. 0 leaves DuckDB's default
        /// (unbounded); 4096 holds the ~10 GB shard urban working set.
        #[arg(long, default_value_t = 4096)]
        memory_mb: u64,
        /// Query execution deadline in ms for HTTP and Flight. 0 disables;
        /// exceeded queries are interrupted and surface as backend failures.
        #[arg(long, default_value_t = 30000)]
        query_timeout_ms: u64,
        /// Per-stream Flight buffer in MiB. Bounds one stream's buffered
        /// Arrow bytes; oversized single batches drain first.
        #[arg(long, default_value_t = 32)]
        flight_stream_mb: u64,
        /// Process-wide Flight buffer in MiB across all streams.
        #[arg(long, default_value_t = 128)]
        flight_total_mb: u64,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    match Args::parse().command {
        Command::Build(build) => build.run()?,
        Command::Index(index) => index.run()?,
        Command::Serve {
            shard,
            data_base,
            s3_endpoint,
            s3_key_id,
            s3_secret,
            listen,
            flight_listen,
            connections,
            max_wait_ms,
            max_waiters,
            flight_concurrency,
            threads,
            memory_mb,
            query_timeout_ms,
            flight_stream_mb,
            flight_total_mb,
        } => {
            let bulk_limit = if flight_concurrency == 0 {
                connections.into()
            } else {
                flight_concurrency.into()
            };
            // Publish-time serving index, resolved beside the catalog so
            // workers prune to candidate files without touching catalog
            // metadata per query. Absent/untrusted documents fall back
            // inside Store::open_config.
            let index_json = fetch_index_document(&shard).await;
            let config = store::StoreConfig {
                location: shard.clone(),
                data_bases: data_base.clone(),
                index_json,
                s3_endpoint: s3_endpoint.clone(),
                s3_key_id: s3_key_id.clone(),
                s3_secret: s3_secret.clone(),
                connections: connections.into(),
                max_waiters: max_waiters.into(),
                max_wait: std::time::Duration::from_millis(max_wait_ms),
                bulk_limit,
                threads,
                memory_mb,
                query_timeout: std::time::Duration::from_millis(query_timeout_ms),
                flight_stream_bytes: (flight_stream_mb.max(1) * 1024 * 1024) as usize,
                flight_total_bytes: (flight_total_mb.max(1) * 1024 * 1024) as usize,
            };
            let store = Arc::new(store::Store::open_config(config.clone())?);
            tracing::info!(%listen, %flight_listen, %shard, snapshot = store.snapshot, "serving shard");
            let http = poem::Server::new(poem::listener::TcpListener::bind(listen))
                .run(api::routes(store.clone()));
            let flight = tonic::transport::Server::builder()
                .add_service(
                    arrow_flight::flight_service_server::FlightServiceServer::new(
                        flight::ShardFlight { store },
                    ),
                )
                .serve(flight_listen);
            // A bind/runtime failure in either listener terminates the service.
            tokio::select! {
                result = http => result?,
                result = flight => result?,
                result = tokio::signal::ctrl_c() => {
                    result?;
                }
            }
        }
    }
    Ok(())
}

/// Load the serving-index sidecar for a catalog location: `<location>`
/// plus `.serving.json`, read from disk or fetched over HTTP(S). Returns
/// `None` (with a warning) when absent or unreadable; the store then
/// serves without per-request pruning.
async fn fetch_index_document(location: &str) -> Option<String> {
    let path = crate::index::index_path_for(location);
    if path.starts_with("http://") || path.starts_with("https://") {
        // Same request config DuckDB sends via its HTTP secret: without
        // `C0-Config: fps=true` Cachey attempts virtual-hosted bucket
        // addressing and the lookup fails. Cachey also rejects bare GETs
        // (400) and open-ended ranges (416), so fetch in closed chunks.
        let client = reqwest::Client::new();
        let mut body = Vec::new();
        let mut start: u64 = 0;
        loop {
            let end = start + 65535;
            let resp = match client
                .get(&path)
                .header("C0-Config", "fps=true")
                .header("Range", format!("bytes={start}-{end}"))
                .send()
                .await
            {
                Ok(resp) => resp,
                Err(e) => {
                    tracing::warn!(error = %e, "serving index fetch failed");
                    return None;
                }
            };
            let total: Option<u64> = resp.headers().get("content-range").and_then(|v| {
                v.to_str()
                    .ok()
                    .and_then(|s| s.split('/').next_back()?.parse::<u64>().ok())
            });
            let status = resp.status();
            let chunk = match resp.bytes().await {
                Ok(chunk) => chunk,
                Err(e) => {
                    tracing::warn!(error = %e, "serving index fetch failed");
                    return None;
                }
            };
            if !status.is_success() && status.as_u16() != 206 {
                tracing::warn!(%status, "serving index fetch failed");
                return None;
            }
            let n = chunk.len() as u64;
            body.extend_from_slice(&chunk);
            match total {
                Some(total) if (start + n) >= total => break,
                _ if n == 0 => break,
                _ => start += n,
            }
            if body.len() > 256 * 1024 * 1024 {
                tracing::warn!("serving index fetch failed: document too large");
                return None;
            }
        }
        match String::from_utf8(body) {
            Ok(text) => Some(text),
            Err(e) => {
                tracing::warn!(error = %e, "serving index unreadable");
                None
            }
        }
    } else {
        match std::fs::read_to_string(&path) {
            Ok(text) => Some(text),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("no serving index published; no per-request pruning");
                None
            }
            Err(e) => {
                tracing::warn!(path, error = %e, "serving index unreadable");
                None
            }
        }
    }
}
