mod api;
mod filter;
mod flight;
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
    about = "Build a DuckDB shard; serve OGC Features, MVT and Arrow Flight"
)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Build(materialize::Build),
    Serve {
        #[arg(long, env = "IRON_FEATHER_SHARD")]
        /// Local path, HTTP(S) URL, or public/preconfigured s3:// URL.
        shard: String,
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
        /// Response-cache budget for HTTP bytes. Flight runs uncached.
        #[arg(long, default_value_t = 256)]
        cache_mb: u32,
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
        /// Disable DuckDB HTTP metadata cache (default: enabled for remote).
        #[arg(long)]
        disable_http_metadata_cache: bool,
        /// Disable DuckDB Parquet metadata cache (default: enabled).
        #[arg(long)]
        disable_parquet_metadata_cache: bool,
        /// Disable HTTP connection reuse (default: enabled).
        #[arg(long)]
        disable_connection_cache: bool,
        /// Enable prefetching for all Parquet files (default: remote-only).
        /// Opt-in; helps wide scans, hurts tiny lookups.
        #[arg(long)]
        enable_parquet_prefetch: bool,
        /// Re-enable external-file-cache validation (default: NO_VALIDATION
        /// for remote immutable shards). Only set for mutable URLs.
        #[arg(long)]
        enable_cache_validation: bool,
        /// Persistent on-disk block cache dir via cache_httpfs (opt-in).
        /// Empty/absent disables; survives restarts, shared by all conns.
        #[arg(long)]
        duck_disk_cache_dir: Option<String>,
        /// Block size in KiB for the on-disk cache (64–1024).
        #[arg(long, default_value_t = 512)]
        duck_disk_cache_block_kb: u64,
        /// Max parallel sub-requests for on-disk cache fanout (0=unlimited).
        #[arg(long, default_value_t = 0)]
        duck_disk_cache_fanout: u64,
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
        Command::Serve {
            shard,
            listen,
            flight_listen,
            connections,
            max_wait_ms,
            max_waiters,
            flight_concurrency,
            cache_mb,
            threads,
            memory_mb,
            query_timeout_ms,
            flight_stream_mb,
            flight_total_mb,
            disable_http_metadata_cache,
            disable_parquet_metadata_cache,
            disable_connection_cache,
            enable_parquet_prefetch,
            enable_cache_validation,
            duck_disk_cache_dir,
            duck_disk_cache_block_kb,
            duck_disk_cache_fanout,
        } => {
            let bulk_limit = if flight_concurrency == 0 {
                connections.into()
            } else {
                flight_concurrency.into()
            };
            let block_bytes = (duck_disk_cache_block_kb.clamp(16, 4096) * 1024) as usize;
            let store = Arc::new(store::Store::open_config(store::StoreConfig {
                location: shard.clone(),
                connections: connections.into(),
                cache_bytes: u64::from(cache_mb) * 1024 * 1024,
                max_waiters: max_waiters.into(),
                max_wait: std::time::Duration::from_millis(max_wait_ms),
                bulk_limit,
                threads,
                memory_mb,
                query_timeout: std::time::Duration::from_millis(query_timeout_ms),
                flight_stream_bytes: (flight_stream_mb.max(1) * 1024 * 1024) as usize,
                flight_total_bytes: (flight_total_mb.max(1) * 1024 * 1024) as usize,
                http_metadata_cache: !disable_http_metadata_cache,
                parquet_metadata_cache: !disable_parquet_metadata_cache,
                http_connection_cache: !disable_connection_cache,
                parquet_prefetch_all: enable_parquet_prefetch,
                no_validation: !enable_cache_validation,
                disk_cache_dir: duck_disk_cache_dir.filter(|s| !s.is_empty()),
                disk_cache_block_bytes: block_bytes,
                disk_cache_fanout: duck_disk_cache_fanout as usize,
            })?);
            tracing::info!(%listen, %flight_listen, %shard, "serving shard");
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
                result = tokio::signal::ctrl_c() => result?,
            }
        }
    }
    Ok(())
}
