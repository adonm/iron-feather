//! iron-feather: TiPG-esque OGC API Features + vector tiles,
//! plus a standalone raw Arrow Flight fast path for one shard.
//!
//! HTTP is Poem with a derived OpenAPI contract (`/api`). Hot reads come
//! from local versioned DuckDB shards (R-tree + ART), tiny hot fragments
//! from an embedded Turso/libSQL cache, tile math from Martin
//! (`martin-tile-utils`) as a library. Flight streams Arrow straight out of
//! DuckDB over S3 Parquet with no ADBC layer.

mod api;
mod filter;
mod store;
mod tiles;

#[cfg(feature = "serve")]
mod flight;
#[cfg(feature = "serve")]
mod guard;
#[cfg(feature = "serve")]
mod serve;

use clap::Parser;
use poem::{get, listener::TcpListener, EndpointExt, Route, Server};
use poem_openapi::OpenApiService;
use std::sync::Arc;
use store::Store;

/// State shared by the plain-Poem routes (tiles, health).
#[derive(Clone)]
pub struct AppState {
    pub store: Arc<Store>,
}

#[derive(Parser, Debug)]
#[command(
    name = "iron-feather",
    version,
    about = "OGC Features + tiles + Flight over DuckDB shards"
)]
struct Args {
    /// HTTP listen address, e.g. 127.0.0.1:3000
    #[arg(long, env = "IRON_FEATHER_LISTEN", default_value = "0.0.0.0:3000")]
    listen: String,

    /// Directory holding versioned `*.duckdb` shard files (Poem live path).
    /// Requires `--features serve`; otherwise the in-memory stub serves demo data.
    #[arg(long, env = "IRON_FEATHER_SHARD_DIR")]
    shard_dir: Option<std::path::PathBuf>,

    /// Arrow Flight listen address, e.g. 127.0.0.1:50051 (serve feature only).
    #[arg(
        long,
        env = "IRON_FEATHER_FLIGHT_LISTEN",
        default_value = "127.0.0.1:50051"
    )]
    flight_listen: String,

    /// Shard source for Flight: S3 or local Parquet path/URL,
    /// e.g. s3://bucket/shard/*.parquet (serve feature only).
    #[arg(long, env = "IRON_FEATHER_SHARD_SOURCE")]
    shard_source: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    let store = Arc::new(build_store(&args).await?);

    #[cfg(feature = "serve")]
    if let Some(source) = &args.shard_source {
        let service = flight::ShardFlight::open(source)?;
        let addr = args.flight_listen.clone();
        let _flight = tokio::spawn(async move {
            if let Err(e) = flight::serve(addr, service).await {
                tracing::error!(error = %e, "flight server exited");
            }
        });
    } else {
        tracing::warn!("no --shard-source; Arrow Flight fast path disabled");
    }
    #[cfg(not(feature = "serve"))]
    if args.shard_source.is_some() {
        tracing::warn!("--shard-source set but built without `serve`; Flight disabled");
    }

    let api = api::Api {
        store: store.clone(),
        serving_version: "v0-stub".to_string(),
    };
    let api_service = OpenApiService::new(api, "Iron Feather", env!("CARGO_PKG_VERSION"))
        .description(
            "TiPG-esque OGC API Features plus vector tiles. DuckDB shards, Turso fragment cache, Martin tile math.",
        );
    let spec = api_service.spec_endpoint();

    let app = Route::new()
        .nest("/", api_service)
        .at("/api", spec)
        .at("/healthz", get(tiles::healthz))
        .at("/collections/:collection/tiles/:z/:x/:y", get(tiles::tile))
        .data(AppState { store });

    tracing::info!(listen = %args.listen, "iron-feather http up");
    Server::new(TcpListener::bind(&args.listen))
        .run(app)
        .await?;
    Ok(())
}

async fn build_store(args: &Args) -> Result<Store, Box<dyn std::error::Error>> {
    #[cfg(feature = "serve")]
    if let Some(dir) = &args.shard_dir {
        let live = serve::ServeStore::open(dir).await?;
        tracing::info!(shard_dir = %dir.display(), "serving live DuckDB shard");
        return Ok(Store::Serve(live));
    }
    if args.shard_dir.is_some() {
        tracing::warn!("--shard-dir set but built without `serve`; using stub store");
    }
    Ok(Store::Stub(store::StubStore))
}
