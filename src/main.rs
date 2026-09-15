mod api;
mod filter;
mod flight;
mod materialize;
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
        /// Total response-cache budget, split evenly between HTTP and Flight.
        #[arg(long, default_value_t = 256)]
        cache_mb: u32,
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
            cache_mb,
        } => {
            let store = Arc::new(store::Store::open(
                &shard,
                connections.into(),
                u64::from(cache_mb) * 1024 * 1024,
            )?);
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
