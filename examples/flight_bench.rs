//! Load generator for the Flight fast path: N concurrent tasks hammering
//! `DoGet` with the same (hot) or jittered (cold) ticket.
//!
//! ```sh
//! # shell 1: cargo run --features serve -- --shard-source ./fixtures/osm-buildings.parquet
//! # shell 2: just bench-flight            # or: ADDR=.. CONC=64 REQ=200 just bench-flight
//! ```
//!
//! Reports client-observed rps + p50/p99. Loopback numbers validate the
//! serving stack (pool, moka L1, Turso L2, Arrow encode); size the network
//! separately for production rps claims.

use arrow_flight::{flight_service_client::FlightServiceClient, Ticket};
use futures::{StreamExt, TryStreamExt};
use std::{sync::Arc, time::Instant};

#[derive(clap::Parser, Debug, Clone)]
#[command(name = "flight_bench", about = "Hammer Flight DoGet, report rps")]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    /// Requests per task (total = concurrency * requests).
    #[arg(long, default_value_t = 100)]
    requests: usize,
    /// Offset each request's bbox slightly so every query misses the cache.
    #[arg(long, default_value_t = false)]
    jitter: bool,
    #[arg(long, default_value_t = 1000)]
    limit: u32,
}

async fn one(
    mut client: FlightServiceClient<tonic::transport::Channel>,
    task: usize,
    args: Arc<Args>,
) -> Vec<std::time::Duration> {
    let mut latencies = Vec::with_capacity(args.requests);
    for i in 0..args.requests {
        let dx = if args.jitter {
            (task * args.requests + i) as f64 * 0.0001
        } else {
            0.0
        };
        let ticket = serde_json::json!({
            "collection": "buildings",
            "bbox": [-87.35 + dx, 13.95, -87.05 + dx, 14.2],
            "columns": ["id", "x", "y", "name"],
            "limit": args.limit,
            "sources": [1, 2, 3],
        });
        let request = tonic::Request::new(Ticket {
            ticket: ticket.to_string().into_bytes().into(),
        });
        let start = Instant::now();
        let stream = client
            .do_get(request)
            .await
            .expect("do_get")
            .into_inner()
            .map_err(arrow_flight::error::FlightError::from);
        let mut batches =
            arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(stream);
        let mut rows = 0usize;
        while let Some(batch) = batches.try_next().await.expect("batch") {
            rows += batch.num_rows();
        }
        std::hint::black_box(rows);
        latencies.push(start.elapsed());
    }
    latencies
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Arc::new(<Args as clap::Parser>::parse());
    let start = Instant::now();
    let mut handles = Vec::with_capacity(args.concurrency);
    for task in 0..args.concurrency {
        let channel = tonic::transport::Endpoint::from_shared(args.addr.clone())?
            .connect()
            .await?;
        let client = FlightServiceClient::new(channel);
        let args = args.clone();
        handles.push(tokio::spawn(async move { one(client, task, args).await }));
    }
    let mut all = Vec::new();
    for handle in handles {
        all.extend(handle.await?);
    }
    all.sort_unstable();
    let total = all.len() as f64;
    let secs = start.elapsed().as_secs_f64();
    let pct = |p: f64| all[(total * p).min(total - 1.0) as usize];
    println!(
        "requests={} secs={:.2} rps={:.0} p50={:?} p99={:?}",
        all.len(),
        secs,
        total / secs,
        pct(0.5),
        pct(0.99)
    );
    Ok(())
}
