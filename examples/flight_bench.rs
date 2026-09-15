//! Closed-loop Flight benchmark. Counts overload/errors separately from
//! successful queries and decodes the entire response (including geometry).
use arrow_flight::{
    decode::FlightRecordBatchStream, flight_service_client::FlightServiceClient, Ticket,
};
use futures::TryStreamExt;
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(clap::Parser, Debug)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:50051")]
    addr: String,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    #[arg(long, default_value_t = 100)]
    requests: usize,
    #[arg(long)]
    jitter: bool,
    #[arg(long, default_value_t = 1000)]
    limit: u32,
    #[arg(
        long,
        num_args = 4,
        value_delimiter = ',',
        allow_hyphen_values = true,
        default_value = "13.35,52.48,13.45,52.55"
    )]
    bbox: Vec<f64>,
}

#[derive(Default)]
struct Stats {
    latencies: Vec<Duration>,
    statuses: BTreeMap<String, usize>,
    rows: usize,
    bytes: usize,
}

async fn worker(
    mut client: FlightServiceClient<tonic::transport::Channel>,
    task: usize,
    args: Arc<Args>,
) -> Stats {
    let mut stats = Stats::default();
    for i in 0..args.requests {
        let dx = if args.jitter {
            (task * args.requests + i) as f64 * 1e-8
        } else {
            0.0
        };
        let ticket = serde_json::json!({"collection": "buildings", "sources": [1], "limit": args.limit,
            "bbox": [args.bbox[0] + dx, args.bbox[1], args.bbox[2] + dx, args.bbox[3]]});
        let start = Instant::now();
        let result = async {
            let stream = client
                .do_get(Ticket::new(ticket.to_string()))
                .await
                .map_err(|e| format!("{:?}", e.code()))?
                .into_inner()
                .map_err(arrow_flight::error::FlightError::from);
            let mut stream = FlightRecordBatchStream::new_from_flight_data(stream);
            let (mut rows, mut bytes) = (0, 0);
            while let Some(batch) = stream
                .try_next()
                .await
                .map_err(|_| "stream_error".to_string())?
            {
                rows += batch.num_rows();
                bytes += batch.get_array_memory_size();
            }
            Ok::<_, String>((rows, bytes))
        }
        .await;
        let status = match result {
            Ok((rows, bytes)) => {
                stats.latencies.push(start.elapsed());
                stats.rows += rows;
                stats.bytes += bytes;
                "OK".to_string()
            }
            Err(status) => status,
        };
        *stats.statuses.entry(status).or_default() += 1;
    }
    stats
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Arc::new(<Args as clap::Parser>::parse());
    if args.concurrency == 0 || args.requests == 0 {
        return Err("concurrency and requests must be positive".into());
    }
    let client = FlightServiceClient::new(
        tonic::transport::Endpoint::from_shared(args.addr.clone())?
            .connect()
            .await?,
    );
    let start = Instant::now();
    let workers =
        (0..args.concurrency).map(|task| tokio::spawn(worker(client.clone(), task, args.clone())));
    let mut total = Stats::default();
    for stats in futures::future::join_all(workers).await {
        let stats = stats?;
        total.latencies.extend(stats.latencies);
        total.rows += stats.rows;
        total.bytes += stats.bytes;
        for (status, count) in stats.statuses {
            *total.statuses.entry(status).or_default() += count;
        }
    }
    let secs = start.elapsed().as_secs_f64();
    total.latencies.sort_unstable();
    let pct = |p: f64| {
        total
            .latencies
            .get((total.latencies.len() as f64 * p) as usize)
            .copied()
            .unwrap_or_default()
    };
    println!("requests={} secs={secs:.2} success_rps={:.0} p50={:?} p99={:?} rows={} arrow_MB/s={:.1} statuses={:?}",
        args.concurrency * args.requests, total.latencies.len() as f64 / secs, pct(0.5), pct(0.99),
        total.rows, total.bytes as f64 / secs / 1e6, total.statuses);
    Ok(())
}
