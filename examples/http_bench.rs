//! Closed-loop HTTP benchmark with separate warmup and timed phases.
//! Duration-based load against one or more running servers (comma-separated
//! --base for client-side round-robin); reports successful rps, p50, p99,
//! rejections and wire throughput. Jitter draws from a shared atomic sequence
//! so every request in a run is unique, and a fresh --seed per run keeps
//! repeat runs missing the response cache instead of re-measuring it.
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

#[derive(clap::Parser, Debug, Clone)]
struct Args {
    /// One base URL, or several comma-separated for round-robin.
    #[arg(long, default_value = "http://127.0.0.1:3000")]
    base: String,
    #[arg(long, default_value = "items")]
    route: String,
    #[arg(long, default_value_t = 32)]
    concurrency: usize,
    /// Timed seconds when --passes is 0; ignored in fixed-pass mode.
    #[arg(long, default_value_t = 15)]
    duration: u64,
    /// Complete workload passes in the timed phase (0 = duration mode).
    /// Fixed passes cover identical request sets, so backends are comparable.
    #[arg(long, default_value_t = 0)]
    passes: u64,
    #[arg(long, default_value_t = 3)]
    warmup_secs: u64,
    #[arg(long, default_value_t = 10)]
    limit: u32,
    #[arg(long, default_value = "1")]
    sources: String,
    #[arg(long, default_value = "13.35,52.48,13.45,52.55")]
    bbox: String,
    #[arg(long)]
    jitter: bool,
    #[arg(long, default_value_t = 0)]
    seed: u64,
    /// Send `Accept-Encoding: gzip` and count wire bytes (server compresses).
    #[arg(long)]
    gzip: bool,
    /// File of URL paths (one per line, `#` comments allowed) replacing
    /// generated jitter URLs. Workers share one atomic sequence, so every
    /// request in a run is unique until the file wraps.
    #[arg(long)]
    workload: Option<String>,
}

#[derive(Default)]
struct Stats {
    latencies: Vec<Duration>,
    statuses: BTreeMap<u16, usize>,
    errors: usize,
    bytes: u64,
    nonempty: u64,
}

struct Target {
    bbox: [f64; 4],
    tile: (u32, u32),
}

fn parse_bbox(bbox: &str) -> Result<Target, String> {
    let numbers = bbox
        .split(',')
        .map(|v| v.trim().parse::<f64>())
        .collect::<Result<Vec<_>, _>>()
        .map_err(|_| "bbox must contain numbers".to_string())?;
    if numbers.len() != 4 {
        return Err("bbox must have 4 numbers".to_string());
    }
    let (west, south, east, north) = (numbers[0], numbers[1], numbers[2], numbers[3]);
    let z = 14u32;
    let x = (((west + east) / 2.0 + 180.0) / 360.0 * (2f64).powi(z as i32)) as u32;
    let lat = ((south + north) / 2.0).to_radians();
    let y = ((1.0 - lat.tan().asinh() / std::f64::consts::PI) / 2.0 * (2f64).powi(z as i32)) as u32;
    Ok(Target {
        bbox: [west, south, east, north],
        tile: (x, y),
    })
}

fn path_for(args: &Args, target: &Target, n: u64) -> String {
    // One global sequence per run: every request is unique, and seeds step by
    // 5e-3 degrees while a run drifts at most 5e-4, so seeds never overlap.
    if args.route == "tiles" {
        let (x, y) = target.tile;
        let tx = if args.jitter {
            (x.wrapping_add(n as u32)) % (1 << 14)
        } else {
            x
        };
        return format!(
            "/collections/buildings/tiles/14/{tx}/{y}?sources={}",
            args.sources
        );
    }
    let [west, south, east, north] = target.bbox;
    let dx = if args.jitter {
        (n % 50_000) as f64 * 1e-8 + args.seed as f64 * 5e-3
    } else {
        0.0
    };
    format!(
        "/collections/buildings/items?bbox={},{},{},{}&limit={}&sources={}",
        west + dx,
        south,
        east + dx,
        north,
        args.limit,
        args.sources
    )
}

struct Ctx {
    client: reqwest::Client,
    bases: Arc<Vec<String>>,
    args: Arc<Args>,
    target: Arc<Target>,
    seq: Arc<AtomicU64>,
    workload: Arc<Vec<String>>,
}

/// Compact server encoding; avoids client JSON parsing.
fn is_nonempty(route: &str, body: &[u8]) -> bool {
    if route == "tiles" {
        !body.is_empty()
    } else {
        !body
            .windows(b"\"numberReturned\":0".len())
            .any(|window| window == b"\"numberReturned\":0")
    }
}

async fn round(ctx: Ctx, task: usize, secs: u64, measured: bool) -> Stats {
    round_limit(ctx, task, secs, measured, None).await
}

async fn round_limit(
    ctx: Ctx,
    task: usize,
    secs: u64,
    measured: bool,
    max_requests: Option<u64>,
) -> Stats {
    let mut stats = Stats::default();
    let deadline = max_requests
        .is_none()
        .then(|| Instant::now() + Duration::from_secs(secs));
    let base = &ctx.bases[task % ctx.bases.len()];
    let mut done = 0u64;
    loop {
        if let Some(max) = max_requests {
            if done >= max {
                break;
            }
        } else if Instant::now() >= deadline.unwrap() {
            break;
        }
        let n = ctx.seq.fetch_add(1, Ordering::Relaxed);
        done += 1;
        let path = if ctx.workload.is_empty() {
            path_for(&ctx.args, &ctx.target, n)
        } else {
            ctx.workload[n as usize % ctx.workload.len()].clone()
        };
        let url = format!("{}{}", base, path);
        let mut request = ctx.client.get(&url).timeout(Duration::from_secs(30));
        if ctx.args.gzip {
            request = request.header("Accept-Encoding", "gzip");
        }
        let start = Instant::now();
        let outcome = async {
            let response = request.send().await?;
            let status = response.status().as_u16();
            let bytes = response.bytes().await?;
            Ok::<_, reqwest::Error>((status, bytes))
        }
        .await;
        match outcome {
            Ok((status, body)) => {
                if !measured {
                    continue;
                }
                *stats.statuses.entry(status).or_default() += 1;
                if matches!(status, 200 | 204) {
                    stats.latencies.push(start.elapsed());
                    stats.bytes += body.len() as u64;
                    if !ctx.args.gzip && is_nonempty(&ctx.args.route, &body) {
                        stats.nonempty += 1;
                    }
                }
            }
            Err(_) => {
                if measured {
                    stats.errors += 1;
                }
            }
        }
    }
    stats
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Arc::new(<Args as clap::Parser>::parse());
    if args.concurrency == 0 {
        return Err("concurrency must be positive".into());
    }
    if !["items", "tiles"].contains(&args.route.as_str()) {
        return Err("route must be items or tiles".into());
    }
    let bases: Arc<Vec<String>> = Arc::new(
        args.base
            .split(',')
            .map(|base| base.trim().trim_end_matches('/').to_string())
            .collect(),
    );
    if bases.iter().any(|base| base.is_empty()) {
        return Err("bases must not be empty".into());
    }
    let target = Arc::new(parse_bbox(&args.bbox)?);
    let workload: Arc<Vec<String>> = Arc::new(match &args.workload {
        Some(path) => std::fs::read_to_string(path)
            .map_err(|e| format!("workload {path}: {e}"))?
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .map(str::to_string)
            .collect(),
        None => Vec::new(),
    });
    if args.workload.is_some() && workload.is_empty() {
        return Err("workload file has no paths".into());
    }
    let client = reqwest::Client::builder()
        .connect_timeout(Duration::from_secs(10))
        .pool_max_idle_per_host(args.concurrency)
        .build()?;
    let seq = Arc::new(AtomicU64::new(0));
    let ctx_duration = |task: usize, secs: u64, measured: bool| {
        let ctx = Ctx {
            client: client.clone(),
            bases: bases.clone(),
            args: args.clone(),
            target: target.clone(),
            seq: seq.clone(),
            workload: workload.clone(),
        };
        tokio::spawn(round(ctx, task, secs, measured))
    };
    let ctx_fixed = |task: usize, max_requests: u64| {
        let ctx = Ctx {
            client: client.clone(),
            bases: bases.clone(),
            args: args.clone(),
            target: target.clone(),
            seq: seq.clone(),
            workload: workload.clone(),
        };
        tokio::spawn(round_limit(ctx, task, u64::MAX, true, Some(max_requests)))
    };
    // Warmup is unmeasured: hot caches and pool before timing. The sequence
    // continues into the timed phase, so warmup never overlaps measurement.
    // Fixed-pass mode resets the sequence so the timed phase covers identical
    // complete passes regardless of warmup length.
    let warmup = futures::future::join_all(
        (0..args.concurrency).map(|task| ctx_duration(task, args.warmup_secs, false)),
    )
    .await;
    for task in warmup {
        task?;
    }
    let start = Instant::now();
    let mode;
    let workers: Vec<_> = if args.passes > 0 {
        if workload.is_empty() {
            return Err("passes mode needs --workload".into());
        }
        mode = format!("passes={}", args.passes);
        seq.store(0, Ordering::Relaxed);
        let total = args.passes * workload.len() as u64;
        let per_task = total.div_ceil(args.concurrency as u64);
        (0..args.concurrency)
            .map(|task| {
                let remaining = total.saturating_sub(task as u64 * per_task).min(per_task);
                ctx_fixed(task, remaining)
            })
            .collect()
    } else {
        mode = format!("duration={}s", args.duration);
        (0..args.concurrency)
            .map(|task| ctx_duration(task, args.duration, true))
            .collect()
    };
    let mut latencies = Vec::new();
    let mut statuses = BTreeMap::new();
    let (mut errors, mut bytes, mut nonempty) = (0usize, 0u64, 0u64);
    for stats in futures::future::join_all(workers).await {
        let stats = stats?;
        latencies.extend(stats.latencies);
        bytes += stats.bytes;
        errors += stats.errors;
        nonempty += stats.nonempty;
        for (status, count) in stats.statuses {
            *statuses.entry(status).or_default() += count;
        }
    }
    let secs = start.elapsed().as_secs_f64();
    latencies.sort_unstable();
    let pct = |p: f64| {
        latencies
            .get((latencies.len() as f64 * p) as usize)
            .copied()
            .unwrap_or_default()
    };
    let rejected: usize = statuses
        .iter()
        .filter(|(status, _)| **status == 429)
        .map(|(_, count)| *count)
        .sum();
    let total: usize = statuses.values().sum();
    println!(
        "mode={mode} requests={total} secs={secs:.2} success_rps={:.0} p50={:?} p99={:?} wire_MB/s={:.1} nonempty={nonempty} rejected={rejected} errors={errors} statuses={statuses:?}",
        latencies.len() as f64 / secs,
        pct(0.5),
        pct(0.99),
        bytes as f64 / secs / 1e6,
    );
    Ok(())
}
