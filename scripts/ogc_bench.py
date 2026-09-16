#!/usr/bin/env python3
"""Closed-loop HTTP benchmark: persistent connections, successful rps, status counts.

Start `just run` first. Use --jitter for distinct requests, --route tiles for MVT.
The default bbox matches `just fixture-osm` (Berlin).

Capacity method: warm the server, then sweep client concurrency at fixed pool
sizes (`--connections 4/8/16/32`). Compare successful rps, p50/p99 and the
429 rate together; size the pool from measured throughput and latency with a
target such as p99 < 100 ms and < 1% rejected requests.

Cache-miss tests: pass a fresh --seed per run (jitter URLs otherwise repeat
across runs and hit the response cache) or serve with --cache-mb 0.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import http.client
import math
import time
from urllib.parse import urlsplit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="http://127.0.0.1:3000")
    parser.add_argument("--route", choices=["tiles", "items"], default="items")
    parser.add_argument("--concurrency", type=int, default=32)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--sources", default="1")
    parser.add_argument("--bbox", default="13.35,52.48,13.45,52.55")
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--jitter", action="store_true")
    parser.add_argument("--seed", type=int, default=0,
                        help="offset jitter sequence so repeat runs miss the cache")
    parser.add_argument("--warmup", type=int, default=0,
                        help="unmeasured requests per worker before timing")
    args = parser.parse_args()
    if min(args.concurrency, args.requests, args.limit) < 1:
        parser.error("concurrency, requests and limit must be positive")
    if args.warmup < 0 or args.seed < 0:
        parser.error("warmup and seed must be non-negative")
    west, south, east, north = map(float, args.bbox.split(","))
    base = urlsplit(args.base)
    connection = http.client.HTTPSConnection if base.scheme == "https" else http.client.HTTPConnection
    z = 14
    x = int(((west + east) / 2 + 180) / 360 * 2**z)
    lat = math.radians((south + north) / 2)
    y = int((1 - math.asinh(math.tan(lat)) / math.pi) / 2 * 2**z)

    import itertools
    import threading
    seq = itertools.count()
    seq_lock = threading.Lock()

    def path_for() -> str:
        # One global sequence per run: every request is unique, and seeds
        # step by 5e-3 degrees while a run drifts at most 5e-4.
        n = None
        if args.jitter:
            with seq_lock:
                n = next(seq)
        if args.route == "tiles":
            tx = (x + n) % 2**z if args.jitter else x
            return f"/collections/buildings/tiles/{z}/{tx}/{y}?sources={args.sources}"
        dx = ((n % 50_000) * 1e-8 + args.seed * 5e-3) if args.jitter else 0
        return (f"/collections/buildings/items?bbox={west + dx},{south},{east + dx},{north}"
                f"&limit={args.limit}&sources={args.sources}")

    def fetch(conn, path: str):
        conn.request("GET", base.path.rstrip("/") + path)
        response = conn.getresponse()
        payload = response.read()
        return response.status, payload

    def worker(task: int):
        conn = connection(base.hostname, base.port, timeout=30)
        statuses, latencies, nonempty, transferred = Counter(), [], 0, 0
        for _ in range(args.warmup):
            try:
                fetch(conn, path_for())
            except (OSError, http.client.HTTPException):
                conn.close()
                conn = connection(base.hostname, base.port, timeout=30)
        for _ in range(args.requests):
            path = path_for()
            start = time.perf_counter()
            try:
                status, payload = fetch(conn, path)
                if status in (200, 204):
                    latencies.append(time.perf_counter() - start)
                    transferred += len(payload)
                    if args.route == "tiles":
                        nonempty += int(bool(payload))
                    else:
                        # Compact server encoding; avoids client JSON parsing.
                        nonempty += int(b'"numberReturned":0' not in payload)
            except (OSError, http.client.HTTPException):
                status = 0
                conn.close()
                conn = connection(base.hostname, base.port, timeout=30)
            statuses[status] += 1
        conn.close()
        return statuses, latencies, nonempty, transferred

    start = time.perf_counter()
    statuses, latencies, nonempty, transferred = Counter(), [], 0, 0
    with ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        for counts, times, full, size in pool.map(worker, range(args.concurrency)):
            statuses.update(counts)
            latencies.extend(times)
            nonempty += full
            transferred += size
    elapsed = time.perf_counter() - start
    latencies.sort()
    percentile = lambda p: latencies[min(int(len(latencies) * p), len(latencies) - 1)] * 1000 if latencies else 0
    print(f"requests={sum(statuses.values())} secs={elapsed:.2f} success_rps={len(latencies) / elapsed:.0f} "
          f"p50={percentile(.5):.1f}ms p99={percentile(.99):.1f}ms "
          f"nonempty={nonempty} MB/s={transferred / elapsed / 1e6:.1f} statuses={dict(statuses)}")


if __name__ == "__main__":
    main()
