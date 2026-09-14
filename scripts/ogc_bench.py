#!/usr/bin/env python3
"""Hammer the Poem OGC HTTP endpoint, report rps + p50/p99.

Stdlib only. Start the server first, e.g.:
    cargo run --features serve -- --shard-dir ./fixtures
    BASE=http://127.0.0.1:3000 CONC=64 REQ=100 just bench-ogc

Target: 2k+ rps on tiles/items (hot cache path). Use --jitter to force
cold DuckDB work per request.
"""

import argparse
import concurrent.futures
import statistics
import time
import urllib.request

TILES = [(14, 4222, 7544), (14, 4223, 7544), (14, 4222, 7545), (14, 4223, 7545)]


def get(url: str, timeout: float) -> tuple[int, float]:
    start = time.perf_counter()
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            response.read()
            return response.status, time.perf_counter() - start
    except Exception:
        return 0, time.perf_counter() - start


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base", default="http://127.0.0.1:3000")
    parser.add_argument("--route", choices=["tiles", "items"], default="tiles")
    parser.add_argument("--concurrency", type=int, default=64)
    parser.add_argument("--requests", type=int, default=100)
    parser.add_argument("--sources", default="1,2,3")
    parser.add_argument("--jitter", action="store_true")
    args = parser.parse_args()

    def url_for(i: int) -> str:
        if args.route == "tiles":
            z, x, y = TILES[i % len(TILES)]
            if args.jitter:
                x += (i // len(TILES)) % 8
            return f"{args.base}/collections/buildings/tiles/{z}/{x}/{y}"
        dx = (i * 0.0001) if args.jitter else 0.0
        return (
            f"{args.base}/collections/buildings/items"
            f"?bbox={-87.35 + dx},13.95,{-87.05 + dx},14.2&limit=10&sources={args.sources}"
        )

    total = args.concurrency * args.requests
    latencies: list[float] = []
    statuses: dict[int, int] = {}
    start = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=args.concurrency) as pool:
        futures = [
            pool.submit(get, url_for(i), 30.0) for i in range(total)
        ]
        for future in concurrent.futures.as_completed(futures):
            status, latency = future.result()
            statuses[status] = statuses.get(status, 0) + 1
            latencies.append(latency)
    secs = time.perf_counter() - start
    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.50)]
    p99 = latencies[int(len(latencies) * 0.99)]
    print(
        f"requests={total} secs={secs:.2f} rps={total / secs:.0f} "
        f"p50={p50 * 1000:.1f}ms p99={p99 * 1000:.1f}ms statuses={statuses}"
    )


if __name__ == "__main__":
    main()
