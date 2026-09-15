#!/usr/bin/env python3
"""Closed-loop HTTP benchmark: persistent connections, successful rps, status counts.

Start `just run` first. Use --jitter for distinct requests, --route tiles for MVT.
The default bbox matches `just fixture-osm` (Berlin).
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import http.client
import json
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
    args = parser.parse_args()
    if min(args.concurrency, args.requests, args.limit) < 1:
        parser.error("concurrency, requests and limit must be positive")
    west, south, east, north = map(float, args.bbox.split(","))
    base = urlsplit(args.base)
    connection = http.client.HTTPSConnection if base.scheme == "https" else http.client.HTTPConnection
    z = 14
    x = int(((west + east) / 2 + 180) / 360 * 2**z)
    lat = math.radians((south + north) / 2)
    y = int((1 - math.asinh(math.tan(lat)) / math.pi) / 2 * 2**z)

    def worker(task: int):
        conn = connection(base.hostname, base.port, timeout=30)
        statuses, latencies, nonempty, transferred = Counter(), [], 0, 0
        for i in range(args.requests):
            index = task * args.requests + i
            if args.route == "tiles":
                tx = (x + index) % 2**z if args.jitter else x
                path = f"/collections/buildings/tiles/{z}/{tx}/{y}?sources={args.sources}"
            else:
                dx = index * 1e-8 if args.jitter else 0
                path = (f"/collections/buildings/items?bbox={west + dx},{south},{east + dx},{north}"
                        f"&limit={args.limit}&sources={args.sources}")
            start = time.perf_counter()
            try:
                conn.request("GET", base.path.rstrip("/") + path)
                response = conn.getresponse()
                payload = response.read()
                status = response.status
                if status in (200, 204):
                    latencies.append(time.perf_counter() - start)
                    transferred += len(payload)
                    nonempty += int(bool(payload) if args.route == "tiles" else json.loads(payload)["numberReturned"] > 0)
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
