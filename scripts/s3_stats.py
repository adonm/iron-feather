#!/usr/bin/env python3
"""S3 accounting for loopback `rclone serve s3` benchmark runs.

Usage: s3_stats.py LOG MARKER [REQUESTS]
Counts GET OBJECT operations after the last MARKER line (exact) and sums
ChunkedReader.Read lengths (rclone backend reads, an approximation of wire
bytes: requested buffer lengths, not measured response bodies). With
REQUESTS, also prints per-request GETs and MB so fixed-pass runs with
identical request sets are directly comparable across backends.

Fixed-pass benchmarking (`http_bench --passes N --workload FILE`) is
preferred over duration mode: every backend serves the same complete passes
instead of different duration-sliced prefixes.
"""
import re
import sys

log, marker = sys.argv[1], sys.argv[2]
requests = int(sys.argv[3]) if len(sys.argv) > 3 else 0
lines = open(log, errors="replace").read().splitlines()
start = max((i for i, line in enumerate(lines) if marker in line), default=-1)
tail = lines[start + 1 :]
gets = sum(1 for line in tail if "serve s3: GET OBJECT" in line)
byte_re = re.compile(r"ChunkedReader\.Read at \d+ length (\d+)")
served = sum(int(m.group(1)) for line in tail for m in [byte_re.search(line)] if m)
out = f"gets={gets} served_bytes={served} served_MB={served / 1e6:.1f}"
if requests > 0:
    out += f" per_req_gets={gets / requests:.2f} per_req_MB={served / requests / 1e6:.2f}"
print(out)
print(
    "note: bytes sum requested ChunkedReader.Read lengths (backend reads), "
    "not measured S3 response bodies; GET counts are exact",
)
