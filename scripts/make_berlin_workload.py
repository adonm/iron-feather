#!/usr/bin/env python3
"""Deterministic Berlin workload for kind benchmarks (seed 7, byte-stable).

Writes <out-dir>/mixed.txt: a 10x10 grid over the Berlin fixture bbox with
100m/1km/2km windows plus 200 seeded scatter URLs, mixed limits.
"""
import argparse
import random
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument("--out-dir", default="workloads/berlin")
args = parser.parse_args()

random.seed(7)
paths = ["# berlin 20k deterministic mix"]
for i in range(10):
    for j in range(10):
        lon = 13.35 + i * 0.01 + 0.005
        lat = 52.48 + j * 0.007 + 0.0035
        for half, lim in [(0.0005, 10), (0.005, 100), (0.01, 100)]:
            paths.append(
                f"/collections/buildings/items?"
                f"bbox={lon - half:.6f},{lat - half:.6f},"
                f"{lon + half:.6f},{lat + half:.6f}&limit={lim}&sources=1"
            )
for _ in range(200):
    lon = random.uniform(13.35, 13.45)
    lat = random.uniform(52.48, 52.55)
    half = random.choice([0.0005, 0.005, 0.01])
    lim = random.choice([10, 100, 1000])
    paths.append(
        f"/collections/buildings/items?"
        f"bbox={lon - half:.6f},{lat - half:.6f},"
        f"{lon + half:.6f},{lat + half:.6f}&limit={lim}&sources=1"
    )
out = Path(args.out_dir)
out.mkdir(parents=True, exist_ok=True)
(out / "mixed.txt").write_text("\n".join(paths) + "\n")
print(f"{len(paths) - 1} urls -> {out / 'mixed.txt'}")
