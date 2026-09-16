#!/usr/bin/env python3
"""Generate saved, reproducible OGC workloads for a completed shard.

Each output file holds one URL path per line (`#` comments allowed) for
`http_bench --workload`. Cities are filtered to the shard region, scatter
points use a fixed seed, and window sizes (100m/1km/10km) plus limits
(10/100/1000) span the selectivity range. Regenerate for a new shard with
--region matching its build bbox.
"""
import argparse
import random
from pathlib import Path

# Dense urban cores (lon, lat); filtered to the shard region at runtime.
CITIES = [
    ("paris", 2.35, 48.85),
    ("brussels", 4.35, 50.85),
    ("amsterdam", 4.90, 52.37),
    ("rotterdam", 4.48, 51.92),
    ("antwerp", 4.40, 51.22),
    ("lille", 3.06, 50.63),
    ("ghent", 3.73, 51.05),
    ("the-hague", 4.30, 52.07),
    ("utrecht", 5.12, 52.09),
    ("eindhoven", 5.47, 51.44),
    ("charleroi", 4.44, 50.41),
    ("liege", 5.57, 50.65),
    ("amiens", 2.30, 49.89),
    ("reims", 4.03, 49.26),
]
# Sparse land points (lon, lat); no sea, no cities.
RURAL = [
    ("veluwe", 5.80, 52.30),
    ("ardennes-edge", 3.90, 49.60),
    ("picardy-fields", 2.70, 49.30),
    ("friesland", 5.30, 53.10),
]
# Reliably empty: North Sea.
SEA = [
    ("north-sea-1", 2.20, 53.60, 2.80, 54.00),
    ("north-sea-2", 3.20, 53.55, 3.80, 53.95),
]
# Window half-widths in degrees (~100m / 1km / 10km at these latitudes).
SIZES = [0.0005, 0.005, 0.05]
LIMITS = [10, 100, 1000]


def items(bbox, limit, sources=1):
    w, s, e, n = bbox
    return (f"/collections/buildings/items?"
            f"bbox={w:.6f},{s:.6f},{e:.6f},{n:.6f}&limit={limit}&sources={sources}")


def window(lon, lat, half):
    return (lon - half, lat - half, lon + half, lat + half)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region", default="2,48,6,54",
                        help="west,south,east,north of the shard")
    parser.add_argument("--out-dir", default="workloads/nw-europe")
    args = parser.parse_args()
    west, south, east, north = map(float, args.region.split(","))
    out = Path(args.out_dir)
    out.mkdir(parents=True, exist_ok=True)

    def inside(lon, lat, margin=0.12):
        return west + margin <= lon <= east - margin and south + margin <= lat <= north - margin

    cities = [(name, lon, lat) for name, lon, lat in CITIES if inside(lon, lat)]
    rural = [(name, lon, lat) for name, lon, lat in RURAL if inside(lon, lat)]
    if not cities:
        parser.error("no known cities inside region; pass --region covering them")

    files = {}

    # Hot set: a few dense windows hit by every worker -> Moka hits.
    hot = []
    for name, lon, lat in cities[:3]:
        hot.append(items(window(lon, lat, 0.005), 10))
    files["hot.txt"] = hot * 8

    # Urban / rural sweeps across sizes and limits.
    urban, rural_lines = [], []
    for name, lon, lat in cities:
        for half in SIZES:
            for limit in LIMITS:
                urban.append(items(window(lon, lat, half), limit))
    for name, lon, lat in rural:
        for half in SIZES:
            for limit in LIMITS:
                rural_lines.append(items(window(lon, lat, half), limit))
    files["urban.txt"] = urban
    files["rural.txt"] = rural_lines

    # Scattered fixed-seed points across the region.
    rng = random.Random(42)
    scatter = []
    for _ in range(3000):
        lon = rng.uniform(west, east)
        lat = rng.uniform(south, north)
        half = rng.choice(SIZES)
        scatter.append(items(window(lon, lat, half), rng.choice(LIMITS)))
    files["scatter.txt"] = scatter

    # Broad windows: quarter-region slices with large limits.
    mid_lon, mid_lat = (west + east) / 2, (south + north) / 2
    broad = []
    for box in [(west, south, mid_lon, mid_lat), (mid_lon, mid_lat, east, north),
                (west, mid_lat, east, north), (west, south, east, north)]:
        for limit in (100, 1000):
            broad.append(items(box, limit))
    files["broad.txt"] = broad

    # Deliberately empty sea windows, reported separately.
    files["empty.txt"] = [
        items((lon, lat, lon2, lat2), limit)
        for _, lon, lat, lon2, lat2 in SEA
        for limit in LIMITS
    ]

    # Deep pages via direct offsets (cursor chains need link-following).
    dense = cities[0]
    files["deep.txt"] = [
        f"/collections/buildings/items?bbox={west:.6f},{south:.6f},{east:.6f},{north:.6f}"
        f"&limit=10&offset={offset}&sources=1"
        for offset in (1000, 5000, 10000)
    ] + [items(window(dense[1], dense[2], 0.05), 10)]

    files["mixed.txt"] = (hot + urban + rural_lines + scatter[:500] + broad
                          + files["empty.txt"] + files["deep.txt"])

    for name, lines in files.items():
        path = out / name
        header = f"# {args.region} deterministic workload: {len(lines)} urls\n"
        path.write_text(header + "".join(line + "\n" for line in lines))
        print(f"{path}: {len(lines)} urls")


if __name__ == "__main__":
    main()
