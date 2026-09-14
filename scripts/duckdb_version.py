#!/usr/bin/env python3
"""Print the DuckDB release (x.y.z) matching Cargo.lock's duckdb crate.

duckdb-rs versions as 1.MAJOR_MINOR_PATCH.x where the second component
encodes the DuckDB release (see libduckdb-sys build.rs), e.g. crate
1.10505.0 -> DuckDB 1.5.5.
"""

import re
import sys
from pathlib import Path


def main() -> None:
    lock = Path("Cargo.lock").read_text()
    m = re.search(r'name = "duckdb"\nversion = "1\.(\d+)\.(\d+)"', lock)
    if not m:
        print("error: duckdb crate not found in Cargo.lock", file=sys.stderr)
        sys.exit(1)
    enc = int(m.group(1))
    major, minor, patch = enc // 10_000, (enc // 100) % 100, enc % 100
    print(f"{major}.{minor}.{patch}")


if __name__ == "__main__":
    main()
