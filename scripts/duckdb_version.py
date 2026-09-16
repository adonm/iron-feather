#!/usr/bin/env python3
"""Pinned DuckDB 2.0 nightly matching duckdb-rs fdd481e7's C API.

Engine builds come from the rolling v2.0-cyanoptera artifacts (full GETs
are rejected there; only Range reads succeed). The SHA-256 pins the exact
bytes: a moved rolling build fails loudly instead of mixing versions.
"""

VERSION = "v2.0.0-alpha42069"
BASE_URL = "https://artifacts.duckdb.org/v2.0-cyanoptera"
ARCHIVES = {
    "shared-libs": "13f9b355b36f5f9266a4bc44ef3c6fb29f548886fcbf9572b6e001844f1b6046",
    "cli": "6102465b2f3270041cfa81f3628f60d3cf8f1bb70a7cbc457f00add2313e6fec",
}

if __name__ == "__main__":
    print(VERSION)
