#!/usr/bin/env python3
"""Install the pinned prebuilt engine and CLI atomically; no C++ build."""

import hashlib
import os
from pathlib import Path
import platform
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request

from duckdb_version import ARCHIVES, BASE_URL, VERSION


def ranged_download(url: str, dest: Path, sha256: str) -> None:
    # The artifact host forbids full GETs (403) but serves Range reads (206).
    chunk = 4 * 1024 * 1024
    probe = urllib.request.Request(
        url, headers={"Range": "bytes=0-0", "User-Agent": "curl/8.14.1"}
    )
    with urllib.request.urlopen(probe, timeout=120) as response:
        total = int(response.headers["Content-Range"].split("/")[-1])
    digest = hashlib.sha256()
    with dest.open("wb") as output:
        offset = 0
        while offset < total:
            end = min(offset + chunk - 1, total - 1)
            request = urllib.request.Request(
                url,
                headers={"Range": f"bytes={offset}-{end}", "User-Agent": "curl/8.14.1"},
            )
            with urllib.request.urlopen(request, timeout=120) as response:
                while True:
                    block = response.read(1 << 20)
                    if not block:
                        break
                    digest.update(block)
                    output.write(block)
            offset = end + 1
    if digest.hexdigest() != sha256:
        raise SystemExit(
            f"checksum mismatch for {url}: rolling nightly moved, bump the pin"
        )


def main():
    root = Path(__file__).resolve().parent.parent
    dest = root / ".deps/duckdb"
    marker = f"{VERSION}\n"
    if (dest / "version").exists() and (dest / "version").read_text() == marker:
        return
    arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[platform.machine()]
    system = platform.system()
    target = "osx-universal" if system == "Darwin" else f"linux-{arch}"
    if system not in ("Linux", "Darwin"):
        raise SystemExit(f"unsupported OS: {system}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".duckdb-", dir=dest.parent) as tmp:
        staging = Path(tmp)
        for kind, sha256 in ARCHIVES.items():
            url = f"{BASE_URL}/duckdb-{kind}-{target}.tar.gz"
            archive = staging / f"{kind}.tar.gz"
            print(f"Downloading {url}", flush=True)
            ranged_download(url, archive, sha256)
            with tarfile.open(archive) as bundle:
                bundle.extractall(staging, filter="data")
            archive.unlink()
        env = dict(os.environ, LD_LIBRARY_PATH=tmp, DYLD_LIBRARY_PATH=tmp)
        actual = subprocess.check_output(
            [str(staging / "duckdb"), ":memory:", "-csv", "-noheader", "SELECT version();"],
            env=env, text=True,
        ).strip()
        if actual != VERSION:
            raise SystemExit(f"engine version mismatch: {actual}, expected {VERSION}")
        dest.mkdir(exist_ok=True)
        for file in staging.iterdir():
            os.replace(file, dest / file.name)
        (dest / "version").write_text(marker)
    print(f"Installed DuckDB {VERSION}")


if __name__ == "__main__":
    main()
