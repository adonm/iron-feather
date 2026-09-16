#!/usr/bin/env python3
"""Install the pinned ZeroFS binary for the local lake rig; no package manager.

Downloads the upstream multiplatform PGO tarball, verifies its SHA-256,
and extracts the Linux amd64/arm64 (or macOS arm64) `zerofs` binary plus the
`mount` FUSE client into `.deps/zerofs/`. Bump VERSION + TARBALL_SHA256
together; a moved upstream build fails loudly instead of mixing versions.
"""

import hashlib
import platform
import shutil
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path

VERSION = "2.3.3"
TARBALL_URL = (
    "https://github.com/Barre/zerofs/releases/download/"
    f"v{VERSION}/zerofs-pgo-multiplatform.tar.gz"
)
TARBALL_SHA256 = (
    "17cdf8fffdd57c7a3373d71a04a84f2f727561120faa25e5f29e0b8de8d4b633"
)
MEMBERS = {
    ("Linux", "x86_64"): "zerofs-linux-amd64-pgo",
    ("Linux", "aarch64"): "zerofs-linux-arm64-pgo",
    ("Linux", "arm64"): "zerofs-linux-arm64-pgo",
    ("Darwin", "arm64"): "zerofs-darwin-aarch64-pgo",
    ("Darwin", "x86_64"): "zerofs-darwin-x86_64-pgo",
}


def main():
    root = Path(__file__).resolve().parent.parent
    dest = root / ".deps" / "zerofs"
    marker = f"{VERSION}\n"
    if (dest / "version").exists() and (dest / "version").read_text() == marker:
        return
    key = (platform.system(), platform.machine())
    member = MEMBERS.get(key)
    if member is None:
        raise SystemExit(f"unsupported platform: {key}")
    dest.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".zerofs-", dir=dest.parent) as tmp:
        staging = Path(tmp)
        archive = staging / "zerofs.tar.gz"
        print(f"Downloading {TARBALL_URL}", flush=True)
        digest = hashlib.sha256()
        request = urllib.request.Request(
            TARBALL_URL, headers={"User-Agent": "curl/8.14.1"}
        )
        with urllib.request.urlopen(request, timeout=300) as response:
            with archive.open("wb") as output:
                while True:
                    block = response.read(1 << 20)
                    if not block:
                        break
                    digest.update(block)
                    output.write(block)
        if digest.hexdigest() != TARBALL_SHA256:
            raise SystemExit("zerofs checksum mismatch: upstream moved, bump the pin")
        with tarfile.open(archive) as bundle:
            bundle.extract(member, path=staging, filter="data")
        binary = staging / member
        binary.chmod(0o755)
        actual = subprocess.check_output(
            [str(binary), "--version"], text=True
        ).strip()
        if VERSION not in actual:
            raise SystemExit(f"zerofs version mismatch: {actual}")
        dest.mkdir(exist_ok=True)
        shutil.move(str(binary), dest / "zerofs")
        (dest / "version").write_text(marker)
    print(f"Installed zerofs {VERSION}")


if __name__ == "__main__":
    main()
