"""
Vendor-side tool: build a fully offline OASIS installation bundle.

Downloads the Python 3.10 embeddable zip, get-pip.py, and all dependency wheels
so the client installer can run on machines with zero internet access.

    python build_offline_bundle.py                    # build into runtime/
    python build_offline_bundle.py --out-dir release  # custom output folder

Result layout:
    runtime/
        python-3.10.11-embed-amd64.zip   (embeddable Python)
        get-pip.py                        (pip bootstrapper)
        wheelhouse/                       (all .whl files for offline pip install)
"""

from __future__ import annotations

import argparse
import hashlib
import os
import shutil
import subprocess
import sys
import urllib.request

# ── Configuration ────────────────────────────────────────────────────────────

PYTHON_VERSION = "3.10.11"
PYTHON_EMBED_URL = (
    f"https://www.python.org/ftp/python/{PYTHON_VERSION}/"
    f"python-{PYTHON_VERSION}-embed-amd64.zip"
)
PYTHON_EMBED_SHA256 = (
    "608619f8619075629c9c69f361352a0da6ed7e62f83a0e19c63e0ea32eb7629d"
)
GET_PIP_URL = "https://bootstrap.pypa.io/get-pip.py"

ROOT = os.path.dirname(os.path.abspath(__file__))
DEFAULT_OUT = os.path.join(ROOT, "runtime")
REQUIREMENTS_LOCK = os.path.join(ROOT, "requirements.lock.txt")
REQUIREMENTS_MAIN = os.path.join(ROOT, "requirements.txt")


def _download(url: str, dest: str, expected_sha256: str | None = None) -> str:
    """Download a URL to a local file with optional SHA-256 verification."""
    if os.path.exists(dest):
        if expected_sha256:
            actual = _sha256(dest)
            if actual == expected_sha256:
                print(f"  [CACHED] {os.path.basename(dest)} (SHA-256 OK)")
                return dest
            print(f"  [REDOWNLOAD] SHA-256 mismatch for {os.path.basename(dest)}")
        else:
            print(f"  [CACHED] {os.path.basename(dest)}")
            return dest

    print(f"  [DOWNLOAD] {url}")
    print(f"             -> {dest}")
    urllib.request.urlretrieve(url, dest)

    if expected_sha256:
        actual = _sha256(dest)
        if actual != expected_sha256:
            raise RuntimeError(
                f"SHA-256 mismatch for {os.path.basename(dest)}!\n"
                f"  Expected: {expected_sha256}\n"
                f"  Got:      {actual}"
            )
        print(f"  [VERIFIED] SHA-256 OK")
    return dest


def _sha256(path: str) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def _download_wheels(out_dir: str, requirements: str) -> int:
    """Download all dependency wheels into a wheelhouse directory."""
    wheelhouse = os.path.join(out_dir, "wheelhouse")
    os.makedirs(wheelhouse, exist_ok=True)

    print(f"\n[3/3] Downloading wheels into {wheelhouse}")
    print(f"      Using requirements: {requirements}")

    # Use the current Python to download wheels for win_amd64 + cpython 3.10
    cmd = [
        sys.executable, "-m", "pip", "download",
        "--dest", wheelhouse,
        "--platform", "win_amd64",
        "--python-version", "3.10",
        "--implementation", "cp",
        "--abi", "cp310",
        "--only-binary=:all:",
        "--extra-index-url", "https://download.pytorch.org/whl/cpu",
        "-r", requirements,
    ]
    print(f"      Running: {' '.join(cmd[:6])} ...")

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        # Some packages (pure Python) may not have binary wheels; retry with
        # no-binary fallback for those
        print("      [INFO] Some packages need source fallback, retrying...")
        cmd_fallback = [
            sys.executable, "-m", "pip", "download",
            "--dest", wheelhouse,
            "--extra-index-url", "https://download.pytorch.org/whl/cpu",
            "-r", requirements,
        ]
        result2 = subprocess.run(cmd_fallback, capture_output=True, text=True)
        if result2.returncode != 0:
            print(f"      [WARNING] pip download had errors:\n{result2.stderr[-500:]}")

    # Count downloaded wheels
    wheels = [f for f in os.listdir(wheelhouse)
              if f.endswith(".whl") or f.endswith(".tar.gz")]
    print(f"      [OK] {len(wheels)} packages in wheelhouse")
    return len(wheels)


def build_bundle(out_dir: str | None = None) -> dict:
    """Build the complete offline runtime bundle.

    Returns a summary dict with paths and counts.
    """
    out_dir = out_dir or DEFAULT_OUT
    os.makedirs(out_dir, exist_ok=True)

    print("=" * 60)
    print("O.A.S.I.S. Offline Bundle Builder")
    print("=" * 60)

    # 1. Python embeddable zip
    print(f"\n[1/3] Python {PYTHON_VERSION} embeddable (amd64)")
    embed_zip = os.path.join(out_dir, f"python-{PYTHON_VERSION}-embed-amd64.zip")
    _download(PYTHON_EMBED_URL, embed_zip, PYTHON_EMBED_SHA256)

    # 2. get-pip.py
    print(f"\n[2/3] get-pip.py (pip bootstrapper)")
    get_pip = os.path.join(out_dir, "get-pip.py")
    _download(GET_PIP_URL, get_pip)

    # 3. Dependency wheels
    req_file = REQUIREMENTS_LOCK if os.path.exists(REQUIREMENTS_LOCK) else REQUIREMENTS_MAIN
    if not os.path.exists(req_file):
        raise FileNotFoundError(f"No requirements file found at {req_file}")
    n_wheels = _download_wheels(out_dir, req_file)

    # Summary
    total_size = 0
    for dirpath, _, filenames in os.walk(out_dir):
        for fn in filenames:
            total_size += os.path.getsize(os.path.join(dirpath, fn))
    size_mb = round(total_size / 1e6, 1)

    print(f"\n{'=' * 60}")
    print(f"Bundle ready: {out_dir}")
    print(f"  Python:     {PYTHON_VERSION} embed amd64")
    print(f"  Wheels:     {n_wheels} packages")
    print(f"  Total size: {size_mb} MB")
    print(f"{'=' * 60}")

    return {
        "out_dir": out_dir,
        "python_zip": embed_zip,
        "get_pip": get_pip,
        "n_wheels": n_wheels,
        "size_mb": size_mb,
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Build an offline OASIS installation bundle"
    )
    parser.add_argument(
        "--out-dir", default=DEFAULT_OUT,
        help=f"Output directory (default: {DEFAULT_OUT})"
    )
    args = parser.parse_args()
    build_bundle(args.out_dir)
