"""
Release packager — turn the working tree into a shippable client artifact.

    python entrypoint.py --mode package-release
        -> dist/OASIS_v<VERSION>.zip

The zip contains the application (code, launchers, migrations, install.bat,
VERSION) and NOTHING that belongs to this machine: no venv, no git history, no
databases or client spreadsheets, no license keys/salt, no vault, no derived
graphs, no backups or generated reports. What ships is exactly what a fresh
client install needs; data is built on-site via the documented onboarding modes.

Pure decision logic (should_ship) is unit-tested; build_release is the zipper.
"""

from __future__ import annotations

import os
import zipfile
from typing import Optional, Tuple

#: directories never shipped (matched on any path segment)
_EXCLUDE_DIRS = {
    ".git", ".oasis_venv", "__pycache__", ".pytest_cache", ".ruff_cache",
    "dist", "backups", "reports", "oasis_vault", "neutral_network_export",
    "node_modules", ".claude", ".idea", ".vscode", "scripts",
    # local model weights (huge AND not redistributable), side projects,
    # duplicate/legacy trees, and build artifacts
    "models", "oasis-portal", "vj_canvas", ".next", "build",
    "oasis_checkpoint_before_refactor", "Allocation_Engine_Release",
}

#: exact file names never shipped (secrets / machine state)
_EXCLUDE_FILES = {
    ".env", "oasis_license.key", ".oasis_install_state.json",
    "moq_failures.json", "journey_state.json",
}

#: extensions never shipped (data/weights live on the client, not the release)
_EXCLUDE_EXTS = {".db", ".db-wal", ".db-shm", ".xlsx", ".xls", ".pyc",
                 ".zip", ".log", ".pdf", ".gguf", ".joblib", ".pt", ".pth",
                 ".onnx", ".mp3", ".mp4", ".csv"}

#: anything bigger than this is data/derived, not application code
_MAX_FILE_MB = 20.0


def should_ship(relpath: str, size_bytes: int = 0) -> Tuple[bool, str]:
    """(ship?, reason) for a repo-relative path. Pure."""
    parts = relpath.replace("\\", "/").split("/")
    for seg in parts[:-1]:
        if seg in _EXCLUDE_DIRS:
            return False, f"excluded dir '{seg}'"
    name = parts[-1]
    if name in _EXCLUDE_FILES or name.endswith(".key"):
        return False, "secret/machine state"
    ext = os.path.splitext(name)[1].lower()
    # allow the double-suffix sqlite sidecars
    if name.endswith(".db-wal") or name.endswith(".db-shm"):
        return False, "database sidecar"
    if ext in _EXCLUDE_EXTS:
        return False, f"excluded extension '{ext}'"
    if size_bytes > _MAX_FILE_MB * 1e6:
        return False, f"exceeds {_MAX_FILE_MB:.0f}MB size cap (data, not code)"
    return True, "ok"


def build_release(root: str, out_dir: Optional[str] = None,
                  version: Optional[str] = None) -> dict:
    """Zip the shippable subset of `root` into dist/OASIS_v<version>.zip."""
    if version is None:
        try:
            with open(os.path.join(root, "VERSION"), "r", encoding="utf-8") as f:
                version = f.read().strip()
        except OSError:
            version = "0.0.0"
    out_dir = out_dir or os.path.join(root, "dist")
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, f"OASIS_v{version}.zip")

    shipped = skipped = 0
    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
        for dirpath, dirnames, filenames in os.walk(root):
            # prune excluded dirs in-place so we never descend into them
            dirnames[:] = [d for d in dirnames if d not in _EXCLUDE_DIRS]
            for fn in filenames:
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, root)
                try:
                    size = os.path.getsize(full)
                except OSError:
                    size = 0
                ship, _ = should_ship(rel, size)
                if not ship:
                    skipped += 1
                    continue
                zf.write(full, arcname=os.path.join(f"OASIS_v{version}", rel))
                shipped += 1

    return {"zip": zip_path, "version": version, "files_shipped": shipped,
            "files_skipped": skipped,
            "size_mb": round(os.path.getsize(zip_path) / 1e6, 1)}
