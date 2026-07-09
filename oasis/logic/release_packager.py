"""
Release packager — turn the working tree into a shippable client artifact.

    python entrypoint.py --mode package-release
        -> dist/OASIS_v<VERSION>.zip

    python entrypoint.py --mode package-release --bundle-runtime
        -> dist/OASIS_v<VERSION>.zip (with embedded Python + wheelhouse)

The zip contains the application (code, launchers, migrations, install.bat,
VERSION) and NOTHING that belongs to this machine: no venv, no git history, no
databases or client spreadsheets, no license keys/salt, no vault, no derived
graphs, no backups or generated reports. What ships is exactly what a fresh
client install needs; data is built on-site via the documented onboarding modes.

With --bundle-runtime, the packager also builds an offline runtime bundle
(Python embeddable zip + get-pip.py + all dependency wheels) so the client
installer can run on machines with zero internet access and no Python installed.

Pure decision logic (should_ship) is unit-tested; build_release is the zipper.
"""

from __future__ import annotations

import os
import shutil
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
    # runtime/ is handled separately when --bundle-runtime is used
    "runtime", "python_runtime",
    # test/dev artifacts
    "dist_release", "test_data", "test_oasis",
    ".devcontainer", ".github",
}

#: exact file names never shipped (secrets / machine state)
_EXCLUDE_FILES = {
    ".env", "oasis_license.key", ".oasis_install_state.json",
    "moq_failures.json", "journey_state.json",
    ".oasis_telemetry_cache",
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


def _add_runtime_bundle(zf: zipfile.ZipFile, root: str,
                        arc_prefix: str) -> dict:
    """Build and add the offline runtime bundle into the zip.

    Returns a summary dict with counts and sizes.
    """
    runtime_dir = os.path.join(root, "runtime")
    runtime_files = 0
    runtime_bytes = 0

    # If runtime/ exists and has content, include it directly
    if os.path.isdir(runtime_dir):
        for dirpath, dirnames, filenames in os.walk(runtime_dir):
            for fn in filenames:
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, root)
                size = os.path.getsize(full)
                zf.write(full, arcname=os.path.join(arc_prefix, rel))
                runtime_files += 1
                runtime_bytes += size
    else:
        # Try to build the bundle on the fly
        try:
            from build_offline_bundle import build_bundle
            print("[runtime] Building offline bundle (this may take a few minutes)...")
            result = build_bundle(runtime_dir)
            # Now add the freshly built runtime
            for dirpath, dirnames, filenames in os.walk(runtime_dir):
                for fn in filenames:
                    full = os.path.join(dirpath, fn)
                    rel = os.path.relpath(full, root)
                    size = os.path.getsize(full)
                    zf.write(full, arcname=os.path.join(arc_prefix, rel))
                    runtime_files += 1
                    runtime_bytes += size
        except Exception as e:
            print(f"[runtime] WARNING: Could not build runtime bundle: {e}")
            print("[runtime] Run 'python build_offline_bundle.py' manually first.")

    return {"runtime_files": runtime_files,
            "runtime_mb": round(runtime_bytes / 1e6, 1)}


def build_release(root: str, out_dir: Optional[str] = None,
                  version: Optional[str] = None,
                  bundle_runtime: bool = False) -> dict:
    """Zip the shippable subset of `root` into dist/OASIS_v<version>.zip.

    Args:
        root: The project root directory.
        out_dir: Output directory for the zip (default: root/dist).
        version: Version string (default: read from VERSION file).
        bundle_runtime: If True, include the embedded Python runtime and
            pre-downloaded dependency wheels for fully offline installation.
    """
    if version is None:
        try:
            with open(os.path.join(root, "VERSION"), "r", encoding="utf-8") as f:
                version = f.read().strip()
        except OSError:
            version = "0.0.0"
    out_dir = out_dir or os.path.join(root, "dist")
    os.makedirs(out_dir, exist_ok=True)
    zip_path = os.path.join(out_dir, f"OASIS_v{version}.zip")
    arc_prefix = f"OASIS_v{version}"

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
                zf.write(full, arcname=os.path.join(arc_prefix, rel))
                shipped += 1

        # Add runtime bundle if requested
        runtime_info = {}
        if bundle_runtime:
            print(f"[package] Including embedded runtime bundle...")
            runtime_info = _add_runtime_bundle(zf, root, arc_prefix)
            shipped += runtime_info.get("runtime_files", 0)
            print(f"[package] Runtime: {runtime_info.get('runtime_files', 0)} files, "
                  f"{runtime_info.get('runtime_mb', 0)} MB")

    result = {
        "zip": zip_path,
        "version": version,
        "files_shipped": shipped,
        "files_skipped": skipped,
        "size_mb": round(os.path.getsize(zip_path) / 1e6, 1),
        "bundle_runtime": bundle_runtime,
    }
    result.update(runtime_info)
    return result
