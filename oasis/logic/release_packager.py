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
                 ".onnx", ".mp3", ".mp4", ".csv", ".jpg", ".jpeg"}


#: path-glob patterns never shipped (data caches, generated maps, big JSON, etc.)
_EXCLUDE_GLOBS = (
    "oasis/data/*.json", "oasis/data/*.bak_*", "oasis/data/outputs/*",
    "*.json.bak_*", "product_*_map.json", "barcode_*_map.json",
    "rhapta_master_metrics.json", "supplier_*_analysis.json",
    "supplier_weekly_schedule.json", "supplier_predictive_analysis.json",
    "supplier_schedule*.json", "shadow_health.json",
    "*_intelligence_cache.json", "aneek_analysis.json", "kapa_*.json",
    "moq_failures.json", "journey_state.json", "transfers_registry.json",
    "zip_contents.txt", "oasis_exchange_text.txt",
    "TouchDesigner_Vibe/*", "*.png",
)

#: anything bigger than this is data/derived, not application code
_MAX_FILE_MB = 20.0


# ── STRICT WHITELIST MODE (client releases) ──────────────────────────────
# The blacklist above is a "keep everything unless" filter — great for CI, but
# a working tree accumulates hundreds of exploration scripts a client should
# never see. For client releases we ship exactly the files below and nothing
# else.

#: exact root files that ARE the shipping application
_ROOT_WHITELIST = {
    # entry points
    "entrypoint.py", "app.py", "app_intel.py", "ops_dashboard.py",
    "home_app.py", "st_gat_dashboard.py", "allocation_app.py",
    "intraday_sim.py",
    # install + config
    "install.bat", "OASIS.bat", "VERSION", "requirements.txt", "alembic.ini",
    "branding.example.json", "oasis_client_config.template.json",
    # launchers the client actually runs
    "run_oasis_home.bat",
    "run_oasis_live.bat", "run_oasis_intel_live.bat", "run_command_center_live.bat",
    "run_multi_pos.bat", "run_mock_pos.bat", "run_command_center_multi.bat",
    "run_market_intelligence_tool.bat",
    # doc
    "README.md",
}

#: oasis/ sub-packages that ship (schema modules only from oasis/data)
_OASIS_WHITELIST_SUBPKGS = {
    "__init__.py", "logging_config.py", "models.py",
    "api/", "logic/", "ui/", "simulation/", "analytics/",
    "tools/", "data_validation/",
}

#: exact files from oasis/data that ship (schema code, not data files)
_OASIS_DATA_WHITELIST = {
    "supplier_calendar.py", "__init__.py",
}

#: everything in migrations/ ships (alembic run-time)
_MIGRATIONS_ROOT = "migrations/"


def should_ship_clean(relpath: str, size_bytes: int = 0) -> Tuple[bool, str]:
    """Whitelist decision for client releases. Pure.

    Only files explicitly named in the whitelists ship; everything else is
    dropped by default. This ends the "191 root scripts by accident" problem.
    """
    rel = relpath.replace("\\", "/")
    name = rel.rsplit("/", 1)[-1]

    # 1. Root files: exact whitelist only
    if "/" not in rel:
        if rel in _ROOT_WHITELIST:
            return True, "root whitelist"
        return False, "root file not on whitelist"

    # 2. migrations/ — the alembic tree ships whole
    if rel.startswith(_MIGRATIONS_ROOT):
        if "__pycache__" in rel or name.endswith(".pyc"):
            return False, "migration cache"
        return True, "migration"

    # 3. oasis/ — sub-package whitelist
    if rel.startswith("oasis/"):
        sub = rel[len("oasis/"):]
        if "__pycache__" in sub or name.endswith(".pyc"):
            return False, "pycache"
        # allow the top-level files (logging_config.py etc.)
        top = sub.split("/", 1)[0]
        if "/" not in sub:  # oasis/<file>
            return (top in _OASIS_WHITELIST_SUBPKGS,
                    "oasis root file " + ("ok" if top in _OASIS_WHITELIST_SUBPKGS else "not on whitelist"))
        # oasis/data — schema modules only
        if top == "data":
            leaf = sub[len("data/"):]
            if "/" not in leaf and leaf in _OASIS_DATA_WHITELIST:
                return True, "oasis/data schema"
            return False, "oasis/data payload"
        # oasis/<pkg>/... — allowed if pkg is whitelisted
        if (top + "/") in _OASIS_WHITELIST_SUBPKGS:
            # apply the general size cap; no .db/.xlsx/etc even within allowed pkgs
            ext = os.path.splitext(name)[1].lower()
            if ext in _EXCLUDE_EXTS:
                return False, f"excluded extension in oasis/{top}"
            if size_bytes > _MAX_FILE_MB * 1e6:
                return False, "size cap in oasis pkg"
            return True, f"oasis/{top}"
        return False, f"oasis/{top} not on whitelist"

    return False, "not on whitelist"


def should_ship(relpath: str, size_bytes: int = 0) -> Tuple[bool, str]:
    """(ship?, reason) for a repo-relative path. Pure."""
    import fnmatch
    rel = relpath.replace("\\", "/")
    parts = rel.split("/")
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
    for pat in _EXCLUDE_GLOBS:
        if fnmatch.fnmatch(rel, pat) or fnmatch.fnmatch(name, pat):
            return False, f"matches exclude glob '{pat}'"
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
                  bundle_runtime: bool = False,
                  clean: bool = True) -> dict:
    """Zip the shippable subset of `root` into dist/OASIS_v<version>.zip.

    Args:
        root: The project root directory.
        out_dir: Output directory for the zip (default: root/dist).
        version: Version string (default: read from VERSION file).
        bundle_runtime: If True, include the embedded Python runtime and
            pre-downloaded dependency wheels for fully offline installation.
        clean: If True (default), use the STRICT WHITELIST (client-ready
            release — root_whitelist + oasis subpackages + migrations). If
            False, fall back to the legacy blacklist mode.
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
                if clean:
                    ship, _ = should_ship_clean(rel, size)
                else:
                    ship, _ = should_ship(rel, size)
                if not ship:
                    skipped += 1
                    continue
                zf.write(full, arcname=os.path.join(arc_prefix, rel))
                shipped += 1

        # Add runtime bundle if requested
        runtime_info = {}
        if bundle_runtime:
            print("[package] Including embedded runtime bundle...")
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
