"""
Backup / restore for the OASIS SQLite store (client-side data safety).

    python entrypoint.py --mode backup                 # timestamped copy + prune
    python entrypoint.py --mode restore --file <path>  # put a backup back

Backups use sqlite3's online backup API (safe against a live WAL writer) into
``<db_dir>/backups/<name>_YYYYmmdd_HHMMSS.db``, keeping the newest
OASIS_BACKUP_KEEP (default 10). Restore refuses to run while the DB is locked
by a live writer and takes a safety copy of the current DB first.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import List, Optional


def backup_db(db_path: str, backup_dir: Optional[str] = None, keep: int = 10) -> dict:
    """Online-backup db_path into backup_dir; prune to the newest `keep`."""
    if not os.path.exists(db_path):
        raise FileNotFoundError(db_path)
    backup_dir = backup_dir or os.path.join(os.path.dirname(os.path.abspath(db_path)), "backups")
    os.makedirs(backup_dir, exist_ok=True)
    stem = os.path.splitext(os.path.basename(db_path))[0]
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = os.path.join(backup_dir, f"{stem}_{stamp}.db")

    src = sqlite3.connect(db_path, timeout=30.0)
    try:
        dst = sqlite3.connect(out_path)
        try:
            src.backup(dst)          # consistent even mid-write (WAL-safe)
        finally:
            dst.close()
    finally:
        src.close()

    pruned = _prune(backup_dir, stem, keep)
    return {"backup": out_path, "size_mb": round(os.path.getsize(out_path) / 1e6, 1),
            "kept": keep, "pruned": pruned}


def _prune(backup_dir: str, stem: str, keep: int) -> List[str]:
    """Delete all but the newest `keep` backups for this stem."""
    entries = sorted(
        (f for f in os.listdir(backup_dir)
         if f.startswith(stem + "_") and f.endswith(".db")),
        reverse=True)
    doomed = entries[keep:]
    for f in doomed:
        try:
            os.remove(os.path.join(backup_dir, f))
        except OSError:
            pass
    return doomed


def list_backups(db_path: str, backup_dir: Optional[str] = None) -> List[dict]:
    """Newest-first list of this DB's backups: ``{index, path, size_mb, taken}``.

    Restore is the one command an operator runs on their worst day, and it
    needs a path they do not have memorised. Without a way to SEE the backups,
    ``--mode restore`` was reachable only by someone who already knew the
    filename — which is nobody, mid-incident.
    """
    backup_dir = backup_dir or os.path.join(
        os.path.dirname(os.path.abspath(db_path)), "backups")
    if not os.path.isdir(backup_dir):
        return []
    stem = os.path.splitext(os.path.basename(db_path))[0]
    names = sorted((f for f in os.listdir(backup_dir)
                    if f.startswith(stem + "_") and f.endswith(".db")),
                   reverse=True)
    out = []
    for i, name in enumerate(names, start=1):
        full = os.path.join(backup_dir, name)
        try:
            st = os.stat(full)
        except OSError:
            continue
        out.append({
            "index": i,
            "path": full,
            "size_mb": round(st.st_size / 1e6, 1),
            "taken": datetime.fromtimestamp(st.st_mtime).strftime("%Y-%m-%d %H:%M:%S"),
        })
    return out


def resolve_backup(db_path: str, ref: str, backup_dir: Optional[str] = None) -> str:
    """Resolve a backup reference — either a path or a 1-based index from
    :func:`list_backups` — to an absolute path. Pure lookup; touches nothing."""
    if os.path.exists(ref):
        return ref
    if ref.isdigit():
        entries = list_backups(db_path, backup_dir)
        idx = int(ref)
        if 1 <= idx <= len(entries):
            return entries[idx - 1]["path"]
        raise FileNotFoundError(
            f"no backup #{idx}; there are {len(entries)}. "
            f"Run --mode list-backups to see them.")
    raise FileNotFoundError(ref)


def restore_db(db_path: str, backup_file: str) -> dict:
    """Restore backup_file over db_path (after a safety copy of the current DB)."""
    if not os.path.exists(backup_file):
        raise FileNotFoundError(backup_file)
    # verify the backup is a readable sqlite db before touching anything
    probe = sqlite3.connect(backup_file)
    try:
        probe.execute("SELECT count(*) FROM sqlite_master")
    finally:
        probe.close()

    safety = None
    if os.path.exists(db_path):
        safety = db_path + ".pre_restore"
        # fails with PermissionError if a console/stream holds the file — good:
        # restoring under a live writer would corrupt state.
        os.replace(db_path, safety)
        for suffix in ("-wal", "-shm"):
            p = db_path + suffix
            if os.path.exists(p):
                os.remove(p)

    import shutil
    shutil.copy2(backup_file, db_path)
    return {"restored": db_path, "from": backup_file, "previous_saved_as": safety}
