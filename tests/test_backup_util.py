"""Tests for backup/restore of the OASIS store DB."""

import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.backup_util import backup_db, restore_db


def _mini(path, marker):
    c = sqlite3.connect(path)
    c.execute("CREATE TABLE t (v TEXT)")
    c.execute("INSERT INTO t VALUES (?)", (marker,))
    c.commit()
    c.close()


def _marker(path):
    c = sqlite3.connect(path)
    try:
        return c.execute("SELECT v FROM t").fetchone()[0]
    finally:
        c.close()


class TestBackup:
    def test_backup_creates_readable_copy(self, tmp_path):
        db = str(tmp_path / "store.db")
        _mini(db, "v1")
        res = backup_db(db, keep=5)
        assert os.path.exists(res["backup"])
        assert _marker(res["backup"]) == "v1"

    def test_retention_prunes_oldest(self, tmp_path):
        db = str(tmp_path / "store.db")
        _mini(db, "v1")
        bdir = str(tmp_path / "backups")
        # fabricate 4 old backups, keep=3 → after a new one, oldest pruned
        os.makedirs(bdir)
        for i in range(4):
            _mini(os.path.join(bdir, f"store_2026010{i}_000000.db"), f"old{i}")
        res = backup_db(db, backup_dir=bdir, keep=3)
        assert len(res["pruned"]) == 2   # 5 total → keep newest 3
        remaining = [f for f in os.listdir(bdir) if f.endswith(".db")]
        assert len(remaining) == 3


class TestRestore:
    def test_restore_roundtrip_with_safety_copy(self, tmp_path):
        db = str(tmp_path / "store.db")
        _mini(db, "v1")
        res = backup_db(db, keep=5)
        # mutate the live DB, then restore the backup
        c = sqlite3.connect(db)
        c.execute("UPDATE t SET v='v2'")
        c.commit()
        c.close()
        out = restore_db(db, res["backup"])
        assert _marker(db) == "v1"                       # restored
        assert out["previous_saved_as"] and os.path.exists(out["previous_saved_as"])
        assert _marker(out["previous_saved_as"]) == "v2"  # safety copy kept

    def test_restore_missing_backup_raises(self, tmp_path):
        db = str(tmp_path / "store.db")
        _mini(db, "v1")
        try:
            restore_db(db, str(tmp_path / "nope.db"))
            assert False
        except FileNotFoundError:
            pass
