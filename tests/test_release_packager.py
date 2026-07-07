"""Tests for the release packager (ship/exclude rules + zip build)."""

import os
import sys
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.release_packager import build_release, should_ship


class TestShouldShip:
    def test_code_and_launchers_ship(self):
        for p in ["entrypoint.py", "oasis/logic/order_engine.py",
                  "run_oasis_home.bat", "install.bat", "VERSION",
                  "requirements.txt", "migrations/env.py", "alembic.ini"]:
            assert should_ship(p)[0], p

    def test_secrets_and_machine_state_never_ship(self):
        for p in [".env", "oasis_license.key", "any/dir/client.key",
                  ".oasis_install_state.json",
                  "oasis/data/.oasis_install_state.json"]:
            assert not should_ship(p)[0], p

    def test_data_and_derived_never_ship(self):
        for p in ["oasis/data/rhapta_pos.db", "oasis/data/rhapta_pos.db-wal",
                  "oasis/data/dept_1_50.xlsx", "backups/x.db",
                  "reports/OASIS_Value_Report_2026-07.md",
                  "oasis_vault/Nodes/SKUs/x.md",
                  "neutral_network_export/nodes.csv",
                  ".oasis_venv/Scripts/python.exe", ".git/HEAD",
                  "oasis/logic/__pycache__/x.pyc", "dist/OASIS_v1.zip"]:
            assert not should_ship(p)[0], p


class TestBuildRelease:
    def test_zip_contains_app_not_secrets(self, tmp_path):
        root = tmp_path / "app"
        (root / "oasis" / "logic").mkdir(parents=True)
        (root / ".git").mkdir()
        (root / "VERSION").write_text("9.9.9")
        (root / "entrypoint.py").write_text("print('hi')")
        (root / "oasis" / "logic" / "x.py").write_text("x=1")
        (root / ".env").write_text("SECRET=1")
        (root / "oasis_license.key").write_text("{}")
        (root / ".git" / "HEAD").write_text("ref")
        (root / "store.db").write_text("bin")

        res = build_release(str(root), out_dir=str(tmp_path / "dist"))
        assert res["version"] == "9.9.9"
        with zipfile.ZipFile(res["zip"]) as zf:
            names = zf.namelist()
        assert any(n.endswith("entrypoint.py") for n in names)
        assert any(n.endswith("VERSION") for n in names)
        assert not any(".env" in n or ".key" in n or ".git" in n
                       or n.endswith(".db") for n in names)
        assert res["files_shipped"] == 3 and res["files_skipped"] >= 2
