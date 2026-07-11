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


class TestShouldShipClean:
    """Whitelist mode — the client-release contract."""

    def test_root_whitelist_ships(self):
        from oasis.logic.release_packager import should_ship_clean
        for f in ("entrypoint.py", "app.py", "ops_dashboard.py",
                  "home_app.py", "install.bat", "VERSION",
                  "requirements.txt", "alembic.ini",
                  "run_oasis_home.bat", "branding.example.json"):
            assert should_ship_clean(f)[0], f

    def test_root_ad_hoc_scripts_dropped(self):
        from oasis.logic.release_packager import should_ship_clean
        for f in ("find_cost_mismatch.py", "inspect_db.py",
                  "audit_barcodes.py", "generate_kapa_docx.py",
                  "kuber_terminal.py", "pitch_app.py",
                  "compare_engines.py", "extract_scorecards.py"):
            assert not should_ship_clean(f)[0], f

    def test_oasis_logic_and_ui_ship(self):
        from oasis.logic.release_packager import should_ship_clean
        for f in ("oasis/logic/order_engine.py", "oasis/ui/theme.py",
                  "oasis/api/bridge.py", "oasis/simulation/scenarios.py"):
            assert should_ship_clean(f)[0], f

    def test_oasis_data_only_schema(self):
        from oasis.logic.release_packager import should_ship_clean
        # schema modules ship
        assert should_ship_clean("oasis/data/supplier_calendar.py")[0]
        # payload does NOT
        for f in ("oasis/data/mock_pos_erp.db",
                  "oasis/data/product_department_map.json",
                  "oasis/data/rhapta_pos.db",
                  "oasis/data/outputs/aneek_analysis.json"):
            assert not should_ship_clean(f)[0], f

    def test_oasis_vault_and_side_projects_dropped(self):
        from oasis.logic.release_packager import should_ship_clean
        for f in ("oasis/Oasis/Nodes/x.md",
                  "oasis/Runs/2026-01/x.md",
                  "oasis/Neural_Archive/x.md",
                  "oasis/exchange/kuber_bridge_hook.py",
                  "pages/1_Phase_1.py", "sandbox/x.py"):
            assert not should_ship_clean(f)[0], f

    def test_tests_and_dev_dropped(self):
        from oasis.logic.release_packager import should_ship_clean
        for f in ("tests/test_x.py", "tests/conftest.py",
                  ".env.example", ".gitignore", ".dockerignore"):
            assert not should_ship_clean(f)[0], f

    def test_cloud_hub_never_ships(self):
        """The hub holds the license salt server-side — it must NEVER be in a
        client release, or the salt-issuing surface leaks to customers."""
        from oasis.logic.release_packager import should_ship_clean
        for f in ("oasis_hub/app.py", "oasis_hub/licensing.py",
                  "oasis_hub/models.py", "oasis_hub/routers/admin.py"):
            assert not should_ship_clean(f)[0], f

    def test_erp_connectors_never_ship(self):
        """Connectors ship to the customer's ERP (Odoo etc.), not inside the
        OASIS on-prem client install — keep them out of the client zip."""
        from oasis.logic.release_packager import should_ship_clean
        for f in ("connectors/odoo/xmlrpc_sync.py",
                  "connectors/odoo/oasis_connector/mapping.py",
                  "connectors/odoo/oasis_connector/__manifest__.py"):
            assert not should_ship_clean(f)[0], f

    def test_migrations_ship(self):
        from oasis.logic.release_packager import should_ship_clean
        assert should_ship_clean("migrations/env.py")[0]
        assert should_ship_clean("migrations/versions/001_baseline.py")[0]
