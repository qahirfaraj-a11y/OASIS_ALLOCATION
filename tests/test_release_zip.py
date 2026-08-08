"""Cold-start regression: the release ZIP itself must always be shippable.

Complements the shell-scripted `scripts/cold_start_proof_local.sh` (which does
the full pip-install in a fresh venv). This test runs on every commit and
catches the cheap-to-detect failures — missing installer files, bloat
regressions, or accidental secret/data leaks — without paying for a full pip
resolve on every CI run.
"""

import os
import re
import sys
import zipfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

import pytest

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
DIST = os.path.join(ROOT, "dist")


def _zips():
    if not os.path.isdir(DIST):
        return []
    return sorted(f for f in os.listdir(DIST)
                  if re.match(r"^OASIS_v.+\.zip$", f))


@pytest.mark.skipif(not _zips(), reason="no release zip in dist/ — run --mode package-release")
class TestReleaseZipContract:
    """The contract every OASIS release ZIP must satisfy to be shippable."""

    @pytest.fixture(scope="class")
    def zip_info(self):
        z = os.path.join(DIST, _zips()[-1])
        with zipfile.ZipFile(z) as zf:
            names = zf.namelist()
        return z, names, os.path.getsize(z)

    def test_installer_essentials_present(self, zip_info):
        _, names, _ = zip_info
        expected = ("install.bat", "VERSION", "entrypoint.py", "requirements.txt")
        for tail in expected:
            assert any(n.endswith("/" + tail) for n in names), \
                f"release ZIP missing {tail}"

    def test_no_secret_or_key_leak(self, zip_info):
        _, names, _ = zip_info
        for n in names:
            base = os.path.basename(n)
            assert not base.endswith(".key"), f"KEY LEAK: {n}"
            assert base not in (".env", ".oasis_install_state.json"), \
                f"SECRET/STATE LEAK: {n}"

    def test_no_data_or_git_leak(self, zip_info):
        _, names, _ = zip_info
        for n in names:
            assert not n.endswith((".db", ".xlsx", ".xls", ".gguf", ".joblib")), \
                f"DATA LEAK: {n}"
            assert ".git/" not in n and ".oasis_venv/" not in n, \
                f"DEV STATE LEAK: {n}"

    def test_release_stays_lean(self, zip_info):
        # Whitelist mode is the default; a shipping release must be tiny.
        _, _, size = zip_info
        cap_mb = 5
        assert size < cap_mb * 1024 * 1024, \
            f"release bloat: {size / 1e6:.1f} MB > {cap_mb} MB cap"

    def test_no_ad_hoc_root_scripts(self, zip_info):
        """The whitelist packager must stop the 191-root-scripts problem —
        ad-hoc find_*/inspect_*/generate_* etc. at repo ROOT (only). Under
        oasis/ these prefixes are legitimate library modules and stay."""
        _, names, _ = zip_info
        forbidden = (
            "find_", "inspect_", "check_", "analyze_", "extract_",
            "compare_", "calculate_", "hayat_", "kapa_", "peek_",
            "cleanup_", "recover_",
        )
        for n in names:
            # zip entries look like "OASIS_v.../<rel>"; keep only root-level rel
            rel = n.split("/", 1)[-1]
            if "/" in rel:
                continue           # nested — under oasis/, tests/, etc.
            for p in forbidden:
                assert not rel.startswith(p), \
                    f"ad-hoc script leaked into release root: {n}"

    def test_no_tests_or_side_projects(self, zip_info):
        _, names, _ = zip_info
        for n in names:
            rel = n.split("/", 1)[-1]
            assert not rel.startswith("tests/"), f"tests/ leaked: {n}"
            for bad in ("scripts/archive/", "sandbox/", "scratch/",
                        "pages/", "pipeline_logs/", "hayat_analysis/",
                        "oasis-portal/", "TouchDesigner_Vibe/", "oasis_hub/",
                        "oasis/Oasis/", "oasis/Runs/", "oasis/Neural_Archive/"):
                assert bad not in n, f"junk tree leaked: {n}"

    def test_no_oversized_files_inside(self, zip_info):
        z_path, _, _ = zip_info
        with zipfile.ZipFile(z_path) as zf:
            oversized = [(i.filename, i.file_size)
                         for i in zf.infolist() if i.file_size > 20 * 1024 * 1024]
        assert not oversized, f"oversized files in release: {oversized}"

    def test_install_bat_sets_seed_password(self, zip_info):
        """Regression: the cold-start proof caught install.bat leaving 9 users
        with random one-time passwords. install.bat must handle SEED_PASSWORD."""
        z_path, names, _ = zip_info
        with zipfile.ZipFile(z_path) as zf:
            bat = next(n for n in names if n.endswith("/install.bat"))
            content = zf.read(bat).decode("utf-8", "replace")
        assert "OASIS_SEED_PASSWORD" in content, \
            "install.bat must handle OASIS_SEED_PASSWORD before preflight"

    def test_desktop_app_is_in_the_built_zip(self, zip_info):
        """The whole Flet app was missing from every release (see below)."""
        _z, names, _ = zip_info
        desktop = [n for n in names if "/oasis/desktop/" in n and n.endswith(".py")]
        assert desktop, "oasis/desktop is absent from the built release zip"
        for required in ("app.py", "data.py", "views/ops_view.py"):
            assert any(n.endswith("/oasis/desktop/" + required) for n in desktop), \
                f"oasis/desktop/{required} missing from the release zip"

    def test_entrypoint_dashboards_exist_in_zip(self, zip_info):
        """Catch E-3: Every script entrypoint.py dispatches to must be in the zip."""
        z_path, names, _ = zip_info
        try:
            entrypoint_file = next(n for n in names if n.endswith("/entrypoint.py"))
        except StopIteration:
            pytest.fail("entrypoint.py not found in zip")
        
        with zipfile.ZipFile(z_path) as zf:
            content = zf.read(entrypoint_file).decode("utf-8")
            
        import ast
        map_match = re.search(r'DASHBOARD_MAP\s*=\s*({[^}]+})', content)
        assert map_match, "Could not find DASHBOARD_MAP in entrypoint.py"
        dash_map = ast.literal_eval(map_match.group(1))
        
        root_dir = entrypoint_file.rsplit("/", 1)[0] + "/" if "/" in entrypoint_file else ""
        
        for script in dash_map.values():
            expected = root_dir + script
            assert expected in names, f"Entrypoint dispatches to {script}, but it is missing from the release zip!"

    def test_every_oasis_package_a_shipped_script_imports_is_in_the_zip(self, zip_info):
        """The package-level twin of E-3, and it was live.

        ops_dashboard.py imports ``from oasis.llm.inference import RuleBasedLLM``
        at MODULE level, but oasis/llm was not on the release whitelist — so the
        Streamlit Command Center, OASIS.bat option 4 and the architecture the
        native app is being built against, died with ModuleNotFoundError before
        rendering anything on every client install. Nothing caught it: the
        dashboard-script guard checks scripts, not the packages they import.
        """
        z_path, names, _ = zip_info
        root = next(n for n in names if n.endswith("/entrypoint.py")).rsplit("/", 1)[0] + "/"
        shipped_roots = [n for n in names
                         if n.startswith(root) and n.endswith(".py")
                         and "/" not in n[len(root):]]
        assert shipped_roots, "no root scripts in the zip"

        shipped_pkgs = {n[len(root) + len("oasis/"):].split("/", 1)[0]
                        for n in names
                        if n.startswith(root + "oasis/") and n.endswith(".py")}

        missing = {}
        with zipfile.ZipFile(z_path) as zf:
            for script in shipped_roots:
                src = zf.read(script).decode("utf-8", errors="replace")
                for pkg in set(re.findall(r"^\s*from oasis\.(\w+)", src, re.M)):
                    if pkg not in shipped_pkgs:
                        missing.setdefault(script.rsplit("/", 1)[-1],
                                           set()).add(pkg)
        assert not missing, (
            f"shipped scripts import oasis packages that are not in the zip: "
            f"{ {k: sorted(v) for k, v in missing.items()} }")

    def test_every_root_module_a_shipped_script_imports_is_in_the_zip(self, zip_info):
        """Sibling-module twin of the oasis-package guard, and it was also live.

        ``retail_simulator`` sat at the repo root, so default-deny excluded it
        from every release — while ops_dashboard imported it for the Simulation
        Lab tab and intraday_sim imported it at MODULE level, which meant that
        shipped script could not start at all.

        Only modules that exist in the working tree count: a name that is not a
        local file is a third-party package and pip's problem, not the zip's.
        """
        import os as _os
        z_path, names, _ = zip_info
        repo = _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__)))
        local_modules = {f[:-3] for f in _os.listdir(repo) if f.endswith(".py")}

        root = next(n for n in names if n.endswith("/entrypoint.py")).rsplit("/", 1)[0] + "/"
        shipped = {n[len(root):] for n in names
                   if n.startswith(root) and "/" not in n[len(root):]}
        shipped_modules = {f[:-3] for f in shipped if f.endswith(".py")}

        missing = {}
        with zipfile.ZipFile(z_path) as zf:
            for script in sorted(f for f in shipped if f.endswith(".py")):
                src = zf.read(root + script).decode("utf-8", errors="replace")
                for mod in set(re.findall(
                        r"^\s*(?:from|import)\s+([a-z_][a-z0-9_]*)", src, re.M)):
                    if mod in local_modules and mod not in shipped_modules:
                        missing.setdefault(script, set()).add(mod)
        assert not missing, (
            f"shipped scripts import root modules that are not in the zip: "
            f"{ {k: sorted(v) for k, v in missing.items()} }")

    def test_no_client_trading_data_ships(self):
        """One retailer's P&L must never land in another's install.

        The allocation scorecard carries per-SKU revenue, margin, GMROI and
        named supplier terms for a real customer — and competitors of theirs are
        named in the product's own scenario templates. It is excluded today only
        by the accident of root files being default-deny; this makes it a rule.
        """
        from oasis.logic.release_packager import should_ship_clean
        for f in ("Full_Product_Allocation_Scorecard_v7.csv",
                  "Full_Product_Allocation_Scorecard_v3.csv",
                  "supplier_scorecards.csv", "active_sku_scorecards.csv"):
            assert not should_ship_clean(f)[0], f

    def test_every_menu_mode_is_a_real_entrypoint_mode(self, zip_info):
        """The menu's twin of E-3: a front-door option that cannot run.

        The P3.3 "Demo / sample data" submenu shipped dispatching to
        --mode demo-single / demo-multi / mock-pos, none of which existed in
        the argparse choices — so all three options died with a usage error on
        a client install. The dashboard guard above did not catch it because
        these are --mode values, not DASHBOARD_MAP scripts.
        """
        z_path, names, _ = zip_info
        entrypoint_file = next(n for n in names if n.endswith("/entrypoint.py"))
        bat_file = next(n for n in names if n.endswith("/OASIS.bat"))

        with zipfile.ZipFile(z_path) as zf:
            entry_src = zf.read(entrypoint_file).decode("utf-8")
            bat_src = zf.read(bat_file).decode("utf-8", errors="replace")

        choices = set(re.findall(r'"([a-z0-9-]+)"',
                                 re.search(r'--mode",\s*\n?\s*choices=\[(.*?)\]',
                                           entry_src, re.S).group(1)))
        assert "desktop" in choices, "failed to parse --mode choices"

        used = set(re.findall(r'--mode\s+([a-z0-9-]+)', bat_src))
        missing = sorted(used - choices)
        assert not missing, (
            f"OASIS.bat dispatches to --mode {missing}, which entrypoint.py "
            f"does not accept — those menu options fail on a client install")


class TestPackagingDecisions:
    """Pure whitelist checks — no built zip needed, so these always run.

    build_release() calls should_ship_CLEAN() in its default clean mode, not
    should_ship(). The two disagree: the blacklist-mode should_ship() answered
    "ok" for oasis/desktop while the strict whitelist silently dropped it,
    because _OASIS_WHITELIST_SUBPKGS is default-deny and nobody added the new
    package. The entire native desktop app — which OASIS.bat option 0 and
    --mode desktop both dispatch into — was therefore missing from every client
    release, and a spot-check against the wrong function said it was fine.

    Assert against the function the builder actually uses.
    """

    def _clean(self):
        from oasis.logic.release_packager import should_ship_clean
        return should_ship_clean

    def test_desktop_package_ships(self):
        sc = self._clean()
        for p in ("oasis/desktop/__init__.py", "oasis/desktop/app.py",
                  "oasis/desktop/data.py", "oasis/desktop/theme.py",
                  "oasis/desktop/views/__init__.py",
                  "oasis/desktop/views/ops_view.py",
                  "oasis/desktop/views/intel_view.py",
                  "oasis/desktop/views/home_view.py",
                  "oasis/desktop/views/settings_view.py"):
            ship, why = sc(p, 5000)
            assert ship, f"{p} would not ship ({why})"

    def test_desktop_pycache_never_ships(self):
        ship, _ = self._clean()("oasis/desktop/__pycache__/app.cpython-310.pyc", 5000)
        assert not ship

    def test_every_shipped_oasis_package_is_importable_from_the_zip(self):
        """A package that ships must ship its __init__.py, or the import fails."""
        sc = self._clean()
        for pkg in ("logic", "ui", "desktop", "desktop/views"):
            ship, why = sc(f"oasis/{pkg}/__init__.py", 100)
            assert ship, f"oasis/{pkg} ships without __init__.py ({why})"

    def test_engine_defaults_still_ship_and_tuned_config_does_not(self):
        """S1 guard, restated against the clean-mode function."""
        sc = self._clean()
        ship_default, _ = sc("oasis/data/oasis_engines_config.default.json", 5000)
        ship_tuned, _ = sc("oasis/data/oasis_engines_config.json", 5000)
        assert ship_default, "shipped engine defaults must ship (S1)"
        assert not ship_tuned, "the per-install tuned config must never ship"
