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
        _, _, size = zip_info
        cap_mb = 50
        assert size < cap_mb * 1024 * 1024, \
            f"release bloat: {size / 1e6:.1f} MB > {cap_mb} MB cap"

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
