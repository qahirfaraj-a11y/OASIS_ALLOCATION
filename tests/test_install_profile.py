"""Tests for the install profile (single-store vs multi-store)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.install_profile import (
    detected_db_path, is_multi_store, load_profile, save_profile,
)


class TestProfileRoundtrip:
    def test_empty_when_missing(self, tmp_path):
        # temp root has no profile file
        assert load_profile(str(tmp_path)) == {}
        assert not is_multi_store(str(tmp_path))
        assert detected_db_path(str(tmp_path)) is None

    def test_save_then_load(self, tmp_path):
        p = save_profile({"profile": "single", "db_path": "/tmp/x.db"}, root=str(tmp_path))
        assert p.endswith(".oasis_install_profile.json")
        loaded = load_profile(str(tmp_path))
        assert loaded["profile"] == "single"
        assert loaded["db_path"] == "/tmp/x.db"
        assert "written_at" in loaded

    def test_multi_detection(self, tmp_path):
        save_profile({"profile": "multi", "db_path": "/x/net.db"}, root=str(tmp_path))
        assert is_multi_store(str(tmp_path))
        assert detected_db_path(str(tmp_path)) == "/x/net.db"


class TestInitInputs:
    def test_bad_profile_rejected(self, tmp_path):
        from oasis.logic.install_profile import init_install
        import pytest
        with pytest.raises(SystemExit):
            init_install(profile="fleet", root=str(tmp_path))
