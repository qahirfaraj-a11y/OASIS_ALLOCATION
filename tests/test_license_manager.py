"""Tests for offline licensing (issue, verify, expiry, trial)."""

import json
import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.license_manager import (
    BUNDLES,
    KNOWN_MODULES,
    OfflineLicenseManager,
    allowed_modules,
)


def _mgr(tmp_path, salt="test-salt", monkeypatch=None):
    if monkeypatch:
        monkeypatch.setenv("OASIS_LICENSE_SALT", salt)
    return OfflineLicenseManager(key_path=str(tmp_path / "key.json"),
                                 state_path=str(tmp_path / "state.json"))


class TestIssueAndVerify:
    def test_issue_then_licensed(self, tmp_path, monkeypatch):
        m = _mgr(tmp_path, monkeypatch=monkeypatch)
        exp = (date.today() + timedelta(days=365)).isoformat()
        m.issue("ACME", ["ops", "intel"], exp)
        assert m.status("ops")["mode"] == "licensed"
        assert m.status("intel")["days_left"] >= 364
        # module not in the key -> locked
        assert m.status("command")["mode"] == "locked"
        assert m.verify_license("ops") is True

    def test_expired_license_locks(self, tmp_path, monkeypatch):
        m = _mgr(tmp_path, monkeypatch=monkeypatch)
        exp = (date.today() - timedelta(days=1)).isoformat()
        m.issue("ACME", ["ops"], exp)
        s = m.status("ops")
        assert s["mode"] == "locked" and "expired" in s["reason"]

    def test_tampered_signature_locks(self, tmp_path, monkeypatch):
        m = _mgr(tmp_path, monkeypatch=monkeypatch)
        exp = (date.today() + timedelta(days=30)).isoformat()
        m.issue("ACME", ["ops"], exp)
        key = json.loads((tmp_path / "key.json").read_text())
        key["expiry_date"] = (date.today() + timedelta(days=3650)).isoformat()  # extend on-site
        (tmp_path / "key.json").write_text(json.dumps(key))
        s = m.status("ops")
        assert s["mode"] == "locked" and "signature" in s["reason"]

    def test_issue_requires_salt(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OASIS_LICENSE_SALT", "")
        m = OfflineLicenseManager(key_path=str(tmp_path / "k.json"),
                                  state_path=str(tmp_path / "s.json"))
        try:
            m.issue("ACME", ["ops"], "2030-01-01")
            assert False, "should have raised"
        except RuntimeError:
            pass


class TestModuleSkus:
    def test_bundles_are_valid_and_include_core(self):
        for name, mods in BUNDLES.items():
            assert "core" in mods, f"bundle '{name}' must include core"
            assert set(mods) <= set(KNOWN_MODULES)
        assert set(BUNDLES["enterprise"]) == set(KNOWN_MODULES)

    def test_evaluation_unlocks_everything(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OASIS_TRIAL_DAYS", "14")
        m = _mgr(tmp_path, monkeypatch=monkeypatch)
        assert allowed_modules(m) == set(KNOWN_MODULES)

    def test_licensed_gets_exactly_its_modules(self, tmp_path, monkeypatch):
        m = _mgr(tmp_path, monkeypatch=monkeypatch)
        exp = (date.today() + timedelta(days=365)).isoformat()
        m.issue("ACME", ["core", "ordering"], exp)
        assert allowed_modules(m) == {"core", "ordering"}

    def test_core_is_mandatory(self, tmp_path, monkeypatch):
        # a key without core grants nothing (consoles won't even start)
        m = _mgr(tmp_path, monkeypatch=monkeypatch)
        exp = (date.today() + timedelta(days=365)).isoformat()
        m.issue("ACME", ["revenue", "api"], exp)
        assert allowed_modules(m) == set()

    def test_page_module_tagging(self):
        from oasis.ui.intel import build_intel_registry
        from oasis.ui.shell import build_registry
        shell_mods = {p.key: p.module for p in build_registry()}
        assert shell_mods["home"] == "core"
        assert shell_mods["ordering"] == "ordering"
        assert shell_mods["suppliers"] == "ordering"
        assert shell_mods["transfers"] == "network"
        assert shell_mods["allocation"] == "network"
        intel_mods = {p.key: p.module for p in build_intel_registry()}
        assert intel_mods["baskets"] == "revenue"
        assert intel_mods["pulse"] == "core"


class TestTrial:
    def test_no_key_within_trial_is_evaluation(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OASIS_TRIAL_DAYS", "14")
        m = _mgr(tmp_path, monkeypatch=monkeypatch)
        s = m.status("ops")
        assert s["mode"] == "evaluation"
        assert s["trial_days_left"] == 14

    def test_trial_expired_locks(self, tmp_path, monkeypatch):
        monkeypatch.setenv("OASIS_TRIAL_DAYS", "14")
        m = _mgr(tmp_path, monkeypatch=monkeypatch)
        old = (date.today() - timedelta(days=20)).isoformat()
        (tmp_path / "state.json").write_text(json.dumps({"first_run": old}))
        s = m.status("ops")
        assert s["mode"] == "locked" and "evaluation period ended" in s["reason"]
