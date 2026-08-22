"""Audit D1: the trial clock restarts (once) when real data onboards."""

import os
import sys
from datetime import date, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import onboarding as OB
from oasis.logic.license_manager import OfflineLicenseManager

# Every test in this module IS the trial clock, so the autouse
# _trial_is_not_a_clock fixture must stand aside. It patches _first_run, and
# these tests establish their posture by WRITING a first-run date — which the
# patch reads straight past, so _trial_days_left() returned a flat 14 whatever
# they set up. Three of the four failed; the fourth passed for the wrong
# reason, asserting the same 14 the patch hands out unconditionally.
pytestmark = pytest.mark.real_trial_clock


@pytest.fixture
def root(tmp_path, monkeypatch):
    r = tmp_path.as_posix()
    os.makedirs(os.path.join(r, "oasis", "data"), exist_ok=True)
    monkeypatch.setenv("OASIS_DB_PATH", os.path.join(r, "oasis", "data", "s.db"))
    yield r


def _mgr(root):
    return OfflineLicenseManager(state_path=os.path.join(
        root, "oasis", "data", ".oasis_install_state.json"))


def _burn_days(root, days):
    """Backdate the trial stamp as if the install has run for `days` days."""
    _mgr(root)._write_legacy_anchor(date.today() - timedelta(days=days))


def test_empty_store_restarts_a_demo_burned_trial(root):
    _burn_days(root, 9)
    assert _mgr(root)._trial_days_left() == 5           # 14 - 9
    OB.apply_demo(root=root)                            # sample first…
    OB.apply_empty(store_name="Real Store", root=root)  # …then real data
    assert _mgr(root)._trial_days_left() == 14, \
        "trial must restart when real data onboards"
    ob = OB.load_onboarding(root)
    assert ob["trial_restarted"] is True and ob["trial_restarted_at"]


def test_restart_fires_only_once(root):
    OB.apply_empty(store_name="Real", root=root)        # first real onboard → reset
    _burn_days(root, 6)                                 # simulate time passing
    # switching real→real (re-run setup) must NOT hand out another 14 days
    OB.apply_empty(store_name="Real again", root=root)
    assert _mgr(root)._trial_days_left() == 8           # 14 - 6, untouched


def test_connect_path_also_restarts(root):
    _burn_days(root, 9)
    # build a tiny canonical DB to connect to
    OB.apply_demo(root=root)
    url = "sqlite:///" + os.environ["OASIS_DB_PATH"]
    res = OB.apply_connect(url, root=root)
    assert res["ok"]
    assert _mgr(root)._trial_days_left() == 14


def test_demo_only_never_touches_the_clock(root):
    _burn_days(root, 9)
    OB.apply_demo(root=root)
    assert _mgr(root)._trial_days_left() == 5, \
        "sample exploration alone must not reset the trial"
