"""Wave A of the 2026-07-25 deep analysis: S2, S3, S4.

Each test here locks down a value that was written in one place and read from
another — the shape every Wave A finding took:

  S2  the wizard recorded a POS url nothing ever read, so a connected console
      silently opened a local default file that a connect-only install lacks
  S3  the multi-store DEMO built from client catalogue spreadsheets, which the
      release deliberately never ships, so the card only worked in this tree
  S4  apply_init wrote source="init", a value SOURCES / the badge / the trial
      restart all failed to recognise
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import db as oasis_db
from oasis.logic import onboarding as OB


@pytest.fixture
def root(tmp_path):
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    return str(tmp_path)


# ── S2: the connected POS url must reach the runtime ─────────────────────
def test_sqlite_url_resolves_to_its_file():
    p = oasis_db.sqlite_path_from_url("sqlite:///C:/pos/store.db")
    assert p and p.replace("\\", "/").endswith("pos/store.db")


def test_non_sqlite_url_has_no_path():
    """A Postgres POS cannot be expressed as a path — say None, don't guess."""
    assert oasis_db.sqlite_path_from_url("postgresql://u:p@host/db") is None
    assert oasis_db.sqlite_path_from_url(None) is None


def test_pos_url_prefers_env_then_wizard(monkeypatch):
    monkeypatch.setattr(OB, "load_onboarding",
                        lambda root=None: {"source": "connect",
                                           "db_url": "postgresql://w/wizard"})
    monkeypatch.setenv("OASIS_POS_DB_URL", "postgresql://e/env")
    assert oasis_db.get_pos_db_url() == "postgresql://e/env"
    monkeypatch.delenv("OASIS_POS_DB_URL")
    # the tier that did not exist before S2
    assert oasis_db.get_pos_db_url() == "postgresql://w/wizard"


def test_pos_url_ignores_non_connect_sources(monkeypatch):
    monkeypatch.delenv("OASIS_POS_DB_URL", raising=False)
    monkeypatch.setattr(OB, "load_onboarding",
                        lambda root=None: {"source": "demo", "db_path": "d.db"})
    assert not oasis_db.has_distinct_pos()
    assert oasis_db.get_pos_db_url() == oasis_db.get_db_url()


def test_has_distinct_pos_true_after_connecting(monkeypatch):
    monkeypatch.delenv("OASIS_POS_DB_URL", raising=False)
    monkeypatch.setattr(OB, "load_onboarding",
                        lambda root=None: {"source": "connect",
                                           "db_url": "sqlite:///x.db"})
    assert oasis_db.has_distinct_pos(), \
        "a wizard connection must split the POS from the store, not just the env var"


def test_resolved_db_path_follows_a_connected_sqlite_pos(root, monkeypatch):
    monkeypatch.delenv("OASIS_DB_PATH", raising=False)
    pos = os.path.join(root, "oasis", "data", "their_pos.db")
    open(pos, "w").close()
    OB.save_onboarding({"source": "connect", "db_url": f"sqlite:///{pos}"}, root)
    assert OB.resolved_db_path(root) == os.path.abspath(pos), \
        "the console must open the POS it connected to, not the default file"
    assert OB.connected_pos_url(root) == f"sqlite:///{pos}"


# ── S3: catalogue gating + a code-resident multi-store demo ──────────────
def test_catalog_absent_is_reported_not_attempted(root):
    cat = OB.catalog_available(root)
    assert cat["ok"] is False and cat["files"] == 0


def test_catalog_detected_when_present(root):
    open(os.path.join(root, "oasis", "data", "dept_01.xlsx"), "w").close()
    assert OB.catalog_available(root)["ok"] is True


def test_multi_demo_builds_with_no_spreadsheets(root, monkeypatch):
    """The regression: this card used to need dept_*.xlsx and so always failed
    on a clean zip. It must now build from oasis.logic.demo_seed alone."""
    import sqlite3

    def _boom(*a, **k):
        raise AssertionError("multi demo must not read catalogue spreadsheets")

    monkeypatch.setattr("oasis.logic.rhapta_catalog.load_catalog", _boom)
    monkeypatch.setattr("oasis.logic.multi_store_pos.seed_multi_store_history",
                        lambda db, **kw: {})          # history is timed separately
    monkeypatch.setenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
    monkeypatch.setenv("OASIS_DB_PATH", os.path.join(root, "oasis", "data", "m.db"))

    assert OB.catalog_available(root)["ok"] is False, "precondition: a clean install"
    s = OB.apply_multi_demo(root=root)

    assert s["stores"] == 5 and "catalog_error" not in s
    conn = sqlite3.connect(s["db_path"])
    try:
        orgs = conn.execute("SELECT COUNT(*) FROM ORGANIZATION_MST").fetchone()[0]
        items = conn.execute("SELECT COUNT(*) FROM ITEM_MST").fetchone()[0]
        spread = conn.execute("SELECT SM_ORG_CD, COUNT(*) FROM STOCK_MASTER "
                              "GROUP BY SM_ORG_CD").fetchall()
    finally:
        conn.close()
    assert orgs == 5 and items > 0
    counts = [n for _org, n in spread]
    assert len(counts) == 5
    assert min(counts) < max(counts), \
        "stores must differ in assortment or transfers/allocation have nothing to show"


def test_multi_demo_is_badged_as_sample(root, monkeypatch):
    monkeypatch.setattr("oasis.logic.multi_store_pos.seed_multi_store_history",
                        lambda db, **kw: {})
    monkeypatch.setenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
    monkeypatch.setenv("OASIS_DB_PATH", os.path.join(root, "oasis", "data", "m.db"))
    OB.apply_multi_demo(root=root)
    ob = OB.load_onboarding(root)
    assert ob["source"] == "demo" and ob["multi"] is True
    assert OB.is_demo(root), "the demo network must carry the SAMPLE banner"


def test_demo_history_profiles_are_shallower_than_real():
    from oasis.logic.multi_store_profiles import STORE_PROFILES
    demo = OB._demo_history_profiles()
    assert all(p.history_days == OB.DEMO_HISTORY_DAYS for p in demo)
    real_bills = sum(p.history_days * p.history_bills_per_day for p in STORE_PROFILES)
    demo_bills = sum(p.history_days * p.history_bills_per_day for p in demo)
    assert demo_bills < real_bills / 4, "a first-run click must not seed 62k bills"


def test_staples_department_maps_to_its_group():
    """'STAPLES' matched no keyword and silently fell through to OTHER."""
    from oasis.logic.multi_store_profiles import classify_department
    assert classify_department("Staples") == "STAPLE"
    assert classify_department("Beverages") == "BEVERAGES"


# ── S4: "init" is a first-class source ───────────────────────────────────
def test_init_is_a_known_source():
    assert "init" in OB.SOURCES and "init" in OB.REAL_SOURCES


def _fake_init(summary):
    def _f(profile="single", tenant="", root=None):
        return {"profile": profile, **summary}
    return _f


def test_apply_init_restarts_the_trial(root, monkeypatch):
    """The one path that is unambiguously the operator's own data was the only
    one not getting the fresh 14-day clock."""
    monkeypatch.setattr("oasis.logic.install_profile.init_install",
                        _fake_init({"db_path": "cat.db", "catalog": "900 SKUs",
                                    "tenant": "Acme Duka"}))
    called = {}
    monkeypatch.setattr("oasis.logic.license_manager.restart_trial",
                        lambda r=None: called.setdefault("hit", __import__("datetime").date.today()))
    OB.apply_init("single", root=root)
    assert "hit" in called, "apply_init must restart the trial like empty/connect do"
    ob = OB.load_onboarding(root)
    assert ob["source"] == "init" and ob["trial_restarted"] is True
    assert ob["store_name"] == "Acme Duka" and ob["profile"] == "single"


def test_apply_init_records_nothing_when_the_catalogue_fails(root, monkeypatch):
    monkeypatch.setattr("oasis.logic.install_profile.init_install",
                        _fake_init({"catalog_error": "no dept_*.xlsx"}))
    OB.apply_init("single", root=root)
    assert OB.is_onboarded(root) is False, \
        "a failed build must not leave the install looking onboarded"


class _BadgeSt:
    def __init__(self):
        self.md = []

    def markdown(self, text, **kw):
        self.md.append(text)


def test_badge_names_a_catalogue_store(root, monkeypatch):
    from oasis.ui import onboarding as UI
    monkeypatch.setattr(OB, "load_onboarding",
                        lambda r=None: {"source": "init", "store_name": "Acme Duka",
                                        "profile": "single"})
    st = _BadgeSt()
    UI.data_source_badge(st)
    blob = " ".join(st.md)
    assert "Acme Duka" in blob and "catalogue" in blob
    assert "not onboarded" not in blob, \
        "a real catalogue store was labelled un-onboarded in every console header"
