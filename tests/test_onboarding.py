"""
First-run onboarding: the state machine + the shippable demo/empty store builds.

These prove the fix for "a fresh install silently shows mock (or broken) data":
onboarding is default-deny (not onboarded until a choice is recorded), the demo
store builds from code with NO spreadsheet, and the empty store is a real,
queryable OASIS schema with zero items.
"""

import os
import sys
import sqlite3

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import onboarding as OB
from oasis.logic.demo_seed import demo_catalog_rows, demo_summary


@pytest.fixture
def root(tmp_path):
    # a throwaway project root so onboarding writes under tmp, and point the
    # store DB there too (default_db_path honours OASIS_DB_PATH).
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    old = os.environ.get("OASIS_DB_PATH")
    os.environ["OASIS_DB_PATH"] = str(tmp_path / "oasis" / "data" / "store.db")
    yield str(tmp_path)
    if old is None:
        os.environ.pop("OASIS_DB_PATH", None)
    else:
        os.environ["OASIS_DB_PATH"] = old


# ── state machine ────────────────────────────────────────────────────────
def test_fresh_install_is_not_onboarded(root):
    assert OB.is_onboarded(root) is False
    assert OB.is_demo(root) is False


def test_save_and_reset(root):
    OB.save_onboarding({"source": "empty"}, root)
    assert OB.is_onboarded(root) is True
    OB.reset_onboarding(root)
    assert OB.is_onboarded(root) is False


# ── demo seed (shippable, no xlsx) ───────────────────────────────────────
def test_demo_catalog_is_self_contained_and_deterministic():
    a = demo_catalog_rows()
    b = demo_catalog_rows()
    assert a == b                      # deterministic
    assert len(a) >= 30
    for r in a:
        assert {"itm_cd", "name", "dept", "vendor", "price", "stock"} <= set(r)
        assert r["price"] > 0
    s = demo_summary()
    assert s["departments"] >= 5 and s["suppliers"] >= 5


def test_apply_demo_builds_a_real_store(root):
    summary = OB.apply_demo(root=root)
    assert summary["items"] == len(demo_catalog_rows())
    assert OB.is_onboarded(root) and OB.is_demo(root)
    # the DB is a real OASIS store the consoles can query
    db = OB.default_db_path(root)
    conn = sqlite3.connect(db)
    try:
        n_items = conn.execute("SELECT COUNT(*) FROM ITEM_MST").fetchone()[0]
        n_sp = conn.execute("SELECT COUNT(*) FROM BASIC_SP_MST").fetchone()[0]
        n_stock = conn.execute("SELECT COUNT(*) FROM STOCK_MASTER").fetchone()[0]
    finally:
        conn.close()
    assert n_items == summary["items"] and n_sp == n_items and n_stock == n_items


def test_apply_empty_builds_schema_with_zero_items(root):
    summary = OB.apply_empty(store_name="Acme Duka", root=root)
    assert summary["items"] == 0
    assert OB.is_onboarded(root) and not OB.is_demo(root)
    db = OB.default_db_path(root)
    conn = sqlite3.connect(db)
    try:
        # schema exists (queryable) but empty of catalogue
        n_items = conn.execute("SELECT COUNT(*) FROM ITEM_MST").fetchone()[0]
        # the org row carries the chosen store name
        name = conn.execute("SELECT ORG_NAME FROM ORGANIZATION_MST").fetchone()[0]
    finally:
        conn.close()
    assert n_items == 0
    assert name == "Acme Duka"


# ── connect: verifies before recording ───────────────────────────────────
def test_connect_rejects_a_non_pos_database(root, tmp_path):
    # a sqlite file with no ITEM_MST table is not a POS
    stray = tmp_path / "not_a_pos.db"
    sqlite3.connect(str(stray)).close()
    res = OB.apply_connect(f"sqlite:///{stray.as_posix()}", root=root)
    assert res["ok"] is False
    assert OB.is_onboarded(root) is False       # nothing recorded on failure


def test_connect_accepts_a_real_oasis_store(root, tmp_path):
    # build a demo store, then connect to it as an "external POS"
    OB.apply_demo(root=root)
    db = OB.default_db_path(root)
    OB.reset_onboarding(root)                    # forget the demo choice
    res = OB.apply_connect(f"sqlite:///{db}", root=root)
    assert res["ok"] is True and res["items"] > 0
    rec = OB.load_onboarding(root)
    assert rec["source"] == "connect"
