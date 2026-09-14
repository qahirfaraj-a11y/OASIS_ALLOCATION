"""A minimum-order failure is a fact about ONE data source.

moq_failures.json was keyed only {org_cd: {itm_cd}}. Org codes repeat across
data sources -- ORG001 in the single-store POS is Rhapta Road, ORG001 in the
multi-store network is a different store entirely -- so failures written by an
ordering run against one database became transfer pull triggers when the scan
ran against another.

Measured on the full-catalogue 5-store network, through the shipped
network_transfer_scan: 4,142 single-store failures, every one an item in the
network and 4,077 carried at its ORG001. They produced 26 transfers that exist
only because of the foreign file -- including milk pulled into a store already
holding 185.8 days of it, from a donor holding 22.7.

What stays pinned:
  * a failure recorded on one source is not a trigger on another
  * recording for a store on one source does not wipe it on another
  * entries with no source are not triggers once the source is known
  * the source follows the active POS without every caller passing it
  * no credential ever reaches the file, and rotating a password does not
    orphan the store's failures
  * one SQLite file spelled two ways is one source
  * the consequence: a foreign failure no longer moves stock
"""
import json
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
from oasis.logic.moq_failure_store import (
    current_source,
    load_moq_failures,
    record_moq_failures,
    source_fingerprint,
)

SINGLE = source_fingerprint("sqlite:///C:/stores/single_store.db")
NETWORK = source_fingerprint("sqlite:///C:/stores/network.db")


def _items(*codes, qty=5):
    return [{"item_code": c, "recommended_quantity": qty} for c in codes]


def test_a_failure_on_one_source_is_not_a_trigger_on_another(tmp_path):
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG001", _items("MILK"), source=SINGLE)
    assert load_moq_failures(path, source=NETWORK) == {}, (
        "a failure recorded against one database became a pull trigger on another")
    assert load_moq_failures(path, source=SINGLE) == {"ORG001": {"MILK": 5.0}}


def test_recording_on_one_source_does_not_wipe_the_other(tmp_path):
    """Per-store replace has to be per store PER SOURCE, or running Smart
    Ordering on the network would erase the single store's ORG001 history."""
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG001", _items("SKU1"), source=SINGLE)
    record_moq_failures(path, "ORG001", _items("SKU2"), source=NETWORK)
    assert load_moq_failures(path, source=SINGLE) == {"ORG001": {"SKU1": 5.0}}
    assert load_moq_failures(path, source=NETWORK) == {"ORG001": {"SKU2": 5.0}}


def test_the_same_item_on_two_sources_is_kept_twice(tmp_path):
    """De-duplication must include the source, or one source's quantity
    silently replaces the other's."""
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG001", _items("SKU1", qty=5), source=SINGLE)
    record_moq_failures(path, "ORG001", _items("SKU1", qty=9), source=NETWORK)
    assert load_moq_failures(path, source=SINGLE) == {"ORG001": {"SKU1": 5.0}}
    assert load_moq_failures(path, source=NETWORK) == {"ORG001": {"SKU1": 9.0}}


def test_entries_without_a_source_are_not_triggers(tmp_path):
    """The shape every failure had before this fix. The module already drops
    un-timestamped legacy entries on load; unsourced ones follow the same rule
    rather than being trusted on whatever database happens to be active."""
    path = str(tmp_path / "moq.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump([{"org_cd": "ORG001", "itm_cd": "MILK", "qty": 1,
                    "ts": datetime.now().isoformat()}], f)
    assert load_moq_failures(path, source=NETWORK) == {}


def test_the_source_follows_the_active_pos(tmp_path, monkeypatch):
    """Seven call sites write or read this file. None should have to remember
    to pass a source for the scoping to hold."""
    monkeypatch.delenv("OASIS_ERP", raising=False)
    path = str(tmp_path / "moq.json")
    single = "sqlite:///" + str(tmp_path / "single.db")
    network = "sqlite:///" + str(tmp_path / "network.db")

    monkeypatch.setenv("OASIS_POS_DB_URL", single)
    record_moq_failures(path, "ORG001", _items("MILK"))

    monkeypatch.setenv("OASIS_POS_DB_URL", network)
    assert load_moq_failures(path) == {}

    monkeypatch.setenv("OASIS_POS_DB_URL", single)
    assert load_moq_failures(path) == {"ORG001": {"MILK": 5.0}}


def test_no_credential_reaches_the_file(tmp_path, monkeypatch):
    """A live POS URL carries a login. The stored identity is derived from the
    URL with credentials removed BEFORE hashing -- a hash of a password is
    still a derivative of it."""
    monkeypatch.delenv("OASIS_ERP", raising=False)
    monkeypatch.setenv(
        "OASIS_POS_DB_URL",
        "mssql+pyodbc://oasis_ro:S3cretPass@db-host/RXL?driver=ODBC+Driver+17+for+SQL+Server")
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG001", _items("SKU1"))
    with open(path, encoding="utf-8") as f:
        raw = f.read()
    assert "S3cretPass" not in raw
    assert "oasis_ro" not in raw
    assert load_moq_failures(path) == {"ORG001": {"SKU1": 5.0}}


def test_rotating_a_password_does_not_orphan_failures():
    old = "mssql+pyodbc://oasis_ro:OldPass@db-host/RXL?driver=ODBC+Driver+17"
    new = "mssql+pyodbc://oasis_ro:NewPass@db-host/RXL?driver=ODBC+Driver+17"
    elsewhere = "mssql+pyodbc://oasis_ro:OldPass@other-host/RXL?driver=ODBC+Driver+17"
    assert source_fingerprint(old) == source_fingerprint(new)
    assert source_fingerprint(old) != source_fingerprint(elsewhere)


def test_one_sqlite_file_spelled_two_ways_is_one_source(tmp_path):
    p = str(tmp_path / "store.db")
    forward = "sqlite:///" + p.replace("\\", "/")
    native = "sqlite:///" + p
    assert source_fingerprint(forward) == source_fingerprint(native)
    if os.name == "nt":
        assert source_fingerprint(forward) == source_fingerprint(forward.upper().replace("SQLITE:", "sqlite:"))


def test_odoo_is_a_source_of_its_own(monkeypatch):
    monkeypatch.setenv("OASIS_ERP", "odoo")
    monkeypatch.setenv("ODOO_URL", "http://localhost:8069")
    monkeypatch.setenv("ODOO_DB", "oasis")
    odoo = current_source()
    assert odoo
    assert odoo not in (SINGLE, NETWORK)


# ── the consequence, through the scan ────────────────────────────────────────
def _product(itm, name, stock, ads):
    return {"item_code": itm, "product_name": name, "current_stocks": stock,
            "avg_daily_sales": ads, "department": "GENERAL", "selling_price": 100.0,
            "reorder_point": 0.0, "is_fresh": False, "supplier_name": "ACME", "uom": "EA"}


def _scan(moq):
    # the shape test_network_scan uses to show an MOQ failure acting as the
    # only trigger: the recipient sits at 8 days, neither a deficit nor below ROP
    stock = {
        "ORG001": [_product("SKU1", "RICE 1KG", stock=200, ads=5.0)],
        "ORG002": [_product("SKU1", "RICE 1KG", stock=32, ads=4.0)],
        "ORG003": [],
    }
    svc = ConsolidatedTransferService(
        org_names={"ORG001": "A", "ORG002": "B", "ORG003": "C"},
        stock_data=stock, cold_node_days=60, hot_node_days=14)
    scan = svc.scan_network_opportunities(moq_failures=moq)
    return [o for o in scan.opportunities if o.to_org == "ORG002"]


def test_a_foreign_failure_no_longer_moves_stock(tmp_path):
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG002", _items("SKU1", qty=24), source=SINGLE)

    # guard against a vacuous pass: on its own source the failure still pulls
    assert _scan(load_moq_failures(path, source=SINGLE)), (
        "the MOQ trigger produced nothing -- this test proves nothing")
    assert _scan(load_moq_failures(path, source=NETWORK)) == [], (
        "a failure recorded on another database moved stock on this one")
