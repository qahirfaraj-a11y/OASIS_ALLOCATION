"""A store that has never carried an item must not be sent it.

Zero stock means two different things: the store sold out, or the store never
ranged the item. The transfer scan's zero-demand trigger --
`ads == 0 and eff_stock < 1.0`, with a fixed 2-unit target -- could not tell
them apart, because the adapter's LEFT JOIN turned a missing stock row into 0.

Measured on the 5-store network before the fix: 21 of 54 transfers (39%) went
to stores with no stock row and no sales ever; 13 of them could be queued
without a human; every one was valued at KES 0 because the receiving store has
no price for an item it does not carry. On the real catalogue a single store
has 15,405 items that met the ungated rule.

What stays pinned:
  * a store that carries the item and sold out is still sent stock
  * a store that never ranged it is not
  * where ranging is unknown, the previous behaviour holds
  * the gate is narrow -- it does not touch lines that actually sell
  * the adapter reports the difference the join used to erase
"""
import os
import sqlite3
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService

ORG_NAMES = {"ORG001": "Donor", "ORG002": "Sold out", "ORG003": "Never ranged"}


def _product(itm, stock, ads, is_ranged=None, dept="GENERAL", price=100.0):
    p = {
        "item_code": itm,
        "product_name": f"ITEM {itm}",
        "current_stocks": stock,
        "avg_daily_sales": ads,
        "department": dept,
        "selling_price": price,
        "reorder_point": 0.0,
        "is_fresh": False,
        "supplier_name": "ACME",
        "uom": "EA",
    }
    if is_ranged is not None:
        p["is_ranged"] = is_ranged
    return p


def _scan(stock_data):
    svc = ConsolidatedTransferService(
        org_names=ORG_NAMES, stock_data=stock_data,
        cold_node_days=60, hot_node_days=14)
    return svc.scan_network_opportunities()


def _pulls_to(scan, org):
    return [o for o in scan.opportunities if o.type == "PULL" and o.to_org == org]


# a donor with plenty to spare: 600 units at 5/day is 120 days of cover
_DONOR = _product("SKU1", stock=600, ads=5.0, is_ranged=True)


def test_a_store_that_carries_it_and_sold_out_is_still_sent_stock():
    scan = _scan({
        "ORG001": [_DONOR],
        "ORG002": [_product("SKU1", stock=0, ads=0.0, is_ranged=True)],
    })
    assert _pulls_to(scan, "ORG002"), (
        "a genuine stock-out at a store that ranges the item must still be "
        "plugged -- the gate is for items the store does not carry")


def test_a_store_that_never_ranged_it_is_not_sent_it():
    """Both recipients look identical on stock and demand. Only ranging
    differs, so this is the gate and nothing else."""
    scan = _scan({
        "ORG001": [_DONOR],
        "ORG002": [_product("SKU1", stock=0, ads=0.0, is_ranged=True)],
        "ORG003": [_product("SKU1", stock=0, ads=0.0, is_ranged=False)],
    })
    # guards against a vacuous pass: the scan must still be issuing pulls
    assert _pulls_to(scan, "ORG002"), "no pulls at all -- this proves nothing"
    assert not _pulls_to(scan, "ORG003"), (
        "stock was sent to a store that has never carried the item")


def test_unknown_ranging_keeps_the_previous_behaviour():
    """Callers that cannot tell -- fixtures, devkit books, an adapter not yet
    taught -- must not have zero-demand pulls switched off underneath them."""
    scan = _scan({
        "ORG001": [_DONOR],
        "ORG002": [_product("SKU1", stock=0, ads=0.0)],   # no is_ranged key
    })
    assert _pulls_to(scan, "ORG002")


def test_the_gate_does_not_touch_a_line_that_sells():
    """A line with measurable demand is short on evidence, whatever a stock
    row says. The gate lives on the zero-demand branch only."""
    scan = _scan({
        "ORG001": [_DONOR],
        "ORG002": [_product("SKU1", stock=2, ads=4.0, is_ranged=False)],
    })
    assert _pulls_to(scan, "ORG002"), (
        "the carried-only gate reached a line that is actually selling")


def test_the_adapter_reports_what_the_join_used_to_erase(tmp_path):
    """Two items, both at zero stock. One has a stock row (sold out), one does
    not (never carried). Before the fix the adapter returned them identically."""
    from oasis.logic.db_connector import SchemaMapper, UniversalConnector
    from oasis.logic.mock_pos_erp import SCHEMA_SQL
    from oasis.logic.pos_erp_adapter import PosErpAdapter

    db = str(tmp_path / "ranging.db")
    c = sqlite3.connect(db)
    c.executescript(SCHEMA_SQL)
    c.executemany(
        "INSERT INTO ITEM_MST (ITM_CD, ITM_LONG_NAME, ITM_SHORT_NAME, SCAN_ITM_CD,"
        " UOM_CD, UOM_DESC, DEPARTMENT, SUPPLIER_CD, ITM_TYPE, ACTIVE_FLAG)"
        " VALUES (?,?,?,?,?,?,?,?,?,?)",
        [("SOLD", "SOLD OUT ITEM", "SOLD", "SOLD", "EA", "EACH", "GROCERY", None, "F", "Y"),
         ("NEVER", "NEVER RANGED", "NEVER", "NEVER", "EA", "EACH", "GROCERY", None, "F", "Y")])
    c.execute(
        "INSERT INTO STOCK_MASTER (SM_ORG_CD, SM_ITM_CD, SM_LOC_CD, SM_QTY, SM_WAC,"
        " SM_LAST_RECV_DT) VALUES ('ORG001','SOLD','MAIN',0,10,'2025-11-01')")
    c.commit()
    c.close()

    adapter = PosErpAdapter(UniversalConnector(f"sqlite:///{db}",
                                               SchemaMapper.for_pos_erp()))
    rows = {p["item_code"]: p for p in adapter.fetch_enriched_products("ORG001")}

    assert rows["SOLD"]["current_stocks"] == 0.0
    assert rows["NEVER"]["current_stocks"] == 0.0     # identical on stock
    assert rows["SOLD"]["is_ranged"] is True
    assert rows["NEVER"]["is_ranged"] is False
