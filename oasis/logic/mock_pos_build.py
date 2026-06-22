"""
Build a clean mock POS/ERP database from the real Rhapta catalog snapshot.

Reuses the canonical RXL schema (mock_pos_erp.SCHEMA_SQL) and that builder's
system-table seeds (users / tax / counters / config — so the consoles still log
in), but replaces the synthetic product/stock generation with the REAL dept_*.xlsx
catalogue and leaves POS_SALES empty. This is the "stock snapshot from which we
start running POS sales": a single Rhapta store, real SKUs/departments/vendors/
prices/on-hand, ready for the affinity-aware simulator to ring up bills.
"""

from __future__ import annotations

import os
import sqlite3
from datetime import datetime
from typing import List

from .mock_pos_erp import SCHEMA_SQL, MockPosErpBuilder

COST_RATIO = 0.82   # estimated cost as a fraction of sell price (~18% margin)


def _reset_db(db_path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = db_path + suffix
        if os.path.exists(p):
            os.remove(p)


def build_pos_db_from_catalog(rows: List[dict], db_path: str, org_cd: str = "ORG001",
                              org_name: str = "Chandarana Foodplus - Rhapta Road",
                              reset: bool = True) -> dict:
    """Create db_path from catalogue rows. Returns a summary dict."""
    if reset:
        _reset_db(db_path)
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    b = MockPosErpBuilder(db_path=db_path)
    b.conn = sqlite3.connect(db_path)
    conn = b.conn
    conn.execute("PRAGMA journal_mode=WAL")
    b.org_codes = [org_cd]
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        conn.executescript(SCHEMA_SQL)

        # single Rhapta store
        conn.execute(
            "INSERT OR REPLACE INTO ORGANIZATION_MST "
            "(ORG_CD, ORG_NAME, ORG_SHORT_NAME, ORG_ADDRESS, ORG_CITY, ORG_STATE, "
            " ORG_COUNTRY, CURRENCY_CD, LEVEL_NUMBER, ACTIVE_FLAG) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (org_cd, org_name, "RHAPTA", "Rhapta Road, Westlands", "Nairobi",
             "Nairobi", "KE", "KES", 1, "Y"))

        # reuse the canonical system seeds so the consoles still authenticate.
        # Default the demo seed password so logins work out of the box (override
        # with OASIS_SEED_PASSWORD); without it the seeder generates random ones.
        os.environ.setdefault("OASIS_SEED_PASSWORD", "oasis2026")
        b._seed_system_preferences()
        b._seed_tax_plans()
        b._seed_counters()
        b._seed_customers()
        b._seed_oasis_users()
        b._seed_system_config()

        # suppliers from the catalogue's distinct vendors
        vendors = sorted({r["vendor"] for r in rows if r["vendor"]})
        vmap = {v: f"SUP{i:05d}" for i, v in enumerate(vendors, start=1)}
        conn.executemany(
            "INSERT OR IGNORE INTO SUPPLIER_MST "
            "(SUPPLIER_CD, SUPPLIER_NAME, ACTIVE_FLAG) VALUES (?,?,?)",
            [(cd, v, "Y") for v, cd in vmap.items()])

        # items
        conn.executemany(
            "INSERT OR REPLACE INTO ITEM_MST "
            "(ITM_CD, ITM_LONG_NAME, ITM_SHORT_NAME, SCAN_ITM_CD, UOM_CD, UOM_DESC, "
            " DEPARTMENT, SUPPLIER_CD, ITM_TYPE, ACTIVE_FLAG) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(r["itm_cd"], r["name"] or r["itm_cd"], (r["name"] or r["itm_cd"])[:40],
              r["itm_cd"], "EA", "EACH", r["dept"], vmap.get(r["vendor"]), "F", "Y")
             for r in rows])

        # prices (sell + estimated cost) and stock
        sp, cp, sm = [], [], []
        for r in rows:
            price = float(r["price"] or 0)
            cost = round(price * COST_RATIO, 2)
            qty = max(0.0, float(r["stock"] or 0))
            sp.append((org_cd, r["itm_cd"], price, price, today))
            cp.append((org_cd, r["itm_cd"], cost, today))
            sm.append((org_cd, r["itm_cd"], "MAIN", qty, cost, today))
        conn.executemany(
            "INSERT OR REPLACE INTO BASIC_SP_MST "
            "(BSP_ORG_CD, BSP_ITEM_CD, BSP_SP, BSP_MRP, BSP_EFF_DATE) VALUES (?,?,?,?,?)", sp)
        conn.executemany(
            "INSERT OR REPLACE INTO BASIC_CP_MST "
            "(BCP_ORG_CD, BCP_ITEM_CD, BCP_CP, BCP_EFF_DATE) VALUES (?,?,?,?)", cp)
        conn.executemany(
            "INSERT OR REPLACE INTO STOCK_MASTER "
            "(SM_ORG_CD, SM_ITM_CD, SM_LOC_CD, SM_QTY, SM_WAC, SM_LAST_RECV_DT) "
            "VALUES (?,?,?,?,?,?)", sm)

        conn.commit()
        return {
            "db_path": db_path, "org": org_cd, "items": len(rows),
            "suppliers": len(vmap),
            "in_stock": sum(1 for r in rows if (r["stock"] or 0) > 0),
            "departments": len({r["dept"] for r in rows}),
            "sales_bills": 0,
        }
    finally:
        conn.close()
        b.conn = None


def build_from_xlsx(data_dir: str, db_path: str, org_cd: str = "ORG001") -> dict:
    """Load the dept_*.xlsx catalogue and build the clean POS DB."""
    from .rhapta_catalog import load_catalog
    rows = load_catalog(data_dir)
    return build_pos_db_from_catalog(rows, db_path, org_cd=org_cd)
