"""
Build a clean mock POS/ERP database from a catalogue snapshot.

Reuses the canonical RXL schema (mock_pos_erp.SCHEMA_SQL) and that builder's
system-table seeds (users / tax / counters / config — so the consoles still log
in), but replaces the synthetic product/stock generation with the REAL dept_*.xlsx
catalogue and leaves POS_SALES empty. This is the "stock snapshot from which we
start running POS sales": a single sample store, with SKUs/departments/vendors/
prices/on-hand, ready for the affinity-aware simulator to ring up bills.
"""

from __future__ import annotations

import os
import sqlite3

from .demo_identity import DEMO_BRANCHES, DEMO_CITY, single_store_name
from datetime import datetime
from typing import List, Optional

from .mock_pos_erp import SCHEMA_SQL, MockPosErpBuilder

COST_RATIO = 0.82   # estimated cost as a fraction of sell price (~18% margin)


def _reset_db(db_path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = db_path + suffix
        if os.path.exists(p):
            os.remove(p)


def build_pos_db_from_catalog(rows: List[dict], db_path: str, org_cd: str = "ORG001",
                              org_name: str = "",
                              reset: bool = True,
                              seed_password: Optional[str] = None,
                              recv_dates: Optional[dict] = None) -> dict:
    """Create db_path from catalogue rows. Returns a summary dict.

    ``seed_password`` sets the seeded accounts' password. Pass one only for
    SAMPLE data, where frictionless sign-in is the point. Leave it None for a
    real store: the seeder then generates a random one-time password and the
    first-run setup asks the operator to choose the real one. This used to
    ``setdefault("OASIS_SEED_PASSWORD", "oasis2026")`` unconditionally, so every
    real catalogue-built store shipped with a publicly-known credential.
    """
    org_name = org_name or single_store_name()
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

        # single sample store
        conn.execute(
            "INSERT OR REPLACE INTO ORGANIZATION_MST "
            "(ORG_CD, ORG_NAME, ORG_SHORT_NAME, ORG_ADDRESS, ORG_CITY, ORG_STATE, "
            " ORG_COUNTRY, CURRENCY_CD, LEVEL_NUMBER, ACTIVE_FLAG) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            (org_cd, org_name, DEMO_BRANCHES[0][1], DEMO_BRANCHES[0][2], DEMO_CITY,
             "Nairobi", "KE", "KES", 1, "Y"))

        # reuse the canonical system seeds so the consoles still authenticate.
        # A caller-supplied password applies to SAMPLE builds only; otherwise the
        # seeder generates a random one-time password (see _resolve_seed_password).
        if seed_password:
            os.environ.setdefault("OASIS_SEED_PASSWORD", seed_password)
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
        #
        # ITM_CD AND SCAN_ITM_CD ARE TWO IDENTITIES, NOT ONE.
        # This wrote the catalogue's barcode into BOTH columns, so every one of
        # the 39,728 rows had ITM_CD == SCAN_ITM_CD. That collapse is not
        # cosmetic: the ordering methodology matches item code, THEN barcode,
        # then name, and with one value in both slots the first two branches
        # are the same test run twice. It is also why nothing joined to the
        # cashier exports, whose "Itm Code" is the till's article number and
        # shares no digits with a barcode.
        #
        # ITM_CD now carries the till code where the monthly cash files know
        # one (77.6% of names), falling back to the barcode otherwise -- a
        # mixed space, deliberately, because an item absent from ten months of
        # sales still has to exist in the catalogue. SCAN_ITM_CD always carries
        # the barcode, so the scan identity is never lost either way.
        conn.executemany(
            "INSERT OR REPLACE INTO ITEM_MST "
            "(ITM_CD, ITM_LONG_NAME, ITM_SHORT_NAME, SCAN_ITM_CD, UOM_CD, UOM_DESC, "
            " DEPARTMENT, SUPPLIER_CD, ITM_TYPE, ACTIVE_FLAG) VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(r.get("till_cd") or r["itm_cd"], r["name"] or r["itm_cd"],
              (r["name"] or r["itm_cd"])[:40],
              r["itm_cd"], "EA", "EACH", r["dept"], vmap.get(r["vendor"]), "F", "Y")
             for r in rows])

        # prices (sell + estimated cost) and stock
        sp, cp, sm = [], [], []
        for r in rows:
            price = float(r["price"] or 0)
            cost = round(price * COST_RATIO, 2)
            qty = max(0.0, float(r["stock"] or 0))
            # THE SAME KEY ITEM_MST WAS GIVEN, or these tables join to nothing.
            # Prices and stock are looked up by ITM_CD, so the moment ITM_CD
            # became the till code they had to follow it.
            key = r.get("till_cd") or r["itm_cd"]
            # Per-SKU last receipt where the GRN history knows one. Writing the
            # build date on every row -- which is what this did -- gives all
            # 39,728 SKUs one identical value, and days_since_delivery is then
            # a constant. The stale-fresh and dead-stock gates read that field
            # and so could never fire, on any SKU, ever.
            recv = (recv_dates or {}).get(_norm_key(r.get("name"))) or today
            sp.append((org_cd, key, price, price, today))
            cp.append((org_cd, key, cost, today))
            sm.append((org_cd, key, "MAIN", qty, cost, recv))
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
    """Load the dept_*.xlsx catalogue and build the clean POS DB.

    Two identities and a real receipt date are attached here rather than in
    the writer, so the writer stays a writer:

      till_cd     the cashier's article number from the monthly *_cash.xlsx
                  exports, bridged to the catalogue by normalised name. It
                  becomes ITM_CD; the barcode stays in SCAN_ITM_CD. Where the
                  cash files know no code the barcode does both jobs.
      recv_dates  each SKU's own last GRN receipt, so days_since_delivery
                  varies per line instead of being one constant.

    Both are best-effort: a checkout without the cash exports or the receipt
    history still builds a working store, just with the previous flat values.
    """
    from .catalog_snapshot import load_catalog
    rows = load_catalog(data_dir)

    # Till identity, by normalised name (the only field the two sides share).
    matched = 0
    try:
        from .real_demand import load_cash_codes, normalise_name
        cash_dir = os.getenv("OASIS_CASH_DIR", data_dir)
        codes = load_cash_codes(cash_dir)
        if not codes and cash_dir != data_dir:
            codes = load_cash_codes(data_dir)

        # A TILL CODE CLAIMED BY TWO CATALOGUE ROWS IS NOT AN IDENTITY.
        # ITM_CD is the primary key, and the insert is INSERT OR REPLACE, so a
        # code assigned to two rows silently deletes one of them: 114 such
        # codes cost 645 items on the first run of this. Where a code is not
        # unique the barcode keeps the slot -- the same rule the forecast
        # re-key uses, for the same reason.
        want = {}
        for r in rows:
            c = codes.get(normalise_name(r.get("name") or ""))
            if c:
                want.setdefault(c, []).append(r)
        for c, claimants in want.items():
            if len(claimants) == 1:
                claimants[0]["till_cd"] = c
                matched += 1
    except Exception:
        matched = 0

    # Per-SKU last receipt from the GRN history, keyed the way the ledger keys
    # it -- by product NAME, not by code. Looking it up by barcode returned
    # nothing at all, which read as "no history" rather than as a bad join.
    recv = _receipt_dates(data_dir)

    summary = build_pos_db_from_catalog(rows, db_path, org_cd=org_cd,
                                        recv_dates=recv)
    summary["till_codes"] = matched
    summary["receipt_dates"] = len(recv)
    return summary


def _receipt_dates(data_dir: str) -> dict:
    """{itm_cd: 'YYYY-MM-DD'} — each SKU's latest GRN receipt, or {}.

    Read through devkit's stock ledger when it is present. It is dev tooling
    and does not ship, so its absence is normal and silent: the builder then
    writes the build date as before.
    """
    try:
        from devkit import stock_ledger as SL           # type: ignore
        led = SL.load()
    except Exception:
        return {}
    out = {}
    for itm, receipts in getattr(led, "receipts", {}).items():
        dates = [r.date for r in receipts if getattr(r, "date", None)]
        if dates:
            out[_norm_key(itm)] = max(dates).strftime("%Y-%m-%d")
    return out


def _norm_key(name) -> str:
    """One spelling of a product name, shared by both sides of the join."""
    try:
        from .real_demand import normalise_name
        return normalise_name(name)
    except Exception:
        return str(name or "").strip().upper()
