"""
Mock POS injector (live demo).

Writes synthetic POS transactions into the POS/ERP database **while the consoles
are running**, so you can watch the dashboards react. SQLite is in WAL mode, so
this separate writer coexists with the Streamlit readers; changes surface on each
console's cache-TTL expiry or a Regenerate/Re-scan click.

Each batch: rings up a few SKUs (today-dated bills in POS_SALES_HDR/DTL) and
**decrements STOCK_MASTER** to simulate sell-through — optionally depleting some
SKUs to zero (to trigger stockout/at-risk surfaces) and restocking a few (GRN-
style). It prints what it did so you can correlate with the dashboards.

    python entrypoint.py --mode pos-inject --batches 10 --interval 15

This MUTATES the target DB (the demo data). Point OASIS_DB_PATH at a copy/showcase
DB if you want to preserve the baseline mock.
"""

from __future__ import annotations

import random
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


@dataclass(frozen=True)
class SaleLine:
    itm_cd: str
    item_name: str
    qty: float
    sell_price: float


def build_bill(org: str, bill_no: str, bill_dt: str, lines: List[SaleLine]) -> tuple:
    """(header dict, [detail dicts]) for a POS bill (pure)."""
    total_qty = sum(line.qty for line in lines)
    net = round(sum(line.qty * line.sell_price for line in lines), 2)
    hdr = {
        "ORG_CD": org, "BILL_NO": bill_no, "BILL_DT": bill_dt,
        "CUST_CD": "WALKIN", "COUNTER_CD": "INJ", "LEVEL_NUMBER": 1,
        "TOTAL_QTY": total_qty, "TOTAL_AMT": net, "NET_AMT": net,
        "TAX_AMT": 0.0, "DISC_AMT": 0.0, "PAYMENT_MODE": "CASH",
        "VOID_FLAG": "F", "CUS_REF_CODE": "", "CUS_REF_REMARKS": "MOCK_INJECT",
    }
    dtl = []
    for i, line in enumerate(lines, start=1):
        amt = round(line.qty * line.sell_price, 2)
        dtl.append({
            "ORG_CD": org, "BILL_NO": bill_no, "BILL_DT": bill_dt, "SERIAL_NO": i,
            "ITM_CD": line.itm_cd, "ITEM_NAME": line.item_name, "QTY": line.qty,
            "SELL_PRICE": line.sell_price, "NET_AMT": amt, "TAX_AMT": 0.0,
            "DISC_AMT": 0.0, "NET_TAX_AMT": amt, "TOTAL_VALUE": amt,
            "UOM_CD": "EA", "UOM_DESC": "EACH", "VOID_FLAG": "F",
            "PROMO_ITEM_FLAG": "N", "SCAN_ITM_CD": "", "TAX_PLAN_CD": "",
        })
    return hdr, dtl


# ── integration ──────────────────────────────────────────────────────────────
def _connect(db_path: str):
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def _insert(conn, table: str, row: dict):
    cols = ",".join(row)
    ph = ",".join(["?"] * len(row))
    conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({ph})", list(row.values()))


def inject_once(db_path: str, org: Optional[str] = None, sku_count: int = 8,
                max_qty: int = 5, deplete: bool = True, restock: int = 2,
                seed: Optional[int] = None) -> dict:
    """Ring up one mock bill + decrement stock (+ optional restock). Returns a summary."""
    rng = random.Random(seed)
    today = datetime.now().strftime("%Y-%m-%d")
    conn = _connect(db_path)
    try:
        if not org:
            r = conn.execute("SELECT ORG_CD FROM ORGANIZATION_MST WHERE ACTIVE_FLAG='Y' "
                             "ORDER BY ORG_CD LIMIT 1").fetchone()
            org = r[0] if r else "ORG001"
        # candidate SKUs with stock, joined to name + price
        rows = conn.execute("""
            SELECT s.SM_ITM_CD, COALESCE(i.ITM_LONG_NAME,''), s.SM_QTY,
                   COALESCE(sp.BSP_SP, 100.0)
            FROM STOCK_MASTER s
            LEFT JOIN ITEM_MST i ON i.ITM_CD = s.SM_ITM_CD
            LEFT JOIN BASIC_SP_MST sp ON sp.BSP_ITEM_CD = s.SM_ITM_CD AND sp.BSP_ORG_CD = s.SM_ORG_CD
            WHERE s.SM_ORG_CD = ? AND s.SM_QTY > 0
            ORDER BY RANDOM() LIMIT ?""", (org, sku_count * 3)).fetchall()
        if not rows:
            return {"org": org, "error": "no stocked SKUs"}
        picks = rows[:sku_count]

        lines, sold = [], []
        for itm, name, stock, price in picks:
            stock = float(stock or 0)
            if deplete and rng.random() < 0.35:
                qty = stock                      # drive this SKU to zero (at-risk demo)
            else:
                qty = min(stock, float(rng.randint(1, max_qty)))
            if qty <= 0:
                continue
            lines.append(SaleLine(str(itm), str(name), qty, float(price or 100.0)))
            new_stock = round(stock - qty, 3)
            conn.execute("UPDATE STOCK_MASTER SET SM_QTY=?, SM_LAST_ISSUE_DT=? "
                         "WHERE SM_ORG_CD=? AND SM_ITM_CD=?", (new_stock, today, org, itm))
            sold.append({"itm": str(itm), "name": str(name)[:30], "qty": qty,
                         "new_stock": new_stock})

        bill_no = f"INJ{datetime.now().strftime('%H%M%S%f')[:11]}"
        hdr, dtl = build_bill(org, bill_no, today, lines)
        _insert(conn, "POS_SALES_HDR", hdr)
        for d in dtl:
            _insert(conn, "POS_SALES_DTL", d)

        restocked = []
        if restock > 0:
            depleted = conn.execute("SELECT SM_ITM_CD FROM STOCK_MASTER "
                                    "WHERE SM_ORG_CD=? AND SM_QTY<=1 ORDER BY RANDOM() LIMIT ?",
                                    (org, restock)).fetchall()
            for (itm,) in depleted:
                conn.execute("UPDATE STOCK_MASTER SET SM_QTY=SM_QTY+?, SM_LAST_RECV_DT=? "
                             "WHERE SM_ORG_CD=? AND SM_ITM_CD=?",
                             (float(rng.randint(20, 80)), today, org, itm))
                restocked.append(str(itm))

        conn.commit()
        return {"org": org, "bill_no": bill_no, "lines": len(lines),
                "units_sold": sum(line.qty for line in lines),
                "sold": sold, "restocked": restocked}
    finally:
        conn.close()


def run_injector(db_path: str, batches: int = 10, interval: float = 15.0,
                 org: Optional[str] = None, sku_count: int = 8) -> None:
    """Inject `batches` bills, `interval` seconds apart, printing each (integration)."""
    import time
    print(f"[pos-inject] target={db_path} batches={batches} interval={interval}s")
    for b in range(1, batches + 1):
        r = inject_once(db_path, org=org, sku_count=sku_count)
        if r.get("error"):
            print(f"[{b}/{batches}] {r['org']}: {r['error']}")
        else:
            depleted = [s["itm"] for s in r["sold"] if s["new_stock"] <= 0]
            print(f"[{b}/{batches}] {r['org']} bill {r['bill_no']}: "
                  f"{r['lines']} lines, {r['units_sold']:.0f} units sold, "
                  f"{len(depleted)} SKU(s) depleted, {len(r['restocked'])} restocked")
        if b < batches:
            time.sleep(max(0.0, interval))
    print("[pos-inject] done.")
