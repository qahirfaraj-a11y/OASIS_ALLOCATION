"""Multi-day ordering on the Rhapta book: order, receive at LATA's lead time, sell.

WHAT THIS ANSWERS
    Day 2 already proved the engine subtracts what is on order. What no run has
    shown is the LOOP: order today, take delivery when the supplier actually
    delivers, sell the stock, order again. That is where an ordering system
    either settles or drifts, and it is the last thing standing between this
    engine and a supervised pilot.

EVERYTHING HERE ALREADY EXISTS
    orders        oasis.desktop.data.generate_smart_orders  (the shipped path)
    push/approve  push_purchase_order / update_po_status
    receipt       update_po_status(po_id, "RECEIVED") -- fetch_pending_po_by_sku
                  counts STATUS IN ('PENDING','APPROVED'), so RECEIVED stops
                  being inbound. No new column, no new function.
    lead time     order_up_to.default_patterns() -> the LATA measured
                  lead_time_days per supplier (535 of Rhapta's 823 suppliers,
                  28,931 of 39,728 items; median 3d, range 1-21). Suppliers
                  LATA cannot speak for fall back to the product's own
                  estimated_delivery_days, which is what the engine does.
    demand        real_demand.load_monthly_demand -> match_to_catalog ->
                  derive_ads over the ten cashier exports: the real Rhapta
                  sales, 20,342 SKUs at 83.9% coverage.

TWO MODELLING CHOICES, STATED RATHER THAN HIDDEN
    Stock is consumed at the cash-derived daily rate, and every unit sold is
    WRITTEN BACK as a POS bill on that day, the way a till records it.

    The first version wrote no bills, to keep demand "the seeded history". It
    did not stay the seeded history: the engine's ADS is a trailing 90-day
    window over POS_SALES_DTL, the seeded bills end 2025-11-24, and as the
    clock advanced the window emptied. Summed weighted ADS went 9,379/day on
    09 Dec -> 8,624 on 23 Dec -> 3,450 on 30 Dec while real demand held at
    ~8,600, so ordering wound down and stock-outs climbed from day 10 -- a
    drift the harness manufactured. The run now starts the day after the last
    seeded bill so the window is full on day 1.

    The same run shows a production property worth knowing: nothing warns
    when the sales feed stops. A POS sync that silently dies produces exactly
    this slow wind-down of orders.

    Arrival is deterministic at the measured lead time. LATA also carries
    lead_time_stdev; putting noise on the arrival date would be a claim about
    supplier reliability this run is not making.

Touches ONLY a copy of the store. The shared oasis/data files that ordering
writes are restored from backup in a finally block and byte-checked.

    python devkit/multiday_sim.py [--days 30] [--start 2025-11-25] [--org ORG001]
"""
from __future__ import annotations

import argparse
import filecmp
import json
import os
import shutil
import sqlite3
import sys
import time
from datetime import datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

CASH_DIR = os.getenv("OASIS_CASH_DIR",
                     os.path.join(os.path.expanduser("~"), "Desktop", "Projects"))
SHARED = ("network_registry.json", "transfers_registry.json", "moq_failures.json")


def _iso(d: datetime) -> str:
    return d.strftime("%Y-%m-%d")


def build_sales_rates(db_path: str) -> dict:
    """{itm_cd: units/day} from the ten cashier exports, via the methodology."""
    from oasis.logic.real_demand import (derive_ads, load_monthly_demand,
                                         match_to_catalog)
    demand, months, files = load_monthly_demand(CASH_DIR)
    matched, cov = match_to_catalog(demand, db_path)
    ads = derive_ads(matched, months)
    print(f"[demand] {files} cash files, {months} months, coverage "
          f"{cov['coverage_pct']}%, {len(ads):,} SKUs with a daily rate",
          flush=True)
    return ads


def lead_times(db_path: str) -> dict:
    """{itm_cd: lead days} — LATA's measured figure, else the ERP's own."""
    from oasis.logic import order_up_to as ou
    pats = ou.default_patterns()
    con = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    rows = con.execute(
        "SELECT i.ITM_CD, s.SUPPLIER_NAME, s.LEAD_TIME_DAYS FROM ITEM_MST i "
        "LEFT JOIN SUPPLIER_MST s ON s.SUPPLIER_CD = i.SUPPLIER_CD").fetchall()
    con.close()
    out, measured = {}, 0
    for itm, name, erp_lead in rows:
        # the engine's own key: SUPPLIER_MST carries double spaces ("DPL FESTIVE
        # LIMITED" with two), and a bare .upper() missed 2,517 items -- the bakery
        # and dairy lines among them -- delivering at 7 days what the engine
        # planned at 1, so fresh shelves emptied behind a correct on-order count
        p = pats.get(ou.supplier_key(name)) or {}
        lt = p.get("lead_time_days")
        # 0 is a measurement (same-day bakery/dairy), not a missing value
        if lt is not None:
            measured += 1
        else:
            lt = erp_lead or 3
        out[str(itm)] = max(1, int(round(float(lt))))
    print(f"[lead] {measured:,} of {len(out):,} items carry a LATA-measured "
          f"lead time; the rest use the ERP's stated one", flush=True)
    return out


def spread_monthly_bills(db_path: str, org: str) -> dict:
    """Turn the seeded store's month-lump bills into the daily history a till has.

    mock_pos_build writes each cash export as ONE bill date carrying the whole
    month (25 Sep, 25 Oct, 24 Nov: 258,730 units each). _calc_weighted_ads
    divides each 30-day bucket by the days it has OBSERVED since the first
    bill, which is right for daily bills and wrong for lumps: on 25 Nov the
    60-90 bucket holds a month of units over 2 observed days, and the engine
    plans on 20,843 units a day against a real 8,624. On 09 Dec it is 9,379.

    Each lump is spread evenly over the 30 days ending on its bill date. Monthly
    totals are unchanged; only the working copy is touched.
    """
    con = sqlite3.connect(db_path)
    lumps = con.execute(
        "SELECT BILL_DT, COUNT(DISTINCT BILL_NO) FROM POS_SALES_DTL "
        "WHERE ORG_CD=? AND VOID_FLAG='F' GROUP BY BILL_DT", (org,)).fetchall()
    before = con.execute("SELECT SUM(QTY) FROM POS_SALES_DTL WHERE ORG_CD=? AND VOID_FLAG='F'",
                         (org,)).fetchone()[0] or 0
    if len(lumps) > 10:            # already daily -- nothing to do
        con.close()
        return {"spread": False, "bill_days": len(lumps)}
    for bill_dt, _ in lumps:
        rows = con.execute(
            "SELECT ITM_CD, SUM(QTY) FROM POS_SALES_DTL WHERE ORG_CD=? AND BILL_DT=? "
            "AND VOID_FLAG='F' GROUP BY ITM_CD", (org, bill_dt)).fetchall()
        con.execute("DELETE FROM POS_SALES_DTL WHERE ORG_CD=? AND BILL_DT=?", (org, bill_dt))
        end = datetime.strptime(str(bill_dt)[:10], "%Y-%m-%d")
        for k in range(30):
            day = _iso(end - timedelta(days=k))
            bill_no = "SPREAD" + day.replace("-", "") + "000001"
            con.executemany(
                "INSERT INTO POS_SALES_DTL (ORG_CD, BILL_NO, BILL_DT, SERIAL_NO, ITM_CD, QTY, VOID_FLAG) "
                "VALUES (?,?,?,?,?,?,'F')",
                [(org, bill_no, day, i + 1, itm, float(q) / 30.0) for i, (itm, q) in enumerate(rows)])
    con.commit()
    after = con.execute("SELECT SUM(QTY) FROM POS_SALES_DTL WHERE ORG_CD=? AND VOID_FLAG='F'",
                        (org,)).fetchone()[0] or 0
    days = con.execute("SELECT COUNT(DISTINCT BILL_DT), MIN(BILL_DT), MAX(BILL_DT) FROM POS_SALES_DTL "
                       "WHERE ORG_CD=?", (org,)).fetchone()
    con.close()
    assert abs(after - before) < 1e-3 * max(before, 1), (before, after)
    return {"spread": True, "lumps": len(lumps), "bill_days": days[0], "first": days[1],
            "last": days[2], "units": round(after)}


def receive_due(D, db_path: str, org: str, day_iso: str, leads: dict,
                placed_on: dict) -> dict:
    """Close every PO whose supplier lead time has elapsed, and land the stock.

    ``placed_on`` is the simulated day each PO was raised. CREATED_DT cannot be
    used: the writer stamps the wall clock, not OASIS_AS_OF, so on a replayed
    calendar every PO looks placed in the future and never falls due.
    """
    con = sqlite3.connect(db_path)
    open_pos = con.execute(
        "SELECT PO_ID, ITM_CD, QUANTITY FROM INTEGRATION_PURCHASE_ORDERS "
        "WHERE ORG_CD=? AND STATUS IN ('PENDING','APPROVED')", (org,)).fetchall()
    con.close()
    today = datetime.strptime(day_iso, "%Y-%m-%d")
    due = []
    for po_id, itm, qty in open_pos:
        if po_id not in placed_on:
            continue
        placed = datetime.strptime(placed_on[po_id], "%Y-%m-%d")
        if (today - placed).days >= leads.get(str(itm), 3):
            due.append((po_id, str(itm), float(qty or 0)))

    # The goods arrive. The shelf moves in ONE write -- a connection per PO is
    # thousands of them by the third day -- and the status still goes through
    # the shipped call, because that is the behaviour under test.
    if due:
        con = sqlite3.connect(db_path)
        con.executemany(
            "UPDATE STOCK_MASTER SET SM_QTY = COALESCE(SM_QTY,0) + ?, "
            "SM_LAST_RECV_DT=? WHERE SM_ORG_CD=? AND SM_ITM_CD=?",
            [(q, day_iso, org, itm) for _, itm, q in due])
        con.commit()
        con.close()
        for po_id, _, _ in due:
            D.update_po_status(po_id, "RECEIVED", "multiday_sim", org, root=ROOT)
    return {"po_lines_received": len(due),
            "units_received": round(sum(q for _, _, q in due))}


def sell(db_path: str, org: str, rates: dict, day_iso: str) -> dict:
    """One day of real demand off the shelf, recorded as that day's POS bill.

    Unmet demand is the service gap. What WAS sold goes into POS_SALES_DTL, so
    the next day's engine measures demand the way it does on a live till --
    including the dip a stock-out causes, which is real behaviour, not noise.
    """
    con = sqlite3.connect(db_path)
    stock = {str(i): float(q or 0) for i, q in con.execute(
        "SELECT SM_ITM_CD, SM_QTY FROM STOCK_MASTER WHERE SM_ORG_CD=?", (org,))}
    demanded = sold = 0.0
    lines_short = 0
    updates, bills = [], []
    bill_no = "SIM" + day_iso.replace("-", "") + "000001"
    for itm, rate in rates.items():
        if rate <= 0 or itm not in stock:
            continue
        have = stock[itm]
        take = min(have, rate)
        demanded += rate
        sold += take
        if take < rate - 1e-9:
            lines_short += 1
        updates.append((have - take, org, itm))
        if take > 0:
            bills.append((org, bill_no, day_iso, len(bills) + 1, itm, take))
    con.executemany("UPDATE STOCK_MASTER SET SM_QTY=? WHERE SM_ORG_CD=? AND SM_ITM_CD=?",
                    updates)
    con.executemany(
        "INSERT INTO POS_SALES_DTL (ORG_CD, BILL_NO, BILL_DT, SERIAL_NO, ITM_CD, QTY, VOID_FLAG) "
        "VALUES (?,?,?,?,?,?,'F')", bills)
    con.commit()
    con.close()
    return {"demanded": round(demanded), "sold": round(sold),
            "lines_short": lines_short,
            "fill_rate": round(sold / demanded, 4) if demanded else None}


def position(D, org: str) -> dict:
    D.reset_adapter()
    rows = D.get_adapter(ROOT).fetch_enriched_products(org) or []
    on_hand = sum(float(r.get("current_stocks") or 0) for r in rows)
    on_order = sum(float(r.get("on_order_qty") or 0) for r in rows)
    selling = [r for r in rows if float(r.get("avg_daily_sales") or 0) > 0]
    out = sum(1 for r in selling if float(r.get("current_stocks") or 0) < 1)
    # the demand the engine is planning on -- if this decays while real demand
    # does not, the run is measuring its own clock, not the engine
    engine_ads = sum(float(r.get("avg_daily_sales") or 0) for r in rows)
    return {"on_hand": round(on_hand), "on_order": round(on_order),
            "selling_skus": len(selling), "stocked_out_selling_skus": out,
            "engine_ads": round(engine_ads)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=7)
    # the day after the last seeded bill, so the 90-day demand window is full
    ap.add_argument("--start", default="2025-11-25")
    ap.add_argument("--org", default="ORG001")
    ap.add_argument("--store", default=os.path.join(ROOT, "oasis", "data", "rhapta_pos.db"))
    ap.add_argument("--work", default=os.path.join(ROOT, "oasis", "data", "_multiday_sim.db"))
    ap.add_argument("--out", default=os.path.join(ROOT, "oasis", "data", "_multiday_sim.json"))
    a = ap.parse_args(argv)

    data_dir = os.path.join(ROOT, "oasis", "data")
    bak = {f: os.path.join(data_dir, f + ".simbak") for f in SHARED}
    for f, b in bak.items():
        shutil.copy2(os.path.join(data_dir, f), b)

    for suffix in ("", "-wal", "-shm"):
        if os.path.exists(a.work + suffix):
            os.remove(a.work + suffix)
    shutil.copy2(a.store, a.work)
    print(f"[setup] working on a copy: {a.work}", flush=True)
    print(f"[history] {spread_monthly_bills(a.work, a.org)}", flush=True)

    os.environ.update({"OASIS_DB_PATH": a.work,
                       "OASIS_POS_DB_URL": "sqlite:///" + a.work,
                       "OASIS_ORDER_MODEL": "order_up_to"})
    os.environ.pop("OASIS_DB_URL", None)

    rates = build_sales_rates(a.work)
    leads = lead_times(a.work)

    from oasis.desktop import data as D
    start = datetime.strptime(a.start, "%Y-%m-%d")
    report = {"start": a.start, "days": a.days, "org": a.org, "daily": []}
    placed_on: dict = {}
    try:
        for n in range(a.days):
            day = start + timedelta(days=n)
            day_iso = _iso(day)
            os.environ["OASIS_AS_OF"] = day_iso
            t0 = time.time()

            got = receive_due(D, a.work, a.org, day_iso, leads, placed_on)

            D.reset_adapter()
            res = D.generate_smart_orders(a.org, root=ROOT)
            recs = [r for r in (res.get("po_recs") or [])
                    if float(r.get("recommended_quantity") or 0) > 0]
            pushed = D.push_purchase_order(a.org, "multiday_sim", recs, ROOT) if recs else {}
            approved = 0
            if recs:
                con = sqlite3.connect(a.work)
                ids = [x[0] for x in con.execute(
                    "SELECT PO_ID FROM INTEGRATION_PURCHASE_ORDERS "
                    "WHERE ORG_CD=? AND STATUS='PENDING'", (a.org,))]
                con.close()
                for pid in ids:
                    placed_on.setdefault(pid, day_iso)
                    if not (D.update_po_status(pid, "APPROVED", "multiday_sim",
                                               a.org, root=ROOT) or {}).get("error"):
                        approved += 1

            sold = sell(a.work, a.org, rates, day_iso)
            pos = position(D, a.org)
            row = {"day": n + 1, "date": day_iso, "error": res.get("error"),
                   "ordered_lines": len(recs),
                   "ordered_units": round(sum(float(r["recommended_quantity"]) for r in recs)),
                   "approved": approved, **got, **sold, **pos,
                   "secs": round(time.time() - t0)}
            report["daily"].append(row)
            with open(a.out, "w", encoding="utf-8") as fh:
                json.dump(report, fh, indent=1, default=str)
            print(f"[day {row['day']}] {day_iso} ordered {row['ordered_lines']:,} lines "
                  f"/ {row['ordered_units']:,} units | received {row['po_lines_received']:,} "
                  f"lines / {row['units_received']:,} units | fill {row['fill_rate']} | "
                  f"on hand {row['on_hand']:,} on order {row['on_order']:,} | "
                  f"stocked out {row['stocked_out_selling_skus']:,} | engine demand "
                  f"{row['engine_ads']:,}/day vs real {row['demanded']:,} ({row['secs']}s)",
                  flush=True)
    finally:
        restored = {}
        for f, b in bak.items():
            shutil.copy2(b, os.path.join(data_dir, f))
            restored[f] = filecmp.cmp(b, os.path.join(data_dir, f), shallow=False)
            os.remove(b)
        report["shared_files_restored"] = restored
        with open(a.out, "w", encoding="utf-8") as fh:
            json.dump(report, fh, indent=1, default=str)
        print("[restore]", restored, flush=True)
        print(f"[done] {a.out}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
