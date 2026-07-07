"""
Value Report — the monthly ROI artifact that justifies the subscription.

Aggregates, over a period (default 30 days), what OASIS did for the store:
sales handled, orders generated/approved, stock health (stockouts, dead-stock
capital), value recovered (journey thread), and system adoption (audit-log
telemetry). Writes a shareable Markdown report + CSV to <root>/reports/.

    python entrypoint.py --mode value-report            # last 30 days
    python entrypoint.py --mode metering-report         # adoption/usage counts

Pure aggregation (compute_value_metrics) is unit-tested; the writer is thin.
"""

from __future__ import annotations

import csv
import os
import sqlite3
from datetime import datetime, timedelta
from typing import Optional


def _q1(conn, sql: str, params=()) -> float:
    try:
        row = conn.execute(sql, params).fetchone()
        return float(row[0] or 0) if row else 0.0
    except sqlite3.Error:
        return 0.0


def usage_summary(db_path: str, since: str) -> dict:
    """Adoption counters from the audit-log telemetry (best-effort), including
    a per-module rollup of page views — the renewal/upsell evidence
    ("your team opened Revenue Intelligence 47 times this month")."""
    out = {"page_views": 0, "operator_actions": 0, "active_users": 0,
           "usage_by_module": {}}
    try:
        conn = sqlite3.connect(db_path, timeout=15.0)
        try:
            out["page_views"] = int(_q1(conn,
                "SELECT COUNT(*) FROM OASIS_AUDIT_LOG "
                "WHERE ACTION='PAGE_VIEW' AND CREATED_DT >= ?", (since,)))
            out["operator_actions"] = int(_q1(conn,
                "SELECT COUNT(*) FROM OASIS_AUDIT_LOG "
                "WHERE ACTION<>'PAGE_VIEW' AND CREATED_DT >= ?", (since,)))
            out["active_users"] = int(_q1(conn,
                "SELECT COUNT(DISTINCT USERNAME) FROM OASIS_AUDIT_LOG "
                "WHERE CREATED_DT >= ?", (since,)))
            from .license_manager import PAGE_MODULES
            by_mod: dict = {}
            for key, n in conn.execute(
                    "SELECT ENTITY_ID, COUNT(*) FROM OASIS_AUDIT_LOG "
                    "WHERE ACTION='PAGE_VIEW' AND CREATED_DT >= ? "
                    "GROUP BY ENTITY_ID", (since,)):
                mod = PAGE_MODULES.get(str(key or ""), "core")
                by_mod[mod] = by_mod.get(mod, 0) + int(n or 0)
            out["usage_by_module"] = by_mod
        finally:
            conn.close()
    except sqlite3.Error:
        pass
    return out


def compute_value_metrics(db_path: str, period_days: int = 30,
                          journey_state_path: Optional[str] = None) -> dict:
    """All report numbers, computed live from the store DB (read-only)."""
    since = (datetime.now() - timedelta(days=period_days)).strftime("%Y-%m-%d")
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        sales_rev = _q1(conn, "SELECT SUM(NET_AMT) FROM POS_SALES_HDR "
                              "WHERE BILL_DT >= ? AND VOID_FLAG='F'", (since,))
        bills = _q1(conn, "SELECT COUNT(*) FROM POS_SALES_HDR "
                          "WHERE BILL_DT >= ? AND VOID_FLAG='F'", (since,))
        po_count = _q1(conn, "SELECT COUNT(*) FROM INTEGRATION_PURCHASE_ORDERS "
                             "WHERE CREATED_DT >= ?", (since,))
        po_approved = _q1(conn, "SELECT COUNT(*) FROM INTEGRATION_PURCHASE_ORDERS "
                                "WHERE CREATED_DT >= ? AND STATUS='APPROVED'", (since,))
        po_value = _q1(conn, "SELECT SUM(TOTAL_COST) FROM INTEGRATION_PURCHASE_ORDERS "
                             "WHERE CREATED_DT >= ?", (since,))
        skus = _q1(conn, "SELECT COUNT(*) FROM STOCK_MASTER")
        stockouts = _q1(conn, "SELECT COUNT(*) FROM STOCK_MASTER WHERE SM_QTY < 1")
        inventory_value = _q1(conn, "SELECT SUM(SM_QTY * SM_WAC) FROM STOCK_MASTER "
                                    "WHERE SM_QTY > 0")
        # dead stock: on-hand with zero sales in the period
        dead_value = _q1(conn, """
            SELECT SUM(s.SM_QTY * s.SM_WAC) FROM STOCK_MASTER s
            WHERE s.SM_QTY > 0 AND NOT EXISTS (
                SELECT 1 FROM POS_SALES_DTL d
                WHERE d.ITM_CD = s.SM_ITM_CD AND d.ORG_CD = s.SM_ORG_CD
                  AND d.BILL_DT >= ? AND d.VOID_FLAG='F')""", (since,))
    finally:
        conn.close()

    value_recovered = 0.0
    try:
        from . import journey_state as JS
        value_recovered = float(JS.load_state(journey_state_path).get("value_recovered", 0) or 0)
    except Exception:
        pass

    m = {
        "period_days": period_days, "since": since,
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "sales_revenue": round(sales_rev, 0), "bills": int(bills),
        "po_generated": int(po_count), "po_approved": int(po_approved),
        "po_value": round(po_value, 0),
        "skus_tracked": int(skus), "stockouts": int(stockouts),
        "stockout_pct": round(100.0 * stockouts / skus, 1) if skus else 0.0,
        "inventory_value": round(inventory_value, 0),
        "dead_stock_value": round(dead_value, 0),
        "dead_stock_pct_of_inventory": (round(100.0 * dead_value / inventory_value, 1)
                                        if inventory_value else 0.0),
        "value_recovered": round(value_recovered, 0),
    }
    m.update(usage_summary(db_path, since))
    return m


def write_value_report(db_path: str, out_dir: str, period_days: int = 30,
                       journey_state_path: Optional[str] = None,
                       tenant: str = "") -> dict:
    """Compute metrics and write the Markdown + CSV artifacts."""
    m = compute_value_metrics(db_path, period_days=period_days,
                              journey_state_path=journey_state_path)
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m")
    md_path = os.path.join(out_dir, f"OASIS_Value_Report_{stamp}.md")
    csv_path = os.path.join(out_dir, f"OASIS_Value_Report_{stamp}.csv")

    title_for = tenant or "your store"
    md = f"""# O.A.S.I.S. Value Report — {stamp}

**Prepared for:** {title_for} · **Period:** last {m['period_days']} days (since {m['since']}) · Generated {m['generated']}

## Commercial outcomes
| Metric | Value |
|---|---|
| Capital recovered (journey) | KES {m['value_recovered']:,.0f} |
| Sales processed | KES {m['sales_revenue']:,.0f} across {m['bills']:,} bills |
| Purchase orders generated | {m['po_generated']:,} ({m['po_approved']:,} approved, KES {m['po_value']:,.0f}) |

## Stock health
| Metric | Value |
|---|---|
| SKUs tracked | {m['skus_tracked']:,} |
| Stockouts | {m['stockouts']:,} ({m['stockout_pct']}%) |
| Inventory on hand | KES {m['inventory_value']:,.0f} |
| Dead stock (no sales this period) | KES {m['dead_stock_value']:,.0f} ({m['dead_stock_pct_of_inventory']}% of inventory) |

## Adoption
| Metric | Value |
|---|---|
| Active users | {m['active_users']} |
| Screens viewed | {m['page_views']:,} |
| Operator actions | {m['operator_actions']:,} |
| Usage by module | {', '.join(f"{k}: {v}" for k, v in sorted(m['usage_by_module'].items())) or '—'} |

*Dead stock and stockout figures are live snapshot values; capital recovered is
the cumulative journey figure. Playbook targets: dead stock <5%, stockouts <2%.*
"""
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["metric", "value"])
        for k, v in m.items():
            w.writerow([k, v])
    return {"markdown": md_path, "csv": csv_path, **m}
