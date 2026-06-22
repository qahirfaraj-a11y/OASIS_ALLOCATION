"""
Live POS activity queries (demo / monitoring).

Surfaces *recent* POS activity so live injections (or a real POS stream) are
visible regardless of how much historical data sits behind them — the fix for
"injections can't be seen": the bulk mock is historical, so filtering to **today**
isolates the live window.

    activity_summary   — today's bills/units + current & freshly-depleted stockouts
    recent_bills       — the latest bills (today first)
    recently_depleted  — SKUs sold today that are now at/below stockout

Lightweight read-only SQLite queries (the demo DB); tolerant of missing columns
so they never break a render. The canonical table names mean they also work
through the RXL views.
"""

from __future__ import annotations

import sqlite3
from typing import List, Optional


def _conn(db_path: str) -> sqlite3.Connection:
    c = sqlite3.connect(db_path, timeout=15.0)
    c.execute("PRAGMA busy_timeout=15000")
    c.row_factory = sqlite3.Row
    return c


def _org_clause(org: Optional[str], col: str):
    return (f" AND {col} = ?", [org]) if org else ("", [])


def activity_summary(db_path: str, org: Optional[str] = None) -> dict:
    """Today's bills/units + current stockouts + SKUs depleted by today's sales."""
    out = {"bills_today": 0, "units_today": 0, "stockouts": 0, "depleted_today": 0}
    try:
        c = _conn(db_path)
        try:
            hc, hp = _org_clause(org, "ORG_CD")
            out["bills_today"] = c.execute(
                "SELECT COUNT(*) FROM POS_SALES_HDR "
                "WHERE BILL_DT = date('now','localtime')" + hc, hp).fetchone()[0]
            out["units_today"] = c.execute(
                "SELECT COALESCE(SUM(QTY),0) FROM POS_SALES_DTL "
                "WHERE BILL_DT = date('now','localtime')" + hc, hp).fetchone()[0]
            sc, sp = _org_clause(org, "SM_ORG_CD")
            out["stockouts"] = c.execute(
                "SELECT COUNT(*) FROM STOCK_MASTER WHERE SM_QTY < 1" + sc, sp).fetchone()[0]
            dc, dp = _org_clause(org, "d.ORG_CD")
            out["depleted_today"] = c.execute(
                "SELECT COUNT(DISTINCT d.ITM_CD) FROM POS_SALES_DTL d "
                "JOIN STOCK_MASTER s ON s.SM_ITM_CD=d.ITM_CD AND s.SM_ORG_CD=d.ORG_CD "
                "WHERE d.BILL_DT = date('now','localtime') AND s.SM_QTY < 1" + dc, dp).fetchone()[0]
        finally:
            c.close()
    except Exception:
        pass
    return out


def recent_bills(db_path: str, org: Optional[str] = None, limit: int = 15) -> List[dict]:
    """Most recent bills (today first), newest first."""
    try:
        c = _conn(db_path)
        try:
            oc, op = _org_clause(org, "ORG_CD")
            rows = c.execute(
                "SELECT ORG_CD, BILL_NO, BILL_DT, TOTAL_QTY, NET_AMT FROM POS_SALES_HDR "
                "WHERE 1=1" + oc + " ORDER BY BILL_DT DESC, BILL_NO DESC LIMIT ?",
                op + [int(limit)]).fetchall()
            return [{"Store": r["ORG_CD"], "Bill": r["BILL_NO"], "Date": r["BILL_DT"],
                     "Units": r["TOTAL_QTY"], "Value (KES)": round(float(r["NET_AMT"] or 0), 0)}
                    for r in rows]
        finally:
            c.close()
    except Exception:
        return []


def recently_depleted(db_path: str, org: Optional[str] = None, limit: int = 20) -> List[dict]:
    """SKUs sold today that are now at/below stockout (the live 'reaction')."""
    try:
        c = _conn(db_path)
        try:
            oc, op = _org_clause(org, "d.ORG_CD")
            rows = c.execute(
                "SELECT DISTINCT d.ITM_CD, d.ITEM_NAME, s.SM_QTY "
                "FROM POS_SALES_DTL d "
                "JOIN STOCK_MASTER s ON s.SM_ITM_CD=d.ITM_CD AND s.SM_ORG_CD=d.ORG_CD "
                "WHERE d.BILL_DT = date('now','localtime') AND s.SM_QTY < 1" + oc
                + " LIMIT ?", op + [int(limit)]).fetchall()
            return [{"Item": str(r["ITEM_NAME"] or r["ITM_CD"])[:45],
                     "Code": r["ITM_CD"], "Stock": round(float(r["SM_QTY"] or 0), 1)}
                    for r in rows]
        finally:
            c.close()
    except Exception:
        return []
