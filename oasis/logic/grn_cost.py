"""
Real cost prices from GRN (Goods Received Note) history.

The catalogue carries only selling prices; true unit cost lives in the GRN files
(``*grnd*.xlsx``: Bar Code, Item Name, Cost Price, GRN Qty, SP, Vendor, GRN Date).
This module aggregates them into a **GRN-quantity-weighted average cost** per
barcode (the WAC concept OASIS already uses) and injects it into the store DB so
ordering economics, transfer economics, dead-stock valuation and margin reporting
all run on real numbers instead of the 0.82×price estimate.

    python entrypoint.py --mode inject-grn-costs

Pure aggregation (aggregate_costs) is unit-tested; the loader + injector are I/O.
"""

from __future__ import annotations

import glob
import os
import sqlite3
from typing import Dict, List


def aggregate_costs(records: List[dict]) -> Dict[str, dict]:
    """{barcode: {cost, sp, qty_received, vendor, receipts}} from GRN line records.

    cost = GRN-qty-weighted average unit cost (WAC); sp = the last seen selling
    price; vendor = the most recent vendor. Pure.
    """
    acc: Dict[str, dict] = {}
    for r in records:
        bc = str(r.get("barcode", "")).strip()
        if not bc:
            continue
        try:
            cost = float(r.get("cost", 0) or 0)
        except (TypeError, ValueError):
            continue
        if cost <= 0:
            continue
        try:
            qty = float(r.get("qty", 0) or 0)
        except (TypeError, ValueError):
            qty = 0.0
        qty = qty if qty > 0 else 1.0
        e = acc.setdefault(bc, {"_cost_qty": 0.0, "_qty": 0.0, "sp": 0.0,
                                "vendor": "", "receipts": 0})
        e["_cost_qty"] += cost * qty
        e["_qty"] += qty
        e["receipts"] += 1
        sp = r.get("sp")
        if sp not in (None, "", 0):
            try:
                e["sp"] = float(sp)
            except (TypeError, ValueError):
                pass
        if r.get("vendor"):
            e["vendor"] = str(r["vendor"])
    out: Dict[str, dict] = {}
    for bc, e in acc.items():
        if e["_qty"] > 0:
            out[bc] = {"cost": round(e["_cost_qty"] / e["_qty"], 4),
                       "sp": round(e["sp"], 2), "vendor": e["vendor"],
                       "qty_received": round(e["_qty"], 0), "receipts": e["receipts"]}
    return out


def _norm_barcode(v) -> str:
    s = str(v).strip()
    return s[:-2] if s.endswith(".0") else s


def load_grn_files(data_dir: str, pattern: str = "*grnd*.xlsx") -> List[dict]:
    """Read every GRN file under data_dir into flat cost records."""
    import pandas as pd
    records: List[dict] = []
    for path in glob.glob(os.path.join(data_dir, pattern)):
        try:
            df = pd.read_excel(path)
        except Exception:
            continue
        cols = {c.strip().lower(): c for c in df.columns}
        bc_col = cols.get("bar code") or cols.get("barcode")
        cp_col = cols.get("cost price")
        if not bc_col or not cp_col:
            continue
        q_col = cols.get("grn qty") or cols.get("qty")
        sp_col = cols.get("sp")
        v_col = cols.get("vendor code - name") or cols.get("vendor")
        for _, row in df.iterrows():
            records.append({
                "barcode": _norm_barcode(row[bc_col]),
                "cost": row[cp_col],
                "qty": row[q_col] if q_col else 1,
                "sp": row[sp_col] if sp_col else None,
                "vendor": row[v_col] if v_col else "",
            })
    return records


def load_grn_costs(data_dir: str) -> Dict[str, dict]:
    """End-to-end: read GRN files -> weighted-average cost per barcode."""
    return aggregate_costs(load_grn_files(data_dir))


def inject_costs_to_db(db_path: str, costs: Dict[str, dict]) -> dict:
    """Write GRN costs into BASIC_CP_MST.BCP_CP and STOCK_MASTER.SM_WAC by barcode."""
    conn = sqlite3.connect(db_path, timeout=60.0)
    conn.execute("PRAGMA busy_timeout=60000")
    try:
        catalog = {str(r[0]).strip() for r in conn.execute("SELECT ITM_CD FROM ITEM_MST")}
        matched = [(bc, c["cost"]) for bc, c in costs.items() if bc in catalog]
        conn.executemany(
            "UPDATE BASIC_CP_MST SET BCP_CP=? WHERE BCP_ITEM_CD=?",
            [(cost, bc) for bc, cost in matched])
        conn.executemany(
            "UPDATE STOCK_MASTER SET SM_WAC=? WHERE SM_ITM_CD=?",
            [(cost, bc) for bc, cost in matched])
        conn.commit()
        return {"grn_barcodes": len(costs), "catalog_skus": len(catalog),
                "matched": len(matched),
                "coverage_pct": round(100.0 * len(matched) / max(1, len(catalog)), 1)}
    finally:
        conn.close()


def inject_from_files(db_path: str, data_dir: str) -> dict:
    return inject_costs_to_db(db_path, load_grn_costs(data_dir))
