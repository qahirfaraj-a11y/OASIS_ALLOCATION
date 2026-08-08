"""
sample catalog loader — the real Meridian Fresh sample stock snapshot.

Reads the dept_*.xlsx exports (VENDOR_NAME, BARCODE, ITM_NAME, DEPARTMENT,
SellPrice Prev, SellPrice, STOCK) into a unified, de-duplicated catalogue used to
(a) seed a clean mock POS/ERP database with real SKUs, departments, vendors,
prices and on-hand stock, and (b) project the vault's supplier affinity prior down
to departments.

Pure helpers (normalise/dedupe/vendor_departments) are unit tested; load_catalog
is the pandas/Excel integration.
"""

from __future__ import annotations

import glob
import os
from collections import Counter, defaultdict
from typing import Dict, List


def _norm(s) -> str:
    return str(s).strip() if s is not None else ""


def normalise_rows(raw: List[dict]) -> List[dict]:
    """Map raw spreadsheet rows to canonical catalogue records."""
    out: List[dict] = []
    for r in raw:
        itm = _norm(r.get("BARCODE"))
        if not itm:
            continue
        try:
            price = float(r.get("SellPrice") or 0)
        except (TypeError, ValueError):
            price = 0.0
        try:
            stock = float(r.get("STOCK") or 0)
        except (TypeError, ValueError):
            stock = 0.0
        out.append({
            "itm_cd": itm,
            "name": _norm(r.get("ITM_NAME")),
            "dept": _norm(r.get("DEPARTMENT")) or "UNKNOWN",
            "vendor": _norm(r.get("VENDOR_NAME")) or "UNKNOWN",
            "price": price,
            "stock": stock,
        })
    return out


def dedupe(rows: List[dict]) -> List[dict]:
    """One record per itm_cd — keep the highest-stock (most informative) row."""
    best: Dict[str, dict] = {}
    for r in rows:
        cur = best.get(r["itm_cd"])
        if cur is None or r["stock"] > cur["stock"]:
            best[r["itm_cd"]] = r
    return list(best.values())


def vendor_departments(rows: List[dict]) -> Dict[str, str]:
    """vendor -> its primary department (where it carries the most SKUs)."""
    by_vendor: Dict[str, Counter] = defaultdict(Counter)
    for r in rows:
        by_vendor[r["vendor"]][r["dept"]] += 1
    return {v: depts.most_common(1)[0][0] for v, depts in by_vendor.items() if depts}


def load_catalog(data_dir: str, pattern: str = "dept_*.xlsx") -> List[dict]:
    """Read + merge + dedupe all dept_*.xlsx under data_dir into catalogue rows."""
    import pandas as pd
    files = sorted(glob.glob(os.path.join(data_dir, pattern)))
    if not files:
        raise FileNotFoundError(f"no catalog files matching {pattern} in {data_dir}")
    frames = [pd.read_excel(f) for f in files]
    raw = pd.concat(frames, ignore_index=True).to_dict("records")
    return dedupe(normalise_rows(raw))


def catalog_summary(rows: List[dict]) -> dict:
    return {
        "skus": len(rows),
        "in_stock": sum(1 for r in rows if r["stock"] > 0),
        "departments": len({r["dept"] for r in rows}),
        "vendors": len({r["vendor"] for r in rows}),
        "stock_units": round(sum(r["stock"] for r in rows if r["stock"] > 0), 0),
    }
