"""
Real demand from the anchor store's monthly sales (the *_cash.xlsx exports).

These ten files are the REAL monthly units-sold per SKU at the anchor store (org 027). We use
them to derive a genuine Average-Daily-Sales (ADS) baseline instead of simulated
history, then seed it into the snapshot so OASIS's ordering normalises demand
against real numbers.

The cash files key items by a 13-digit internal "Itm Code" that does NOT match the
catalogue's barcodes, so we bridge by **normalised item name** (stripping the
leading department code and embedded short codes like AIR223/ELY1006, and the
per-department/grand ``Total`` rows). Coverage is reported honestly.

ADS is reconstructed through the existing adapter pipeline: we write fractional-qty
history lines across the three recency buckets so _calc_weighted_ads returns
exactly the real ADS. Stock is untouched; today is left empty (start of day).

Pure helpers (normalise_name / derive_ads) are unit-tested; load/seed are I/O.
"""

from __future__ import annotations

import glob
import os
import re
import sqlite3
from datetime import datetime, timedelta
from typing import Dict, Tuple

_CODE = re.compile(r"\b[A-Z]{2,5}[0-9]{2,6}\b")   # embedded short codes: AIR223, ELY1006, DZT1142


def normalise_name(s) -> str:
    """Canonical item name for cash↔catalogue matching."""
    s = str(s).upper().replace("#", "")
    s = _CODE.sub("", s)                 # drop embedded short codes
    s = re.sub(r"^[0-9]+\s+", "", s)     # drop leading department code
    s = re.sub(r"[^A-Z0-9 ]", " ", s)    # punctuation → space (handles "(POUCH)")
    return re.sub(r"\s+", " ", s).strip()


def _is_total_row(name: str) -> bool:
    return str(name).strip().lower() in ("total", "grand total", "")


def load_cash_file(path: str):
    """Read one *_cash.xlsx into a {item, qty} frame, robust to layout/header row."""
    import pandas as pd
    raw = pd.read_excel(path, header=None)
    hrow = None
    for i in range(min(8, len(raw))):
        vals = [str(x).strip().lower() for x in raw.iloc[i].tolist()]
        if "item name" in vals and "qty" in vals:
            hrow = i
            break
    if hrow is None:
        return pd.DataFrame(columns=["item", "qty"])
    hv = [str(x).strip().lower() for x in raw.iloc[hrow].tolist()]
    ii, iq = hv.index("item name"), hv.index("qty")
    d = raw.iloc[hrow + 1:]
    df = pd.DataFrame({"item": d.iloc[:, ii],
                       "qty": pd.to_numeric(d.iloc[:, iq], errors="coerce")})
    df = df.dropna(subset=["item", "qty"])
    return df[~df["item"].map(_is_total_row)]


def load_monthly_demand(cash_dir: str, pattern: str = "*_cash.xlsx"):
    """Aggregate units-sold by normalised name across all monthly files.

    Returns (demand {normname: total_qty}, months_loaded, files_found).
    """
    files = sorted(glob.glob(os.path.join(cash_dir, pattern)))
    demand: Dict[str, float] = {}
    months = 0
    for f in files:
        df = load_cash_file(f)
        if df.empty:
            continue
        months += 1
        for it, q in zip(df["item"], df["qty"]):
            n = normalise_name(it)
            if n:
                demand[n] = demand.get(n, 0.0) + float(q)
    return demand, months, len(files)


def match_to_catalog(demand: Dict[str, float], db_path: str) -> Tuple[Dict[str, float], dict]:
    """Map normalised-name demand onto catalogue ITM_CDs. Returns (qty_by_itm, coverage)."""
    conn = sqlite3.connect(db_path)
    try:
        cat: Dict[str, str] = {}
        for itm, name in conn.execute("SELECT ITM_CD, ITM_LONG_NAME FROM ITEM_MST"):
            cat.setdefault(normalise_name(name), str(itm))
    finally:
        conn.close()
    matched: Dict[str, float] = {}
    covered = 0.0
    total = sum(demand.values())
    for n, q in demand.items():
        itm = cat.get(n)
        if itm:
            matched[itm] = matched.get(itm, 0.0) + q
            covered += q
    cov = {"cash_skus": len(demand), "matched_skus": len(matched),
           "qty_total": round(total), "qty_covered": round(covered),
           "coverage_pct": round(100 * covered / max(1.0, total), 1)}
    return matched, cov


def derive_ads(matched_qty: Dict[str, float], months: int,
               days_per_month: float = 30.4) -> Dict[str, float]:
    """Average daily sales = total units / observed days (months × ~30.4)."""
    days = max(1.0, months * days_per_month)
    return {itm: q / days for itm, q in matched_qty.items() if q > 0}


def seed_real_demand(db_path: str, ads_map: Dict[str, float], org: str = "ORG001") -> dict:
    """Write history so the adapter's weighted ADS equals the real ADS.

    For each SKU we emit one fractional-qty line in each of the three recency
    buckets (mid-points −15/−45/−75 days) sized ads×30, so every bucket's per-day
    rate equals ADS and the 60/30/10 weighting returns ADS exactly. Stock is NOT
    decremented; today is left empty.
    """
    from .pos_injector import SaleLine, build_bill
    conn = sqlite3.connect(db_path, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    try:
        meta = {str(r[0]): (str(r[1] or ""), float(r[2] or 0))
                for r in conn.execute(
                    "SELECT i.ITM_CD, i.ITM_LONG_NAME, COALESCE(sp.BSP_SP, 100.0) "
                    "FROM ITEM_MST i LEFT JOIN BASIC_SP_MST sp "
                    "ON sp.BSP_ITEM_CD = i.ITM_CD AND sp.BSP_ORG_CD = ?", (org,))}
        today = datetime.now().date()
        hdr_rows, dtl_rows = [], []
        hdr_cols = dtl_cols = None
        bills = lines = seq = 0

        def flush(bdate, sale_lines, seq):
            nonlocal hdr_cols, dtl_cols, bills, lines
            if not sale_lines:
                return
            bill_no = f"REAL{bdate.replace('-', '')}{seq:06d}"
            hdr, dtl = build_bill(org, bill_no, bdate, sale_lines)
            if hdr_cols is None:
                hdr_cols, dtl_cols = list(hdr), list(dtl[0])
            hdr_rows.append(tuple(hdr[c] for c in hdr_cols))
            dtl_rows.extend(tuple(x[c] for c in dtl_cols) for x in dtl)
            bills += 1
            lines += len(sale_lines)

        for bucket_mid in (15, 45, 75):
            bdate = (today - timedelta(days=bucket_mid)).strftime("%Y-%m-%d")
            batch = []
            for itm, ads in ads_map.items():
                qty = round(ads * 30.0, 3)        # this bucket's 30-day volume
                if qty <= 0 or itm not in meta:
                    continue
                name, price = meta[itm]
                batch.append(SaleLine(itm, name, qty, price))
                if len(batch) >= 100:
                    seq += 1
                    flush(bdate, batch, seq)
                    batch = []
            if batch:
                seq += 1
                flush(bdate, batch, seq)

        if hdr_cols:
            conn.executemany(
                f"INSERT INTO POS_SALES_HDR ({','.join(hdr_cols)}) "
                f"VALUES ({','.join(['?'] * len(hdr_cols))})", hdr_rows)
            conn.executemany(
                f"INSERT INTO POS_SALES_DTL ({','.join(dtl_cols)}) "
                f"VALUES ({','.join(['?'] * len(dtl_cols))})", dtl_rows)
            conn.commit()
        return {"org": org, "skus": len(ads_map), "bills": bills, "lines": lines}
    finally:
        conn.close()


def seed_real_demand_from_files(db_path: str, cash_dir: str, org: str = "ORG001") -> dict:
    """End-to-end: load monthly files → match catalogue → derive ADS → seed."""
    demand, months, files = load_monthly_demand(cash_dir)
    matched, cov = match_to_catalog(demand, db_path)
    ads = derive_ads(matched, months)
    seeded = seed_real_demand(db_path, ads, org=org)
    return {"files": files, "months": months, **cov, "seeded": seeded}
