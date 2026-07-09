"""
SKU-wise deep dive — every SKU judged: velocity, capital, retain-or-cut.

Joins three real data sources per SKU:
  * a FRESH stock snapshot (per-department Excel exports: barcode, price, stock);
  * real velocity from the monthly units-sold files (complete months only);
  * real unit cost from GRN history (quantity-weighted average).

…and issues an explainable verdict per SKU:

  RESTOCK NOW     sells but shelf is empty — lost revenue every day
  RETAIN-CORE     proven seller with thin cover (<7d) — protect supply
  RETAIN          earning its shelf — no action
  REDUCE          seller but overstocked (>120d cover) — stop ordering, sell down
  REVIEW          sporadic (≤2 selling months, <6 units) — candidate to rationalise
  CLEAR & DELIST  stocked but ZERO sales all period — recover the capital
  DELIST (paper)  no sales, no stock — remove from range administratively

Capital is valued at real GRN cost where known (true cash trapped), else retail
(flagged). The output serves the executive level (portfolio capital by verdict,
GP concentration) down to operations (per-action SKU lists + a full appendix of
every SKU).

Pure logic (classify_sku, build_sku_deepdive) is unit-tested; the writer renders
Markdown + full CSV + optional PDF.
"""

from __future__ import annotations

import csv
import os
import re
import statistics
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from .real_demand import load_cash_file, normalise_name

_MONTHS = [("jan", "Jan"), ("feb", "Feb"), ("mar", "Mar"), ("apri", "Apr"),
           ("may", "May"), ("jun", "Jun"), ("jul", "Jul"), ("aug", "Aug"),
           ("sep", "Sep"), ("oct", "Oct"), ("nov", "Nov"), ("dec", "Dec")]
_PARTIAL_FRACTION = 0.30
_DEPT_HINTS = (("spirit", "SPIRITS"), ("wine", "WINES"), ("beer", "BEER"),
               ("cider", "CIDERS"))


def infer_dept(path: str) -> str:
    base = os.path.basename(path).lower()
    for hint, dept in _DEPT_HINTS:
        if hint in base:
            return dept
    return "UNKNOWN"


def list_departments_from_db(db_path: str, org_cd: Optional[str] = None) -> List[dict]:
    """Every DEPARTMENT that has stocked SKUs — one row per (dept, SKU count, in-stock).

    Powers the Intelligence Console's department picker: user sees exactly
    which sections have data before running the deep-dive.
    """
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=15.0)
    try:
        params: List = []
        where = "COALESCE(i.DEPARTMENT,'') <> ''"
        if org_cd:
            where += " AND s.SM_ORG_CD = ?"
            params.append(org_cd)
        rows = conn.execute(
            "SELECT i.DEPARTMENT AS dept, COUNT(*) AS skus, "
            "  SUM(CASE WHEN COALESCE(s.SM_QTY,0)>0 THEN 1 ELSE 0 END) AS in_stock "
            "FROM ITEM_MST i LEFT JOIN STOCK_MASTER s ON s.SM_ITM_CD = i.ITM_CD "
            f"WHERE {where} GROUP BY i.DEPARTMENT ORDER BY 2 DESC", params).fetchall()
        return [{"dept": r[0], "skus": int(r[1] or 0),
                 "in_stock": int(r[2] or 0)} for r in rows]
    finally:
        conn.close()


def snapshot_from_db(db_path: str, departments: List[str],
                     org_cd: Optional[str] = None) -> List[dict]:
    """Live-DB variant of load_snapshot: build the SKU list from ITEM_MST +
    STOCK_MASTER + BASIC_SP_MST + BASIC_CP_MST for one or more departments.

    Returns records shaped like load_snapshot(), so build_sku_deepdive() runs
    unchanged. Cost from BCP is used as the *snapshot* cost — real GRN WAC
    still overrides when available via load_grn_costs() upstream.
    """
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        placeholders = ",".join("?" * len(departments))
        params: List = list(departments)
        org_where = ""
        if org_cd:
            org_where = " AND s.SM_ORG_CD = ? AND sp.BSP_ORG_CD = ? "
            params += [org_cd, org_cd]
        rows = conn.execute(
            "SELECT i.ITM_CD AS barcode, "
            "       COALESCE(i.ITM_LONG_NAME, i.ITM_CD) AS name, "
            "       COALESCE(sup.SUPPLIER_NAME, '') AS vendor, "
            "       i.DEPARTMENT AS dept, "
            "       COALESCE(sp.BSP_SP, 0) AS price, "
            "       COALESCE(s.SM_QTY, 0) AS stock "
            "FROM ITEM_MST i "
            "LEFT JOIN STOCK_MASTER s ON s.SM_ITM_CD = i.ITM_CD "
            "LEFT JOIN BASIC_SP_MST sp ON sp.BSP_ITEM_CD = i.ITM_CD "
            "LEFT JOIN SUPPLIER_MST sup ON sup.SUPPLIER_CD = i.SUPPLIER_CD "
            f"WHERE UPPER(i.DEPARTMENT) IN ({placeholders.upper() if False else placeholders}){org_where}",
            params).fetchall()
        return [{"barcode": str(r[0]), "name": str(r[1] or ""),
                 "vendor": str(r[2] or ""), "dept": str(r[3] or ""),
                 "price": float(r[4] or 0), "stock": max(0.0, float(r[5] or 0))}
                for r in rows]
    finally:
        conn.close()


def demand_from_db(db_path: str, org_cd: Optional[str] = None,
                   days: int = 274) -> Tuple[Dict[str, List[float]], int]:
    """{barcode: [qty per complete month]} + n_months, straight from POS_SALES_DTL.

    Replaces monthly_demand_by_name() for live-DB clients whose demand lives
    in the system already (not in month-of-year Excel files).
    """
    import sqlite3
    from datetime import datetime, timedelta
    conn = sqlite3.connect(db_path, timeout=30.0)
    try:
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        params: List = [cutoff]
        org_where = ""
        if org_cd:
            org_where = " AND d.ORG_CD = ?"
            params.append(org_cd)
        rows = conn.execute(
            "SELECT d.ITM_CD, strftime('%Y-%m', d.BILL_DT) AS mo, SUM(d.QTY) "
            "FROM POS_SALES_DTL d "
            f"WHERE d.BILL_DT >= ? AND COALESCE(d.VOID_FLAG,'F') <> 'T'{org_where} "
            "GROUP BY d.ITM_CD, mo", params).fetchall()
        by_sku: Dict[str, Dict[str, float]] = {}
        months = set()
        for itm, mo, qty in rows:
            by_sku.setdefault(str(itm), {})[str(mo)] = float(qty or 0)
            months.add(str(mo))
        month_order = sorted(months)
        series = {itm: [d.get(m, 0.0) for m in month_order] for itm, d in by_sku.items()}
        return series, len(month_order)
    finally:
        conn.close()


def build_sku_deepdive_live(db_path: str, departments: List[str],
                            org_cd: Optional[str] = None,
                            costs: Optional[Dict[str, dict]] = None) -> dict:
    """End-to-end live-DB deep-dive: pick departments -> read stock+sales+cost
    from the installed OASIS DB -> produce the same verdict portfolio as
    build_sku_deepdive() from snapshot files."""
    snapshot = snapshot_from_db(db_path, departments, org_cd=org_cd)
    series, n_months = demand_from_db(db_path, org_cd=org_cd)
    return _score_snapshot(snapshot, series, n_months, costs or {})


def _score_snapshot(snapshot: List[dict], series: Dict[str, List[float]],
                    n_months: int, costs: Dict[str, dict]) -> dict:
    """Shared scoring core — takes ready-made snapshot + monthly series."""
    from datetime import datetime, timedelta   # noqa: F401 (parity with above)
    days = max(1.0, n_months * 30.4)
    skus: List[dict] = []
    for row in snapshot:
        monthly = series.get(row["barcode"], [])
        units = sum(monthly)
        months_sold = sum(1 for q in monthly if q > 0)
        ads = units / days
        price = row["price"]
        stock = row["stock"]
        c = costs.get(row["barcode"])
        cost = float(c["cost"]) if c and c.get("cost") else None
        margin = (100.0 * (price - cost) / price) if (cost is not None and price > 0) else None
        capital = stock * (cost if cost is not None else price)
        cover = (stock / ads) if ads > 0 else None
        verdict, reason = classify_sku(ads, stock, cover, months_sold, units, n_months)
        gp_day = ads * (price - cost) if cost is not None else None
        skus.append({
            "Item": row["name"][:48], "Dept": row["dept"], "Vendor": row["vendor"][:26],
            "Barcode": row["barcode"], "Price": round(price, 0),
            "Cost": round(cost, 1) if cost is not None else None,
            "Margin %": round(margin, 1) if margin is not None else None,
            "ADS": round(ads, 3), "Units (period)": round(units, 0),
            "Months Sold": months_sold, "Stock": round(stock, 0),
            "Days Cover": round(cover, 1) if cover is not None else None,
            "Capital (KES)": round(capital, 0),
            "Capital Basis": "cost" if cost is not None else "retail",
            "Rev/Day": round(ads * price, 0),
            "GP/Day": round(gp_day, 0) if gp_day is not None else None,
            "Verdict": verdict, "Why": reason,
        })
    by_verdict: Dict[str, dict] = {}
    for s in skus:
        e = by_verdict.setdefault(s["Verdict"], {"skus": 0, "capital": 0.0, "rev_day": 0.0})
        e["skus"] += 1
        e["capital"] += s["Capital (KES)"]
        e["rev_day"] += s["Rev/Day"]
    gp_rows = sorted((s for s in skus if s["GP/Day"]), key=lambda s: s["GP/Day"], reverse=True)
    total_gp_day = sum(s["GP/Day"] for s in gp_rows)
    top20_gp = sum(s["GP/Day"] for s in gp_rows[:20])
    total_capital = sum(s["Capital (KES)"] for s in skus)
    sellers = [s for s in skus if s["ADS"] > 0]
    return {
        "skus": skus, "n_skus": len(skus), "n_sellers": len(sellers),
        "months_used": n_months, "partial_months": [],
        "total_capital": round(total_capital, 0),
        "by_verdict": {k: {"skus": v["skus"], "capital": round(v["capital"], 0),
                           "rev_day": round(v["rev_day"], 0)}
                       for k, v in sorted(by_verdict.items())},
        "gp_day_total": round(total_gp_day, 0),
        "gp_top20_pct": round(100.0 * top20_gp / total_gp_day, 1) if total_gp_day else 0.0,
        "lost_rev_day": round(sum(s["Rev/Day"] for s in skus if s["Verdict"] == "RESTOCK NOW"), 0),
        "clear_capital": round(sum(s["Capital (KES)"] for s in skus if s["Verdict"] == "CLEAR & DELIST"), 0),
        "reduce_capital": round(sum(s["Capital (KES)"] for s in skus if s["Verdict"] == "REDUCE"), 0),
    }


def load_snapshot(files: List[str]) -> Tuple[List[dict], int]:
    """Read the snapshot exports; dedupe by barcode (first file wins).

    Returns (rows, duplicates_dropped). Department is inferred per file.
    """
    import pandas as pd
    seen: Dict[str, dict] = {}
    dupes = 0
    for path in files:
        dept = infer_dept(path)
        df = pd.read_excel(path)
        cols = {str(c).strip().upper(): c for c in df.columns}
        for _, r in df.iterrows():
            bc = str(r[cols["BARCODE"]]).strip()
            bc = bc[:-2] if bc.endswith(".0") else bc
            if not bc or bc.lower() == "nan":
                continue
            if bc in seen:
                dupes += 1
                continue
            try:
                price = float(r[cols["SELLPRICE"]] or 0)
            except (TypeError, ValueError, KeyError):
                price = 0.0
            try:
                stock = float(r[cols["STOCK"]] or 0)
            except (TypeError, ValueError, KeyError):
                stock = 0.0
            seen[bc] = {"barcode": bc, "name": str(r[cols["ITM_NAME"]]).strip(),
                        "vendor": str(r[cols["VENDOR_NAME"]]).strip(),
                        "dept": dept, "price": price, "stock": max(0.0, stock)}
    return list(seen.values()), dupes


def monthly_demand_by_name(cash_dir: str) -> Tuple[Dict[str, List[float]], List[str], List[str]]:
    """{normname: [qty per complete month]} + (complete_months, partial_months)."""
    labels, paths = [], []
    for prefix, label in _MONTHS:
        p = os.path.join(cash_dir, f"{prefix}_cash.xlsx")
        if os.path.exists(p):
            labels.append(label)
            paths.append(p)
    frames = [load_cash_file(p) for p in paths]
    totals = [float(df["qty"].sum()) for df in frames]
    median_total = statistics.median(totals) if totals else 0.0
    partial = [labels[i] for i, t in enumerate(totals)
               if median_total and t < _PARTIAL_FRACTION * median_total]
    complete = [m for m in labels if m not in partial]
    series: Dict[str, List[float]] = {}
    idx = 0
    for label, df in zip(labels, frames):
        if label in partial:
            continue
        for item, qty in zip(df["item"], df["qty"]):
            nn = normalise_name(item)
            if not nn:
                continue
            arr = series.setdefault(nn, [0.0] * len(complete))
            arr[idx] += float(qty)
        idx += 1
    return series, complete, partial


def classify_sku(ads: float, stock: float, days_cover: Optional[float],
                 months_sold: int, units_total: float,
                 n_months: int) -> Tuple[str, str]:
    """(verdict, reason) — the explainable retain/cut rule set."""
    if ads > 0 and stock <= 0:
        return "RESTOCK NOW", "proven seller with empty shelf"
    if ads <= 0 and stock <= 0:
        return "DELIST (paper)", f"no sales in {n_months} months, nothing on hand"
    if ads <= 0:
        return "CLEAR & DELIST", f"zero sales in {n_months} months with stock on hand"
    if months_sold <= 2 and units_total < 6:
        return "REVIEW", (f"sporadic: sold in {months_sold}/{n_months} months "
                          f"({units_total:.0f} units total)")
    if days_cover is not None and days_cover > 120:
        return "REDUCE", f"{days_cover:.0f} days of cover — stop ordering, sell down"
    if days_cover is not None and days_cover < 7:
        return "RETAIN-CORE", f"core seller at {days_cover:.1f} days cover — protect supply"
    return "RETAIN", "earning its shelf"


def build_sku_deepdive(snapshot: List[dict], cash_dir: str,
                       costs: Optional[Dict[str, dict]] = None) -> dict:
    """Every SKU scored + verdict; portfolio rollups for the exec layer."""
    costs = costs or {}
    series, complete, partial = monthly_demand_by_name(cash_dir)
    days = max(1.0, len(complete) * 30.4)
    n_months = len(complete)

    skus: List[dict] = []
    for row in snapshot:
        nn = normalise_name(row["name"])
        monthly = series.get(nn, [])
        units = sum(monthly)
        months_sold = sum(1 for q in monthly if q > 0)
        ads = units / days
        price = row["price"]
        stock = row["stock"]
        c = costs.get(row["barcode"])
        cost = float(c["cost"]) if c and c.get("cost") else None
        margin = (100.0 * (price - cost) / price) if (cost is not None and price > 0) else None
        capital = stock * (cost if cost is not None else price)
        cover = (stock / ads) if ads > 0 else None
        verdict, reason = classify_sku(ads, stock, cover, months_sold, units, n_months)
        gp_day = ads * (price - cost) if cost is not None else None
        skus.append({
            "Item": row["name"][:48], "Dept": row["dept"], "Vendor": row["vendor"][:26],
            "Barcode": row["barcode"], "Price": round(price, 0),
            "Cost": round(cost, 1) if cost is not None else None,
            "Margin %": round(margin, 1) if margin is not None else None,
            "ADS": round(ads, 3), "Units (period)": round(units, 0),
            "Months Sold": months_sold, "Stock": round(stock, 0),
            "Days Cover": round(cover, 1) if cover is not None else None,
            "Capital (KES)": round(capital, 0),
            "Capital Basis": "cost" if cost is not None else "retail",
            "Rev/Day": round(ads * price, 0),
            "GP/Day": round(gp_day, 0) if gp_day is not None else None,
            "Verdict": verdict, "Why": reason,
        })

    # ── portfolio rollups ────────────────────────────────────────────────
    by_verdict: Dict[str, dict] = {}
    for s in skus:
        e = by_verdict.setdefault(s["Verdict"], {"skus": 0, "capital": 0.0,
                                                 "rev_day": 0.0})
        e["skus"] += 1
        e["capital"] += s["Capital (KES)"]
        e["rev_day"] += s["Rev/Day"]
    total_capital = sum(s["Capital (KES)"] for s in skus)
    gp_rows = sorted((s for s in skus if s["GP/Day"]), key=lambda s: s["GP/Day"],
                     reverse=True)
    total_gp_day = sum(s["GP/Day"] for s in gp_rows)
    top20_gp = sum(s["GP/Day"] for s in gp_rows[:20])
    sellers = [s for s in skus if s["ADS"] > 0]

    return {
        "skus": skus, "n_skus": len(skus), "n_sellers": len(sellers),
        "months_used": n_months, "partial_months": partial,
        "total_capital": round(total_capital, 0),
        "by_verdict": {k: {"skus": v["skus"], "capital": round(v["capital"], 0),
                           "rev_day": round(v["rev_day"], 0)}
                       for k, v in sorted(by_verdict.items())},
        "gp_day_total": round(total_gp_day, 0),
        "gp_top20_pct": round(100.0 * top20_gp / total_gp_day, 1) if total_gp_day else 0.0,
        "lost_rev_day": round(sum(s["Rev/Day"] for s in skus
                                  if s["Verdict"] == "RESTOCK NOW"), 0),
        "clear_capital": round(sum(s["Capital (KES)"] for s in skus
                                   if s["Verdict"] == "CLEAR & DELIST"), 0),
        "reduce_capital": round(sum(s["Capital (KES)"] for s in skus
                                    if s["Verdict"] == "REDUCE"), 0),
    }


def _mdtable(rows: List[dict], cols: List[str]) -> str:
    head = "| " + " | ".join(cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n"
    body = "".join("| " + " | ".join("-" if r.get(c) is None else str(r.get(c, ""))
                                     for c in cols) + " |\n" for r in rows)
    return head + body


def write_sku_deepdive(files: List[str], cash_dir: str, data_dir: str,
                       out_dir: str, tenant: str = "", section: str = "alcohol",
                       pdf: bool = False) -> dict:
    snapshot, dupes = load_snapshot(files)
    costs = None
    try:
        from .grn_cost import load_grn_costs
        costs = load_grn_costs(data_dir)
    except Exception:
        pass
    a = build_sku_deepdive(snapshot, cash_dir, costs=costs)
    skus = a["skus"]

    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    slug = re.sub(r"\W+", "_", section.lower())
    md_path = os.path.join(out_dir, f"OASIS_SKU_DeepDive_{slug}_{stamp}.md")
    csv_path = os.path.join(out_dir, f"OASIS_SKU_DeepDive_{slug}_{stamp}.csv")

    verdict_rows = [{"Verdict": k, "SKUs": v["skus"],
                     "Capital (KES)": f"{v['capital']:,.0f}",
                     "Rev/Day": f"{v['rev_day']:,.0f}"}
                    for k, v in a["by_verdict"].items()]
    pick = lambda v: [s for s in skus if s["Verdict"] == v]  # noqa: E731
    restock = sorted(pick("RESTOCK NOW"), key=lambda s: s["Rev/Day"], reverse=True)
    clear = sorted(pick("CLEAR & DELIST"), key=lambda s: s["Capital (KES)"], reverse=True)
    reduce_ = sorted(pick("REDUCE"), key=lambda s: s["Capital (KES)"], reverse=True)
    core = sorted(pick("RETAIN-CORE"), key=lambda s: s["Rev/Day"], reverse=True)

    md = f"""# O.A.S.I.S. SKU Deep Dive — {section.title()} · {tenant or 'Chandarana Rhapta'}

*Snapshot of {a['n_skus']:,} SKUs (deduplicated across {len(files)} files, {dupes}
duplicate lines dropped) crossed with {a['months_used']} complete months of real
sales and real GRN unit costs. Generated {stamp}.*

## Executive summary
| Metric | Value |
|---|---|
| SKUs on file | {a['n_skus']:,} ({a['n_sellers']:,} with sales in the period) |
| Capital on shelf | KES {a['total_capital']:,.0f} |
| **Recoverable via clearance** (zero-sale stock) | **KES {a['clear_capital']:,.0f}** |
| **Overstock capital to sell down** | **KES {a['reduce_capital']:,.0f}** |
| **Lost revenue from empty-shelf sellers** | **KES {a['lost_rev_day']:,.0f} / day** |
| Section gross profit run-rate | KES {a['gp_day_total']:,.0f} / day |
| GP concentration | top 20 SKUs = {a['gp_top20_pct']}% of section GP |

## Portfolio by verdict
{_mdtable(verdict_rows, ['Verdict', 'SKUs', 'Capital (KES)', 'Rev/Day'])}

## OPS ACTION 1 — Restock now (empty shelf, proven demand)
{_mdtable(restock[:20], ['Item', 'Dept', 'ADS', 'Rev/Day', 'Months Sold'])}

## OPS ACTION 2 — Clear & delist (zero sales, capital trapped)
{_mdtable(clear[:20], ['Item', 'Dept', 'Stock', 'Capital (KES)', 'Capital Basis'])}

## OPS ACTION 3 — Reduce (overstocked sellers, stop ordering)
{_mdtable(reduce_[:20], ['Item', 'Dept', 'ADS', 'Stock', 'Days Cover', 'Capital (KES)'])}

## OPS ACTION 4 — Protect supply (core sellers under 7 days cover)
{_mdtable(core[:20], ['Item', 'Dept', 'ADS', 'Stock', 'Days Cover', 'Rev/Day'])}

## Appendix — every SKU (full detail in the CSV)
{_mdtable(sorted(skus, key=lambda s: s['Rev/Day'], reverse=True),
          ['Item', 'Dept', 'ADS', 'Days Cover', 'Margin %', 'Capital (KES)', 'Verdict'])}

---
*Verdict rules: RESTOCK NOW = sells, zero stock. RETAIN-CORE = <7d cover.
REDUCE = >120d cover. REVIEW = sold in ≤2 months, <6 units. CLEAR & DELIST =
zero sales with stock (capital at real GRN cost where known — 'retail' basis
where no GRN cost exists). Velocity from the {a['months_used']} complete sales
months provided{'' if not a['partial_months'] else ' (excl. partial: ' + ', '.join(a['partial_months']) + ')'};
snapshot stock as of the export date.*
"""
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(skus[0].keys()) if skus else ["Item"])
        w.writeheader()
        w.writerows(skus)

    pdf_path = None
    if pdf:
        try:
            from .report_pdf import markdown_to_pdf
            pdf_path = markdown_to_pdf(md, os.path.splitext(md_path)[0] + ".pdf",
                                       title=f"OASIS SKU Deep Dive - {section.title()}")
        except Exception as e:
            print(f"[sku-deepdive] PDF render skipped: {e}")
    return {"markdown": md_path, "csv": csv_path, "pdf": pdf_path,
            **{k: v for k, v in a.items() if k != "skus"}}
