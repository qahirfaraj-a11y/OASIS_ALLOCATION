"""
Category deep-dive report — a comprehensive analysis of one merchandising
section from the real Rhapta data (catalogue snapshot + monthly sales).

Answers the questions a category buyer actually asks:
  * how big is the section (SKUs, stock, capital) and how does it sell;
  * which SKUs earn their shelf (top movers, days-of-cover) vs. which are dead;
  * what's selling but out of stock (ghost sellers — lost revenue);
  * who supplies it and how concentrated the spend is;
  * how it moves through the year (seasonality), robust to partial-month files.

    python entrypoint.py --mode category-report --category alcohol

Pure aggregation (build_category_report) is unit-tested; write_category_report
renders the Markdown + CSV.
"""

from __future__ import annotations

import csv
import os
import statistics
from datetime import datetime
from typing import Dict, List, Optional

from .real_demand import load_cash_file, normalise_name

#: named sections → the catalogue departments that compose them
CATEGORY_PRESETS: Dict[str, List[str]] = {
    "alcohol": ["WINES", "SPIRITS", "BEER", "CIDERS"],
    "dairy": ["FRESH MILK", "YOGHURT", "BUTTER", "CHEESE", "UHT MILK"],
    "beverages": ["SODA", "ENERGY DRINKS", "CANNED DRINKS", "READY TO DRINK",
                  "MINERAL WATER", "BEVERAGES"],
}

#: month-file prefix → calendar order (Rhapta uses 'apri' for April)
_MONTHS = [("jan", "Jan"), ("feb", "Feb"), ("mar", "Mar"), ("apri", "Apr"),
           ("may", "May"), ("jun", "Jun"), ("jul", "Jul"), ("aug", "Aug"),
           ("sep", "Sep"), ("oct", "Oct"), ("nov", "Nov"), ("dec", "Dec")]

_PARTIAL_FRACTION = 0.30   # a month whose total is < 30% of the median = partial export


def _month_files(cash_dir: str):
    for prefix, label in _MONTHS:
        path = os.path.join(cash_dir, f"{prefix}_cash.xlsx")
        if os.path.exists(path):
            yield label, path


def build_category_report(catalog_rows: List[dict], cash_dir: str,
                          departments: List[str], top: int = 20,
                          costs: Optional[Dict[str, dict]] = None) -> dict:
    """Cross the section's catalogue with its real monthly sales. Pure-ish
    (reads the monthly Excel files; all aggregation is deterministic).

    costs: optional {barcode: {"cost": float}} from GRN — when present, movers
    gain real margin and gross profit, and dead stock is valued at cost.
    """
    costs = costs or {}

    def _cost(row):
        c = costs.get(str(row.get("itm_cd", "")).strip())
        return float(c["cost"]) if c and c.get("cost") else None

    dept_set = {d.upper() for d in departments}
    cat = [r for r in catalog_rows if str(r.get("dept", "")).upper() in dept_set]
    by_name: Dict[str, dict] = {}
    for r in cat:
        by_name.setdefault(normalise_name(r.get("name", "")), r)

    # ── monthly series, robust to truncated files ───────────────────────
    monthly_all: Dict[str, float] = {}      # section units per month (raw)
    monthly_qty_by_name: Dict[str, float] = {}
    file_totals: Dict[str, float] = {}      # ALL-dept total per month → partial detection
    for label, path in _month_files(cash_dir):
        df = load_cash_file(path)
        file_totals[label] = float(df["qty"].sum())
        month_units = 0.0
        for item, qty in zip(df["item"], df["qty"]):
            nn = normalise_name(item)
            if nn in by_name:
                monthly_all[label] = monthly_all.get(label, 0.0) + float(qty)
                monthly_qty_by_name[nn] = monthly_qty_by_name.get(nn, 0.0) + float(qty)
                month_units += float(qty)

    median_total = statistics.median(file_totals.values()) if file_totals else 0.0
    partial = {m for m, t in file_totals.items()
               if median_total and t < _PARTIAL_FRACTION * median_total}
    complete_months = [m for m in monthly_all if m not in partial]
    # ADS uses complete months only so a truncated export can't understate demand
    complete_units: Dict[str, float] = {}
    if complete_months:
        for label, path in _month_files(cash_dir):
            if label in partial:
                continue
            df = load_cash_file(path)
            for item, qty in zip(df["item"], df["qty"]):
                nn = normalise_name(item)
                if nn in by_name:
                    complete_units[nn] = complete_units.get(nn, 0.0) + float(qty)
    days = max(1.0, len(complete_months) * 30.4)

    # ── per-SKU economics ───────────────────────────────────────────────
    movers, dead, ghost = [], [], []
    dead_value = 0.0
    revenue_period = 0.0
    gross_profit_period = 0.0
    rev_with_cost = 0.0            # revenue of SKUs that HAVE a GRN cost (margin base)
    skus_with_cost = 0
    matched_names = set()
    for nn, row in by_name.items():
        qty_complete = complete_units.get(nn, 0.0)
        qty_total = monthly_qty_by_name.get(nn, 0.0)
        price = float(row.get("price", 0) or 0)
        stock = float(row.get("stock", 0) or 0)
        cost = _cost(row)
        if cost is not None:
            skus_with_cost += 1
        ads = qty_complete / days
        revenue_period += qty_total * price
        margin_pct = (100.0 * (price - cost) / price) if (cost is not None and price > 0) else None
        if cost is not None:
            gross_profit_period += qty_total * (price - cost)
            rev_with_cost += qty_total * price
        if qty_total > 0:
            matched_names.add(nn)
            entry = {"Item": (row.get("name") or "")[:44], "Dept": row.get("dept", ""),
                     "Vendor": (row.get("vendor") or "")[:24], "Price": round(price, 0),
                     "Cost": round(cost, 0) if cost is not None else "-",
                     "Margin %": round(margin_pct, 0) if margin_pct is not None else "-",
                     "ADS": round(ads, 2), "Stock": round(stock, 0),
                     "Days Cover": round(stock / ads, 1) if ads > 0 else None,
                     "Rev/Day": round(ads * price, 0),
                     "GP/Day": round(ads * (price - cost), 0) if cost is not None else "-"}
            movers.append(entry)
            if stock <= 0 and ads > 0:
                ghost.append(entry)
        elif stock > 0:
            # trapped capital at COST when known (what you paid), else retail
            v = stock * (cost if cost is not None else price)
            dead_value += v
            dead.append({"Item": (row.get("name") or "")[:44], "Dept": row.get("dept", ""),
                         "Vendor": (row.get("vendor") or "")[:24],
                         "Stock": round(stock, 0), "Value (KES)": round(v, 0)})

    # ── per-department + supplier rollups ───────────────────────────────
    dept_roll: Dict[str, dict] = {}
    supp_roll: Dict[str, dict] = {}
    for nn, row in by_name.items():
        d = row.get("dept", "")
        v = str(row.get("vendor") or "UNKNOWN")
        price = float(row.get("price", 0) or 0)
        stock = float(row.get("stock", 0) or 0)
        qty = monthly_qty_by_name.get(nn, 0.0)
        for roll, key in ((dept_roll, d), (supp_roll, v)):
            e = roll.setdefault(key, {"skus": 0, "stock_val": 0.0, "units": 0.0, "revenue": 0.0})
            e["skus"] += 1
            e["stock_val"] += stock * price
            e["units"] += qty
            e["revenue"] += qty * price

    inv_retail = sum(float(r.get("stock", 0) or 0) * float(r.get("price", 0) or 0)
                     for r in cat if float(r.get("stock", 0) or 0) > 0)
    in_stock = sum(1 for r in cat if float(r.get("stock", 0) or 0) > 0)
    total_units = sum(monthly_qty_by_name.values())

    movers.sort(key=lambda x: x["Rev/Day"], reverse=True)
    ghost.sort(key=lambda x: x["Rev/Day"], reverse=True)
    dead.sort(key=lambda x: x["Value (KES)"], reverse=True)

    def _roll_rows(roll):
        out = [{"name": k, **v, "stock_val": round(v["stock_val"], 0),
                "units": round(v["units"], 0), "revenue": round(v["revenue"], 0)}
               for k, v in roll.items()]
        out.sort(key=lambda x: x["revenue"], reverse=True)
        return out

    top5_supp = _roll_rows(supp_roll)[:5]
    supp_rev_total = sum(s["revenue"] for s in _roll_rows(supp_roll)) or 1.0
    return {
        "departments": departments, "skus": len(cat), "in_stock": in_stock,
        "inventory_retail": round(inv_retail, 0),
        "months_used": len(complete_months), "partial_months": sorted(partial),
        "units_sold": round(total_units, 0), "revenue_period": round(revenue_period, 0),
        "gross_profit_period": round(gross_profit_period, 0),
        "avg_margin_pct": (round(100.0 * gross_profit_period / rev_with_cost, 1)
                           if rev_with_cost > 0 else None),
        "cost_coverage_pct": round(100.0 * skus_with_cost / max(1, len(cat)), 1),
        "matched_skus": len(matched_names),
        "dead_skus": len(dead), "dead_value": round(dead_value, 0),
        "ghost_sellers": len(ghost),
        "ghost_lost_rev_day": round(sum(g["Rev/Day"] for g in ghost), 0),
        "dept_rollup": _roll_rows(dept_roll),
        "supplier_top5": top5_supp,
        "supplier_top5_pct": round(100.0 * sum(s["revenue"] for s in top5_supp)
                                   / supp_rev_total, 1),
        "seasonality": [{"month": m, "units": round(monthly_all.get(m, 0.0)),
                         "partial": m in partial} for m, _ in
                        [(lbl, p) for lbl, p in _month_files(cash_dir)]],
        "top_movers": movers[:top], "top_dead": dead[:top], "top_ghost": ghost[:top],
    }


def _mdtable(rows: List[dict], cols: List[str]) -> str:
    head = "| " + " | ".join(cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n"
    body = "".join("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |\n"
                   for r in rows)
    return head + body


def write_category_report(data_dir: str, cash_dir: str, category: str,
                          out_dir: str, tenant: str = "",
                          departments: Optional[List[str]] = None,
                          use_grn_costs: bool = True, pdf: bool = False) -> dict:
    from .rhapta_catalog import load_catalog
    depts = departments or CATEGORY_PRESETS.get(category.lower())
    if not depts:
        raise SystemExit(f"unknown category '{category}'. Known: "
                         f"{', '.join(CATEGORY_PRESETS)} — or pass --departments.")
    rows = load_catalog(data_dir)
    costs = None
    if use_grn_costs:
        try:
            from .grn_cost import load_grn_costs
            costs = load_grn_costs(data_dir)
        except Exception:
            costs = None
    a = build_category_report(rows, cash_dir, depts, costs=costs)

    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    slug = category.lower().replace(" ", "_")
    md_path = os.path.join(out_dir, f"OASIS_Category_Report_{slug}_{stamp}.md")
    csv_path = os.path.join(out_dir, f"OASIS_Category_Report_{slug}_{stamp}.csv")

    sea = [s for s in a["seasonality"] if not s["partial"]]
    peak = max(sea, key=lambda s: s["units"]) if sea else None
    trough = min(sea, key=lambda s: s["units"]) if sea else None
    partial_note = (f" (excludes partial-data month(s): {', '.join(a['partial_months'])})"
                    if a["partial_months"] else "")

    md = f"""# O.A.S.I.S. Category Report — {category.title()} · {tenant or 'Chandarana Rhapta'}

*Generated {stamp} from the real catalogue + {a['months_used']} complete months of
sales{partial_note}. Departments: {', '.join(a['departments'])}.*

## Section at a glance
| Metric | Value |
|---|---|
| SKUs in section | {a['skus']:,} ({a['in_stock']:,} in stock) |
| Inventory on hand (retail) | KES {a['inventory_retail']:,.0f} |
| Units sold (period) | {a['units_sold']:,.0f} |
| Revenue (period, at retail) | KES {a['revenue_period']:,.0f} |
| **Gross profit (real GRN cost)** | **KES {a['gross_profit_period']:,.0f}**{f" at {a['avg_margin_pct']}% avg margin" if a['avg_margin_pct'] is not None else ''} |
| Cost coverage (SKUs with GRN cost) | {a['cost_coverage_pct']}% |
| SKUs with sales | {a['matched_skus']:,} of {a['skus']:,} |
| **Dead stock** (stocked, no sales) | **KES {a['dead_value']:,.0f}** across {a['dead_skus']:,} SKUs |
| **Ghost sellers** (sell, zero stock) | **{a['ghost_sellers']:,} SKUs ≈ KES {a['ghost_lost_rev_day']:,.0f}/day lost** |
| Top-5 supplier concentration | {a['supplier_top5_pct']}% of section revenue |

## By department
{_mdtable(a['dept_rollup'], ['name', 'skus', 'units', 'revenue', 'stock_val'])}

## Seasonality (units sold / month)
{_mdtable([{'Month': s['month'], 'Units': f"{s['units']:,}" + (' (partial)' if s['partial'] else '')} for s in a['seasonality']], ['Month', 'Units'])}
{f"Peak **{peak['month']}** ({peak['units']:,} units) vs trough **{trough['month']}** ({trough['units']:,}) — a {round(peak['units']/max(1,trough['units']),1)}× swing." if peak and trough else ''}

## Top movers (real cost & margin from GRN)
{_mdtable(a['top_movers'][:15], ['Item', 'Dept', 'Price', 'Cost', 'Margin %', 'ADS', 'Days Cover', 'GP/Day'])}

## Ghost sellers — revenue walking out (restock these first)
{_mdtable(a['top_ghost'][:15], ['Item', 'Dept', 'Vendor', 'ADS', 'Rev/Day'])}

## Dead stock — capital to recover
{_mdtable(a['top_dead'][:15], ['Item', 'Dept', 'Vendor', 'Stock', 'Value (KES)'])}

## Top suppliers (by section revenue)
{_mdtable(a['supplier_top5'], ['name', 'skus', 'units', 'revenue', 'stock_val'])}

---
*Gross profit and margin use real GRN unit cost (quantity-weighted average) where
available; dead stock is valued at that cost (true trapped capital). Revenue is at
retail from real units sold x catalogue price. Sales-line coverage is limited to
names matched to the section catalogue; a code crosswalk at onboarding closes any
residual gap.*
"""
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)

    pdf_path = None
    if pdf:
        try:
            from .report_pdf import markdown_to_pdf
            pdf_path = markdown_to_pdf(
                md, os.path.splitext(md_path)[0] + ".pdf",
                title=f"OASIS {category.title()} Report")
        except Exception as e:
            pdf_path = None
            print(f"[category-report] PDF render skipped: {e}")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["section", "item", "dept", "vendor", "ads_or_units", "value"])
        for m in a["top_movers"]:
            w.writerow(["top_mover", m["Item"], m["Dept"], m["Vendor"], m["ADS"], m["Rev/Day"]])
        for g in a["top_ghost"]:
            w.writerow(["ghost", g["Item"], g["Dept"], g["Vendor"], g["ADS"], g["Rev/Day"]])
        for d in a["top_dead"]:
            w.writerow(["dead", d["Item"], d["Dept"], d["Vendor"], d["Stock"], d["Value (KES)"]])
    return {"markdown": md_path, "csv": csv_path, "pdf": pdf_path,
            **{k: v for k, v in a.items() if not k.startswith("top_")
               and k not in ("dept_rollup", "supplier_top5", "seasonality")}}
