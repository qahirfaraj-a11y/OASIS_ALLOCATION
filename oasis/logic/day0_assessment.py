"""
Day-0 Demand Assessment — the pre-sales "store X-ray".

From nothing but a client's raw exports (stock snapshot dept_*.xlsx + monthly
units-sold *_cash.xlsx), produce the numbers that open a sales conversation:

  * dead stock  — capital sitting in SKUs with zero recorded sales;
  * ghost sellers — proven sellers currently at zero stock (revenue walking out);
  * top movers  — the real daily demand leaders with days-of-cover;
  * assortment gaps — items that sell but aren't in the stock snapshot at all;
  * honest coverage — how much sales volume we could match to the catalogue.

Runs before any POS connection, so a pilot carries zero integration risk.

    python entrypoint.py --mode assess --tenant "Client Name"

Pure analysis (assess) is unit-tested; write_assessment is the artifact writer.
"""

from __future__ import annotations

import csv
import os
from datetime import datetime
from typing import Dict, List

from .real_demand import normalise_name


def assess(catalog_rows: List[dict], demand: Dict[str, float], months: int,
           days_per_month: float = 30.4, top: int = 25) -> dict:
    """Cross the stock snapshot with real demand. Pure."""
    days = max(1.0, months * days_per_month)
    by_name: Dict[str, dict] = {}
    for r in catalog_rows:
        by_name.setdefault(normalise_name(r.get("name", "")), r)

    total_q = sum(demand.values())
    covered_q = 0.0
    ghost: List[dict] = []          # sells, but stock <= 0
    movers: List[dict] = []         # matched sellers
    matched_names = set()
    for nname, qty in demand.items():
        row = by_name.get(nname)
        if row is None:
            continue
        matched_names.add(nname)
        covered_q += qty
        ads = qty / days
        stock = float(row.get("stock", 0) or 0)
        entry = {
            "Item": (row.get("name") or "")[:40], "Department": row.get("dept", ""),
            "ADS": round(ads, 2), "Stock": round(stock, 0),
            "Days Cover": round(stock / ads, 1) if ads > 0 else None,
            "Lost Rev/Day": round(ads * float(row.get("price", 0) or 0), 0),
        }
        movers.append(entry)
        if stock <= 0 and ads > 0:
            ghost.append(entry)

    dead: List[dict] = []           # stocked, but zero recorded sales
    dead_value = 0.0
    for nname, row in by_name.items():
        stock = float(row.get("stock", 0) or 0)
        if stock > 0 and nname not in demand:
            value = stock * float(row.get("price", 0) or 0)
            dead_value += value
            dead.append({"Item": (row.get("name") or "")[:40],
                         "Department": row.get("dept", ""),
                         "Stock": round(stock, 0), "Value (KES)": round(value, 0)})

    movers.sort(key=lambda r: r["ADS"], reverse=True)
    ghost.sort(key=lambda r: r["Lost Rev/Day"], reverse=True)
    dead.sort(key=lambda r: r["Value (KES)"], reverse=True)
    in_stock = sum(1 for r in catalog_rows if float(r.get("stock", 0) or 0) > 0)
    inventory_value = sum(float(r.get("stock", 0) or 0) * float(r.get("price", 0) or 0)
                          for r in catalog_rows if float(r.get("stock", 0) or 0) > 0)
    return {
        "skus": len(catalog_rows), "in_stock": in_stock,
        "inventory_value": round(inventory_value, 0),
        "months": months, "days_observed": round(days, 0),
        "coverage_pct": round(100.0 * covered_q / total_q, 1) if total_q else 0.0,
        "assortment_gaps": len(demand) - len(matched_names),
        "dead_skus": len(dead), "dead_stock_value": round(dead_value, 0),
        "ghost_sellers": len(ghost),
        "ghost_lost_rev_day": round(sum(g["Lost Rev/Day"] for g in ghost), 0),
        "top_movers": movers[:top], "top_ghost": ghost[:top], "top_dead": dead[:top],
    }


def write_assessment(data_dir: str, cash_dir: str, out_dir: str,
                     tenant: str = "") -> dict:
    """Load raw exports, assess, and write the client-facing artifact."""
    from .real_demand import load_monthly_demand
    from .catalog_snapshot import load_catalog
    rows = load_catalog(data_dir)
    demand, months, files = load_monthly_demand(cash_dir)
    a = assess(rows, demand, months)

    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    md_path = os.path.join(out_dir, f"OASIS_Day0_Assessment_{stamp}.md")
    csv_path = os.path.join(out_dir, f"OASIS_Day0_Assessment_{stamp}.csv")

    def _table(rows_, cols):
        head = "| " + " | ".join(cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n"
        body = "".join("| " + " | ".join(str(r.get(c, "")) for c in cols) + " |\n"
                       for r in rows_)
        return head + body

    md = f"""# O.A.S.I.S. Day-0 Demand Assessment — {tenant or 'your store'}

*Generated {stamp} from your raw exports ({files} sales files covering ~{a['months']} months,
{a['skus']:,} catalogue SKUs). No system integration was required.*

## The headline numbers
| Finding | Value |
|---|---|
| **Dead stock** (on shelf, zero recorded sales) | **KES {a['dead_stock_value']:,.0f}** across {a['dead_skus']:,} SKUs |
| **Ghost sellers** (proven sellers, zero stock now) | **{a['ghost_sellers']:,} SKUs ≈ KES {a['ghost_lost_rev_day']:,.0f} lost revenue / day** |
| Inventory at retail | KES {a['inventory_value']:,.0f} ({a['in_stock']:,} SKUs in stock) |
| Demand data coverage | {a['coverage_pct']}% of sales volume matched to the catalogue |
| Assortment gaps | {a['assortment_gaps']:,} selling items absent from the stock snapshot |

## Ghost sellers — revenue walking out the door
{_table(a['top_ghost'][:15], ["Item", "Department", "ADS", "Lost Rev/Day"])}

## Dead stock — capital to recover
{_table(a['top_dead'][:15], ["Item", "Department", "Stock", "Value (KES)"])}

## Your real demand leaders
{_table(a['top_movers'][:15], ["Item", "Department", "ADS", "Stock", "Days Cover"])}

---
*Coverage below 100% means some sales lines could not be name-matched to the
catalogue (variants / missing items) — a code crosswalk during onboarding closes
this. O.A.S.I.S. converts these findings into daily replenishment, transfer and
pricing actions once connected.*
"""
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["section", "item", "department", "ads", "stock", "value_or_lost_rev"])
        for g in a["top_ghost"]:
            w.writerow(["ghost_seller", g["Item"], g["Department"], g["ADS"], g["Stock"], g["Lost Rev/Day"]])
        for d in a["top_dead"]:
            w.writerow(["dead_stock", d["Item"], d["Department"], "", d["Stock"], d["Value (KES)"]])
    return {"markdown": md_path, "csv": csv_path, **{k: v for k, v in a.items()
            if not k.startswith("top_")}}
