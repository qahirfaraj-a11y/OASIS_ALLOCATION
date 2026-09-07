"""Per-SKU state of the ordering engine, and what the recent changes moved.

WHY A CENSUS AND NOT ANOTHER TOTAL
    A total says the buy fell 15%. It cannot say whether that is the safety
    term, a shelf-life ceiling, a dead-stock refusal or an integer rounding on
    a line selling one unit a fortnight -- and those want opposite responses.
    So this walks every SKU, records which mechanism actually set its number,
    and reports the census. A mechanism that never binds is decoration; one
    that binds on 12,000 lines is the engine, whatever the design says.

WHAT IS COMPARED
    The two recent changes are both switches, so they decompose exactly on
    identical inputs -- no simulator, no generated demand, no re-implemented
    engine:

      demand  static  : the caller's measured ADS withheld, so enrichment
                        falls back to the forecast file. This reproduces the
                        pre-fix behaviour precisely, because the old code
                        ignored a supplied ADS and used hist_ads regardless.
              measured: the POS/validated series survives (HEAD).

      model   classic : the multiplicative path, with the velocity multiplier
                        applied during enrichment.
              derived : order-up-to (OASIS_ORDER_MODEL=order_up_to).

    Enrichment runs once per demand arm and is shared by both model arms, so a
    difference between models is attributable to the quantity decision alone.

USAGE
    python devkit/order_state_census.py [--csv out.csv]
"""
from __future__ import annotations

import copy
import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.order_model_compare import build_book, DATA_DIR, band_of  # noqa: E402

BANDS = ("<=1/day", "1-5/day", "5-10/day", ">10/day")


def binding(rec, oh):
    """Which mechanism actually set this line's number.

    Ordered most-decisive first: a refusal beats a ceiling, a ceiling beats a
    guard, a guard beats the arithmetic. Read off the engine's own terms and
    its own reasoning string, not re-derived.
    """
    if rec.get("auto_order_suppressed"):
        return "suppressed (dead stock)"
    t = rec.get("order_up_to_terms") or {}
    why = str(rec.get("reasoning") or "")
    q = float(rec.get("recommended_quantity") or 0)
    if t.get("feasible") is False:
        return "infeasible (shelf life < R+L)"
    if t.get("clamped") or "clamped" in why.lower():
        return "shelf-life clamp"
    if "GUARD:" in why:
        m = re.search(r"\[GUARD: ([^(\]]+)", why)
        return "guard: " + (m.group(1).strip() if m else "?")
    if q <= 0:
        return "no order (position covers S)"
    if "Pack Rounding" in why:
        return "pack rounding"
    return "order-up-to arithmetic"


def run_arm(book, static_demand, model):
    """One cell of the 2x2. Returns per-SKU rows plus the enrichment used."""
    from oasis.logic.simulation_bridge import SimulationOrderUtil

    products = copy.deepcopy(book)
    if static_demand:
        # Withhold the measurement. The pre-fix code discarded it anyway, so
        # this reproduces the old behaviour rather than approximating it.
        for p in products:
            p["avg_daily_sales"] = 0.0
            p.pop("ads_source", None)

    util = SimulationOrderUtil(DATA_DIR)
    enriched = util.prepare_sku_data(products)
    base = {p["sku"]: float(p.get("current_stock") or 0) for p in book}
    for p in enriched:
        p["current_stock"] = base.get(p.get("sku") or p.get("item_code"), 0.0)

    os.environ["OASIS_ORDER_MODEL"] = "order_up_to" if model == "derived" else "classic"
    recs = util.finalize_orders(util.calculate_order_quantity(
        copy.deepcopy(enriched), use_real_date=True))
    os.environ.pop("OASIS_ORDER_MODEL", None)

    cost = {p["sku"]: p["unit_cost"] for p in book}
    dept = {p["sku"]: p["department"] for p in book}
    rows = {}
    for r in recs:
        k = r.get("sku") or r.get("item_code")
        t = r.get("order_up_to_terms") or {}
        q = float(r.get("recommended_quantity") or 0)
        d = float(r.get("avg_daily_sales") or 0)
        oh = base.get(k, 0.0)
        rows[k] = {
            "sku": k, "dept": dept.get(k, ""), "d": d,
            "ads_source": r.get("ads_source", ""),
            "band": band_of(d), "on_hand": oh, "q": q,
            "kes": q * cost.get(k, 0.0),
            "R": t.get("R"), "L": t.get("L"), "S": t.get("S"),
            "cycle": t.get("cycle_stock"), "sigma_lead": t.get("sigma_lead"),
            "cover_after": (oh + q) / d if d > 0 else 0.0,
            "binding": binding(r, oh),
        }
    return rows


def summarise(rows):
    on = [r for r in rows.values() if r["q"] > 0]
    return {
        "lines": len(on),
        "kes": sum(r["kes"] for r in on),
        "units": sum(r["q"] for r in on),
        "suppressed": sum(1 for r in rows.values()
                          if r["binding"].startswith("suppressed")),
        "infeasible": sum(1 for r in rows.values()
                          if r["binding"].startswith("infeasible")),
    }


def main(argv) -> int:
    out_csv = None
    if "--csv" in argv:
        out_csv = argv[argv.index("--csv") + 1]

    book = build_book()
    print(f"{len(book):,} SKUs priced, demanded and on the shelf\n")

    arms = {}
    for static in (True, False):
        for model in ("classic", "derived"):
            name = ("static " if static else "measured ") + model
            arms[name] = run_arm(book, static, model)

    print("=" * 78)
    print("THE 2x2 — demand source x quantity model, identical inputs")
    print("=" * 78)
    print(f"  {'arm':<20}{'lines':>8}{'order KES':>15}{'units':>12}"
          f"{'suppressed':>12}{'infeasible':>12}")
    for name, rows in arms.items():
        s = summarise(rows)
        print(f"  {name:<20}{s['lines']:>8,}{s['kes']:>15,.0f}"
              f"{s['units']:>12,.0f}{s['suppressed']:>12,}{s['infeasible']:>12,}")

    base_k = summarise(arms["static classic"])["kes"]
    now_k = summarise(arms["measured derived"])["kes"]
    print(f"\n  where we were (static classic)   {base_k:>15,.0f}")
    print(f"  where we are  (measured derived) {now_k:>15,.0f}"
          f"   {100*(now_k/base_k-1):+.1f}%")

    # Attribute the move along both paths, because the two changes interact.
    sc = summarise(arms["static classic"])["kes"]
    mc = summarise(arms["measured classic"])["kes"]
    sd = summarise(arms["static derived"])["kes"]
    md = summarise(arms["measured derived"])["kes"]
    print(f"\n  demand fix alone  (classic): {mc-sc:>+14,.0f}"
          f"   (derived): {md-sd:>+14,.0f}")
    print(f"  model change alone (static): {sd-sc:>+14,.0f}"
          f"  (measured): {md-mc:>+14,.0f}")
    print(f"  interaction                : {(md-mc)-(sd-sc):>+14,.0f}")

    cur = arms["measured derived"]
    print("\n" + "=" * 78)
    print("BINDING CONSTRAINT CENSUS — what actually set each line's number")
    print("=" * 78)
    cen = defaultdict(lambda: [0, 0.0])
    for r in cur.values():
        cen[r["binding"]][0] += 1
        cen[r["binding"]][1] += r["kes"]
    print(f"  {'mechanism':<34}{'lines':>9}{'% of book':>11}{'order KES':>15}")
    for why, (n, k) in sorted(cen.items(), key=lambda x: -x[1][0]):
        print(f"  {why:<34}{n:>9,}{100*n/len(cur):>10.1f}%{k:>15,.0f}")

    print("\n" + "=" * 78)
    print("BY VELOCITY BAND (measured derived)")
    print("=" * 78)
    print(f"  {'band':>10}{'SKUs':>8}{'ordering':>10}{'order KES':>14}"
          f"{'med cover':>11}{'suppressed':>12}")
    for b in BANDS:
        rs = [r for r in cur.values() if r["band"] == b]
        if not rs:
            continue
        on = [r for r in rs if r["q"] > 0]
        cov = sorted(r["cover_after"] for r in rs if r["d"] > 0)
        print(f"  {b:>10}{len(rs):>8,}{len(on):>10,}"
              f"{sum(r['kes'] for r in on):>14,.0f}"
              f"{(cov[len(cov)//2] if cov else 0):>10.1f}d"
              f"{sum(1 for r in rs if r['binding'].startswith('suppressed')):>12,}")

    print("\n" + "=" * 78)
    print("TOP 12 DEPARTMENTS BY MONDAY'S SPEND (measured derived)")
    print("=" * 78)
    dk = defaultdict(lambda: [0, 0.0])
    for r in cur.values():
        if r["q"] > 0:
            dk[r["dept"]][0] += 1
            dk[r["dept"]][1] += r["kes"]
    print(f"  {'department':<28}{'lines':>8}{'order KES':>15}")
    for d, (n, k) in sorted(dk.items(), key=lambda x: -x[1][1])[:12]:
        print(f"  {d[:28]:<28}{n:>8,}{k:>15,.0f}")

    if out_csv:
        with open(out_csv, "w", newline="", encoding="utf-8") as fh:
            cols = list(next(iter(cur.values())).keys())
            w = csv.DictWriter(fh, fieldnames=cols + ["q_static_classic"])
            w.writeheader()
            old = arms["static classic"]
            for k, r in cur.items():
                row = dict(r)
                row["q_static_classic"] = (old.get(k) or {}).get("q", 0)
                w.writerow(row)
        print(f"\n  per-SKU detail written to {out_csv} ({len(cur):,} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
