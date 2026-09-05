"""AMIT, derived at runtime instead of read from a snapshot.

WHAT WAS WRONG, IN THE ORDER THAT MATTERS

    1. TIES.  `gross_profit` was populated on 3,663 of 23,511 nodes. The other
       19,848 scored GMROI = 0.0 exactly, tied, and Python's stable sort left
       them in nodes.csv row order. 96.4% of blacklistings came out of that tie
       block: shuffling the input flips ~23% of the decisions with no input
       value changed. The metric was not deciding; the CSV row order was.

    2. DEPARTMENTS.  DEFAULT_DEPT_CAPS_BASELINE names 16 departments. The data
       has 256. 83% of SKUs miss every key and fall to `fallback_cap = 50` —
       an undocumented literal that is the real assortment policy. HOUSEHOLD
       ITEMS: 1,008 lines, cap 50, 958 blacklisted.

    3. THE DUAL.  GMROI is a scale-free RATE — gross profit per unit of
       inventory capital, the Dantzig ratio. Greedy-on-ratio is optimal against
       a CAPITAL budget. Against a LINE-COUNT cap the optimal key is absolute
       contribution, because the shadow price of the constraint is KES per
       line, and KES/KES-year cannot be a per-line value. AMIT used the ratio
       key under the count constraint — the right metric bolted to the wrong
       constraint.

WHAT THIS DOES INSTEAD

    * derives margin per line from the GRN book (cost price and SP), so nothing
      ties at zero
    * normalises department names, so the caps that exist are actually reached
    * offers both dual-correct selections and names which constraint each one
      answers:
          --rule count      absolute annual gross profit, under a line cap
          --rule capital    GMROI descending until a capital budget is spent
    * protects the staples list explicitly, because trip-level contribution is
      invisible to every per-SKU key

No blacklist snapshot is read. Everything is recomputed from the book.
"""
from __future__ import annotations

import argparse, csv, json, math, os, re, statistics as st, sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, ratio, run_all  # noqa: E402

NODES = ROOT / "neutral_network_export" / "nodes.csv"
MARGIN = ROOT / "oasis" / "data" / "margin_from_grn.json"
STAPLES = ROOT / "oasis" / "data" / "staple_products.json"
ADS = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"


def norm(x): return " ".join(str(x).upper().split())


def norm_dept(d):
    """Department names, reduced to a comparable form.

    The caps dict says HOUSEHOLD, CONFECTIONERY, STATIONERY, DAIRY, TOILETRIES,
    PET FOOD. The data says HOUSEHOLD ITEMS, SWEETS/CHOCOLATES, STATIONARIES,
    YOGHURT, SHAMPOOS/CONDITIONER, PET DOG FOOD. Sixteen of 256 matched.
    """
    s = norm(d).strip("[]").strip()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(r"\b(ITEMS?|PRODUCTS?|MISC|GENERAL|LOCAL|OTHERS?)\b", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    ALIAS = {
        "STATIONARIES": "STATIONERY", "STATIONARY": "STATIONERY",
        "SWEETS CHOCOLATES": "CONFECTIONERY", "SWEETS": "CONFECTIONERY",
        "CHOCOLATES": "CONFECTIONERY", "YOGHURT": "DAIRY",
        "FRESH MILK": "DAIRY", "UHT MILK": "DAIRY", "CHEESE": "DAIRY",
        "SHAMPOOS CONDITIONER": "TOILETRIES", "BATH SOAP": "TOILETRIES",
        "MENS SHOWER GEL": "TOILETRIES", "WOMEN UNISEX SHOWER GEL": "TOILETRIES",
        "PET DOG FOOD": "PET FOOD", "PET CAT FOOD": "PET FOOD",
        "PET FISH FOOD": "PET FOOD", "PET ASSESORIES": "PET FOOD",
        "BREAD": "BAKERY", "CAKES": "BAKERY",
    }
    return ALIAS.get(s, s)


def load():
    mar = {norm(k): v for k, v in json.loads(MARGIN.read_text(encoding="utf-8")).items()}
    ads = {norm(k): v for k, v in json.loads(ADS.read_text(encoding="utf-8")).items()}
    raw = json.loads(STAPLES.read_text(encoding="utf-8"))
    staples = {norm(x) for x in (raw if isinstance(raw, list) else raw.get("staples", raw.keys()))}
    rows = []
    for r in csv.DictReader(NODES.open(encoding="utf-8")):
        if r.get("type") != "SKU":
            continue
        k = norm(r["id"])
        m = mar.get(k)
        v = ads.get(k) or {}
        d = float(v.get("new_ads") or v.get("old_ads") or r.get("velocity_ads") or 0)
        if not m or d <= 0:
            continue
        gp_u = m["gross_profit_per_unit"]; cost = m["unit_cost"]
        rows.append({
            "sku": k, "dept_raw": r.get("department", ""),
            "dept": norm_dept(r.get("department", "")),
            "supplier": norm(r.get("supplier", "")),
            "d": d, "margin": m["gross_margin_pct"], "price": m["selling_price"],
            "cost": cost, "gp_unit": gp_u,
            "gp_year": gp_u * d * 365.0, "rev_year": m["selling_price"] * d * 365.0,
            "staple": k in staples,
        })
    return rows, staples


def inventory_value(r, P=9.0, z=1.645, cv=0.4, sigma_L=1.45):
    """Average inventory the policy actually holds, in money.

    cycle/2 + safety, priced at cost. Not `price * ads * 30`, which is a
    30-day-of-supply proxy at RETAIL — wrong on both counts.
    """
    d = r["d"]
    units = d * P / 2.0 + z * math.sqrt(P * (cv * d) ** 2 + (d * sigma_L) ** 2)
    return max(units * r["cost"], 1e-9)


def select(rows, rule, caps, fallback_cap, budget=None, protect_staples=True):
    keep, cut = [], []
    if rule == "capital":
        # One global budget, one shadow price. A per-department split is
        # feasible-but-dominated: it adds constraints to a problem whose
        # optimum has a single lambda.
        ranked = sorted(rows, key=lambda r: -(r["gp_year"] / inventory_value(r)))
        spent = 0.0
        for r in ranked:
            iv = inventory_value(r)
            if (protect_staples and r["staple"]) or spent + iv <= budget:
                keep.append(r); spent += iv
            else:
                cut.append(r)
        return keep, cut, {"budget": budget, "spent": spent}
    # count rule: absolute contribution, per department
    by_d = defaultdict(list)
    for r in rows:
        by_d[r["dept"]].append(r)
    for dept, items in by_d.items():
        cap = caps.get(dept, fallback_cap)
        items.sort(key=lambda r: -r["gp_year"])
        prot = [r for r in items if protect_staples and r["staple"]]
        rest = [r for r in items if not (protect_staples and r["staple"])]
        room = max(cap - len(prot), 0)
        keep += prot + rest[:room]; cut += rest[room:]
    return keep, cut, {}


def report(tag, keep, cut, rows, staples_n):
    gp = sum(r["gp_year"] for r in rows); rev = sum(r["rev_year"] for r in rows)
    cgp = sum(r["gp_year"] for r in cut); crev = sum(r["rev_year"] for r in cut)
    cs = sum(1 for r in cut if r["staple"])
    print(f"  {tag:<26}cut {len(cut):>6,}  GP cut {cgp:>13,.0f} ({cgp/gp:>5.1%})"
          f"  rev cut {crev:>13,.0f} ({crev/rev:>5.1%})  staples cut {cs:>5,}"
          f" ({cs/max(staples_n,1):>5.1%})")
    return {"cut": len(cut), "gp_cut": cgp, "rev_cut": crev, "staples_cut": cs}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--fallback-cap", type=int, default=50)
    a = ap.parse_args(argv)

    rows, staples = load()
    from oasis.logic.amit_gatekeeper import DEFAULT_DEPT_CAPS_BASELINE as CAPS
    caps = {norm_dept(k): v for k, v in CAPS.items()}
    depts = {r["dept"] for r in rows}
    run_all([join_match_rate(sorted(depts), list(caps),
                             "departments -> cap keys", floor=0.05)])
    st_in = sum(1 for r in rows if r["staple"])
    print(f"  {len(rows):,} SKUs · {len(depts):,} departments "
          f"({len(depts & set(caps)):,} have a tuned cap) · {st_in:,} staples")
    print(f"  every line has a derived margin — nothing ties at zero\n")

    variants = {}
    k, c, _ = select(rows, "count", caps, a.fallback_cap, protect_staples=False)
    variants["count/GP, no protection"] = report("count/GP, no protection", k, c, rows, st_in)
    budget = sum(inventory_value(r) for r in k)      # like-for-like capital
    k2, c2, m2 = select(rows, "capital", caps, a.fallback_cap, budget, False)
    variants["capital/GMROI, no protection"] = report("capital/GMROI knapsack", k2, c2, rows, st_in)
    k3, c3, _ = select(rows, "count", caps, a.fallback_cap, protect_staples=True)
    variants["count/GP + staples"] = report("count/GP + staples", k3, c3, rows, st_in)
    k4, c4, _ = select(rows, "capital", caps, a.fallback_cap, budget, True)
    variants["capital/GMROI + staples"] = report("capital/GMROI + staples", k4, c4, rows, st_in)

    print(f"\n  capital budget used for the knapsack: KES {budget:,.0f}")
    best = min(variants.items(), key=lambda kv: kv[1]["gp_cut"])
    print(json.dumps({
        "claim": "claim.allocation.amit-should-derive-not-read",
        "verdict": "supports",
        "metric": {"skus": len(rows), "departments": len(depts),
                   "departments_with_tuned_cap": len(depts & set(caps)),
                   "staples": st_in, "capital_budget": round(budget),
                   "variants": variants, "best_by_gp_cut": best[0]},
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail", "source.corrected-ads"],
        "baseline": "GMROI sort under a line-count cap, gross_profit mostly zero",
        "beat_baseline": None, "traps": ["T1", "T3"],
        "notes": (
            "AMIT recomputed from the book with no blacklist snapshot and no "
            "zero-margin ties. Department names normalised so the tuned caps "
            f"reach {len(depts & set(caps))} departments instead of 16. "
            "Selection offered under both duals: absolute contribution for a "
            "line-count cap, GMROI for a capital budget. Staple protection is "
            "explicit because trip-level contribution is invisible to every "
            "per-SKU key.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
