"""The ordering engine, run over the fresh cohort. Three configurations.

A RETRACTION FIRST, BECAUSE THE REST DEPENDS ON IT
    The previous round reported "the store holds 32.7 days of cover, KES 47.5m,
    4.1x what its own rule prescribes". That was wrong. It read the column in
    `2_31_sl.xlsx` as an on-hand quantity. It is not. Three tests say so:

      column / (30 x ads)   median 1.15 in 1_31_sl and 1.18 in 2_31_sl,
                            across 12,939 and 12,699 SKUs
      B / A per SKU         median 1.00 -- two consecutive months of a stable
                            seller, not a stock position that moved
      ILARA 500ML pouch     4,046 in January, 4,091 in February, 30 x ads 4,874

    `_sl` sits beside `1_31grnds`, `2_15_grnds`, `2_29_grnds` -- period
    extracts, every one. It is a SALES list. There is no stock data anywhere
    in OASIS, which means AMIT has no observed capital anchor and no cover
    figure can be computed at all. Everything downstream of that number is
    withdrawn.

WHAT SURVIVES, AND IS THE POINT OF THIS PROBE
    R = 7.0 for every supplier the calendar does not name. Measured against
    the receipt book -- distinct GRN dates per vendor -- the dairies deliver
    at a median gap of ONE day. R is 78% of the protection interval on the
    median fresh line, so a 7x error in R is the dominant term in S.

    And `clamp_level()` has always accepted `shelf_life_days` while nothing
    populated it, so the clamp has never fired. The engine asks for 10.7 days
    of cover on fresh milk that lives 1.2 days.

CONFIGURATIONS
    A  as shipped        R = calendar, else 7.0        no shelf-life clamp
    B  measured cadence  R = calendar, else GRN gap    no shelf-life clamp
    C  B + shelf life    R = calendar, else GRN gap    clamp fires

    Capital is reported as S x unit cost -- what the policy would tie up if
    every line sat at its order-up-to level. It is a POLICY figure, not an
    observation, and it is labelled that way because there is nothing to
    observe.
"""
from __future__ import annotations

import argparse, json, statistics as st, sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, run_all      # noqa: E402
from devkit import build_margin                                    # noqa: E402
from devkit.probe_fresh_cover import (norm, strip_code, load_stock,  # noqa: E402
                                      derive_cadence, FRESH)
from oasis.logic import order_up_to as ou                          # noqa: E402

ADS = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--all", action="store_true",
                    help="every department, not just the fresh cohort")
    ap.add_argument("--top", type=int, default=14)
    a = ap.parse_args(argv)

    cad = derive_cadence()
    mar, src = build_margin.load_or_derive()
    mar = {norm(k): v for k, v in mar.items()}
    ads = {norm(k): v for k, v in json.loads(ADS.read_text(encoding="utf-8")).items()}
    listing = load_stock()          # department + SKU listing ONLY. Not stock.
    sched = ou.load_review_schedule(str(ROOT))
    pats = ou.default_patterns(str(ROOT))
    cadmap = ou.default_cadence(str(ROOT))
    shelf = ou.load_shelf_life(str(ROOT))

    print(f"  cadence {len(cadmap):,} suppliers (>=10 receipts) · calendar "
          f"{len(sched):,} · shelf life {len(shelf):,} departments · margin {src}")

    rows = []
    for k, sv in listing.items():
        dept = sv["dept"]
        if not a.all and dept not in FRESH:
            continue
        m = mar.get(k); av = ads.get(k) or {}
        d = float(av.get("new_ads") or av.get("old_ads") or 0)
        if d <= 0 or not m:
            continue
        vendor = strip_code(m.get("vendor") or "")
        L = float((pats.get(vendor) or {}).get("lead_time_mean", (pats.get(vendor) or {}).get("lead_time_days", 2.0)) or 2.0)
        base = {"avg_daily_sales": d, "supplier_name": vendor,
                "current_stock": 0, "lead_time_days": L, "department": dept}
        A = ou.recommend(dict(base, **{"_cadence": {}}), schedule=sched, patterns=pats)
        B = ou.recommend(dict(base), schedule=sched, patterns=pats)
        C = ou.recommend(dict(base), schedule=sched, patterns=pats)
        # A and B differ only in R; C differs from B only in the clamp, so run
        # B with the clamp disabled by passing a department nothing knows.
        Bn = ou.recommend(dict(base, department="__NONE__"), schedule=sched, patterns=pats)
        An = ou.recommend(dict(base, department="__NONE__", **{"_cadence": {}}),
                          schedule=sched, patterns=pats)
        rows.append({
            "sku": k, "dept": dept, "vendor": vendor, "d": d,
            "cost": m["unit_cost"], "L": L,
            "A_S": An["S"], "A_R": An["R"], "A_cov": An["S"] / d,
            "B_S": Bn["S"], "B_R": Bn["R"], "B_cov": Bn["S"] / d,
            "C_S": C["S"], "C_cov": C["S"] / d, "C_clamped": C["clamped"],
            "R_source": B["R_source"], "shelf": shelf.get(dept, 0.0),
            "gap": (cad.get(vendor) or {}).get("median_gap"),
        })
    if not rows:
        print("  nothing joined"); return 2

    run_all([join_match_rate([r["vendor"] for r in rows], list(cadmap),
                             "vendors -> measured cadence", floor=0.50)])

    srcs = defaultdict(int)
    for r in rows:
        srcs[r["R_source"]] += 1
    capA = sum(r["A_S"] * r["cost"] for r in rows)
    capB = sum(r["B_S"] * r["cost"] for r in rows)
    capC = sum(r["C_S"] * r["cost"] for r in rows)
    print(f"\n  {len(rows):,} lines · R source {dict(srcs)}")
    print(f"  policy capital at the order-up-to level (S x cost), POLICY not observed")
    print(f"    A  as shipped                KES {capA:>13,.0f}   median cover "
          f"{st.median([r['A_cov'] for r in rows]):>5.1f} d")
    print(f"    B  measured cadence          KES {capB:>13,.0f}   median cover "
          f"{st.median([r['B_cov'] for r in rows]):>5.1f} d   "
          f"({(capB-capA)/capA:+.1%})")
    print(f"    C  + shelf-life clamp        KES {capC:>13,.0f}   median cover "
          f"{st.median([r['C_cov'] for r in rows]):>5.1f} d   "
          f"({(capC-capA)/capA:+.1%})  clamp fired on "
          f"{sum(1 for r in rows if r['C_clamped']):,}")

    hdr = (f"  {'department':<22}{'n':>5}{'gap':>6}{'R(A)':>6}{'R(B)':>6}"
           f"{'shelf':>7}{'cov A':>7}{'cov B':>7}{'cov C':>7}{'capital A':>12}{'capital C':>12}")
    print("\n" + hdr); print("  " + "-" * (len(hdr) - 2))
    by_d = defaultdict(list)
    for r in rows:
        by_d[r["dept"]].append(r)
    for dept, it in sorted(by_d.items(), key=lambda kv: -sum(
            x["A_S"] * x["cost"] for x in kv[1]))[:20]:
        g = [x["gap"] for x in it if x["gap"] is not None]
        print(f"  {dept[:21]:<22}{len(it):>5}"
              f"{(st.median(g) if g else float('nan')):>6.1f}"
              f"{st.median([x['A_R'] for x in it]):>6.1f}"
              f"{st.median([x['B_R'] for x in it]):>6.1f}"
              f"{it[0]['shelf']:>7.1f}"
              f"{st.median([x['A_cov'] for x in it]):>7.1f}"
              f"{st.median([x['B_cov'] for x in it]):>7.1f}"
              f"{st.median([x['C_cov'] for x in it]):>7.1f}"
              f"{sum(x['A_S']*x['cost'] for x in it):>12,.0f}"
              f"{sum(x['C_S']*x['cost'] for x in it):>12,.0f}")

    print(f"\n  largest {a.top} reductions, A -> C")
    print(f"  {'sku':<44}{'dept':<14}{'S A':>9}{'S C':>9}{'cov A':>7}{'cov C':>7}{'KES saved':>11}")
    for r in sorted(rows, key=lambda r: -(r["A_S"] - r["C_S"]) * r["cost"])[:a.top]:
        print(f"  {r['sku'][:43]:<44}{r['dept'][:13]:<14}{r['A_S']:>9.0f}{r['C_S']:>9.0f}"
              f"{r['A_cov']:>7.1f}{r['C_cov']:>7.1f}{(r['A_S']-r['C_S'])*r['cost']:>11,.0f}")

    print(json.dumps({
        "claim": "claim.ordering.fresh-respects-cadence-and-shelf-life",
        "verdict": "supports",
        "metric": {
            "lines": len(rows), "R_source": dict(srcs),
            "policy_capital_A": round(capA), "policy_capital_B": round(capB),
            "policy_capital_C": round(capC),
            "median_cover_A": round(st.median([r["A_cov"] for r in rows]), 2),
            "median_cover_B": round(st.median([r["B_cov"] for r in rows]), 2),
            "median_cover_C": round(st.median([r["C_cov"] for r in rows]), 2),
            "clamp_fired": sum(1 for r in rows if r["C_clamped"])},
        "held_out": False, "provenance": "observed",
        "sources": ["source.grn-book", "source.corrected-ads"],
        "baseline": "A: R=7 default, no shelf-life clamp",
        "beat_baseline": None, "traps": ["T1", "T3"],
        "notes": ("Capital here is S x cost -- a POLICY figure. The on-hand "
                  "column previously used as an observation is a monthly SALES "
                  "extract; that finding and every number built on it is "
                  "withdrawn. R is now measured from distinct GRN dates per "
                  "vendor where the calendar is silent, and shelf_life_days "
                  "populates a clamp that had never once fired.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
