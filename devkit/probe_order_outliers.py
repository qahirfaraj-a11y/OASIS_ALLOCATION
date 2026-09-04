"""Probe: does the engine ever buy something absurd, and where does it cluster?

A property check says the surface is SHAPED right — monotone, decomposable,
non-negative. It says nothing about whether any individual answer is sane. An
order can obey every structural rule and still be four months of cover on a
slow-moving line, or a safety stock three times its own cycle stock.

So: sweep every decision the procurement agents made and look for the answers a
buyer would refuse.

    O1  cover ordered beyond a quarter of a year, WHERE A SMALLER ORDER EXISTED
    O2  a single order exceeding a quarter of annual demand, same condition
    O3  safety stock dominating cycle stock — the variance term running away
    O4  a demand rate no single store plausibly has
    O5  minimum-order overhang: the SMALLEST possible order is months of cover
    O6  a supplier whose lead time swings by more than ten days

THE CONDITION IN O1 AND O2 IS THE WHOLE PROBE
    The first run of this probe flagged 154 orders of 185 days' cover and called
    them absurd. Every one was a single unit of a line selling five a year. You
    cannot order less than one, so the engine had no smaller answer to give —
    the cover is a property of the ASSORTMENT, not of the ordering logic.

    Check the denominator before raising the alarm. A line whose minimum order
    is six months of stock is a real and expensive fact, but it belongs to the
    range review, not the order engine, and calling it an ordering defect sends
    somebody to fix the wrong thing. It gets its own flag, O5.

Then roll it up BY DEPARTMENT, because an outlier that appears once is a line
and an outlier that appears across a department is a rule.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from oasis.logic.decision_ledger import DecisionLedger          # noqa: E402
from devkit.methodology.traps import join_match_rate, ratio, run_all  # noqa: E402

DEPT_MAP = ROOT / "oasis" / "data" / "product_department_map.json"

COVER_DAYS_LIMIT = 90.0        # a quarter of a year on one order
ANNUAL_SHARE_LIMIT = 0.25      # a quarter of the year's demand in one go
SAFETY_SHARE_LIMIT = 0.75      # safety three times cycle
ADS_LIMIT = 5000.0             # units/day, one store, one line


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=None)
    ap.add_argument("--top", type=int, default=8)
    a = ap.parse_args(argv)

    led = DecisionLedger(a.db)
    rows = led.decisions(kind="order", limit=1_000_000)
    if not rows:
        print(json.dumps({"claim": "claim.ordering.no-absurd-orders",
                          "verdict": "inconclusive", "metric": {},
                          "notes": "no decisions in the ledger"}))
        return 0

    depts = json.loads(DEPT_MAP.read_text(encoding="utf-8")) if DEPT_MAP.exists() else {}
    depts = {" ".join(str(k).upper().split()): v for k, v in depts.items()}

    # The canonical order: an empty shelf, nothing on the water.
    fresh = [r for r in rows
             if r["inputs"].get("on_hand_mult") == 0
             and r["inputs"].get("on_order_mult") == 0]
    skus = sorted({r["entity"] for r in fresh})
    run_all([join_match_rate(skus, list(depts), "SKU -> department", floor=0.50)])

    by_mode = defaultdict(list)
    flags = defaultdict(list)
    per_dept = defaultdict(list)
    for r in fresh:
        i = r["inputs"]
        d, q, S = i["d"], float(r["quantity"] or 0), i["S"]
        cover = (q / d) if d > 0 else 0.0
        annual = d * 365.0
        share = (q / annual) if annual > 0 else 0.0
        safety_share = (i["safety_stock"] / S) if S > 0 else 0.0
        dept = depts.get(" ".join(str(r["entity"]).upper().split()), "UNMAPPED")
        by_mode[i.get("mode", "?")].append(cover)
        per_dept[dept].append({"cover": cover, "safety_share": safety_share,
                               "sku": r["entity"], "qty": q, "d": d})
        rec = {"sku": r["entity"], "store": i.get("store"), "dept": dept,
               "d": round(d, 3), "qty": round(q, 1), "cover_days": round(cover, 1),
               "annual_share": round(share, 3),
               "safety_share": round(safety_share, 3),
               "R": i["R"], "L": i["L"], "sigma_lead": i["sigma_lead"],
               "mode": i.get("mode"), "scheduled": i.get("scheduled")}
        pack = float(i.get("pack_size") or 1.0)
        at_minimum = q <= pack + 1e-9        # no smaller order was available
        rec["at_minimum_order"] = at_minimum
        if cover > COVER_DAYS_LIMIT and not at_minimum:
            flags["O1_cover_over_90d"].append(rec)
        if share > ANNUAL_SHARE_LIMIT and not at_minimum:
            flags["O2_over_quarter_of_annual"].append(rec)
        if safety_share > SAFETY_SHARE_LIMIT and not at_minimum:
            flags["O3_safety_dominates_cycle"].append(rec)
        if d > ADS_LIMIT:
            flags["O4_implausible_demand"].append(rec)
        if at_minimum and cover > COVER_DAYS_LIMIT:
            flags["O5_minimum_order_overhang"].append(rec)
        if i["sigma_lead"] > 10.0:
            flags["O6_wild_supplier_lead_time"].append(rec)

    n = len(fresh)
    print(f"  {n:,} empty-shelf decisions · {len(skus):,} SKUs · "
          f"{len({r['inputs'].get('store') for r in fresh})} stores")
    for m, cs in sorted(by_mode.items()):
        print(f"    {m:<10} median cover {st.median(cs):.1f}d · "
              f"p95 {sorted(cs)[int(0.95 * len(cs))]:.1f}d")
    # The mode only moves lines with NO declared order day. Averaging over the
    # whole book hides the effect inside the half it cannot touch.
    unsched = defaultdict(list)
    for r in fresh:
        i = r["inputs"]
        if not i.get("scheduled") and i["d"] > 0:
            unsched[i.get("mode", "?")].append(float(r["quantity"] or 0) / i["d"])
    if len(unsched) > 1:
        ms = {m: st.median(v) for m, v in unsched.items()}
        print(f"    unscheduled lines only ({len(unsched['scheduled']):,} each): "
              + " · ".join(f"{m} {v:.1f}d" for m, v in sorted(ms.items()))
              + f"  → {ms.get('scheduled', 0) / max(ms.get('on_demand', 1), 1e-9):.2f}x")

    # O5 and O6 are facts about the assortment and the supplier base. They are
    # reported, and they are NOT defects of the ordering engine.
    DEFECTS = ("O1_cover_over_90d", "O2_over_quarter_of_annual",
               "O3_safety_dominates_cycle", "O4_implausible_demand")
    total = sum(len(flags[k]) for k in DEFECTS if k in flags)
    for k in sorted(flags):
        v = flags[k]
        worst = sorted(v, key=lambda x: -x["cover_days"])[:2]
        print(f"    {k:<28} {len(v):>6,}  e.g. "
              + "; ".join(f"{w['sku'][:26]} ({w['dept']}) "
                          f"{w['cover_days']:.0f}d" for w in worst))
    if not total:
        print("    no absurd orders found")

    # ---- department rollup ------------------------------------------------
    chain_median = st.median([x["cover"] for v in per_dept.values() for x in v])
    dept_rows = []
    for dept, xs in per_dept.items():
        if len(xs) < 20:
            continue
        cov = [x["cover"] for x in xs]
        dept_rows.append({
            "department": dept, "lines": len(xs),
            "median_cover_days": round(st.median(cov), 2),
            "p95_cover_days": round(sorted(cov)[int(0.95 * len(cov))], 2),
            "vs_chain": round(st.median(cov) / chain_median, 2) if chain_median else None,
            "median_safety_share": round(
                st.median([x["safety_share"] for x in xs]), 3),
        })
    dept_rows.sort(key=lambda r: -(r["vs_chain"] or 0))
    print(f"\n  chain median cover {chain_median:.1f}d · "
          f"{len(dept_rows)} departments with 20+ lines")
    print(f"    {'department':<30}{'lines':>7}{'median':>9}{'p95':>8}{'vs chain':>10}")
    for r in dept_rows[:a.top]:
        print(f"    {r['department'][:29]:<30}{r['lines']:>7,}"
              f"{r['median_cover_days']:>9.1f}{r['p95_cover_days']:>8.1f}"
              f"{r['vs_chain']:>10.2f}")

    outlier_depts = [r for r in dept_rows if (r["vs_chain"] or 0) >= 3.0]
    if outlier_depts:
        run_all([ratio(outlier_depts[0]["median_cover_days"], chain_median,
                       f"{outlier_depts[0]['department']} cover vs chain",
                       lo=0.2, hi=3.0)])

    print(json.dumps({
        "claim": "claim.ordering.no-absurd-orders",
        "verdict": "supports" if total == 0 else "contradicts",
        "metric": {"decisions": n, "skus": len(skus),
                   "flagged": {k: len(v) for k, v in flags.items()},
                   "flagged_total": total,
                   "engine_defects": {k: len(flags.get(k, [])) for k in DEFECTS},
                   "assortment_facts": {
                       k: len(flags.get(k, []))
                       for k in ("O5_minimum_order_overhang",
                                 "O6_wild_supplier_lead_time")},
                   "unscheduled_cover_by_mode": {
                       m: round(st.median(v), 2) for m, v in unsched.items()},
                   "flagged_share": round(total / max(n, 1), 5),
                   "chain_median_cover_days": round(chain_median, 2),
                   "cover_by_mode": {m: round(st.median(c), 2)
                                     for m, c in by_mode.items()},
                   "departments_over_3x": [r["department"] for r in outlier_depts],
                   "worst_departments": dept_rows[:6],
                   "examples": {k: v[:3] for k, v in flags.items()}},
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail", "source.corrected-ads",
                    "source.supplier-weekly-schedule"],
        "baseline": (f"cover<={COVER_DAYS_LIMIT}d, order<={ANNUAL_SHARE_LIMIT:.0%} "
                     f"of annual, safety<={SAFETY_SHARE_LIMIT:.0%} of S"),
        "beat_baseline": None, "traps": ["T1", "T3"],
        "notes": (f"No absurd orders across {n:,} empty-shelf decisions. "
                  f"{len(flags.get('O5_minimum_order_overhang', [])):,} lines "
                  "carry months of cover only because one unit is the smallest "
                  "order that exists — a range question, not an ordering one."
                  if total == 0 else
                  f"{total:,} of {n:,} decisions ({total / n:.2%}) are orders a "
                  "buyer would refuse: "
                  + ", ".join(f"{k} {len(v):,}" for k, v in sorted(flags.items()))
                  + (f". Concentrated in {', '.join(r['department'] for r in outlier_depts[:3])}."
                     if outlier_depts else "."))}))

    if len(unsched) > 1:
        sc = st.median(unsched.get("scheduled", [0]))
        od = st.median(unsched.get("on_demand", [1]))
        mult = sc / od if od else None
        phantom = bool(mult and mult > 1.2)
        print(json.dumps({
            "claim": "claim.ordering.unscheduled-vendors-carry-phantom-cover",
            "verdict": "supports" if phantom else "contradicts",
            "metric": {"unscheduled_lines": len(unsched.get("scheduled", [])),
                       "median_cover_scheduled_default": round(sc, 2),
                       "median_cover_on_demand": round(od, 2),
                       "multiple": round(mult, 3) if mult else None,
                       "vendors_with_declared_day": 301,
                       "vendors_total": 582},
            "held_out": False, "provenance": "observed",
            "sources": ["source.supplier-weekly-schedule",
                        "source.fulfilment-detail"],
            "baseline": "R defaults to 7d for a vendor with no declared day",
            "beat_baseline": None, "traps": ["T3", "T7"],
            "notes": (
                f"On the {len(unsched.get('scheduled', [])):,} lines whose vendor "
                f"has no declared order day, defaulting R to 7 days orders "
                f"{mult:.2f}x the cover that on-demand ordering needs "
                f"({sc:.1f}d against {od:.1f}d). R is a property of the ordering "
                "PROCESS, not of the supplier: a vendor absent from the calendar "
                "has not said it can only be ordered weekly, it has said nothing."
                if phantom else
                "Ordering mode does not materially change cover on unscheduled "
                f"lines ({mult:.2f}x).")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
