"""The whole ordering pipeline, every SKU, at six real stock positions.

WHY VARY THE STOCK AND NOT THE DEMAND
    S is a property of the SUPPLY CYCLE -- d, R, L, sigma_L, the shelf life.
    It does not move when the shelf does. What moves is Q = S - on_hand, and
    every operational question is about Q: how big is Monday's order, how many
    lines does it touch, how much of the book is already covered, how much is
    stranded above S and cannot be ordered down.

    So the sweep holds the engine fixed and moves the shelf, anchored on the
    REAL snapshot rather than on a guess: 0x (a cold start), 0.25x and 0.5x
    (drawn down), 1.0x (today), 1.5x and 2.0x (overbought). Each position is
    a real question somebody asks -- what do we buy after a stockout week,
    what do we buy when the buyer has run hot.

    Every quantity comes from ou.recommend() with the raw vendor string, so
    this exercises the same code and the same joins production does.
"""
from __future__ import annotations
import json, sys, math
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, run_all   # noqa: E402
from devkit import build_margin                                 # noqa: E402
from devkit.amit_return import norm                             # noqa: E402
from oasis.logic import order_up_to as ou                       # noqa: E402

MULT = (0.0, 0.25, 0.5, 1.0, 1.5, 2.0)


def main() -> int:
    stock = json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json")
                       .read_text(encoding="utf-8"))
    mar = {norm(k): v for k, v in build_margin.load_or_derive()[0].items()}
    ads = {norm(k): v for k, v in json.loads(
        (ROOT / "oasis" / "data" / "corrected_ads_from_pos.json")
        .read_text(encoding="utf-8")).items()}
    sched = ou.load_review_schedule(str(ROOT))
    pats = ou.default_patterns(str(ROOT))

    base = []
    for k, sv in stock.items():
        m = mar.get(k); av = ads.get(k) or {}
        d = float(av.get("new_ads") or av.get("old_ads") or 0)
        if not m or d <= 0:
            continue
        v_raw = " ".join(str(m.get("vendor") or "").upper().split())
        pt = pats.get(ou.supplier_key(v_raw)) or {}
        base.append((k, " ".join(str(sv.get("dept") or "").upper().split()),
                     d, m["unit_cost"], m["gross_profit_per_unit"], v_raw,
                     max(0.5, float(pt.get("lead_time_mean",
                                           pt.get("lead_time_days", 2)) or 2)),
                     max(0.0, float(sv["stock"]))))
    print(f"  {len(base):,} SKUs priced, demanded and on the shelf")
    # THE ASSERTION THAT WOULD HAVE CAUGHT THE REAL BUG.
    # My first version compared the RAW vendor strings against the calendar
    # keys and failed at 0% -- which is the mirror image of the defect, not the
    # defect: the engine strips the code itself now, so raw-vs-stripped SHOULD
    # miss. What matters is not whether two dictionaries share spellings, it is
    # whether a LINE ends up with a measured review period or falls to the
    # blanket default. That is the thing that was silently 100% wrong, and it
    # is the thing to assert on.
    src = defaultdict(int)
    for b in base:
        src[ou._r_source(b[5], sched)] += 1
    resolved = len(base) - src.get("default", 0)
    print(f"  R resolved from calendar or measured cadence: {resolved:,}/{len(base):,} "
          f"({100*resolved/len(base):.1f}%)  {dict(src)}")
    run_all([join_match_rate(["x"] * resolved, ["x"] * len(base),
                             "order lines -> a measured review period", floor=0.80)])

    print(f"\n  {'stock':>7}{'lines':>8}{'order KES':>13}{'suppressed':>12}"
          f"{'infeasible':>12}{'below P':>10}{'above S KES':>14}{'med cover after':>17}")
    per = {}
    for mu in MULT:
        n_ord = n_sup = n_inf = n_low = 0
        kes = above = 0.0
        cov = []
        rows = []
        for (k, dept, d, c, gp, v_raw, L, oh0) in base:
            oh = oh0 * mu
            r = ou.recommend({"avg_daily_sales": d, "supplier_name": v_raw,
                              "current_stock": oh, "lead_time_days": L,
                              "department": dept, "sku": k},
                             schedule=sched, patterns=pats)
            q = r["quantity"]
            if r.get("auto_order_suppressed"): n_sup += 1
            if not r.get("feasible", True): n_inf += 1
            if oh < r["cycle_stock"] - 1e-9: n_low += 1
            if q > 0:
                n_ord += 1; kes += q * c
            else:
                above += max(oh - r["S"], 0) * c
            cov.append((oh + q) / d)
            rows.append((k, dept, d, c, gp, q, r["S"], oh))
        cov.sort()
        per[mu] = rows
        print(f"  {mu:>6.2f}x{n_ord:>8,}{kes:>13,.0f}{n_sup:>12,}{n_inf:>12,}"
              f"{n_low:>10,}{above:>14,.0f}{cov[len(cov)//2]:>16.1f}d")

    print(f"\n  AT TODAY'S POSITION (1.0x), by department -- the Monday order")
    rows = per[1.0]
    byd = defaultdict(lambda: [0, 0, 0.0, 0.0])
    for (k, dept, d, c, gp, q, S, oh) in rows:
        b = byd[dept]; b[0] += 1
        if q > 0: b[1] += 1; b[2] += q * c
        b[3] += max(oh - S, 0) * c
    print(f"  {'department':<28}{'skus':>6}{'ordering':>10}{'order KES':>13}{'stranded above S':>18}")
    for dept, b in sorted(byd.items(), key=lambda kv: -kv[1][2])[:14]:
        print(f"  {dept[:27]:<28}{b[0]:>6,}{b[1]:>10,}{b[2]:>13,.0f}{b[3]:>18,.0f}")
    tot_str = sum(b[3] for b in byd.values())
    print(f"  {'TOTAL':<28}{sum(b[0] for b in byd.values()):>6,}"
          f"{sum(b[1] for b in byd.values()):>10,}"
          f"{sum(b[2] for b in byd.values()):>13,.0f}{tot_str:>18,.0f}")

    print(f"\n  ELASTICITY OF THE BUY TO THE SHELF")
    k0 = sum(q * c for (_, _, _, c, _, q, _, _) in per[0.0])
    k1 = sum(q * c for (_, _, _, c, _, q, _, _) in per[1.0])
    k2 = sum(q * c for (_, _, _, c, _, q, _, _) in per[2.0])
    print(f"    cold start (0x)  KES {k0:>12,.0f}   <- the full policy position")
    print(f"    today     (1.0x) KES {k1:>12,.0f}   {100*k1/max(k0,1):>5.1f}% of it")
    print(f"    overbought(2.0x) KES {k2:>12,.0f}   {100*k2/max(k0,1):>5.1f}%")
    print(f"    The buy falls as the shelf fills and floors at zero, which is the")
    print(f"    order-up-to property: the engine never chases a target it has met.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
