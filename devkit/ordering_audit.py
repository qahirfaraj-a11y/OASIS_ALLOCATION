"""Full-book audit: what is over-ordered, under-ordered, and on its own terms.

DEFINITIONS, STATED RATHER THAN IMPLIED
    The engine's own contract is Q = S - (on_hand + on_order), with
    S = d(R+L) + z*sigma_P clamped by shelf life. So a line is judged against
    what the engine itself decided, not against an outside opinion:

      UNDER   position after ordering < d*(R+L)
              Below the protection interval -- the demand that will certainly
              arrive before the next delivery can land. A planned stockout.

      OVER    position after ordering > S + one pack
              Above the engine's own target by more than the smallest
              orderable step. By construction Q cannot do this: it can only
              arrive from pack rounding up, an MOQ floor, or a guard.

      SPOIL   cover after ordering > the line's shelf life
              Over-ordering that ends in a bin rather than a shelf.

      PARKED  cover after ordering > MAX_AUTO_ORDER_COVER_DAYS
              Capital held past the point the engine calls dead.

    A line can be none of these, which is the intended outcome.

THE EDGE CASES ASKED FOR
    LONG-CADENCE suppliers: R measured above 21 days. One missed review costs
    a month, and R+L interacts with every cap.

    NO-GRN suppliers: the twelve in no_grn_suppliers.json deliver without a
    goods-received record, so there is no measured lead time and no measured
    cadence for them -- both fall back to chain defaults. This is the closest
    thing in the data to a central-warehouse refill, which does not exist
    here: no supplier in the book is an internal source. Where the supply
    terms are UNKNOWN the engine must not become generous, so the audit
    reports whether these lines skew over or under.

USAGE
    python devkit/ordering_audit.py [--csv out.csv]
"""
from __future__ import annotations

import copy
import csv
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.order_model_compare import build_book, DATA_DIR, band_of  # noqa: E402
from oasis.logic import order_up_to as ou                             # noqa: E402

R_BANDS = ((0, 2, "daily (R<=2)"), (2, 8, "weekly (2-8)"),
           (8, 21, "fortnightly (8-21)"), (21, 1e9, "monthly+ (>21)"))


def r_band(R):
    for lo, hi, name in R_BANDS:
        if lo < R <= hi or (lo == 0 and R <= hi):
            return name
    return "monthly+ (>21)"


def main(argv) -> int:
    out_csv = argv[argv.index("--csv") + 1] if "--csv" in argv else None

    from oasis.logic.simulation_bridge import SimulationOrderUtil
    book = build_book()
    util = SimulationOrderUtil(DATA_DIR)
    enriched = util.prepare_sku_data(copy.deepcopy(book))
    base = {p["sku"]: float(p.get("current_stock") or 0) for p in book}
    cost = {p["sku"]: p["unit_cost"] for p in book}
    for p in enriched:
        p["current_stock"] = base.get(p.get("sku"), 0.0)

    try:
        no_grn = {str(s).upper().strip() for s in json.loads(
            (ROOT / "oasis" / "data" / "no_grn_suppliers.json")
            .read_text(encoding="utf-8"))}
    except Exception:
        no_grn = set()

    os.environ["OASIS_ORDER_MODEL"] = "order_up_to"
    recs = util.finalize_orders(util.calculate_order_quantity(
        copy.deepcopy(enriched), use_real_date=True))
    os.environ.pop("OASIS_ORDER_MODEL", None)

    rows = []
    for r in recs:
        k = r.get("sku") or r.get("item_code")
        t = r.get("order_up_to_terms") or {}
        d = float(t.get("d") or r.get("avg_daily_sales") or 0)
        if d <= 0:
            continue
        R, L = float(t.get("R") or 0), float(t.get("L") or 0)
        S = float(t.get("S") or 0)
        P = R + L
        pack = max(1.0, float(r.get("pack_size") or 1))
        oh = base.get(k, 0.0)
        q = float(r.get("recommended_quantity") or 0)
        pos = oh + q
        sup = str(r.get("supplier_name") or "").upper()
        sl = float(t.get("shelf_life") or 0) or ou.shelf_life_for(
            r.get("department") or "")

        # A line with no terms never reached the order-up-to computation: the
        # shared trigger above the model fork did not fire, so it exited on a
        # classic reason ("Above ROP", "Schedule: Gap 7d"). Judging it against
        # an S the engine never computed reads my own default back as the
        # engine's decision, which is how the worst "over-ordered" lines came
        # back showing S = 0 on real demand.
        has_terms = bool(t)

        verdicts = []
        if has_terms and P > 0 and pos < d * P - 1e-9:
            verdicts.append("UNDER")
        if has_terms and S > 0 and pos > S + pack + 1e-9:
            verdicts.append("OVER")
        if sl and sl > 0 and pos / d > sl:
            verdicts.append("SPOIL" if q > 0 else "SPOIL(inherited)")
        if pos / d > ou.MAX_AUTO_ORDER_COVER_DAYS:
            # Separate what the engine BOUGHT from what it merely found on the
            # shelf. It cannot order stock down, only decline to add to it, so
            # calling inherited depth "over-ordering" blames the wrong actor.
            verdicts.append("PARKED" if q > 0 else "PARKED(inherited)")
        if not has_terms:
            verdicts.append("no-target")

        rows.append({
            "sku": k, "dept": r.get("department") or "", "d": d,
            "R": R, "L": L, "S": S, "on_hand": oh, "q": q,
            "cover": pos / d, "P": P, "kes": q * cost.get(k, 0.0),
            "value_at_risk": pos * cost.get(k, 0.0),
            "r_band": r_band(R), "v_band": band_of(d),
            "no_grn": any(n in sup for n in no_grn) if no_grn else False,
            "suppressed": bool(r.get("auto_order_suppressed")),
            "verdict": "+".join(verdicts) or "on target",
        })

    n = len(rows)
    print(f"{n:,} lines with a demand rate\n")

    print("=" * 84)
    print("VERDICT CENSUS")
    print("=" * 84)
    cen = defaultdict(lambda: [0, 0.0])
    for r in rows:
        cen[r["verdict"]][0] += 1
        cen[r["verdict"]][1] += r["value_at_risk"]
    print(f"  {'verdict':<28}{'lines':>9}{'% book':>9}{'position KES':>16}")
    for v, (c, val) in sorted(cen.items(), key=lambda x: -x[1][0]):
        print(f"  {v:<28}{c:>9,}{100*c/n:>8.1f}%{val:>16,.0f}")

    def seg(rs, label, width=26):
        tot = len(rs) or 1
        u = sum(1 for r in rs if "UNDER" in r["verdict"])
        o = sum(1 for r in rs if "OVER" in r["verdict"])
        # Engine-caused only: inherited depth is not an ordering decision.
        s = sum(1 for r in rs if "SPOIL" in r["verdict"]
                and "SPOIL(inherited)" not in r["verdict"])
        p = sum(1 for r in rs if "PARKED" in r["verdict"]
                and "PARKED(inherited)" not in r["verdict"])
        cov = sorted(r["cover"] for r in rs)
        med = cov[len(cov) // 2] if cov else 0
        print(f"  {label:<{width}}{len(rs):>8,}{100*u/tot:>9.1f}%"
              f"{100*o/tot:>9.1f}%{100*s/tot:>9.1f}%{100*p/tot:>9.1f}%"
              f"{med:>11.1f}d")

    hdr = (f"  {'segment':<26}{'lines':>8}{'UNDER':>9}{'OVER':>9}"
           f"{'SPOIL':>9}{'PARKED':>9}{'med cover':>12}")

    print("\n" + "=" * 84)
    print("BY SUPPLIER CADENCE — the slow-supplier edge case")
    print("=" * 84)
    print(hdr)
    for _, _, name in R_BANDS:
        rs = [r for r in rows if r["r_band"] == name]
        if rs:
            seg(rs, name)

    print("\n" + "=" * 84)
    print("BY VELOCITY")
    print("=" * 84)
    print(hdr)
    for name in ("<=1/day", "1-5/day", "5-10/day", ">10/day"):
        rs = [r for r in rows if r["v_band"] == name]
        if rs:
            seg(rs, name)

    print("\n" + "=" * 84)
    print("NO-GRN SUPPLIERS — supply terms unknown, so we must not be generous")
    print("=" * 84)
    print(hdr)
    for label, rs in (("no GRN (defaults)", [r for r in rows if r["no_grn"]]),
                      ("measured supplier", [r for r in rows if not r["no_grn"]])):
        if rs:
            seg(rs, label)

    print("\n" + "=" * 84)
    print("WORST OVER-ORDERS BY CAPITAL AT RISK")
    print("=" * 84)
    over = [r for r in rows if "OVER" in r["verdict"] or "PARKED" in r["verdict"]
            or "SPOIL" in r["verdict"]]
    over.sort(key=lambda r: -r["value_at_risk"])
    print(f"  {'sku':<34}{'d':>7}{'cover':>9}{'S':>9}{'Q':>7}"
          f"{'pos KES':>12}  verdict")
    for r in over[:15]:
        print(f"  {str(r['sku'])[:34]:<34}{r['d']:>7.2f}{r['cover']:>8.0f}d"
              f"{r['S']:>9.0f}{r['q']:>7.0f}{r['value_at_risk']:>12,.0f}  "
              f"{r['verdict']}")

    if out_csv:
        with open(out_csv, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
        print(f"\n  per-SKU detail -> {out_csv} ({len(rows):,} rows)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
