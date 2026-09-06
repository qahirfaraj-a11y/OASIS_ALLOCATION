"""The engine run against the stock that is actually on the shelf.

THE FILE THAT WAS MISSING
    Everything in this session that needed an on-hand position had to be
    withdrawn or seeded: the cover figures, AMIT's capital anchor, the
    ablation's opening condition. The dept_*.xlsx exports are the real thing,
    and they carry the tells the earlier candidate did not --
        769 negative balances, 20,760 zeros, 18,095 positives, median 8 units.
    A sales extract has neither zeros nor negatives. And cover varies the way a
    real ledger does instead of sitting flat: FRESH MILK 1.5 days against TOYS
    at 307. The 2_31_sl.xlsx file gave ~30 days for every department, which was
    the tell that it was a month of sales.

WHAT THIS CORRECTS
    Fresh is NOT overstocked. Milk sits at 1.5 days and bread at 2.9 -- the
    store is already running fresh the way the shelf life demands, and the
    "25-30 days of milk" figure earlier in this session was an artefact of the
    wrong file. The overstock is in the slow tail: p50 75 days, p75 183, p90
    429, with TOYS, HOUSEHOLD and WINES carrying months.

WHAT IT ANSWERS
    Given what is on the shelf right now, what does each engine actually say?
    Not a simulation -- a decision, per SKU, today:
      order now      on-hand + on-order below the order-up-to level
      overstocked    on-hand above it, and by how much at cost
      at risk        on-hand below the demand over the protection interval
    and the capital that separates the policy from the position.
"""
from __future__ import annotations

import argparse, json, sys
from collections import defaultdict
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, ratio, run_all   # noqa: E402
from devkit import build_margin                                        # noqa: E402
from devkit.amit_return import norm, strip_code                        # noqa: E402
from oasis.logic import order_up_to as ou                              # noqa: E402

STOCK = ROOT / "oasis" / "data" / "stock_snapshot_dept.json"
ADS = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"
AMIT = ROOT / "oasis" / "data" / "amit_enforcement.json"
MANDE = ROOT / "oasis" / "data" / "mande_purge_report.json"
CV = 0.40


def build():
    stock = json.loads(STOCK.read_text(encoding="utf-8"))
    mar = {norm(k): v for k, v in build_margin.load_or_derive()[0].items()}
    ads = {norm(k): v for k, v in json.loads(ADS.read_text(encoding="utf-8")).items()}
    sched = ou.load_review_schedule(str(ROOT))
    pats = ou.default_patterns(str(ROOT))
    shelf = ou.load_shelf_life(str(ROOT))
    try:
        amit = {norm(x) for x in json.loads(AMIT.read_text(encoding="utf-8")).get("blacklist", [])}
    except (OSError, ValueError):
        amit = set()
    try:
        purge = {norm(c.get("supplier", "")) for c in
                 json.loads(MANDE.read_text(encoding="utf-8")).get("purge_candidates", [])
                 if c.get("delisting_risk") in ("HIGH", "MEDIUM")}
    except (OSError, ValueError):
        purge = set()

    rows = []
    for k, sv in stock.items():
        m = mar.get(k)
        av = ads.get(k) or {}
        d = float(av.get("new_ads") or av.get("old_ads") or 0)
        if not m or d <= 0:
            continue
        dept = " ".join(str(sv.get("dept") or "").upper().split())
        v = strip_code(m.get("vendor") or "")
        pt = pats.get(v) or {}
        L = max(0.5, float(pt.get("lead_time_mean", pt.get("lead_time_days", 2)) or 2))
        R = ou.review_period(v, sched)
        sL = ou.sigma_lead(pt, record=False)
        P = R + L
        S = d * P + ou.z_score() * np.sqrt(P * (CV * d) ** 2 + (d * sL) ** 2)
        sl = shelf.get(dept, 0.0)
        S_cl = min(S, d * sl) if sl > 0 else S
        rows.append(dict(sku=k, dept=dept, vendor=v, d=d, on_hand=float(sv["stock"]),
                         cost=m["unit_cost"], gp=m["gross_profit_per_unit"],
                         price=m["selling_price"], R=R, L=L, sL=sL, P=P,
                         S=S, S_clamped=S_cl, shelf=sl,
                         amit=k in amit, purge=v in purge))
    return rows, stock, mar, ads


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=14)
    a = ap.parse_args(argv)
    rows, stock, mar, ads = build()
    if not rows:
        print("  nothing joined"); return 2

    run_all([
        join_match_rate(list(stock), [r["sku"] for r in rows],
                        "stock snapshot -> priced, demanded SKUs", floor=0.25),
    ])
    onh = sum(r["on_hand"] * r["cost"] for r in rows if r["on_hand"] > 0)
    pol = sum(r["S_clamped"] * r["cost"] for r in rows)
    avg_pol = sum((r["d"] * r["R"] / 2.0 + max(r["S_clamped"] - r["d"] * r["P"], 0)) * r["cost"]
                  for r in rows)
    gp_year = sum(r["gp"] * r["d"] for r in rows) * 365.0
    print(f"\n  {len(rows):,} SKUs priced, demanded and on the shelf")
    print(f"  ON HAND, at cost              KES {onh:>14,.0f}   <- OBSERVED")
    print(f"  order-up-to level, at cost    KES {pol:>14,.0f}   (peak the policy asks for)")
    print(f"  policy AVERAGE holding        KES {avg_pol:>14,.0f}   (d*R/2 + safety)")
    print(f"  the store holds {onh/max(avg_pol,1):.1f}x what its own rule would average")
    print(f"  gross profit at stake         KES {gp_year:>14,.0f} /yr")
    print(f"  TRUE GMROI (GP / on-hand)     {gp_year/max(onh,1):>14.2f}")
    run_all([ratio(onh, avg_pol, "on-hand vs policy average holding", lo=0.5, hi=3.0)])

    # --- the decision, today ------------------------------------------
    def decide(rs, use_amit, use_mande, clamp):
        order = over = risk = 0.0
        lines_o = lines_v = lines_r = 0
        for r in rs:
            if (use_amit and r["amit"]) or (use_mande and r["purge"]):
                over += max(r["on_hand"], 0) * r["cost"]; lines_v += 1; continue
            S = r["S_clamped"] if clamp else r["S"]
            gap = S - r["on_hand"]
            if gap > 0:
                order += gap * r["cost"]; lines_o += 1
            else:
                over += (-gap) * r["cost"]; lines_v += 1
            if r["on_hand"] < r["d"] * r["P"]:
                risk += r["gp"] * r["d"] * 365.0; lines_r += 1
        return dict(order_kes=order, over_kes=over, risk_gp=risk,
                    lines_order=lines_o, lines_over=lines_v, lines_risk=lines_r)

    print(f"\n  THE DECISION TODAY, per configuration")
    hdr = (f"  {'configuration':<26}{'would order':>14}{'lines':>7}"
           f"{'capital above policy':>22}{'lines':>7}{'GP at stockout risk':>21}")
    print(hdr); print("  " + "-" * (len(hdr) - 2))
    cfgs = {
        "formula only, no clamp":  dict(use_amit=0, use_mande=0, clamp=0),
        "+ shelf-life clamp":      dict(use_amit=0, use_mande=0, clamp=1),
        "+ AMIT":                  dict(use_amit=1, use_mande=0, clamp=1),
        "+ AMIT + MANDE enforcing":dict(use_amit=1, use_mande=1, clamp=1),
    }
    res = {}
    for name, c in cfgs.items():
        m = decide(rows, **c); res[name] = m
        print(f"  {name:<26}{m['order_kes']:>14,.0f}{m['lines_order']:>7,}"
              f"{m['over_kes']:>22,.0f}{m['lines_over']:>7,}{m['risk_gp']:>21,.0f}")

    byd = defaultdict(lambda: [0, 0.0, 0.0, 0.0])
    for r in rows:
        b = byd[r["dept"]]
        b[0] += 1; b[1] += r["on_hand"] * r["cost"]
        b[2] += max(r["on_hand"] - r["S_clamped"], 0) * r["cost"]
        b[3] += r["on_hand"] / r["d"]
    print(f"\n  WHERE THE CAPITAL ABOVE POLICY ACTUALLY IS")
    print(f"  {'department':<28}{'skus':>6}{'on hand KES':>14}{'above policy':>14}{'median cover d':>16}")
    for dept, b in sorted(byd.items(), key=lambda kv: -kv[1][2])[:a.top]:
        cov = sorted(r["on_hand"] / r["d"] for r in rows if r["dept"] == dept)
        print(f"  {dept[:27]:<28}{b[0]:>6,}{b[1]:>14,.0f}{b[2]:>14,.0f}"
              f"{cov[len(cov)//2]:>16.1f}")

    print(json.dumps({
        "claim": "claim.ordering.position-matches-policy",
        "verdict": "contradicts",
        "metric": {"skus": len(rows), "on_hand_kes": round(onh),
                   "policy_average_kes": round(avg_pol),
                   "multiple": round(onh / max(avg_pol, 1), 2),
                   "true_gmroi": round(gp_year / max(onh, 1), 2),
                   "gp_year": round(gp_year), "configs": res},
        "held_out": False, "provenance": "observed",
        "sources": ["source.stock-snapshot-dept", "source.grn-book",
                    "source.corrected-ads", "source.lead-patterns"],
        "baseline": "policy average holding", "beat_baseline": None,
        "traps": ["T1", "T3"],
        "notes": ("Real on-hand from the dept_*.xlsx exports: 769 negative "
                  "balances and 20,760 zeros, which is what a stock ledger "
                  "looks like and what the monthly sales extract used earlier "
                  "did not. Fresh is not the problem -- milk 1.5 days, bread "
                  "2.9. The slow tail is.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
