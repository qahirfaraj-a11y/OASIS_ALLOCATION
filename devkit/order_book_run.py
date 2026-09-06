"""The whole book, one line per SKU, every term the order was built from.

WHY A DECOMPOSITION AND NOT A QUANTITY
    A number nobody can take apart is a number nobody can argue with. This
    emits, for every SKU the engine can price and demand-rate, the full chain

        P     = R + L
        cycle = d * P
        safety= z * sqrt(P*(cv*d)^2 + (d*sigma_L)^2)
        S     = cycle + safety,  then clamped to d * shelf_life
        Q     = max(0, S - on_hand - on_order)

    plus where each input came from (calendar / measured cadence / default for
    R; measured or chain-fallback for sigma_L; observed or fallback margin) and
    the flags that decide whether a line is even allowed to order.

    Outputs oasis/data/order_book.csv for the interrogation agents, and prints
    the pathologies: lines the recommendation would INFLATE and lines it would
    leave SHORT.
"""
from __future__ import annotations

import argparse, csv, json, math, sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import join_match_rate, run_all      # noqa: E402
from devkit import build_margin                                    # noqa: E402
from devkit.amit_return import norm, strip_code                    # noqa: E402
from oasis.logic import order_up_to as ou                          # noqa: E402

STOCK = ROOT / "oasis" / "data" / "stock_snapshot_dept.json"
ADS = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"
AMIT = ROOT / "oasis" / "data" / "amit_enforcement.json"
MANDE = ROOT / "oasis" / "data" / "mande_purge_report.json"
OUT = ROOT / "oasis" / "data" / "order_book.csv"
CV = 0.40

FIELDS = ["sku", "dept", "vendor", "d", "cost", "gp_unit", "price",
          "R", "R_source", "L", "sigma_L", "sigma_L_source", "z", "P",
          "cycle_units", "safety_units", "S_raw", "shelf_life", "S",
          "clamped", "on_hand", "Q", "cover_before", "cover_after",
          "long_life", "suppressed", "feasible", "structurally_short", "below_protection", "amit_blocked",
          "mande_flagged", "order_kes", "excess_kes", "gp_year"]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inflate-days", type=float, default=60.0,
                    help="cover after ordering above which a line is called inflated")
    a = ap.parse_args(argv)

    stock = json.loads(STOCK.read_text(encoding="utf-8"))
    mar = {norm(k): v for k, v in build_margin.load_or_derive()[0].items()}
    ads = {norm(k): v for k, v in json.loads(ADS.read_text(encoding="utf-8")).items()}
    sched = ou.load_review_schedule(str(ROOT))
    pats = ou.default_patterns(str(ROOT))
    cad = ou.default_cadence(str(ROOT))
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

    z = ou.z_score()
    chain_sL = ou.chain_sigma_lead()
    rows = []
    for k, sv in stock.items():
        m = mar.get(k); av = ads.get(k) or {}
        d = float(av.get("new_ads") or av.get("old_ads") or 0)
        if not m or d <= 0:
            continue
        dept = " ".join(str(sv.get("dept") or "").upper().split())
        v_raw = " ".join(str(m.get("vendor") or "").upper().split())
        v = strip_code(v_raw)
        oh_pre = max(0.0, float(sv["stock"]))
        pt = pats.get(v) or {}
        L = max(0.5, float(pt.get("lead_time_mean", pt.get("lead_time_days", 2)) or 2))
        # THE ENGINE, NOT A COPY OF IT. This file used to rebuild S inline --
        # and with a bare min(S, d*shelf) that predated the protection-interval
        # floor, so the audit artefact disagreed with production on the 770
        # structurally-short lines. It also pre-stripped the vendor code, which
        # is exactly how the supplier-key defect stayed invisible. Same call,
        # same strings.
        rec = ou.recommend({"avg_daily_sales": d, "supplier_name": v_raw,
                            "current_stock": oh_pre, "lead_time_days": L,
                            "department": dept, "sku": k},
                           schedule=sched, patterns=pats)
        R = rec["R"]; sL = rec["sigma_lead"]; P = rec["P"]
        cvx = ou.demand_cv(d)
        cyc = rec["cycle_stock"]; saf = rec["safety_stock"]
        S_raw = rec["S_unclamped"]; S = rec["S"]; sl = rec["shelf_life_days"]
        oh = oh_pre
        Q = rec["quantity"]
        rows.append(dict(
            sku=k, dept=dept, vendor=v, d=round(d, 4), cost=round(m["unit_cost"], 4),
            gp_unit=round(m["gross_profit_per_unit"], 4), price=round(m["selling_price"], 4),
            R=round(R, 3), R_source=ou._r_source(v_raw, sched), L=round(L, 3),
            sigma_L=round(sL, 3),
            sigma_L_source=("measured" if (pt.get("lead_time_stdev") is not None
                                           and int(pt.get("samples") or 0) >= ou.MIN_SIGMA_SAMPLES)
                            else "chain_fallback"),
            z=round(z, 4), P=round(P, 3), cycle_units=round(cyc, 3),
            safety_units=round(saf, 3), S_raw=round(S_raw, 3), shelf_life=sl,
            S=round(S, 3), clamped=int(S < S_raw - 1e-9), on_hand=round(oh, 2),
            Q=round(Q, 3), cover_before=round(oh / d, 2), cover_after=round((oh + Q) / d, 2),
            long_life=int(ou.is_long_life(k)),
            suppressed=int(bool(rec.get("auto_order_suppressed"))),
            feasible=int(bool(rec.get("feasible", True))),
            structurally_short=int(sl > 0 and S < cyc - 1e-9),
            below_protection=int(oh < cyc - 1e-9),
            amit_blocked=int(k in amit), mande_flagged=int(v in purge),
            order_kes=round(Q * m["unit_cost"], 2),
            excess_kes=round(max(oh - S, 0) * m["unit_cost"], 2),
            gp_year=round(m["gross_profit_per_unit"] * d * 365.0, 2)))

    with OUT.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=FIELDS); w.writeheader(); w.writerows(rows)

    run_all([join_match_rate(list(stock), [r["sku"] for r in rows],
                             "stock snapshot -> priced, demanded SKUs", floor=0.25)])
    n = len(rows)
    print(f"\n  {n:,} SKUs · wrote {OUT.relative_to(ROOT)}")
    print(f"  R source     {dict((k, sum(1 for r in rows if r['R_source'] == k)) for k in {r['R_source'] for r in rows})}")
    print(f"  sigma_L      measured {sum(1 for r in rows if r['sigma_L_source']=='measured'):,} · "
          f"chain fallback {sum(1 for r in rows if r['sigma_L_source']=='chain_fallback'):,} "
          f"(chain value {chain_sL:.2f} d)")

    ordering = [r for r in rows if r["Q"] > 0]
    infl = [r for r in ordering if r["cover_after"] > a.inflate_days]
    short = [r for r in rows if r["below_protection"]]
    ss = [r for r in rows if r["structurally_short"]]
    print(f"\n  WOULD ORDER          {len(ordering):>6,} lines · KES "
          f"{sum(r['order_kes'] for r in ordering):>12,.0f}")
    print(f"  INFLATED (>{a.inflate_days:.0f}d cover after ordering)  {len(infl):>6,} lines · KES "
          f"{sum(r['order_kes'] for r in infl):>12,.0f}  "
          f"({100*sum(r['order_kes'] for r in infl)/max(sum(r['order_kes'] for r in ordering),1):.1f}% of the buy)")
    print(f"  BELOW PROTECTION now {len(short):>6,} lines · GP at risk KES "
          f"{sum(r['gp_year'] for r in short):>12,.0f}/yr")
    print(f"  STRUCTURALLY SHORT   {len(ss):>6,} lines (shelf life under the protection interval)")
    print(f"  ALREADY ABOVE S      {sum(1 for r in rows if r['excess_kes']>0):>6,} lines · KES "
          f"{sum(r['excess_kes'] for r in rows):>12,.0f} above the order-up-to level")

    print(f"\n  the safety term as a share of S -- where the capital is going")
    sh = sorted(r["safety_units"] / max(r["S_raw"], 1e-9) for r in rows)
    q = lambda x: sh[int(x * len(sh))]
    print(f"    p10 {q(.1):.0%} · p25 {q(.25):.0%} · median {q(.5):.0%} · p75 {q(.75):.0%} · p90 {q(.9):.0%}")

    print(f"\n  top inflated buys")
    print(f"  {'sku':<44}{'dept':<16}{'cover→':>8}{'Q':>9}{'KES':>11}{'R':>5}{'sL':>6}")
    for r in sorted(infl, key=lambda r: -r["order_kes"])[:12]:
        print(f"  {r['sku'][:43]:<44}{r['dept'][:15]:<16}{r['cover_after']:>8.0f}"
              f"{r['Q']:>9.0f}{r['order_kes']:>11,.0f}{r['R']:>5.0f}{r['sigma_L']:>6.2f}")

    print(json.dumps({
        "claim": "claim.ordering.book-is-neither-inflated-nor-short",
        "verdict": "contradicts",
        "metric": {"skus": n, "would_order_lines": len(ordering),
                   "would_order_kes": round(sum(r["order_kes"] for r in ordering)),
                   "inflated_lines": len(infl),
                   "inflated_kes": round(sum(r["order_kes"] for r in infl)),
                   "below_protection_lines": len(short),
                   "below_protection_gp_year": round(sum(r["gp_year"] for r in short)),
                   "structurally_short": len(ss),
                   "above_S_kes": round(sum(r["excess_kes"] for r in rows)),
                   "safety_share_median": round(q(.5), 3)},
        "held_out": False, "provenance": "observed",
        "sources": ["source.stock-snapshot-dept", "source.grn-book",
                    "source.corrected-ads", "source.lead-patterns"],
        "baseline": None, "beat_baseline": None, "traps": ["T1", "T3"],
        "notes": "Full term decomposition per SKU in oasis/data/order_book.csv."}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
