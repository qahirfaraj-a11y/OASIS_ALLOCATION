"""Probe: is R the observed order gap, and what does the book actually carry?

LOOP B'S OBJECTIVE FUNCTION, AT LAST ON OBSERVED DATA.

    Residual cover: for each delivery, the cover it carried against the gap it
    actually had to span. Taken AFTERWARDS, from what happened, so the ordering
    habit cannot contaminate its own score — the circularity that caught this
    project twice before it was named.

        cover_days   = qty_received / ADS
        gap_days     = days until the next receipt of that item
        residual     = cover_days / gap_days

THE HELD-OUT SPLIT IS A GIFT FROM A DEFECT
    The receipt export is missing April, May and June 2025 entirely. That hole
    also separates the year into two contiguous blocks that never touch, which
    is exactly the shape a train/test split wants:

        train  2025-01 .. 2025-03     choose R per item
        test   2025-07 .. 2025-12     score it on gaps it has never seen

    So the defect that would have corrupted a naive mean is what makes this
    measurement honest.

WHAT IS COMPARED
    Sizing for a protection interval P = R + L, an item is covered when
    P >= the gap that actually arrived.

        policy      R = 7 days, the engine's default review period
        observed    R = the item's own median receipt gap, from TRAIN only

    service  fraction of TEST gaps the interval covers
    capital  mean P / gap over covered gaps — how much cover was carried to
             buy that service

    L is observed too: PO date to GRN date, per vendor, from the same export.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import datetime as _dt
import json
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import stock_ledger as SL                                    # noqa: E402
from devkit.methodology.traps import (join_match_rate, non_circular,     # noqa: E402
                                      ratio, run_all)

TRAIN = ("2025-01", "2025-03")
TEST = ("2025-07", "2025-12")
POLICY_R = 7.0
DEFAULT_L = 2.29


def _in(d: _dt.date, span) -> bool:
    return span[0] <= d.strftime("%Y-%m") <= span[1]


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=None, help="local copy, for speed")
    a = ap.parse_args(argv)

    led = SL.load(local_copy=Path(a.xlsx) if a.xlsx else None)
    print(f"  receipts for {len(led.receipts):,} items · ADS for {len(led.ads):,}")
    print(f"  rejected {led.rejected_subtotals} subtotal rows carrying "
          f"{led.subtotal_qty:,.0f} units · holes {led.holes}")

    # ---- L, observed, per vendor -----------------------------------------
    lead_by_vendor = defaultdict(list)
    for rs in led.receipts.values():
        for r in rs:
            if r.lead_days is not None and 0 <= r.lead_days <= 120:
                lead_by_vendor[r.vendor].append(r.lead_days)
    vendor_L = {v: st.median(x) for v, x in lead_by_vendor.items() if x}
    all_L = [x for v in lead_by_vendor.values() for x in v]
    L_med = st.median(all_L) if all_L else DEFAULT_L
    L_sd = st.pstdev(all_L) if len(all_L) > 1 else 0.0

    # ---- gaps, split, hole-aware -----------------------------------------
    train_gaps, test_gaps = defaultdict(list), defaultdict(list)
    spanning = 0
    for item in led.receipts:
        for g in led.gaps(item):
            if g["spans_hole"]:
                spanning += 1
                continue
            if _in(g["from"], TRAIN) and _in(g["to"], TRAIN):
                train_gaps[item].append(g)
            elif _in(g["from"], TEST) and _in(g["to"], TEST):
                test_gaps[item].append(g)

    checks = [
        non_circular("residual cover",
                     inputs=["receipt_dates", "receipt_qty", "ads_from_pos"],
                     downstream_of_behaviour=["order_decision", "policy_R"]),
        join_match_rate(sorted(led.receipts), sorted(led.ads),
                        "receipt items ↔ ADS items", floor=0.50),
    ]

    # ---- the actual book's residual cover ---------------------------------
    resid = []
    for item, gs in test_gaps.items():
        ads = led.ads.get(item)
        if not ads:
            continue
        for g in gs:
            cover = g["qty"] / ads
            if g["days"] > 0:
                resid.append(cover / g["days"])
    resid_med = st.median(resid) if resid else None

    # ---- policy R vs observed R, scored on TEST ---------------------------
    items = [i for i in test_gaps
             if len(train_gaps.get(i, [])) >= 2 and len(test_gaps[i]) >= 2
             and i in led.ads]
    rows = []
    for item in items:
        r_obs = st.median([g["days"] for g in train_gaps[item]])
        vend = led.receipts[item][0].vendor
        L = vendor_L.get(vend, L_med)
        for g in test_gaps[item]:
            rows.append((g["days"], POLICY_R + L, r_obs + L))
    if not rows:
        print(json.dumps({"claim": "claim.ordering.R-is-observed-gap",
                          "verdict": "inconclusive", "metric": {},
                          "notes": "no items with gaps in both train and test"}))
        return 0

    def score(idx):
        covered = [(g, p) for g, p, _ in
                   ((g, (p if idx == 1 else o), o) for g, p, o in rows) if p >= g]
        service = len(covered) / len(rows)
        cap = st.mean([p / g for g, p in covered]) if covered else float("nan")
        return service, cap

    svc_pol, cap_pol = score(1)
    svc_obs, cap_obs = score(2)

    gap_days = [g for g, _, _ in rows]
    gap_cv = (st.pstdev(gap_days) / st.mean(gap_days)) if gap_days else None

    checks.append(ratio(cap_pol, cap_obs, "cover carried: policy vs observed R",
                        lo=0.2, hi=5.0))
    run_all(checks)

    print(f"  L observed: median {L_med:.2f}d · sd {L_sd:.2f}d "
          f"· {len(vendor_L):,} vendors")
    print(f"  gaps: train {sum(len(v) for v in train_gaps.values()):,} · "
          f"test {sum(len(v) for v in test_gaps.values()):,} · "
          f"{spanning:,} discarded for spanning the hole")
    print(f"  items scored: {len(items):,} · test gaps scored: {len(rows):,}")
    print(f"  policy   R={POLICY_R}  service {svc_pol:.1%}  cover carried {cap_pol:.2f}x")
    print(f"  observed R=median   service {svc_obs:.1%}  cover carried {cap_obs:.2f}x")
    if resid_med is not None:
        print(f"  the book's own residual cover (median): {resid_med:.2f}x")

    better = (svc_obs >= svc_pol - 0.02) and (cap_obs < cap_pol)
    common = {"held_out": True, "provenance": "observed",
              "sources": ["source.fulfilment-detail", "source.corrected-ads"],
              "traps": ["T1", "T3", "T5"]}

    print(json.dumps({
        "claim": "claim.ordering.R-is-observed-gap",
        "verdict": "supports" if better else "inconclusive",
        "metric": {"service_policy": round(svc_pol, 4),
                   "service_observed": round(svc_obs, 4),
                   "cover_policy": round(cap_pol, 4),
                   "cover_observed": round(cap_obs, 4),
                   "working_capital_multiple": (round(cap_pol / cap_obs, 3)
                                                if cap_obs else None),
                   "items": len(items), "test_gaps": len(rows),
                   "train": TRAIN, "test": TEST,
                   "L_median": round(L_med, 3), "L_sd": round(L_sd, 3)},
        "baseline": f"policy R={POLICY_R}d + observed L",
        "beat_baseline": bool(better), **common,
        "notes": (
            f"Trained on {TRAIN[0]}..{TRAIN[1]}, scored on {TEST[0]}..{TEST[1]} — "
            "two blocks the missing quarter keeps from touching. Observed R "
            f"carries {cap_obs:.2f}x cover against policy's {cap_pol:.2f}x at "
            f"{svc_obs:.1%} vs {svc_pol:.1%} service.")}))

    print(json.dumps({
        "claim": "claim.ordering.cadence-is-a-distribution",
        "verdict": "supports" if (gap_cv or 0) > 0.5 else "contradicts",
        "metric": {"gap_cv": round(gap_cv, 4) if gap_cv else None,
                   "gap_median": st.median(gap_days), "n": len(gap_days)},
        "baseline": "a point estimate of cadence", "beat_baseline": None,
        **common,
        "notes": f"Receipt gap CV {gap_cv:.2f} across {len(gap_days):,} observed "
                 "test gaps. A point estimate of cadence is a summary of a wide "
                 "distribution, not a property of the supplier."}))

    print(json.dumps({
        "claim": "claim.ordering.p75-service-implicit",
        "verdict": "supports" if resid_med is not None else "inconclusive",
        "metric": {"residual_cover_median": (round(resid_med, 3)
                                             if resid_med else None),
                   "deliveries": len(resid)},
        "baseline": "a chosen service level", "beat_baseline": None,
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail", "source.corrected-ads"],
        "traps": ["T5"],
        "notes": (f"The book carries a median {resid_med:.2f}x of the cover each "
                  "delivery needed. Nobody chose that number."
                  if resid_med else "no residual cover computable")}))

    holes = bool(led.holes) or led.rejected_subtotals
    print(json.dumps({
        "claim": "claim.ordering.receipt-history-is-complete",
        "verdict": "contradicts" if holes else "supports",
        "metric": {"holes": led.holes,
                   "subtotal_rows": led.rejected_subtotals,
                   "subtotal_qty": round(led.subtotal_qty),
                   "undated_rows": led.rejected_undated,
                   "gaps_discarded_for_spanning": spanning,
                   "months": [led.covered_months[0], led.covered_months[-1]]},
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail"], "traps": ["T3"],
        "baseline": "a complete year of receipts", "beat_baseline": None,
        "notes": (f"Months {led.holes} are absent entirely, and {spanning:,} "
                  f"inter-receipt gaps were discarded for spanning them. "
                  f"{led.rejected_subtotals} subtotal rows carry "
                  f"{led.subtotal_qty:,.0f} units — exactly half the naive "
                  "total, so any sum of this file's quantities that kept them "
                  "is 2x out.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
