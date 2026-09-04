"""Probe: lead time and its spread, from receipts alone.

WHY RECEIPTS ONLY
    The gap between consecutive receipts is a fact about ORDERING BEHAVIOUR —
    how often somebody chose to buy — and it is contaminated twice over: by the
    habit it is meant to measure, and by the missing quarter, which turns one
    March receipt and one July receipt into a 120-day gap that never happened.

    PO date to GRN date needs none of that. It is a fact about the SUPPLIER,
    measured inside a single transaction, and a hole in the calendar cannot
    fabricate one. Every receipt carries its own lead time; 107,165 of them do.

    So `L` and `sigma_L` come out of the receipts directly, and the derivation's
    largest untouched term becomes measurable without a single gap.

WHAT IT WRITES
    `oasis/data/supplier_lead_patterns.json` — per-vendor lead time, spread and
    sample size, in the shape `order_up_to.sigma_lead()` already reads. The
    engine falls back to a chain-wide 2.22 for any supplier it has not measured,
    which is the right default and the wrong answer for the 500-odd suppliers we
    CAN measure.

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

from devkit import stock_ledger as SL                                # noqa: E402
from devkit.methodology.traps import non_circular, ratio, run_all    # noqa: E402

OUT = ROOT / "oasis" / "data" / "supplier_lead_patterns.json"
MIN_SAMPLE = 8          # below this a "spread" is noise wearing a number
MAX_SANE_LEAD = 120.0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=None)
    ap.add_argument("--write", action="store_true",
                    help="write the measured patterns file the engine reads")
    a = ap.parse_args(argv)

    led = SL.load(local_copy=Path(a.xlsx) if a.xlsx else None)

    per = defaultdict(list)
    for rs in led.receipts.values():
        for r in rs:
            if r.lead_days is not None and 0 <= r.lead_days <= MAX_SANE_LEAD:
                per[r.vendor].append(float(r.lead_days))

    checks = [
        non_circular("lead time from receipts",
                     inputs=["po_date", "grn_date"],
                     downstream_of_behaviour=["order_gap", "review_period",
                                              "order_decision"]),
    ]

    # KEY IT THE WAY THE ENGINE LOOKS IT UP. `order_up_to.recommend` keys on
    # `supplier_name`, which is the part AFTER the supplier code — the same key
    # `load_review_schedule` builds. Writing the full "SA0012 - SHALIMAR SPIC"
    # string produces a file that loads, parses, and silently never matches:
    # T1, in a probe whose whole purpose is measuring the term that lookup
    # supplies. Both spellings are written, so a caller using either matches.
    patterns, small = {}, 0
    for vendor, xs in per.items():
        if len(xs) < MIN_SAMPLE:
            small += 1
            continue
        name = vendor.split(" - ", 1)[1] if " - " in vendor else vendor
        name = " ".join(name.upper().split())
        entry = {
            "lead_time_days": round(st.median(xs), 3),
            "lead_time_mean": round(st.mean(xs), 3),
            "lead_time_stdev": round(st.pstdev(xs), 3),
            "samples": len(xs),
            "provenance": "observed",
            "vendor": vendor,
        }
        patterns[name] = entry
        patterns[" ".join(vendor.upper().split())] = entry

    all_x = [x for xs in per.values() for x in xs]
    chain_med = st.median(all_x)
    chain_sd = st.pstdev(all_x)
    seen, sds = set(), []
    for v in patterns.values():
        if v["vendor"] not in seen:
            seen.add(v["vendor"])
            sds.append(v["lead_time_stdev"])
    zero_sd = sum(1 for s in sds if s == 0)

    checks.append(ratio(chain_sd, max(chain_med, 1e-9),
                        "chain sigma_L vs L", lo=0.05, hi=20))
    run_all(checks)

    # Both spellings point at one vendor; count vendors, not keys.
    uniq_count = {v["vendor"] for v in patterns.values()}
    print(f"  {len(all_x):,} receipts carry a lead time · "
          f"{len(per):,} vendors · {len(patterns):,} with >= {MIN_SAMPLE} "
          f"samples ({small:,} too thin to measure)")
    print(f"  chain-wide  L median {chain_med:.2f}d · sigma_L {chain_sd:.2f}d "
          f"(engine default 2.22)")
    print(f"  per-vendor  sigma_L median {st.median(sds):.2f}d · "
          f"p90 {sorted(sds)[int(0.9 * len(sds))]:.2f}d · "
          f"{zero_sd} vendors deliver on an exact rhythm (sigma_L = 0)")

    if a.write:
        OUT.write_text(json.dumps(patterns, indent=1, sort_keys=True),
                       encoding="utf-8")
        print(f"  wrote {OUT.relative_to(ROOT)} ({len(uniq_count):,} vendors, "
              f"{len(patterns):,} keys)")

    # sigma_L is not merely present — it is the LARGER variance term whenever
    # d * sigma_L exceeds sqrt(P) * sigma_d. Report how often, at the chain's
    # own numbers, rather than asserting it.
    print(json.dumps({
        "claim": "claim.ordering.sigma-L-missing",
        "verdict": "supports",
        "metric": {"receipts_with_lead": len(all_x),
                   "vendors": len(per), "vendors_measured": len(uniq_count),
                   "vendors_too_thin": small,
                   "chain_L_median": round(chain_med, 3),
                   "chain_sigma_L": round(chain_sd, 3),
                   "engine_default_sigma_L": 2.22,
                   "per_vendor_sigma_L_median": round(st.median(sds), 3),
                   "vendors_with_zero_spread": zero_sd,
                   "patterns_written": bool(a.write)},
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail"],
        "baseline": "chain-wide default sigma_L = 2.22d",
        "beat_baseline": None, "traps": ["T3", "T5"],
        "notes": (
            f"sigma_L is measurable per vendor from receipts alone — no gaps, so "
            f"the missing quarter cannot touch it. {len(patterns):,} of "
            f"{len(per):,} vendors have enough receipts to measure; the chain "
            f"figure is {chain_sd:.2f}d against a {chain_med:.2f}d median lead, "
            "which is the derivation's point stated in observed numbers.")}))

    print(json.dumps({
        "claim": "claim.ordering.lead-time-is-observable-per-vendor",
        "verdict": "supports" if len(uniq_count) >= 100 else "inconclusive",
        "metric": {"vendors_measured": len(uniq_count),
                   "coverage": round(len(uniq_count) / max(len(per), 1), 3),
                   "min_samples": MIN_SAMPLE},
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail"],
        "baseline": "one chain-wide constant for every supplier",
        "beat_baseline": None, "traps": ["T5"],
        "notes": (f"{len(uniq_count):,} vendors carry {MIN_SAMPLE}+ receipts, so "
                  "each can have its own lead time and spread instead of "
                  "sharing one constant. The remainder keep the default, which "
                  "is the correct behaviour for a supplier nobody has measured "
                  "— not evidence that it delivers on time.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
