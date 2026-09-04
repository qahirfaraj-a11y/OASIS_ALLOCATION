"""Derive the LATA supplier multiplier instead of capping it.

THE PROBLEM WITH A CEILING
    LATA inflates the replenishment safety buffer for suppliers whose lead time
    is unreliable. It was doing that well — against lead-time CV measured
    independently from 92,181 receipts it correlates at rho = 0.93 — right up to
    its ceiling, where 187 of 599 suppliers (31%) sat at exactly 3.0. Among the
    worst third of the supplier base it distinguished nothing: a merely
    unreliable supplier and a catastrophic one bought identical cover.

    A cap is a decision disguised as a constant. It also has to be maintained:
    the code comment said the shield topped out at 2.0 while the data went to
    3.0, and nobody noticed because nothing checked.

THE DERIVATION
    The safety term already knows the answer. Safety stock is

        z * sqrt(P*sigma_d^2 + d^2*sigma_L^2)

    so the inflation caused by lead-time variance is exactly the ratio of that
    term to the same term without it:

        LATA = sqrt(P*sigma_d^2 + d^2*sigma_L^2) / sqrt(P*sigma_d^2)
             = sqrt(1 + sigma_L^2 / (P * cv^2))          [sigma_d = cv * d]

    The demand rate cancels. What is left is the supplier's own lead-time spread
    against its protection interval and the line's demand variability — all
    measurable, none chosen. The multiplier is no longer a free parameter; it is
    implied by the formula the engine already uses.

WHAT REPLACES THE CEILING
    Nothing clips it. A supplier whose derived multiplier is 16x is not a number
    to trim — it is a supplier that cannot be replenished sanely, and saying so
    is more useful than quietly buying three times cover and calling it shielded.
    Those are flagged for escalation instead, above a percentile of the derived
    distribution rather than a constant.
"""
from __future__ import annotations

import argparse
import json
import math
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import stock_ledger as SL                    # noqa: E402
from oasis.logic import order_up_to as OU                # noqa: E402

OUT = ROOT / "oasis" / "data" / "lata_derived.json"
MIN_SAMPLE = 8
CHAIN_CV = 0.4              # the engine's own default demand CV
ESCALATE_PCTILE = 0.99


def derive(sigma_L: float, P: float, cv: float = CHAIN_CV) -> float:
    """sqrt(1 + sigma_L^2 / (P * cv^2)). No ceiling, by design."""
    if P <= 0 or cv <= 0:
        return 1.0
    return math.sqrt(1.0 + (sigma_L ** 2) / (P * cv * cv))


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=None)
    ap.add_argument("--write", action="store_true")
    ap.add_argument("--cv", type=float, default=CHAIN_CV)
    a = ap.parse_args(argv)

    led = SL.load(local_copy=Path(a.xlsx) if a.xlsx else None)
    schedule = OU.load_review_schedule(str(ROOT))

    per = defaultdict(list)
    for rs in led.receipts.values():
        for r in rs:
            if r.lead_days is not None and 0 <= r.lead_days <= 120:
                per[r.vendor].append(float(r.lead_days))

    out, rows = {}, []
    for vendor, xs in per.items():
        if len(xs) < MIN_SAMPLE:
            continue
        name = " ".join((vendor.split(" - ", 1)[1] if " - " in vendor
                         else vendor).upper().split())
        L = st.median(xs)
        sL = st.pstdev(xs)
        R = schedule.get(name, OU.DEFAULT_REVIEW_DAYS)
        P = R + L
        mult = derive(sL, P, a.cv)
        entry = {"lata_variance_multiplier": round(mult, 4),
                 "lead_time_days": round(L, 3), "lead_time_stdev": round(sL, 3),
                 "review_days": round(R, 3), "protection_days": round(P, 3),
                 "samples": len(xs), "vendor": vendor, "derived": True,
                 "provenance": "observed"}
        out[name] = entry
        out[" ".join(vendor.upper().split())] = entry
        rows.append((name, mult, sL, L, P, len(xs)))

    mults = sorted(r[1] for r in rows)
    p = lambda q: mults[min(int(q * len(mults)), len(mults) - 1)]
    cutoff = p(ESCALATE_PCTILE)
    escalate = [r for r in rows if r[1] >= cutoff]

    print(f"  {len(rows):,} suppliers derived from {sum(r[5] for r in rows):,} receipts")
    print(f"  multiplier  min {mults[0]:.2f} · p25 {p(.25):.2f} · median "
          f"{p(.5):.2f} · p75 {p(.75):.2f} · p95 {p(.95):.2f} · max {mults[-1]:.2f}")
    print(f"  distinct values {len(set(round(m, 4) for m in mults)):,} "
          f"(the old table had 228 across 599, with 187 tied at the cap)")
    print(f"  escalate above p{ESCALATE_PCTILE:.0%} = {cutoff:.2f}: "
          f"{len(escalate)} supplier(s)")
    for name, mult, sL, L, P, n in sorted(escalate, key=lambda r: -r[1])[:6]:
        print(f"    {name[:38]:<40} x{mult:>7.2f}  sigma_L {sL:>6.2f}d  "
              f"L {L:>5.1f}d  P {P:>5.1f}d  n={n}")

    if a.write:
        OUT.write_text(json.dumps(out, indent=1, sort_keys=True), encoding="utf-8")
        print(f"  wrote {OUT.relative_to(ROOT)} "
              f"({len(rows):,} suppliers, {len(out):,} keys)")
    print(json.dumps({"suppliers": len(rows), "median": round(p(.5), 4),
                      "p95": round(p(.95), 4), "max": round(mults[-1], 4),
                      "escalate_cutoff": round(cutoff, 4),
                      "escalate": [r[0] for r in escalate]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
