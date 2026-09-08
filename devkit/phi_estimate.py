"""An empirical UPPER BOUND on phi, from the real monthly extracts.

WHY THIS IS NOW POSSIBLE AND WAS NOT BEFORE
    phi enters only demand_cv(d), and demand_cv(d) was unreachable on 96.6% of
    lines while a cv of MONTHLY totals was being consumed as a daily one. With
    that fixed, phi is live -- so its magnitude finally matters, and is worth
    an estimate rather than an assertion.

WHY NOT JUST SWEEP IT IN THE SIMULATOR
    Because that is circular, and this project has made that exact mistake
    once. multistore/readiness GENERATE demand as Poisson x lognormal with
    GEN_CV = 0.40 and the planner assumes cv = sqrt(1/d + phi^2), which is the
    cv of that law. Sweeping the planning phi against a generator built on phi
    recovers the generator, not the world. The only non-circular evidence is
    the real series.

THE ESTIMATOR
    If days are i.i.d. with cv_daily^2 = 1/d + phi^2, then a 30-day total M
    has Var(M) = 30d + 30 d^2 phi^2 and E(M) = 30d, so

        cv_M^2 = 1/(30d) + phi^2/30      and      phi = sqrt(30 (cv_M^2 - 1/M_bar))

    The bracket is the EXCESS over what pure Poisson would produce at monthly
    scale. Pure Poisson gives cv_M^2 = 1/M_bar exactly and phi = 0.

WHY IT IS AN UPPER BOUND, AND NOT A LITTLE ONE
    Everything that moves a month which is not daily noise lands in phi here:
    trend, seasonality, promotions, price changes, assortment edits, a
    competitor opening. The seven extracts are also NON-CONSECUTIVE -- jan,
    mar, may, jun, jul, sep, oct, spanning ten months -- so seasonal drift is
    inside the variance being attributed to daily overdispersion. A month is
    also not exactly 30 days.

    So: phi_true <= phi_hat. If phi_hat comes back near 0.40 the shipped value
    is generous rather than conservative; if it comes back well above, 0.40 is
    understating and the safety term is thin. Either is worth knowing. What
    this CANNOT do is produce a point estimate, and it should not be quoted as
    one.

USAGE
    python devkit/phi_estimate.py
"""
from __future__ import annotations

import glob
import math
import os
import statistics
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from oasis.logic import order_up_to as ou   # noqa: E402

MIN_MONTHS = 5          # fewer, and the variance estimate is noise
BANDS = ((0.0, 1.0, "<=1/day"), (1.0, 5.0, "1-5/day"),
         (5.0, 10.0, "5-10/day"), (10.0, 1e9, ">10/day"))


def band_of(d):
    for lo, hi, name in BANDS:
        if (lo == 0.0 and d <= hi) or (lo < d <= hi):
            return name
    return ">10/day"


def main() -> int:
    files = sorted(glob.glob(str(ROOT / "oasis" / "data" / "*_cash.xlsx")))
    print(f"{len(files)} monthly extracts: "
          f"{[os.path.basename(f) for f in files]}\n")

    per_sku = defaultdict(dict)
    for f in files:
        month = os.path.basename(f).split("_")[0]
        df = pd.read_excel(f, header=1)
        cols = {str(c).strip().lower(): c for c in df.columns}
        name_c = cols.get("item name")
        qty_c = cols.get("qty")
        if name_c is None or qty_c is None:
            print(f"  skipped {os.path.basename(f)}: columns {list(df.columns)}")
            continue
        g = df.groupby(df[name_c].astype(str).str.strip())[qty_c] \
              .apply(lambda s: pd.to_numeric(s, errors="coerce").fillna(0).sum())
        for sku, q in g.items():
            if sku and q > 0:
                per_sku[sku][month] = float(q)

    print(f"  {len(per_sku):,} SKUs seen in at least one month")

    rows = []
    for sku, months in per_sku.items():
        vals = list(months.values())
        if len(vals) < MIN_MONTHS:
            continue
        m = statistics.mean(vals)
        if m <= 0 or len(vals) < 2:
            continue
        sd = statistics.stdev(vals)
        cv_m = sd / m
        excess = cv_m ** 2 - 1.0 / m       # over pure Poisson at month scale
        phi = math.sqrt(30.0 * excess) if excess > 0 else 0.0
        rows.append((sku, m / 30.0, cv_m, phi))

    print(f"  {len(rows):,} SKUs present in >= {MIN_MONTHS} months\n")
    if not rows:
        return 1

    def q(xs, f):
        xs = sorted(xs)
        return xs[int(f * (len(xs) - 1))]

    phis = [r[3] for r in rows]
    print("=" * 74)
    print("PHI-HAT — an UPPER BOUND, not a point estimate")
    print("=" * 74)
    print(f"  p10 {q(phis,.10):.3f}   p25 {q(phis,.25):.3f}   "
          f"median {q(phis,.50):.3f}   p75 {q(phis,.75):.3f}   "
          f"p90 {q(phis,.90):.3f}")
    print(f"  shipped value: {ou.DEMAND_OVERDISPERSION:.2f}")
    below = sum(1 for p in phis if p <= ou.DEMAND_OVERDISPERSION)
    print(f"  SKUs whose bound sits at or below the shipped 0.40: "
          f"{below:,}/{len(phis):,} ({100*below/len(phis):.1f}%)")
    zero = sum(1 for p in phis if p == 0.0)
    print(f"  SKUs with NO excess over Poisson at all (phi_hat = 0): "
          f"{zero:,} ({100*zero/len(phis):.1f}%)")

    print("\n  by velocity — phi is the term that survives at high d, so the")
    print("  fast bands are where it is actually load-bearing:")
    print(f"  {'band':>10}{'SKUs':>8}{'median phi_hat':>17}{'p90':>9}")
    for _, _, name in BANDS:
        b = [r[3] for r in rows if band_of(r[1]) == name]
        if b:
            print(f"  {name:>10}{len(b):>8,}{q(b,.5):>17.3f}{q(b,.9):>9.3f}")

    print("\n  Reading it: phi_true <= phi_hat, and the gap is everything that")
    print("  moves a month without being daily noise -- trend, season,")
    print("  promotion, a price change. These seven months are also not")
    print("  consecutive, so seasonal drift is inside the bound.")

    # ---- tighten it two ways before declaring the data insufficient -------
    print("\n" + "=" * 74)
    print("TIGHTENING THE BOUND")
    print("=" * 74)

    # (a) CONSECUTIVE months only. may-jun-jul are adjacent, so the gap
    #     between observations is one month rather than up to four, and less
    #     seasonal drift can accumulate inside the variance.
    run = ("may", "jun", "jul")
    tight = []
    for sku, months in per_sku.items():
        vals = [months[m] for m in run if m in months]
        if len(vals) < 3:
            continue
        m = statistics.mean(vals)
        if m <= 0:
            continue
        ex = (statistics.stdev(vals) / m) ** 2 - 1.0 / m
        tight.append(math.sqrt(30.0 * ex) if ex > 0 else 0.0)
    if tight:
        print(f"  consecutive may-jun-jul, {len(tight):,} SKUs: "
              f"median {q(tight,.5):.3f}  p75 {q(tight,.75):.3f}  "
              f"p90 {q(tight,.90):.3f}")

    # (b) DETRENDED across all seven: take residuals about a straight line in
    #     month order, which removes a steady rise or fall and leaves the
    #     wobble. Anything a linear trend cannot explain stays in.
    order = {n: i for i, n in enumerate(
        ["jan", "mar", "may", "jun", "jul", "sep", "oct"])}
    det = []
    for sku, months in per_sku.items():
        pts = sorted(((order[k], v) for k, v in months.items() if k in order))
        if len(pts) < MIN_MONTHS:
            continue
        xs = [p[0] for p in pts]
        ys = [p[1] for p in pts]
        m = statistics.mean(ys)
        if m <= 0:
            continue
        mx = statistics.mean(xs)
        sxx = sum((x - mx) ** 2 for x in xs)
        b = (sum((x - mx) * (y - m) for x, y in pts) / sxx) if sxx else 0.0
        resid = [y - (m + b * (x - mx)) for x, y in pts]
        # one degree of freedom spent on the slope
        var = sum(r * r for r in resid) / max(1, len(resid) - 2)
        ex = var / (m * m) - 1.0 / m
        det.append(math.sqrt(30.0 * ex) if ex > 0 else 0.0)
    if det:
        print(f"  detrended over all seven, {len(det):,} SKUs: "
              f"median {q(det,.5):.3f}  p75 {q(det,.75):.3f}  "
              f"p90 {q(det,.90):.3f}")
        z2 = sum(1 for p in det if p == 0.0)
        print(f"    with no excess left at all: {z2:,} "
              f"({100*z2/len(det):.1f}%)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
