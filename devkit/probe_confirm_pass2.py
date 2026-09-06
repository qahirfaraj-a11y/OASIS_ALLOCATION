"""CONFIRMATION PASS 2 -- optimality, numeric search.

probe_order_optimum.py already derives the FIRST-ORDER-CONDITION optimum z*
in closed form (1-Phi(z*) = h*c*R/(365*Cu)) and reports it -- that derivation
assumes daily-then-aggregated demand over the protection interval is
approximately GAUSSIAN, which is exactly the assumption baked into using a
z-score at all. This probe drops that assumption for a stratified sample: it
draws the EXACT demand-over-protection-interval distribution implied by the
engine's own stated law (Poisson counts with Gamma-mixed overdispersion,
phi=0.40 -- the production default), by Monte Carlo, and grid-searches S
directly against the resulting empirical cost curve. Where the empirical
optimum agrees with the closed-form z*, the Gaussian step was harmless. Where
it does not, that is a second, independent defect on top of the flat z=1.28.

T5 WARNING, EXPLICIT: this still assumes phi=0.40 and Poisson-overdispersion
are the right daily law -- it is a check of the GAUSSIAN TAIL STEP within that
law, not a validation of the law itself. Validating the law against reality is
PASS 3's job, using real POS extracts the law was never fitted to. Do not
quote this probe's numbers as if they establish that phi=0.40 is correct.
"""
from __future__ import annotations
import csv, math, sys, random
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import ratio, run_all      # noqa: E402

BOOK = ROOT / "oasis" / "data" / "order_book.csv"
H = 0.1438 + 0.10
PHI = 0.40
N_MC = 8000
rng = np.random.default_rng(11)


def load():
    return list(csv.DictReader(BOOK.open(encoding="utf-8")))


def draw_DP(d, P, sigma_L, phi=PHI, n=N_MC):
    """Exact (given the model) demand over the protection interval:
    sum of round(P) days of Gamma-Poisson(mean d, var d+phi^2 d^2), plus an
    additive N(0, d*sigma_L) term for lead-time variance (matches the
    d^2*sigma_L^2 contribution to sigma_P exactly in variance)."""
    ndays = max(1, round(P))
    if d <= 0:
        return np.zeros(n)
    shape = 1.0 / (phi * phi)
    scale = phi * phi * d
    lam = rng.gamma(shape, scale, size=(ndays, n))
    daily = rng.poisson(lam)
    D = daily.sum(axis=0).astype(float)
    if sigma_L > 0:
        D = D + rng.normal(0.0, d * sigma_L, size=n)
    return np.clip(D, 0.0, None)


def cost_of_S(S, D, d, R, P, c, gp, h=H):
    """Same functional form as probe_order_optimum's C(z,R), but E[short]
    is the Monte Carlo mean of max(D-S,0) under the EXACT distribution
    instead of sigma_P*G(z) under Gaussian D."""
    avg_on_hand = max(0.0, d * R / 2.0 + (S - d * P))
    hold = h * c * avg_on_hand
    short_units_per_cycle = float(np.mean(np.maximum(D - S, 0.0)))
    short_cost = gp * (365.0 / max(R, 0.1)) * short_units_per_cycle
    return hold + short_cost


def optimal_S(D, d, R, P, c, gp, S_engine):
    lo, hi = 0.0, max(S_engine * 2.5, d * P * 2.5, 1.0)
    grid = np.linspace(lo, hi, 240)
    costs = np.array([cost_of_S(S, D, d, R, P, c, gp) for S in grid])
    i = int(np.argmin(costs))
    return grid[i], costs[i], cost_of_S(S_engine, D, d, R, P, c, gp)


def main():
    rows = load()
    unclamped = [r for r in rows if int(r["clamped"]) == 0 and int(r["structurally_short"]) == 0
                and float(r["d"]) > 0]
    ds = sorted(float(r["d"]) for r in unclamped)
    d_lo, d_hi = ds[len(ds)//5], ds[4*len(ds)//5]

    random.seed(3)
    fast = random.sample([r for r in unclamped if float(r["d"]) >= d_hi], 15)
    mid = random.sample([r for r in unclamped if d_lo <= float(r["d"]) < d_hi], 15)
    slow = random.sample([r for r in unclamped if float(r["d"]) < d_lo], 15)
    sample = [("fast", r) for r in fast] + [("mid", r) for r in mid] + [("slow", r) for r in slow]

    print(f"PASS 2 -- OPTIMALITY, exact (Gamma-Poisson) numeric search, "
          f"h={H:.2%}, phi={PHI}, N_MC={N_MC}")
    print(f"{len(sample)} unclamped, non-structurally-short SKUs sampled "
          f"(15 fast / 15 mid / 15 slow by d)\n")
    print(f"  {'stratum':<6}{'sku':<32}{'d':>8}{'R':>5}{'P':>6}{'S_eng':>8}"
          f"{'S_opt':>8}{'ratio':>7}{'cost@eng':>11}{'cost@opt':>11}{'save/yr':>10}")

    ratios = {"fast": [], "mid": [], "slow": []}
    saves = {"fast": [], "mid": [], "slow": []}
    for strat, r in sample:
        d = float(r["d"]); R = float(r["R"]); L = float(r["L"])
        sL = float(r["sigma_L"]); c = float(r["cost"]); gp = float(r["gp_unit"]) or 0.01
        S_eng = float(r["S"]); P = R + L
        D = draw_DP(d, P, sL)
        S_opt, cost_opt, cost_eng = optimal_S(D, d, R, P, c, gp, S_eng)
        rat = S_eng / S_opt if S_opt > 0 else float("nan")
        ratios[strat].append(rat)
        saves[strat].append(cost_eng - cost_opt)
        print(f"  {strat:<6}{r['sku'][:31]:<32}{d:>8.3f}{R:>5.1f}{P:>6.2f}"
              f"{S_eng:>8.1f}{S_opt:>8.1f}{rat:>7.2f}{cost_eng:>11,.0f}"
              f"{cost_opt:>11,.0f}{cost_eng-cost_opt:>10,.0f}")

    print("\n  DISTRIBUTION OF S_engine / S_optimal (exact distribution, this sample)")
    for strat in ("fast", "mid", "slow"):
        v = sorted(x for x in ratios[strat] if not math.isnan(x))
        if not v:
            continue
        q = lambda p: v[min(len(v)-1, int(p*len(v)))]
        print(f"    {strat:<6} n={len(v):>3}  p25 {q(.25):.2f} · median {q(.5):.2f} "
              f"· p75 {q(.75):.2f}   (1.00 = engine already optimal)")
        s = saves[strat]
        print(f"           median annual saving if moved to S_opt: KES {sorted(s)[len(s)//2]:,.0f}/SKU/yr "
              f"(engine cost minus optimum cost at that SKU's own d,R,P)")

    all_ratios = [x for v in ratios.values() for x in v if not math.isnan(x)]
    run_all([ratio(float(np.median(all_ratios)), 1.0,
                   "median S_engine/S_optimal (exact dist) vs 1.0", lo=0.3, hi=3.0)])

    print("\n  READING: this isolates the GAUSSIAN-TAIL step of the z=1.28 formula, "
          "holding the Poisson-overdispersion(phi=0.40) law fixed (T5: this cannot "
          "and does not test whether that law itself is right -- see PASS 3).")

if __name__ == "__main__":
    raise SystemExit(main())
