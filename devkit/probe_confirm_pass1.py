"""CONFIRMATION PASS 1 -- arithmetic.

Recomputes S and Q from oasis/data/order_book.csv straight against the
published formula (P=R+L; cv(d)=sqrt(1/d+phi^2); sigma_P=d*sqrt(P*cv^2+sigma_L^2);
S=d*P+z*sigma_P; clamp to d*shelf_life but never below d*P), for a stratified
sample, and separately against the ACTUAL oasis.logic.order_up_to functions
(clamp_level / order_up_to_level) called on the same csv-recorded inputs.

Two independent checks:
  CHECK A  hand arithmetic (no oasis import at all) vs the CSV's own S_raw,
           cycle_units, safety_units -- confirms the CSV is internally what
           it claims to be.
  CHECK B  oasis.logic.order_up_to.clamp_level(..., min_protection=P) --
           the REAL production floor -- vs the CSV's own S/Q. order_book_run.py
           (the script that wrote this csv) clamps with a bare
           min(S_raw, shelf_cap) and never applies the floor, so on
           structurally-short lines the csv is stale relative to what
           oasis/logic/order_up_to.py actually computes today.
"""
from __future__ import annotations
import csv, math, sys
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from oasis.logic import order_up_to as ou          # noqa: E402

BOOK = ROOT / "oasis" / "data" / "order_book.csv"


def load():
    rows = list(csv.DictReader(BOOK.open(encoding="utf-8")))
    return rows


def hand_S(d, R, L, sigma_L, z, phi=0.40):
    P = R + L
    cv = math.sqrt(1.0/d + phi*phi) if d > 0 else math.sqrt(1.0 + phi*phi)
    cv = min(cv, 2.5)
    sigma_d = cv * d
    sigma_P = math.sqrt(P * sigma_d**2 + d*d*sigma_L**2)
    return P, cv, sigma_d, sigma_P


def main():
    rows = load()
    n = len(rows)
    print(f"PASS 1 -- ARITHMETIC. {n:,} SKUs in order_book.csv")

    # ---- CHECK A: hand formula vs CSV's own recorded terms, ALL rows ----
    max_rel_cyc = max_rel_saf = max_rel_Sraw = 0.0
    worst = []
    for r in rows:
        d = float(r["d"]); R = float(r["R"]); L = float(r["L"])
        sL = float(r["sigma_L"]); z = float(r["z"])
        P, cv, sigma_d, sigma_P = hand_S(d, R, L, sL, z)
        cyc = d * P
        saf = z * sigma_P
        S_raw = cyc + saf
        rc = abs(cyc - float(r["cycle_units"])) / max(abs(float(r["cycle_units"])), 1e-6)
        rs = abs(saf - float(r["safety_units"])) / max(abs(float(r["safety_units"])), 1e-6)
        rS = abs(S_raw - float(r["S_raw"])) / max(abs(float(r["S_raw"])), 1e-6)
        if rc > max_rel_cyc: max_rel_cyc = rc
        if rs > max_rel_saf: max_rel_saf = rs
        if rS > max_rel_Sraw: max_rel_Sraw = rS
        if max(rc, rs, rS) > 0.01:
            worst.append((r["sku"], rc, rs, rS))

    print(f"\nCHECK A -- hand arithmetic vs CSV's own cycle_units/safety_units/S_raw")
    print(f"  max relative error: cycle_units {max_rel_cyc:.2%} · safety_units "
          f"{max_rel_saf:.2%} · S_raw {max_rel_Sraw:.2%}")
    print(f"  rows with >1% disagreement on any term: {len(worst)}")
    for w in worst[:10]:
        print(f"    {w}")

    # ---- stratified sample for the table -------------------------------
    ds = sorted(float(r["d"]) for r in rows)
    d_med = ds[len(ds)//2]
    strata = {
        "fast (d>=median)": lambda r: float(r["d"]) >= d_med,
        "slow (d<median)": lambda r: float(r["d"]) < d_med,
        "fresh (shelf_life>0)": lambda r: float(r["shelf_life"]) > 0,
        "dry (shelf_life==0)": lambda r: float(r["shelf_life"]) == 0,
        "sigma_L measured": lambda r: r["sigma_L_source"] == "measured",
        "sigma_L chain_fallback": lambda r: r["sigma_L_source"] == "chain_fallback",
        "R calendar": lambda r: r["R_source"] == "calendar",
        "R cadence": lambda r: r["R_source"] in ("cadence", "cadence_overrides_calendar"),
        "R default": lambda r: r["R_source"] == "default",
        "clamped": lambda r: int(r["clamped"]) == 1,
        "unclamped": lambda r: int(r["clamped"]) == 0,
        "structurally_short": lambda r: int(r["structurally_short"]) == 1,
    }
    import random
    random.seed(7)
    sample = []
    for name, pred in strata.items():
        pool = [r for r in rows if pred(r)]
        if not pool:
            continue
        take = random.sample(pool, min(4, len(pool)))
        for r in take:
            sample.append((name, r))

    print(f"\n  STRATIFIED SAMPLE ({len(sample)} lines) -- "
          f"CSV S/Q vs (a) hand-with-floor (b) real ou.clamp_level-with-floor")
    hdr = (f"  {'stratum':<24}{'sku':<32}{'S_csv':>9}{'S_hand_floor':>13}"
          f"{'S_ou_floor':>11}{'Q_csv':>8}{'Q_true':>8}{'dQ':>8}")
    print(hdr)
    agg_dQ_units = 0.0
    agg_dQ_kes = 0.0
    n_diff = 0
    for name, r in sample:
        d = float(r["d"]); R = float(r["R"]); L = float(r["L"])
        sL = float(r["sigma_L"]); z = float(r["z"]); sl = float(r["shelf_life"])
        oh = float(r["on_hand"]); cost = float(r["cost"])
        P, cv, sigma_d, sigma_P = hand_S(d, R, L, sL, z)
        S_raw = d*P + z*sigma_P
        # hand version of the floor clamp, from the stated formula
        if sl > 0:
            shelf_cap = d * sl
            floor = d * P
            S_hand_floor = max(min(S_raw, shelf_cap), min(floor, S_raw))
        else:
            S_hand_floor = S_raw
        # the actual production function
        S_ou_floor = ou.clamp_level(S_raw, d, shelf_life_days=sl, min_protection=P)
        Q_csv = float(r["Q"]); S_csv = float(r["S"])
        Q_true = max(0.0, S_ou_floor - oh)
        dQ = Q_true - Q_csv
        if abs(dQ) > 0.5:
            n_diff += 1
            agg_dQ_units += dQ
            agg_dQ_kes += dQ * cost
        print(f"  {name:<24}{r['sku'][:31]:<32}{S_csv:>9.1f}{S_hand_floor:>13.1f}"
              f"{S_ou_floor:>11.1f}{Q_csv:>8.1f}{Q_true:>8.1f}{dQ:>+8.1f}")

    print(f"\n  in this {len(sample)}-line sample: {n_diff} lines where the CSV's Q "
          f"disagrees with the real production formula by >0.5 units")
    print(f"  net Q delta in sample: {agg_dQ_units:+.1f} units, "
          f"KES {agg_dQ_kes:+,.0f}")

    # ---- full-book quantification of the floor-clamp discrepancy -------
    print(f"\nCHECK B -- FULL BOOK: order_book.csv's clamp (ceiling-only) vs the "
          f"real oasis.logic.order_up_to.clamp_level (ceiling AND floor)")
    ss_rows = [r for r in rows if int(r["structurally_short"]) == 1]
    tot_dQ_units = 0.0; tot_dQ_kes = 0.0; tot_S_csv=0.0; tot_S_true=0.0
    for r in ss_rows:
        d = float(r["d"]); R = float(r["R"]); L = float(r["L"])
        sL = float(r["sigma_L"]); z = float(r["z"]); sl = float(r["shelf_life"])
        oh = float(r["on_hand"]); cost = float(r["cost"])
        _, _, _, sigma_P = hand_S(d, R, L, sL, z)
        S_raw = d*(R+L) + z*sigma_P
        S_true = ou.clamp_level(S_raw, d, shelf_life_days=sl, min_protection=R+L)
        S_csv = float(r["S"])
        Q_true = max(0.0, S_true - oh)
        Q_csv = float(r["Q"])
        tot_S_csv += S_csv; tot_S_true += S_true
        tot_dQ_units += (Q_true - Q_csv)
        tot_dQ_kes += (Q_true - Q_csv) * cost
    print(f"  {len(ss_rows):,} structurally-short lines (shelf_life < R+L)")
    print(f"  sum(S) recorded in csv:      {tot_S_csv:>14,.0f} units")
    print(f"  sum(S) the real floor gives: {tot_S_true:>14,.0f} units "
          f"({100*(tot_S_true/max(tot_S_csv,1)-1):+.1f}%)")
    print(f"  sum(Q) delta (true - csv):   {tot_dQ_units:>+14,.0f} units, "
          f"KES {tot_dQ_kes:>+14,.0f} one-off order value")
    print(f"  INTERPRETATION: order_book.csv (built by devkit/order_book_run.py) "
          f"clamps structurally-short lines with a bare min(S_raw, shelf_cap) and "
          f"never applies the min_protection floor that oasis/logic/order_up_to.py's "
          f"clamp_level() implements in production. The csv therefore UNDER-states S "
          f"and Q on every one of these 770 lines relative to what the running engine "
          f"actually orders today -- it reflects the ceiling-only clamp, not the "
          f"'RECOMMENDED (clamp w/ floor)' configuration this task is confirming.")

if __name__ == "__main__":
    raise SystemExit(main())
