"""Where in the ordering cycle the stock actually sits, and where it runs out.

An average hides the sawtooth. Stock peaks the morning a delivery lands and
troughs the hour before the next one, and EVERY stockout happens at the bottom.
A book that looks comfortable on 18 days of average cover can still be empty on
the last day of every cycle -- which is what a customer experiences.

This tracks, for each SKU, cover and stockout incidence by PHASE of its own
review cycle (0 = delivery day, R-1 = the day before the next order lands), and
reports the phase profile by department. It also isolates the one real tension
in the recommended configuration: LATA lifts FRESH MILK and BREAD from 45% to
92% service by reviewing them daily instead of weekly, and the shelf-life clamp
then caps their cover at 1.2 days and hands most of that back. Those two
changes fight over exactly the same 1,173 lines.
"""
from __future__ import annotations
import json, sys
from collections import defaultdict
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.engine_ablation import build, CONFIGS, WARM, DAYS, CV, PHI   # noqa: E402
from devkit.amit_return import norm                                      # noqa: E402
from oasis.logic import order_up_to as ou                                # noqa: E402

keys, f = build()
n = len(keys)
snap = json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json").read_text(encoding="utf-8"))
dept = np.array([" ".join(str((snap.get(k) or {}).get("dept") or "").upper().split()) for k in keys])

MAXPH = 14


def run(shelf_on, seeds=6, days=DAYS):
    R = np.maximum(1.0, f["R_on"]); sL = f["sL_on"]; L = f["L"]
    z = ou.z_score(); P = R + L
    cvv = np.array([ou.demand_cv(x) for x in f["d"]])
    S = f["d"] * P + z * np.sqrt(P * (cvv * f["d"]) ** 2 + (f["d"] * sL) ** 2)
    if shelf_on:
        cap = np.where(f["shelf"] > 0, f["d"] * f["shelf"], np.inf)
        S = np.minimum(S, cap)
    Rint = np.maximum(1, np.round(R).astype(int))
    maxlead = int(max(2, np.ceil(L.max() + 4 * sL.max()) + 2))
    ph_cover = np.zeros((MAXPH, n)); ph_out = np.zeros((MAXPH, n)); ph_n = np.zeros(MAXPH)
    dem_t = sold_t = 0.0
    for s in range(seeds):
        rng = np.random.default_rng(4242 + s)
        on = np.where(np.isnan(f["open_real"]), S, f["open_real"]).copy()
        pipe = np.zeros((maxlead + 1, n))
        off = rng.integers(0, Rint, n) if Rint.max() > 1 else np.zeros(n, dtype=int)
        for t in range(days):
            on += pipe[0]; pipe[:-1] = pipe[1:]; pipe[-1] = 0.0
            dem = rng.poisson(f["d"]) * np.exp(rng.normal(-0.5 * PHI ** 2, PHI, n))
            sold = np.minimum(dem, on); on -= sold
            keepmax = np.where(f["shelf"] > 0, f["d"] * f["shelf"], np.inf)
            on -= np.where(f["shelf"] > 0, np.maximum(0.0, on - keepmax) /
                           np.maximum(f["shelf"], 1.0), 0.0)
            phase = ((t + off) % Rint)
            for k in range(MAXPH):
                m = phase == k
                if m.any():
                    ph_cover[k][m] += on[m] / np.maximum(f["d"][m], 1e-9)
                    ph_out[k][m] += (dem[m] > sold[m] + 1e-9)
                    ph_n[k] += 1
            dem_t += dem.sum(); sold_t += sold.sum()
            due = phase == 0
            if due.any():
                pos = on + pipe.sum(axis=0)
                q = np.where(due, np.maximum(0.0, S - pos), 0.0)
                lead = np.clip(np.round(rng.normal(L, sL)), 0, maxlead).astype(int)
                idx = np.nonzero(q > 0)[0]
                if idx.size:
                    np.add.at(pipe, (lead[idx], idx), q[idx])
    return ph_cover, ph_out, dem_t, sold_t, S, Rint


for tag, shelf in (("clamp ON  (recommended)", True), ("clamp OFF (LATA only)", False)):
    pc, po, dt, st_, S, Rint = run(shelf)
    print(f"\n  {tag} -- service {st_/dt:.2%}")
    print(f"  {'cycle phase':<14}{'lines':>8}{'median cover d':>16}{'stockout rate':>15}")
    for k in range(6):
        m = Rint > k
        if not m.any(): continue
        obs = pc[k][m]; cnt = po[k][m]
        occ = np.maximum((Rint[m] > k).astype(float), 1)
        print(f"  {'day ' + str(k) + (' (delivery)' if k == 0 else ''):<14}"
              f"{int(m.sum()):>8,}{np.median(obs / np.maximum(obs > 0, 1e-9)) if False else np.median(obs[obs>0])/ (DAYS/np.maximum(np.median(Rint[m]),1)) / 6:>16.1f}"
              f"{cnt.sum() / max(obs.size * DAYS / max(np.median(Rint[m]), 1) * 6, 1):>14.1%}")
    for d0 in ("FRESH MILK", "BREAD", "WINES", "BISCUITS"):
        m = dept == d0
        if not m.any(): continue
        tot_out = sum(po[k][m].sum() for k in range(MAXPH))
        print(f"    {d0:<16} stockout-days {tot_out:>10,.0f} · S/d median "
              f"{np.median(S[m] / np.maximum(f['d'][m], 1e-9)):>5.2f} d · R median "
              f"{np.median(Rint[m]):>4.1f}")
