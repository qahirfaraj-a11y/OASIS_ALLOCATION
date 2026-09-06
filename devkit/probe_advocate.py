"""Advocate probe: the one config the sweep table doesn't show directly --
LATA + shelf-life clamp, AMIT and MANDE OFF (their 'reporting' mode is
identical to off in this simulation: a report doesn't veto an order) --
plus a cv-formula ablation and a seed-noise check.
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.engine_ablation import build, simulate  # noqa: E402

H = 0.1438
keys, f = build()

def run(name, cfg, **kw):
    t0 = time.time()
    seeds = kw.pop("seeds", 12)
    m = simulate(f, cfg, seeds, real_open=True, warm=0,
                 cv_mode=kw.pop("cv_mode", "poisson"), demand_law="poisson",
                 rng_base=kw.pop("rng_base", 12345), **kw)
    I = m["avg_stock_kes"]; gp = m["gp_realised_year"]; w = m["waste_kes_year"]
    cogs = gp / 0.25 * 0.75 if gp else 0.0
    roi = (gp - I * H - w) / max(I, 1)
    dt = time.time() - t0
    out = dict(service=m["service"], stock=I, gmroi=m["gmroi"], roi=roi,
               turns=cogs / max(I, 1), waste=w, gp=gp, blocked=m["lines_blocked"],
               secs=dt)
    print(f"  {name:<40}{m['service']:>8.2%}{I:>13,.0f}{m['gmroi']:>8.2f}"
          f"{roi:>8.2f}{cogs/max(I,1):>7.1f}{w:>12,.0f}{gp:>14,.0f}"
          f"  ({dt:.1f}s)", flush=True)
    return out

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    results = {}
    if which in ("all", "rec"):
        results["RECOMMENDED"] = run(
            "RECOMMENDED (LATA+shelf,no-block)",
            dict(amit=0, lata=1, dharam=0, mande=0, shelf=1))
    if which in ("all", "dharam"):
        results["RECOMMENDED+dharam1"] = run(
            "RECOMMENDED + dharam=1",
            dict(amit=0, lata=1, dharam=1, mande=0, shelf=1))
    if which in ("all", "noise"):
        results["RECOMMENDED_seed999"] = run(
            "RECOMMENDED, rng_base=999",
            dict(amit=0, lata=1, dharam=0, mande=0, shelf=1), rng_base=999)
    if which in ("all", "cvflat_rec"):
        results["RECOMMENDED_cvflat"] = run(
            "RECOMMENDED, cv=flat 0.40",
            dict(amit=0, lata=1, dharam=0, mande=0, shelf=1), cv_mode="flat")
    if which in ("all", "cvflat_none"):
        results["NONE_cvflat"] = run(
            "none, cv=flat 0.40",
            dict(amit=0, lata=0, dharam=0, mande=0, shelf=0), cv_mode="flat")
    if which in ("all", "lata_only_cvflat"):
        results["LATA_only_cvflat"] = run(
            "LATA only, cv=flat 0.40",
            dict(amit=0, lata=1, dharam=0, mande=0, shelf=0), cv_mode="flat")
    out_path = ROOT / "devkit" / f"probe_advocate_{which}.json"
    out_path.write_text(json.dumps(results, indent=1))
    print(f"wrote {out_path}")
