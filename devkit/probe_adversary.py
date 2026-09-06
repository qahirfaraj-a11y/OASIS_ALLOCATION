"""ADVERSARY probe: does the cv-formula advantage survive a demand law the
planner does NOT assume?

engine_ablation.simulate()'s "normal" branch draws demand ~ N(d, CV*d)
truncated at zero with CV fixed at the module-level flat 0.40 constant --
i.e. under demand_law="normal" the TRUE dispersion of every SKU is exactly
what the "flat cv" policy assumes, and NOT the sqrt(1/d+phi^2) shape the
"poisson" cv_mode assumes. Running both cv_mode values against this
generator isolates whether the cv-formula's claimed +4.28M/yr edge is a real
forecasting advantage or an artifact of testing the formula against (close
to) the exact process it was derived from.
"""
from __future__ import annotations
import json, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.engine_ablation import build, simulate  # noqa: E402

H = 0.1438
keys, f = build()

def ep(m):
    gp = m["gp_realised_year"]; I = m["avg_stock_kes"]; w = m["waste_kes_year"]
    return gp - I * H - w, gp, I, w

def run(name, cfg, cv_mode, demand_law, seeds=12, rng_base=12345):
    t0 = time.time()
    m = simulate(f, cfg, seeds, real_open=True, warm=0,
                 cv_mode=cv_mode, demand_law=demand_law, rng_base=rng_base)
    EP, gp, I, w = ep(m)
    dt = time.time() - t0
    print(f"  {name:<38}law={demand_law:<7}cv={cv_mode:<6}"
          f"service={m['service']:.2%}  EP={EP:>15,.0f}  GP={gp:>14,.0f}  "
          f"stock={I:>13,.0f}  waste={w:>12,.0f}  ({dt:.1f}s)", flush=True)
    return dict(service=m["service"], EP=EP, gp=gp, stock=I, waste=w)

if __name__ == "__main__":
    which = sys.argv[1] if len(sys.argv) > 1 else "all"
    out = {}
    jobs = []
    if which in ("all", "rec_normal"):
        jobs.append(("REC_poissoncv_normaldemand",
                      dict(amit=0, lata=1, dharam=0, mande=0, shelf=1), "poisson", "normal"))
        jobs.append(("REC_flatcv_normaldemand",
                      dict(amit=0, lata=1, dharam=0, mande=0, shelf=1), "flat", "normal"))
    if which in ("all", "none_normal"):
        jobs.append(("NONE_poissoncv_normaldemand",
                      dict(amit=0, lata=0, dharam=0, mande=0, shelf=0), "poisson", "normal"))
        jobs.append(("NONE_flatcv_normaldemand",
                      dict(amit=0, lata=0, dharam=0, mande=0, shelf=0), "flat", "normal"))
    for name, cfg, cvm, dl in jobs:
        out[name] = run(name, cfg, cvm, dl)
    outp = ROOT / "devkit" / f"probe_adversary_{which}.json"
    outp.write_text(json.dumps(out, indent=1))
    print("wrote", outp)

def dept_breakdown(cfg, cv_mode="poisson", demand_law="poisson", tag=""):
    import json as _json
    from collections import defaultdict
    from devkit.amit_return import norm as _norm
    snap = {}
    try:
        snap = _json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json").read_text(encoding="utf-8"))
    except Exception:
        pass
    dept = {_norm(k): " ".join(str(v.get("dept") or "").upper().split()) for k, v in snap.items()}
    m = simulate(f, cfg, 12, real_open=True, warm=0, cv_mode=cv_mode,
                 demand_law=demand_law, per_sku=True)
    sku_out = m["per_sku"]
    by = defaultdict(lambda: [0, 0.0, 0.0, 0.0, 0.0, 0.0])
    for i, k in enumerate(keys):
        d0 = dept.get(k, "?")
        b = by[d0]
        dem = sku_out["dem"][i]; sold = sku_out["sold"][i]
        b[0] += 1; b[1] += sku_out["stock"][i]
        b[2] += sold * f["gp"][i] / 252.0 * 365.0
        b[3] += sku_out["waste"][i]; b[4] += dem; b[5] += sold
    return by
