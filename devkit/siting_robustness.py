"""What actually moves a site recommendation, and by how much.

DEV TOOLING — never ships.

WHY
    Every input to this model is uncertain: the distance exponent was never
    fitted, the competitor field is measurably incomplete, floor areas are
    mostly chain defaults, and the catchment radius is a convention. Arguing
    about them in the abstract is cheap. This perturbs each one in turn against
    a real shortlist and reports how far the ANSWER moves.

    The output is an ordering of what to fix first, backed by numbers rather
    than by which defect happens to be most embarrassing.

METHOD
    Take the top N candidate sites for a chain under baseline assumptions.
    Re-score that pool under each perturbation. Report:

      top-1 held      did the single recommendation survive?
      top-10 overlap  how many of the ten came back?
      rank drift      mean absolute change in position

    A pool rather than a full re-sweep, because the question is whether the
    RECOMMENDATION is stable, and a site outside the top few hundred was never
    going to be recommended.

    python devkit/siting_robustness.py --chain Chandarana [--pool 240]
"""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

_S: dict = {}


def _init(root: str, chain: str) -> None:
    from oasis.logic.geo_sources import load_competitors
    from oasis.logic.population import load_population
    rows = load_competitors(root=root, include_own=True)["rows"]
    pts = [{"lat": float(r["Latitude"]), "lon": float(r["Longitude"]),
            "size_sqft": float(r.get("size_sqft") or 15_000.0),
            "chain": (r.get("Chain") or "").strip(),
            "pull": r.get("pull")} for r in rows]
    _S["own"] = [p for p in pts if p["chain"].lower() == chain.lower()]
    _S["rivals"] = [p for p in pts if p["chain"].lower() != chain.lower()]
    _S["pop"] = load_population(root=root)["grid"]


def _run(task: tuple) -> tuple:
    """One condition: score the whole pool, return (label, ordered coords)."""
    from oasis.logic.site_scoring import CATCHMENT_KM, score_site
    label, pool, opts = task
    own, rivals, pop = _S["own"], _S["rivals"], _S["pop"]

    riv = rivals
    if opts.get("drop"):
        rnd = random.Random(opts["seed"])
        keep = 1.0 - opts["drop"]
        riv = [r for r in rivals if rnd.random() < keep]
    if opts.get("uniform_size"):
        riv = [dict(r, size_sqft=opts["uniform_size"], pull=None) for r in riv]
        own = [dict(o, size_sqft=opts["uniform_size"], pull=None) for o in own]
    if opts.get("extra_rivals"):
        riv = riv + opts["extra_rivals"]

    beta = opts.get("beta", 2.0)
    catch = opts.get("catchment", CATCHMENT_KM)

    scored = []
    for lat, lon in pool:
        if not pop.covers(lat, lon, catch):
            continue
        r = score_site(lat, lon, own, riv, size_sqft=10_000.0,
                       catchment_km=catch, population=pop, beta=beta)
        people = r.get("captured_population") or 0.0
        scored.append(((lat, lon),
                       people * (1 - r["cannibalisation_pct"] / 100.0)))
    scored.sort(key=lambda t: -t[1])
    return label, [c for c, _v in scored]


def _compare(base: list, other: list, k: int = 10) -> dict:
    pos = {c: i for i, c in enumerate(other)}
    drift = [abs(pos[c] - i) for i, c in enumerate(base[:k]) if c in pos]
    return {
        "top1_held": bool(other and base and other[0] == base[0]),
        "top10_overlap": len(set(base[:k]) & set(other[:k])),
        "rank_drift": round(statistics.mean(drift), 1) if drift else None,
    }


def main(chain: str, pool_n: int, workers: int) -> None:
    from oasis.logic.geo_sources import load_competitors
    from oasis.logic.population import load_population
    from oasis.logic.site_scoring import CATCHMENT_KM

    path = os.path.join(_ROOT, "devkit",
                        f"siting_{chain.lower().replace(' ', '_')}.json")
    pop = load_population(root=_ROOT)["grid"]
    lats = [c[0] for c in pop.cells]; lons = [c[1] for c in pop.cells]
    south, north, west, east = min(lats), max(lats), min(lons), max(lons)
    step = 0.01
    grid = [(round(south + i * step, 4), round(west + j * step, 4))
            for i in range(int((north - south) / step) + 1)
            for j in range(int((east - west) / step) + 1)]
    grid = [g for g in grid if pop.covers(g[0], g[1], CATCHMENT_KM)]
    rnd = random.Random(11)
    pool = rnd.sample(grid, min(pool_n, len(grid)))

    rows = load_competitors(root=_ROOT, include_own=True)["rows"]
    n_riv = sum(1 for r in rows if (r.get("Chain") or "").lower()
                != chain.lower())
    # 17 branches OSM has never had, scattered where people live, to ask what a
    # KNOWN missing competitor does to a recommendation.
    dense = sorted(pop.cells, key=lambda c: -c[2])[:400]
    extra = [{"lat": c[0], "lon": c[1], "size_sqft": 4000.0,
              "chain": "Missing", "pull": None}
             for c in rnd.sample(dense, 17)]

    conditions = [("baseline", {})]
    for b in (1.5, 2.5, 3.0):
        conditions.append((f"beta {b}", {"beta": b}))
    for c in (5.0, 15.0, 20.0):
        conditions.append((f"catchment {c:.0f}km", {"catchment": c}))
    conditions.append(("all shops 15k sqft", {"uniform_size": 15_000.0}))
    for d in (0.10, 0.20, 0.30):
        for s in range(4):
            conditions.append((f"drop {int(d*100)}% rivals #{s}",
                               {"drop": d, "seed": 100 + s}))
    conditions.append(("17 unmapped rivals added", {"extra_rivals": extra}))

    print(f"  chain {chain}: {n_riv} rivals, pool of {len(pool)} covered sites")
    print(f"  {len(conditions)} conditions\n")

    t0 = time.time()
    tasks = [(label, pool, dict(opts)) for label, opts in conditions]

    results = {}
    with ProcessPoolExecutor(max_workers=workers, initializer=_init,
                             initargs=(_ROOT, chain)) as ex:
        for label, order in ex.map(_run, tasks):
            results[label] = order
    print(f"  scored in {time.time() - t0:.0f}s\n")

    base = results["baseline"]
    print(f"  {'condition':<26}{'top-1 held':>12}{'top-10 back':>13}"
          f"{'mean rank drift':>18}")
    for label, _ in conditions:
        if label == "baseline":
            continue
        c = _compare(base, results[label])
        print(f"  {label:<26}{('yes' if c['top1_held'] else 'NO'):>12}"
              f"{str(c['top10_overlap']) + '/10':>13}"
              f"{(c['rank_drift'] if c['rank_drift'] is not None else '-'):>18}")

    out = os.path.join(_ROOT, "devkit", "siting_robustness.json")
    json.dump({"chain": chain, "pool": len(pool),
               "conditions": {k: _compare(base, v) for k, v in results.items()}},
              open(out, "w", encoding="utf-8"), indent=1)
    print(f"\n  written {out}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--chain", default="Chandarana")
    ap.add_argument("--pool", type=int, default=240)
    ap.add_argument("--workers", type=int, default=8)
    a = ap.parse_args()
    main(a.chain, a.pool, a.workers)
