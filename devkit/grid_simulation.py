"""Monte Carlo site scoring across the region, once per chain.

DEV TOOLING — never ships (devkit/ is excluded from the release).

WHY
    A capture score of 1.5% means nothing on its own. Is that a good site or a
    poor one? The number only becomes a judgement once there is a distribution
    behind it, and the way to get one is to score the whole city rather than
    the three points somebody happened to ask about.

WHAT IT DOES
    Drops random pins across the metropolitan area and scores each one — once
    for every chain in the matrix, from THAT chain's point of view. Playing as
    a given banner, its own branches are the "own stores" term in the Huff
    denominator and every other chain is competition. So the same pin yields
    six different answers, which is the point: whitespace for one retailer is
    somebody else's back yard.

    The output is a percentile table. A real candidate can then be read as
    "83rd percentile for a store of this size, from this chain's position"
    instead of as a bare fraction.

WHY PROCESSES AND NOT THREADS
    Scoring is pure-Python CPU work — haversines over the population grid — so
    threads would serialise on the GIL and buy nothing. Each worker builds its
    own grids once in an initialiser, because the population grid is far too
    large to pickle per task.

    Sampling is seeded. An unseeded simulation cannot be re-run to check a
    surprising result, which is one of the specific failures this codebase
    already carries in its own store-GNN review.

    python devkit/grid_simulation.py [--points 400] [--seed 7] [--workers 8]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import random
import statistics
import sys
import time
from concurrent.futures import ProcessPoolExecutor

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

#: The metropolitan area the matrix was fetched for.
BBOX = (-1.60, 36.50, -1.00, 37.30)          # south, west, north, east

#: A pin with fewer people than this in reach is farmland, water or park. It
#: scores a trivially perfect share of nothing, and including it would drag
#: every percentile down while telling us nothing.
MIN_CATCHMENT_PEOPLE = 2_000.0

_STATE: dict = {}


def _worker_init(root: str) -> None:
    """Build the grids once per process."""
    from oasis.logic.affluence import load_affluence
    from oasis.logic.geo_sources import load_own_chain
    from oasis.logic.population import load_population

    # The matrix, WITHOUT any own-banner exclusion: for this simulation each
    # chain takes its turn as the operator, so every row must stay available.
    own_names = load_own_chain(root)
    if own_names:
        os.environ["_OASIS_SIM_RESTORE_OWN"] = json.dumps(own_names)

    _STATE["population"] = load_population(root=root)["grid"]
    _STATE["affluence"] = load_affluence(root=root)["grid"]
    _STATE["rows"] = _read_matrix(root)


def _read_matrix(root: str) -> list:
    """Every store in the matrix, chain-labelled, sizes applied."""
    from oasis.logic.geo_sources import (apply_sizes, cache_path,
                                         legacy_cache_path,
                                         load_chain_profiles)
    path = cache_path(root)
    if not os.path.exists(path):
        path = legacy_cache_path(root)
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        rows = list(csv.DictReader(f))
    return apply_sizes(rows, load_chain_profiles(root))


def _as_point(row: dict) -> dict:
    return {"lat": float(row["Latitude"]), "lon": float(row["Longitude"]),
            "size_sqft": float(row.get("size_sqft") or 15_000.0),
            "chain": row.get("Chain") or "", "pull": row.get("pull")}


def score_batch(task: tuple) -> list:
    """Score one chain against a batch of pins. Runs in a worker process."""
    chain, size_sqft, points = task
    from oasis.logic.site_scoring import score_site

    rows = _STATE["rows"]
    pop = _STATE["population"]
    own = [_as_point(r) for r in rows
           if (r.get("Chain") or "").strip().lower() == chain.lower()]
    rivals = [_as_point(r) for r in rows
              if (r.get("Chain") or "").strip().lower() != chain.lower()]

    out = []
    for lat, lon in points:
        s = score_site(lat, lon, own, rivals, size_sqft=size_sqft,
                       population=pop)
        if (s.get("catchment_population") or 0) < MIN_CATCHMENT_PEOPLE:
            continue
        out.append({
            "chain": chain, "lat": lat, "lon": lon,
            "capture": s["adjusted_capture_pct"],
            "people": s["captured_population"] or 0.0,
            "cannibalisation": s["cannibalisation_pct"],
            "catchment": s["catchment_population"] or 0.0,
            "nearest_own_km": s["nearest_own_km"],
        })
    return out


def sample_points(n: int, seed: int, grid=None) -> list:
    """Pins to score, drawn where PEOPLE are rather than where LAND is.

    Uniform sampling over a bounding box was the first attempt and it produced
    a nonsense yardstick: median capture 60-79% and everything above the third
    quartile at 100%. The pins were landing in farmland, where a store faces no
    competitor within 10km and therefore takes a full share — of almost nobody.
    Technically correct, commercially meaningless, and it dragged the whole
    distribution somewhere no retailer would ever build.

    Sampling each cell with probability proportional to its population asks the
    question a retailer actually cares about: across the places people live,
    how does this site rank? Pins then concentrate where there is both demand
    and competition, which is where a percentile means something.
    """
    rng = random.Random(seed)
    south, west, north, east = BBOX
    cells = [c for c in getattr(grid, "cells", []) or []
             if south <= c[0] <= north and west <= c[1] <= east]
    if not cells:
        return [(rng.uniform(south, north), rng.uniform(west, east))
                for _ in range(n)]

    weights = [c[2] for c in cells]
    picks = rng.choices(cells, weights=weights, k=n)
    # Jitter inside the cell so pins are not stacked on grid centroids.
    half = 0.00416667                      # half of a 30 arc-second cell
    return [(p[0] + rng.uniform(-half, half), p[1] + rng.uniform(-half, half))
            for p in picks]


def percentiles(values: list, qs=(5, 25, 50, 75, 90, 95, 99)) -> dict:
    if not values:
        return {}
    v = sorted(values)
    out = {}
    for q in qs:
        pos = (q / 100.0) * (len(v) - 1)
        lo = int(pos)
        hi = min(lo + 1, len(v) - 1)
        out[q] = v[lo] + (v[hi] - v[lo]) * (pos - lo)
    return out


def main(points: int, seed: int, workers: int, out_path: str) -> None:
    from oasis.logic.geo_sources import load_chain_profiles

    rows = _read_matrix(_ROOT)
    chains = sorted({(r.get("Chain") or "").strip() for r in rows if r.get("Chain")})
    profiles = load_chain_profiles(_ROOT)

    def size_for(chain: str) -> float:
        rec = profiles.get(chain.lower())
        if rec:
            return float(rec["size_sqft"])
        for k, v in profiles.items():
            if k in chain.lower() or chain.lower() in k:
                return float(v["size_sqft"])
        return 15_000.0

    from oasis.logic.population import load_population
    pins = sample_points(points, seed, load_population(root=_ROOT)["grid"])
    print(f"  region     {BBOX}")
    print("  sampling   population-weighted (uniform-over-land gives a "
          "nonsense null)")
    print(f"  pins       {points:,} (seed {seed})")
    print(f"  chains     {len(chains)}  {chains}")
    print(f"  workers    {workers}\n")

    # One task per chain per slice, so a slow chain cannot hold a core idle.
    slice_n = max(20, points // max(1, workers))
    tasks = []
    for chain in chains:
        sz = size_for(chain)
        for i in range(0, len(pins), slice_n):
            tasks.append((chain, sz, pins[i:i + slice_n]))

    t0 = time.time()
    results: list = []
    with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init,
                             initargs=(_ROOT,)) as ex:
        for got in ex.map(score_batch, tasks):
            results.extend(got)
    elapsed = time.time() - t0

    scored = len(results)
    print(f"  scored     {scored:,} pin-chain pairs in {elapsed:.1f}s "
          f"({scored / max(elapsed, 1e-9):,.0f}/s)")
    kept = scored / max(1, points * len(chains))
    print(f"  inhabited  {kept:.0%} of pins had a catchment worth scoring\n")

    by_chain: dict = {}
    for r in results:
        by_chain.setdefault(r["chain"], []).append(r)

    print("CAPTURE PERCENTILES, playing as each chain")
    print(f"  {'chain':<13}{'sqft':>8}{'n':>6}" +
          "".join(f"{'p'+str(q):>8}" for q in (5, 25, 50, 75, 90, 95, 99)))
    summary = {}
    for chain in sorted(by_chain, key=lambda c: -len(by_chain[c])):
        caps = [r["capture"] for r in by_chain[chain]]
        p = percentiles(caps)
        summary[chain] = {"size_sqft": size_for(chain), "n": len(caps),
                          "capture": p,
                          "people": percentiles([r["people"] for r in by_chain[chain]]),
                          "median_cannibalisation": statistics.median(
                              [r["cannibalisation"] for r in by_chain[chain]])}
        print(f"  {chain:<13}{size_for(chain):>8,.0f}{len(caps):>6}" +
              "".join(f"{p[q]:>8.2f}" for q in (5, 25, 50, 75, 90, 95, 99)))

    print("\nMEDIAN CANNIBALISATION — how much of a win comes from your own network")
    for chain in sorted(summary, key=lambda c: summary[c]["median_cannibalisation"]):
        print(f"  {chain:<13}{summary[chain]['median_cannibalisation']:>7.1f}%")

    print("\nBEST UNCONTESTED WHITESPACE per chain "
          "(top capture with cannibalisation under 25%)")
    for chain in sorted(by_chain):
        clean = [r for r in by_chain[chain] if r["cannibalisation"] < 25.0]
        if not clean:
            print(f"  {chain:<13} none found")
            continue
        best = max(clean, key=lambda r: r["people"])
        print(f"  {chain:<13} {best['lat']:.4f}, {best['lon']:.4f}  "
              f"capture {best['capture']:.2f}%  people {best['people']:,.0f}  "
              f"cannib {best['cannibalisation']:.0f}%")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"bbox": BBOX, "points": points, "seed": seed,
                   "scored": scored, "elapsed_s": round(elapsed, 1),
                   "summary": summary,
                   "results": results}, f, indent=2, default=float)
    print(f"\n  written    {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--points", type=int, default=400)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--workers", type=int, default=max(2, (os.cpu_count() or 4) - 1))
    ap.add_argument("--out", default=os.path.join(
        _ROOT, "devkit", "grid_simulation_result.json"))
    a = ap.parse_args()
    main(a.points, a.seed, a.workers, a.out)
