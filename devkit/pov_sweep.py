"""Every chain's view of the same city, point by point.

DEV TOOLING — never ships (devkit/ is excluded from the release).

THE QUESTION
    Not "where is the best site?" but "where is the best site FOR YOU?" — asked
    once per chain, over a grid covering the whole metropolitan area, so the
    answer is a map rather than a shortlist.

WHAT ACTUALLY DIFFERS BETWEEN CHAINS, AND WHAT DOES NOT
    Measured before this tool was written, at one point in the dense core, all
    six chains scored an IDENTICAL 0.95% capture and 31,773 people. That is not
    a bug — it falls out of the model being right. Since the own/rival
    asymmetry was fixed, an existing store exerts the same pull on the Huff
    denominator whoever owns it, so "how much trade is available at this spot"
    is a property of the MARKET, not of who is asking.

    What differs is how much of that trade you would be taking from yourself:

        Jaza        2.3% cannibalised   nearest own branch 2.52 km
        Cleanshelf  4.3%                                   3.08 km
        Chandarana  7.9%                                   1.17 km
        Quickmart  16.3%                                   1.09 km
        Carrefour  22.3%                                   0.31 km
        Naivas     47.0%                                   0.19 km

    So the per-chain answer is NET NEW people — captured minus what comes out
    of your own network. That is the number this tool maps, and it is the
    precise mechanism behind "whitespace for one retailer is somebody else's
    back yard".

WHY ONE PASS SERVES EVERY CHAIN
    The entrant's utility is chain-independent (matrix_sweep passes pull=1.0
    explicitly, so the big-box name heuristic never fires), and the incumbent
    field is the same set of stores however it is partitioned. So a single
    displacement() per point yields the capture AND every chain's share of the
    loss — six points of view for the price of one, using the conservation-
    checked decomposition that already exists rather than a second copy of it.

    python devkit/pov_sweep.py [--step 0.012] [--workers 8]
"""

from __future__ import annotations

import argparse
import json
import math
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

from devkit.matrix_sweep import ENTRANT_SQFT, _read_matrix, displacement

#: Only score where people actually live. A regular lattice over a bounding box
#: spends most of its points on empty land, and an empty catchment is not a
#: finding — it is a wasted haversine.
MIN_CATCHMENT_PEOPLE = 20_000.0

_STATE: dict = {}


def _worker_init(root: str) -> None:
    from oasis.logic.population import load_population
    _STATE["population"] = load_population(root=root)["grid"]
    _STATE["stores"] = _read_matrix(root)


def _score_point(task: tuple) -> dict:
    from oasis.logic.site_scoring import CATCHMENT_KM
    lat, lon = task
    pop = _STATE["population"]
    stores = _STATE["stores"]

    # A catchment cut off by the DOWNLOAD boundary is not uncontested, it is
    # unmeasured — its people are missing and so are its rivals, so it scores as
    # gloriously empty. See PopulationGrid.covers for what that cost on a real
    # shortlist.
    if not pop.covers(lat, lon, CATCHMENT_KM):
        return {}

    people_here = sum(c[2] for c in pop.near(lat, lon, CATCHMENT_KM))
    if people_here < MIN_CATCHMENT_PEOPLE:
        return {}

    captured, untapped, lost = displacement(
        lat, lon, ENTRANT_SQFT, "", stores, pop, CATCHMENT_KM)
    if captured <= 0:
        return {}

    chains = sorted({s["chain"] for s in stores if s["chain"]})
    out = {"lat": lat, "lon": lon,          # already the rounded lattice point
           "catchment": round(people_here, 0),
           "captured": round(captured, 1),
           "untapped": round(untapped, 1),
           "chains": {}}
    for c in chains:
        own_loss = lost.get(c, 0.0)
        out["chains"][c] = {
            # What the entrant wins that is NOT taken from its own network.
            # The only figure in this model that is genuinely chain-specific.
            "net_new": round(captured - own_loss, 1),
            "cannibalised_pct": round(100.0 * own_loss / captured, 2),
        }
    return out


def main(step: float, workers: int, out_path: str) -> None:
    from oasis.logic.population import load_population

    stores = _read_matrix(_ROOT)
    chains = sorted({s["chain"] for s in stores if s["chain"]})
    pop = load_population(root=_ROOT)["grid"]
    if not pop:
        print("  no population grid on this install — fetch a region first.")
        return

    lats = [c[0] for c in pop.cells]
    lons = [c[1] for c in pop.cells]
    south, north = min(lats), max(lats)
    west, east = min(lons), max(lons)

    # Build the lattice by INDEX and round it here, so the coordinate that gets
    # scored is exactly the coordinate that gets published. Accumulating
    # `la += step` and rounding only on output meant the map named a point a few
    # metres from the one measured — worth 0.4% of captured population at the
    # top site, and worth more than that in trust: a coordinate somebody acts on
    # has to be the coordinate the number came from. Accumulation also drifts.
    n_lat = int((north - south) / step) + 1
    n_lon = int((east - west) / step) + 1
    pts = [(round(south + i * step, 4), round(west + j * step, 4))
           for i in range(n_lat) for j in range(n_lon)]

    print(f"  market     {len(stores)} stores, {len(chains)} chains: {chains}")
    print(f"  grid       {len(pts):,} points at {step} deg "
          f"(~{step * 111:.1f} km spacing)")
    print(f"  entrant    {ENTRANT_SQFT:,.0f} sqft, held constant for every chain")
    print(f"  workers    {workers}\n")

    t0 = time.time()
    results = []
    with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init,
                             initargs=(_ROOT,)) as ex:
        for i, got in enumerate(ex.map(_score_point, pts, chunksize=8), 1):
            if got:
                results.append(got)
            if i % 200 == 0:
                print(f"    {i:,}/{len(pts):,} points, {len(results):,} populated"
                      f"  ({time.time() - t0:.0f}s)")

    print(f"\n  scored {len(results):,} populated points in {time.time() - t0:.0f}s")

    # Per-chain best sites, and the spread that makes the point.
    print(f"\n  {'chain':<12}{'best net-new':>14}{'at':>22}"
          f"{'median cannib':>15}{'worst cannib':>14}")
    best_by_chain = {}
    for c in chains:
        rows = sorted(results, key=lambda r: -r["chains"][c]["net_new"])
        top = rows[0]
        cann = sorted(r["chains"][c]["cannibalised_pct"] for r in results)
        best_by_chain[c] = [
            {"lat": r["lat"], "lon": r["lon"], "catchment": r["catchment"],
             "captured": r["captured"],
             "net_new": r["chains"][c]["net_new"],
             "cannibalised_pct": r["chains"][c]["cannibalised_pct"]}
            for r in rows[:10]]
        where = f"{top['lat']:.3f}, {top['lon']:.3f}"
        print(f"  {c:<12}{top['chains'][c]['net_new']:>14,.0f}{where:>22}"
              f"{cann[len(cann)//2]:>14.1f}%{cann[-1]:>13.1f}%")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"step": step, "chains": chains,
                   "stores": len(stores), "points": results,
                   "best_by_chain": best_by_chain}, f)
    print(f"\n  written {out_path} ({os.path.getsize(out_path)/1024:.0f} KB)")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--step", type=float, default=0.012)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=os.path.join(_ROOT, "devkit",
                                                  "pov_sweep_result.json"))
    a = ap.parse_args()
    main(a.step, a.workers, a.out)
