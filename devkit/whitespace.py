"""Where the city is underserved by supermarkets, and by how much.

DEV TOOLING — never ships (devkit/ is excluded from the release).

THE MEASURE
    The grid simulation answers "how does this pin rank?". This answers the
    prior question: where is there demand that no existing store is serving?

    Both terms come from data already on the install:

        demand  = people within the catchment          (WorldPop)
        supply  = SUM over every store of A / d^2       (the matrix)
        gap     = demand / supply

    ``supply`` is deliberately the same distance-decayed attractiveness the
    Huff model uses, not a store count. A hypermarket two kilometres away
    serves a neighbourhood far better than a kiosk next door, and a plain count
    within a radius cannot tell the two apart.

    The result is people per unit of effective retail supply — a gap measure
    that belongs to the CITY, not to any one retailer. It is the same question
    whichever banner is asking, which is what makes it a starting point for a
    market map rather than a per-client answer.

A SWEEP, NOT PINS
    Random sampling finds the distribution; it does not reliably find the
    maxima. Whitespace is a search for extremes, so this walks the population
    grid itself — every inhabited cell is a candidate, and nothing is missed
    because the dice did not land there.

DEDUPLICATION MATTERS MORE THAN IT SOUNDS
    Adjacent cells describe the SAME opportunity. Without a minimum separation
    the top twenty is one neighbourhood listed twenty times, which reads as
    twenty findings and is one.

    python devkit/whitespace.py [--top 20] [--separation 3.0]
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

BBOX = (-1.60, 36.50, -1.00, 37.30)

#: A cell thinner than this is not a retail catchment worth walking to.
MIN_CELL_PEOPLE = 150.0

#: Catchment radius, matching the site scorer: the area whose people we claim.
CATCHMENT_KM = 10.0

#: How far out a store still counts as competition — deliberately larger than
#: the catchment, and imported so the two definitions cannot drift. Truncating
#: supply at the catchment was what made the periphery look empty: the top
#: site's gap ignored 83% of the pull acting on it.
from oasis.logic.site_scoring import SUPPLY_KM  # noqa: E402

#: THE SWEEP IS INSET FROM THE FETCHED REGION BY ONE CATCHMENT.
#:
#: The competitor matrix was fetched for BBOX, so a store just outside that
#: boundary does not exist as far as this analysis is concerned. A candidate
#: sitting two kilometres inside the northern edge therefore has half its
#: catchment un-surveyed, reads as unserved, and tops the ranking on an
#: artefact of where the download stopped.
#:
#: The first run of this tool did exactly that: nineteen of the top twenty
#: sites had their nearest store at 8.5-10.0 km — i.e. right at the catchment
#: boundary — and the "entirely unserved" list was every cell whose nearest
#: store was 10.1-10.3 km away. That is the edge of the DATA, not the edge of
#: the market.
#:
#: Sweeping only the interior guarantees every scored point has its full
#: catchment covered by the matrix. It costs area; the alternative costs
#: correctness.
_EDGE_DEG = CATCHMENT_KM / 111.0

#: Distance floor, so a candidate sitting on top of a store does not divide by
#: zero and report infinite supply.
MIN_DISTANCE_KM = 0.1

_STATE: dict = {}


def _worker_init(root: str) -> None:
    from oasis.logic.population import load_population
    _STATE["population"] = load_population(root=root)["grid"]
    _STATE["stores"] = _read_matrix(root)


def _read_matrix(root: str) -> list:
    """Through ``load_competitors``, so this sweeps the SAME market the console
    scores against — the shipped pack, the operator's corrections and the
    exclusion of chains that have closed all reach it now. Reading the cache
    file directly (in three separate devkit copies) meant none of that did."""
    from oasis.logic.geo_sources import load_competitors
    sized = load_competitors(root=root, include_own=True)["rows"]
    return [(float(r["Latitude"]), float(r["Longitude"]),
             float(r.get("size_sqft") or 15_000.0),
             (r.get("Chain") or "").strip()) for r in sized]


def supply_at(lat: float, lon: float, stores: list) -> tuple:
    """Distance-decayed retail supply at a point, and the nearest store."""
    from oasis.logic.population import haversine_km
    total = 0.0
    nearest = None
    nearest_chain = ""
    within = 0
    for slat, slon, sqft, chain in stores:
        d = haversine_km(lat, lon, slat, slon)
        if nearest is None or d < nearest:
            nearest, nearest_chain = d, chain
        if d <= CATCHMENT_KM:
            within += 1
        # Supply counts well past the catchment: a rival just outside it still
        # serves the people at its edge.
        if d <= SUPPLY_KM:
            dd = max(d, MIN_DISTANCE_KM)
            total += (sqft / 1000.0) / (dd * dd)
    return total, nearest, nearest_chain, within


#: The hypothetical store used to rank sites, in square feet. A mid-size
#: supermarket: big enough to be a real proposition, small enough that the
#: ranking is not just "wherever a hypermarket would fit".
REFERENCE_SQFT = 10_000.0


def score_cells(cells: list) -> list:
    """Rank score for a batch of grid cells. Runs in a worker process.

    Ranked on the PEOPLE a new reference store would actually win, not on the
    demand/supply ratio. The ratio is unbounded: as supply tends to zero it
    tends to infinity, so a ranking built on it is decided almost entirely by
    its denominator and fills up with places that have one distant store. The
    captured headcount is bounded, is in units a retailer can act on, and
    balances demand against competition by construction.

    The ratio is still reported, because it is the right diagnostic once a
    shortlist exists — it says whether a site is big, or merely neglected.
    """
    from oasis.logic.site_scoring import score_site
    pop = _STATE["population"]
    stores = _STATE["stores"]
    rivals = [{"lat": s[0], "lon": s[1], "size_sqft": s[2], "chain": s[3],
               "pull": 1.0} for s in stores]

    out = []
    for lat, lon in cells:
        people = pop.population_within(lat, lon, CATCHMENT_KM)
        if people <= 0:
            continue
        # A NEW ENTRANT: no own stores, so nothing here is cannibalisation and
        # the answer belongs to the city rather than to a particular banner.
        s = score_site(lat, lon, [], rivals, size_sqft=REFERENCE_SQFT,
                       population=pop)
        supply, nearest, chain, within = supply_at(lat, lon, stores)
        gap = (people / supply) if supply > 0 else None
        out.append({"lat": round(lat, 5), "lon": round(lon, 5),
                    "people": round(people, 0),
                    "winnable": round(s.get("captured_population") or 0.0, 0),
                    "capture_pct": s["adjusted_capture_pct"],
                    "supply": round(supply, 4),
                    "gap": None if gap is None else round(gap, 1),
                    "unserved": supply <= 0,
                    "nearest_km": None if nearest is None else round(nearest, 2),
                    "nearest_chain": chain,
                    "stores_within_10km": within})
    return out


def dedupe(rows: list, separation_km: float, top: int) -> list:
    """Greedy: take the best, drop everything within `separation_km`, repeat.

    Adjacent cells describe the same opportunity. Without this the top twenty
    is one neighbourhood listed twenty times.
    """
    from oasis.logic.population import haversine_km
    kept: list = []
    for r in rows:
        if all(haversine_km(r["lat"], r["lon"], k["lat"], k["lon"])
               >= separation_km for k in kept):
            kept.append(r)
            if len(kept) >= top:
                break
    return kept


def main(top: int, separation: float, workers: int, out_path: str) -> None:
    from oasis.logic.population import load_population

    grid = load_population(root=_ROOT)["grid"]
    south, west, north, east = BBOX
    # Inset by one catchment: see _EDGE_DEG. Sweeping to the boundary ranks
    # the edge of the download instead of the edge of the market.
    s_in, w_in = south + _EDGE_DEG, west + _EDGE_DEG
    n_in, e_in = north - _EDGE_DEG, east - _EDGE_DEG
    cells = [(c[0], c[1]) for c in grid.cells
             if s_in <= c[0] <= n_in and w_in <= c[1] <= e_in
             and c[2] >= MIN_CELL_PEOPLE]
    stores = _read_matrix(_ROOT)
    print(f"  fetched region   {BBOX}")
    print(f"  swept interior   ({s_in:.3f}, {w_in:.3f}, {n_in:.3f}, {e_in:.3f})"
          f"  inset {CATCHMENT_KM:.0f}km")
    print(f"  cells swept      {len(cells):,} inhabited "
          f"(>= {MIN_CELL_PEOPLE:,.0f} people each)")
    print(f"  stores in matrix {len(stores):,}")
    print(f"  workers          {workers}\n")

    chunk = max(50, len(cells) // (workers * 4))
    batches = [cells[i:i + chunk] for i in range(0, len(cells), chunk)]

    t0 = time.time()
    rows: list = []
    with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init,
                             initargs=(_ROOT,)) as ex:
        for got in ex.map(score_cells, batches):
            rows.extend(got)
    print(f"  scored           {len(rows):,} cells in {time.time() - t0:.1f}s")

    unserved = [r for r in rows if r["unserved"]]
    rows.sort(key=lambda r: -r["winnable"])
    print(f"  no store in 10km {len(unserved):,} cells\n")

    best = dedupe(rows, separation, top)
    print(f"TOP {top} WHITESPACE SITES — people a new "
          f"{REFERENCE_SQFT:,.0f} sqft store would win, {separation:.0f}km apart")
    print(f"  {'#':<4}{'lat':>9}{'lon':>9}{'winnable':>10}{'catchment':>11}"
          f"{'share':>8}{'nearest':>9}{'rivals':>8}  nearest chain")
    for i, r in enumerate(best, 1):
        print(f"  {i:<4}{r['lat']:>9.4f}{r['lon']:>9.4f}{r['winnable']:>10,.0f}"
              f"{r['people']:>11,.0f}{r['capture_pct']:>7.1f}%"
              f"{r['nearest_km']:>8.1f}k{r['stores_within_10km']:>8}"
              f"  {r['nearest_chain']}")

    print("\nSAME SITES BY DEMAND-TO-SUPPLY RATIO — is it big, or just neglected?")
    ranked_gap = sorted([r for r in best if r["gap"] is not None],
                        key=lambda r: -r["gap"])[:8]
    for i, r in enumerate(ranked_gap, 1):
        print(f"  {i:<4}{r['lat']:>9.4f}{r['lon']:>9.4f}"
              f"  gap {r['gap']:>10,.0f}  winnable {r['winnable']:>9,.0f}"
              f"  rivals within 10km {r['stores_within_10km']}")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"bbox": BBOX, "swept_inset_km": CATCHMENT_KM,
                   "reference_sqft": REFERENCE_SQFT,
                   "separation_km": separation,
                   "cells_scored": len(rows), "top": best},
                  f, indent=2, default=float)
    print(f"\n  written          {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--top", type=int, default=20)
    ap.add_argument("--separation", type=float, default=3.0)
    ap.add_argument("--workers", type=int,
                    default=max(2, (os.cpu_count() or 4) - 1))
    ap.add_argument("--out", default=os.path.join(
        _ROOT, "devkit", "whitespace_result.json"))
    a = ap.parse_args()
    main(a.top, a.separation, a.workers, a.out)
