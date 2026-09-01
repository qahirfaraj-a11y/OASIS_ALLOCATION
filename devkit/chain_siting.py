"""Best next sites for one chain, using no revenue at all.

DEV TOOLING — never ships. The logic it calls does
(``oasis.logic.site_capital.catchment_observations`` and friends).

WHY THIS EXISTS
    Sales data only exists for the chain running OASIS. Every other retailer in
    the market — and every PROSPECT being shown what the system would say before
    they connect anything — has no revenue we can see, and never will.

    That kills the whole capital chain: implied demand, spend per person,
    revenue per square foot, the stock-to-sales ratio and the leave-one-out gate
    are all defined on money. What survives is everything geographic:

        captured population      an absolute headcount, not a share
        cannibalisation          what comes out of your own network
        net new people           the difference, and the number that ranks
        format fit               against the chain's OWN people-per-sqft habit

    So a revenue-free siting run answers "where, how big, and how much of it is
    genuinely new" — and refuses to answer "what will it earn", because nothing
    in its inputs could support that.

WHAT IT PRODUCES
    A shortlist of coordinates, each with net-new people, cannibalisation, the
    recommended format, and the distance to the chain's nearest existing branch.

    python devkit/chain_siting.py --chain "Chandarana" [--top 12] [--step 0.01]
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

#: A candidate must be at least this far from the chain's own nearest branch.
#: Not a modelling choice — a practical one. Huff will happily rank a site 200 m
#: from your own shop very highly, because it captures the same people; the
#: cannibalisation column says most of it is your own trade, and a shortlist that
#: has to be read with that caveat on every row is a worse shortlist.
MIN_SELF_DISTANCE_KM = 1.5

#: Candidates closer to each other than this collapse to the best of them. A
#: 1.1 km lattice otherwise returns the same opportunity nine times as nine
#: adjacent cells, and a "top ten" that is one place is not a shortlist.
DEDUPE_KM = 3.0

#: Beyond this from the chain's nearest branch, a site is not expansion of the
#: network — it is entry to a new town, with its own supply, staffing and
#: management questions that no catchment model addresses.
NEW_MARKET_KM = 10.0

ENTRANT_SQFT = 10_000.0

_STATE: dict = {}


def _worker_init(root: str, chain: str) -> None:
    from oasis.logic.geo_sources import load_competitors
    from oasis.logic.population import load_population
    rows = load_competitors(root=root, include_own=True)["rows"]
    pts = [{"lat": float(r["Latitude"]), "lon": float(r["Longitude"]),
            "size_sqft": float(r.get("size_sqft") or 15_000.0),
            "chain": (r.get("Chain") or "").strip(),
            "pull": r.get("pull")} for r in rows]
    _STATE["own"] = [p for p in pts if p["chain"].lower() == chain.lower()]
    _STATE["rivals"] = [p for p in pts if p["chain"].lower() != chain.lower()]
    _STATE["population"] = load_population(root=root)["grid"]


def _score(task: tuple) -> dict:
    from oasis.logic.site_scoring import CATCHMENT_KM, haversine_km, score_site
    lat, lon = task
    own, rivals, pop = _STATE["own"], _STATE["rivals"], _STATE["population"]

    # Refuse anything whose catchment runs off the edge of the data. Those sites
    # are not uncontested, they are unmeasured — see PopulationGrid.covers.
    if not pop.covers(lat, lon, CATCHMENT_KM):
        return {"edge": True}

    nearest = min((haversine_km(lat, lon, s["lat"], s["lon"]) for s in own),
                  default=None)
    if nearest is not None and nearest < MIN_SELF_DISTANCE_KM:
        return {}
    r = score_site(lat, lon, own, rivals, size_sqft=ENTRANT_SQFT,
                   population=pop)
    people = r.get("captured_population") or 0.0
    if people <= 0:
        return {}
    return {"lat": lat, "lon": lon,
            "captured": round(people, 1),
            "cannibalised_pct": r["cannibalisation_pct"],
            "net_new": round(people * (1 - r["cannibalisation_pct"] / 100), 1),
            "capture_pct": r["capture_pct"],
            "nearest_own_km": None if nearest is None else round(nearest, 2),
            "rivals_2km": r["competitors_within_2km"],
            "catchment": r.get("catchment_population")}


def main(chain: str, top: int, step: float, workers: int, out_path: str) -> None:
    from oasis.logic import site_capital as SC
    from oasis.logic.geo_sources import load_competitors
    from oasis.logic.population import load_population
    from oasis.logic.site_scoring import (build_field, haversine_km,
                                          score_site)

    rows = load_competitors(root=_ROOT, include_own=True)["rows"]
    pts = [{"lat": float(r["Latitude"]), "lon": float(r["Longitude"]),
            "size_sqft": float(r.get("size_sqft") or 15_000.0),
            "chain": (r.get("Chain") or "").strip(),
            "pull": r.get("pull"), "name": r.get("Store_Name")}
           for r in rows]
    own = [p for p in pts if p["chain"].lower() == chain.lower()]
    rivals = [p for p in pts if p["chain"].lower() != chain.lower()]
    if not own:
        print(f"  no branches found for '{chain}'. Chains present: "
              f"{sorted({p['chain'] for p in pts})}")
        return
    pop = load_population(root=_ROOT)["grid"]

    # ── calibrate the chain on its own footprint, with no revenue ──
    obs = SC.catchment_observations(own, rivals, population=pop)
    cal = SC.calibrate_catchment(obs)
    print(f"  chain      {chain}: {len(own)} branches, {len(rivals)} rivals")
    print(f"  calibrated on {cal['n']} of {cal['considered']}"
          + (f" ({cal['skipped_count']} skipped)" if cal["skipped_count"] else ""))
    for s in cal.get("stores_skipped") or []:
        print(f"     EXCLUDED {s['org_cd']}: {s['detail']}")
    if not cal.get("usable"):
        print(f"  UNUSABLE: {cal.get('reason')}")
        return
    print(f"  its habit   {cal['median_people_per_sqft']:.2f} people per sq ft"
          f"   (spread {cal['format_spread_ratio']:.2f}x across its branches)")
    print(f"  median branch {cal['median_sqft']:,.0f} sqft serving "
          f"{cal['median_captured']:,.0f} people\n")

    lats = [c[0] for c in pop.cells]; lons = [c[1] for c in pop.cells]
    south, north, west, east = min(lats), max(lats), min(lons), max(lons)
    n_lat = int((north - south) / step) + 1
    n_lon = int((east - west) / step) + 1
    grid = [(round(south + i * step, 4), round(west + j * step, 4))
            for i in range(n_lat) for j in range(n_lon)]

    t0 = time.time()
    found, edge = [], 0
    with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init,
                             initargs=(_ROOT, chain)) as ex:
        for got in ex.map(_score, grid, chunksize=8):
            if not got:
                continue
            if got.get("edge"):
                edge += 1
                continue
            found.append(got)
    print(f"  swept {len(grid):,} points, {len(found):,} viable, "
          f"{edge:,} refused for running off the edge of the data, "
          f"{time.time() - t0:.0f}s")
    if edge:
        print(f"     (a catchment cut off by the download boundary looks "
              f"uncontested because nothing beyond it was ever fetched)")

    # ── shortlist: best first, then drop anything too near a better one ──
    found.sort(key=lambda r: -r["net_new"])
    short = []
    for r in found:
        if all(haversine_km(r["lat"], r["lon"], k["lat"], k["lon"]) >= DEDUPE_KM
               for k in short):
            short.append(r)
        if len(short) >= top:
            break

    # ── format for each shortlisted site, on the chain's own ratio ──
    for r in short:
        fld_cache = {}

        def fn(sz, _r=r, _c=fld_cache):
            if "f" not in _c:
                _c["f"] = build_field(_r["lat"], _r["lon"], own, rivals,
                                      population=pop)
            return score_site(_r["lat"], _r["lon"], own, rivals,
                              size_sqft=sz, population=pop, field=_c["f"])

        rec = SC.recommend_size_by_catchment(fn, cal)
        r["recommended_sqft"] = rec["recommended_sqft"]
        r["format"] = rec["format"]
        # Two different propositions, and ranking them in one list hides it.
        # An infill site takes a small slice of a huge contested catchment; a
        # new-market site takes most of a small uncontested one. They are not
        # comparable investments and should not sit in one column.
        r["kind"] = ("infill" if (r["nearest_own_km"] or 999) < NEW_MARKET_KM
                     else "new market")

    print(f"\n  TOP {len(short)} SITES FOR {chain.upper()} — no revenue used\n")
    print(f"  {'#':<3}{'latitude':>10}{'longitude':>11}{'net new':>10}"
          f"{'of catchment':>14}{'cannib':>8}{'own km':>8}{'riv<2km':>8}"
          f"  {'kind':<11} format")
    for i, r in enumerate(short, 1):
        print(f"  {i:<3}{r['lat']:>10.4f}{r['lon']:>11.4f}{r['net_new']:>10,.0f}"
              f"{r['capture_pct']:>13.2f}%{r['cannibalised_pct']:>7.1f}%"
              f"{(r['nearest_own_km'] or 0):>8.2f}{r['rivals_2km']:>8}"
              f"  {r['kind']:<11} {r['format']}")

    json.dump({"chain": chain, "calibration": cal, "sites": short,
               "swept": len(grid), "viable": len(found),
               "min_self_km": MIN_SELF_DISTANCE_KM, "dedupe_km": DEDUPE_KM},
              open(out_path, "w", encoding="utf-8"), indent=1, default=str)
    print(f"\n  written {out_path}")
    print("\n  NO CAPITAL FIGURE: this run used no sales data, so it can say "
          "where and how big,\n  but nothing about what a store here would earn.")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--chain", required=True)
    ap.add_argument("--top", type=int, default=12)
    ap.add_argument("--step", type=float, default=0.01)
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--out", default=None)
    a = ap.parse_args()
    out = a.out or os.path.join(_ROOT, "devkit",
                                f"siting_{a.chain.lower().replace(' ', '_')}.json")
    main(a.chain, a.top, a.step, a.workers, out)
