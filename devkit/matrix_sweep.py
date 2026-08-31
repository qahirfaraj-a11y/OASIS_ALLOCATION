"""Every chain opening at every site, and who pays for it.

DEV TOOLING — never ships (devkit/ is excluded from the release).

WHAT THE OTHER TOOLS LEAVE OUT
    ``grid_simulation`` asks how a site ranks for each chain in turn.
    ``whitespace`` asks where the city is underserved. Neither asks the question
    a competitive market actually turns on: when somebody opens here, WHOSE
    trade do they take?

    That is not an extra analysis bolted on the side — it falls straight out of
    the same Huff arithmetic, and leaving it uncomputed was throwing away the
    half of the model that says who loses.

THE ARITHMETIC
    At a demand point, every store k already holds a share of that point's
    people:

        share_k  =  u_k / SUM(u)              u_k = A_k / d_k^2

    Add an entrant with utility ``u_new`` and every existing share is diluted by
    the same denominator:

        share_k' =  u_k / (SUM(u) + u_new)

    So the people store k loses are ``(share_k - share_k') * w``. Nothing is
    invented: the entrant's gain is decomposed across the incumbents that
    funded it.

    DISPLACED IS NOT ALL OF IT. Where no incumbent is within the catchment
    there is nobody to take trade from — that demand is UNTAPPED, new to the
    market rather than moved within it. The two are different propositions and
    the tool reports them apart:

        captured  =  displaced  +  untapped

    That distinction was not in the first version, which asserted
    ``captured == losses`` and fired on the first site with no store in reach.
    The assertion was right and the decomposition was incomplete: the model was
    telling the truth about a case that had no name yet.

THE FLIP
    Run for every entrant chain in turn and the result is a square matrix per
    site: rows are who opens, columns are who pays. Read down a column and you
    have a chain's exposure — how much it stands to lose whoever moves first.
    Read along a row and you have an entrant's opportunity, split by victim.

    python devkit/matrix_sweep.py [--sites 12] [--workers 8]
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

#: The entrant used for every cell of the matrix. Holding it constant is the
#: point: if each chain entered at its own format the matrix would confound
#: "who is best placed here" with "who builds bigger shops".
ENTRANT_SQFT = 10_000.0

_STATE: dict = {}


def _worker_init(root: str) -> None:
    from oasis.logic.population import load_population
    _STATE["population"] = load_population(root=root)["grid"]
    _STATE["stores"] = _read_matrix(root)


def _read_matrix(root: str) -> list:
    """Through ``load_competitors``, so the matrix here is the market the
    console scores against. Three devkit tools each read the cache file
    directly, so none saw the shipped pack, the corrections, or the chains
    excluded for having closed."""
    from oasis.logic.geo_sources import load_competitors
    sized = load_competitors(root=root, include_own=True)["rows"]
    return [{"lat": float(r["Latitude"]), "lon": float(r["Longitude"]),
             "size_sqft": float(r.get("size_sqft") or 15_000.0),
             "chain": (r.get("Chain") or "").strip(),
             "pull": r.get("pull")} for r in sized]


def displacement(lat: float, lon: float, entrant_sqft: float,
                 entrant_chain: str, stores: list, pop, catchment_km: float):
    """People the entrant wins, decomposed across the chains that lose them.

    Pure given its inputs. Returns ``(captured, untapped, {chain: lost})``,
    where ``captured == untapped + sum(lost.values())``.
    """
    from oasis.logic.site_scoring import _utility, haversine_km, quadrature

    # The SAME quadrature the scorer uses. Keeping a private copy here is how
    # the two drifted: after score_site moved to integrating on population
    # cells this still sampled rings, and the two reported one site's
    # cannibalisation as 45.79% and 45.41%.
    points, weights = quadrature(lat, lon, catchment_km, pop)

    # Every store that competes for this catchment's people, not just those
    # inside it: a rival beyond the boundary still serves the people at the
    # edge, and deleting it from the denominator invents share.
    from oasis.logic.site_scoring import SUPPLY_KM
    near = [s for s in stores
            if haversine_km(lat, lon, s["lat"], s["lon"]) <= SUPPLY_KM]

    captured = 0.0
    untapped = 0.0
    lost: dict = {}
    for (plat, plon), w in zip(points, weights):
        if w <= 0:
            continue
        us = []
        total = 0.0
        for s in near:
            u = _utility(s["size_sqft"], haversine_km(plat, plon, s["lat"], s["lon"]),
                         s["chain"], s.get("pull"))
            us.append((s["chain"], u))
            total += u
        u_new = _utility(entrant_sqft, haversine_km(plat, plon, lat, lon),
                         entrant_chain, 1.0)
        if total + u_new <= 0:
            continue
        won = (u_new / (total + u_new)) * w
        captured += won
        if total <= 0:
            # NOBODY WAS SERVING THESE PEOPLE. Their trade is not displaced
            # from an incumbent, it is new to the market — so it has no victim
            # and must not be charged to one. The conservation check below
            # exists to catch exactly this being conflated: an earlier version
            # asserted captured == losses and fired on the first site with no
            # store within the catchment, which was the model telling the truth
            # about a case the decomposition had not named.
            untapped += won
            continue
        # Every incumbent share is diluted by the same new denominator.
        for chain, u in us:
            lost[chain] = lost.get(chain, 0.0) + (u / total - u / (total + u_new)) * w
    return captured, untapped, lost


def sweep_site(task: tuple) -> dict:
    """One site, every entrant chain. Runs in a worker process."""
    site, chains, catchment_km = task
    stores = _STATE["stores"]
    pop = _STATE["population"]

    rows = {}
    for chain in chains:
        captured, untapped, lost = displacement(
            site["lat"], site["lon"], ENTRANT_SQFT, chain, stores, pop,
            catchment_km)
        # Conservation: what the entrant wins is what the incumbents lose PLUS
        # what nobody was serving. A matrix whose rows do not add up is not
        # describing this model.
        accounted = sum(lost.values()) + untapped
        assert abs(accounted - captured) < max(1.0, captured * 1e-6), (
            f"impact does not conserve at {site['lat']},{site['lon']}: "
            f"captured {captured:,.0f} vs accounted {accounted:,.0f}")
        rows[chain] = {"captured": captured, "untapped": untapped,
                       "self_loss": lost.get(chain, 0.0),
                       "lost": lost}
    return {"site": site, "rows": rows}


def main(n_sites: int, workers: int, out_path: str) -> None:
    from oasis.logic.site_scoring import CATCHMENT_KM

    stores = _read_matrix(_ROOT)
    chains = sorted({s["chain"] for s in stores if s["chain"]})

    ws_path = os.path.join(_ROOT, "devkit", "whitespace_result.json")
    if not os.path.exists(ws_path):
        print("  run devkit/whitespace.py first — this sweeps its top sites.")
        return
    sites = json.load(open(ws_path, encoding="utf-8"))["top"][:n_sites]

    print(f"  sites      {len(sites)} (top whitespace)")
    print(f"  chains     {len(chains)}  {chains}")
    print(f"  entrant    {ENTRANT_SQFT:,.0f} sqft, held constant for every cell")
    print(f"  workers    {workers}\n")

    t0 = time.time()
    tasks = [({"lat": s["lat"], "lon": s["lon"], "people": s["people"],
               "capture_pct": s["capture_pct"]}, chains, CATCHMENT_KM)
             for s in sites]
    results = []
    with ProcessPoolExecutor(max_workers=workers, initializer=_worker_init,
                             initargs=(_ROOT,)) as ex:
        for got in ex.map(sweep_site, tasks):
            results.append(got)
    print(f"  swept      {len(results) * len(chains):,} entrant-site pairs "
          f"in {time.time() - t0:.1f}s\n")

    # ── who is best placed, site by site ────────────────────────────────
    print("BEST PLACED ENTRANT PER SITE — net gain is capture minus what it "
          "takes from itself")
    print(f"  {'#':<3}{'lat':>9}{'lon':>9}  {'best entrant':<13}"
          f"{'net':>10}{'self-loss':>10}{'untapped':>10}   worst placed")
    for i, r in enumerate(results, 1):
        net = {c: v["captured"] - v["self_loss"] for c, v in r["rows"].items()}
        best = max(net, key=net.get)
        worst = min(net, key=net.get)
        print(f"  {i:<3}{r['site']['lat']:>9.4f}{r['site']['lon']:>9.4f}  "
              f"{best:<13}{net[best]:>10,.0f}"
              f"{r['rows'][best]['self_loss']:>10,.0f}"
              f"{r['rows'][best]['untapped']:>10,.0f}   {worst}")

    print("\nDISPLACED VERSUS UNTAPPED — is the prize taken from somebody, or "
          "new to the market?")
    print(f"  {'#':<3}{'lat':>9}{'lon':>9}{'captured':>11}{'displaced':>11}"
          f"{'untapped':>10}{'untapped %':>12}")
    for i, r in enumerate(results, 1):
        # Averaged over entrants: the split is a property of the SITE, and
        # varies only slightly with who opens there.
        cap = sum(v["captured"] for v in r["rows"].values()) / len(r["rows"])
        unt = sum(v["untapped"] for v in r["rows"].values()) / len(r["rows"])
        print(f"  {i:<3}{r['site']['lat']:>9.4f}{r['site']['lon']:>9.4f}"
              f"{cap:>11,.0f}{cap - unt:>11,.0f}{unt:>10,.0f}"
              f"{(100 * unt / cap if cap else 0):>11.0f}%")

    # ── the flip: exposure by chain ─────────────────────────────────────
    print("\nEXPOSURE — people each chain loses across all these sites, "
          "summed over every rival entrant")
    exposure = {c: 0.0 for c in chains}
    offence = {c: 0.0 for c in chains}
    for r in results:
        for entrant, v in r["rows"].items():
            offence[entrant] += v["captured"] - v["self_loss"]
            for victim, amount in v["lost"].items():
                if victim != entrant:
                    exposure[victim] += amount
    print(f"  {'chain':<14}{'exposure':>12}{'offence':>12}{'ratio':>9}")
    for c in sorted(chains, key=lambda c: -exposure[c]):
        ratio = offence[c] / exposure[c] if exposure[c] else float("inf")
        print(f"  {c:<14}{exposure[c]:>12,.0f}{offence[c]:>12,.0f}"
              f"{ratio:>9.2f}")
    print("  exposure = what you lose when others move; offence = what you win "
          "when you move.\n  ratio under 1.0 means a chain has more to defend "
          "than to gain here.")

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump({"entrant_sqft": ENTRANT_SQFT, "chains": chains,
                   "sites": [{"site": r["site"],
                              "rows": {c: {"captured": v["captured"],
                                           "self_loss": v["self_loss"],
                                           "untapped": v["untapped"], "lost": v["lost"]}
                                       for c, v in r["rows"].items()}}
                             for r in results],
                   "exposure": exposure, "offence": offence},
                  f, indent=2, default=float)
    print(f"  written    {out_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sites", type=int, default=12)
    ap.add_argument("--workers", type=int,
                    default=max(2, (os.cpu_count() or 4) - 1))
    ap.add_argument("--out", default=os.path.join(
        _ROOT, "devkit", "matrix_sweep_result.json"))
    a = ap.parse_args()
    main(a.sites, a.workers, a.out)
