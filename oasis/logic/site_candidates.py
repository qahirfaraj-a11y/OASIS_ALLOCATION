"""Where a shop could actually go — the candidate set, not the whole map.

WHY THIS EXISTS

Site scoring was fed a regular lattice: every square kilometre of the region
treated as an equally available place to trade from. Almost none of them are.
A lattice cell has no road frontage, no parking, no zoning, no building and no
landlord, and the cells that maximise reachable population are — predictably —
the middle of dense residential blocks, which is exactly where a supermarket
cannot go.

Measured, that is not a small effect. Scored as candidates, the 131 sites real
supermarket operators actually chose came out WORSE than points drawn at random
in proportion to population:

    real supermarket sites      median 24,018 people captured
    amenity points              median 31,607
    population-weighted random  median 57,304

Only 11% of real sites beat the random median. The first suspicion was that the
model over-penalises clustering — real stores sit a median 0.55 km from a rival
against 3.20 km for random points — but deleting each store's cluster-mates
before scoring barely moved it. Agglomeration is not the explanation.

What the middle row shows is: restricting candidates to places where commerce
demonstrably exists recovers roughly three quarters of the gap, and does it
WITHOUT CHANGING THE SCORING AT ALL. The model was answering its question
correctly. It was being asked the wrong question.

WHAT COUNTS AS A CANDIDATE HERE

The amenity extract already fetched for the affluence layer — shops, cafes,
banks, pharmacies, markets — is a map of where commerce physically happens.
Individual points are far too fine to be candidates (the median amenity is 41 m
from its nearest neighbour, because a mall is twenty of them), so they are
clustered into COMMERCIAL NODES: one high street, one mall, one market is one
candidate.

This is a proxy and should be read as one. It says "trade happens here", not
"a 10,000 sq ft unit is available here at a rent you would pay". It cannot see
vacancy, planning consent or price. It is a filter on the absurd, not a
property search.
"""

from __future__ import annotations

import csv
import math
import os
from typing import Any, Dict, List, Optional, Sequence

from .site_scoring import haversine_km

#: The amenity extract, shared with the affluence layer rather than re-fetched.
AMENITY_FILE = "amenity_poi.csv"

#: How far apart two amenities can be and still be the same commercial place.
#: A mall or a high street is one candidate site, not the twenty shops in it.
#: Measured on the live extract: 5,756 points collapse to 1,907 nodes at 200 m,
#: 1,224 at 400 m and 905 at 600 m. 400 m keeps a mall together without merging
#: two neighbourhoods that a shopper would not treat as one destination.
CLUSTER_KM = 0.4

#: Below this many amenities a "node" is one shop in a residential street, not
#: a place a supermarket would go. The point of the filter is feasibility, and
#: a single café is no more evidence of a retail site than an empty field.
MIN_AMENITIES = 3


def amenity_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", AMENITY_FILE)


def load_amenity_points(root: Optional[str] = None) -> List[tuple]:
    """``(lat, lon, kind)`` for every amenity on this install."""
    path = amenity_path(root)
    if not os.path.exists(path):
        return []
    out: List[tuple] = []
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            for r in csv.DictReader(f):
                try:
                    out.append((float(r["latitude"]), float(r["longitude"]),
                                str(r.get("kind") or "")))
                except (KeyError, TypeError, ValueError):
                    continue
    except OSError:
        return []
    return out


def commercial_nodes(points: Sequence[tuple],
                     cluster_km: float = CLUSTER_KM,
                     min_amenities: int = MIN_AMENITIES) -> List[Dict[str, Any]]:
    """Cluster amenity points into candidate commercial places (pure).

    Greedy, seeded by local density: the busiest unassigned point claims
    everything within ``cluster_km``, and THAT POINT'S coordinate is the
    candidate — not the centroid of its members.

    That distinction matters. A centroid of shops arranged around a roundabout
    or along both banks of a river lands in the roundabout or the river, and the
    whole purpose of this module is to stop emitting coordinates that are not
    places. A seed is always somewhere a real amenity stands.
    """
    pts = [(float(a), float(b), str(k)) for a, b, k in points or []]
    if not pts:
        return []

    # Bucket to keep the neighbour search local; a full pairwise scan over a
    # national extract would dominate.
    deg = max(cluster_km / 110.0, 1e-6)
    buckets: Dict[tuple, List[int]] = {}
    for i, (la, lo, _k) in enumerate(pts):
        buckets.setdefault((int(la / deg), int(lo / deg)), []).append(i)

    def neighbours(i: int) -> List[int]:
        la, lo, _k = pts[i]
        bl, bo = int(la / deg), int(lo / deg)
        out = []
        for dl in (-1, 0, 1):
            for do in (-1, 0, 1):
                for j in buckets.get((bl + dl, bo + do), ()):
                    if haversine_km(la, lo, pts[j][0], pts[j][1]) <= cluster_km:
                        out.append(j)
        return out

    density = [len(neighbours(i)) for i in range(len(pts))]
    order = sorted(range(len(pts)), key=lambda i: -density[i])

    taken = [False] * len(pts)
    nodes: List[Dict[str, Any]] = []
    for i in order:
        if taken[i]:
            continue
        members = [j for j in neighbours(i) if not taken[j]]
        for j in members:
            taken[j] = True
        if len(members) < max(1, int(min_amenities)):
            continue
        kinds = [pts[j][2] for j in members]
        nodes.append({
            "lat": round(pts[i][0], 6), "lon": round(pts[i][1], 6),
            "amenities": len(members),
            "discretionary": sum(1 for k in kinds if k == "discretionary"),
            "staple": sum(1 for k in kinds if k == "staple"),
        })
    nodes.sort(key=lambda n: -n["amenities"])
    return nodes


def candidates(root: Optional[str] = None,
               population: Any = None,
               catchment_km: float = 10.0,
               cluster_km: float = CLUSTER_KM,
               min_amenities: int = MIN_AMENITIES,
               exclude: Sequence[Dict[str, Any]] = (),
               min_distance_km: float = 0.0) -> Dict[str, Any]:
    """Feasible candidate sites for this install.

    ``exclude`` are stores a candidate must stand clear of by
    ``min_distance_km`` — normally the operator's own branches, since a site
    next to your own shop is a relocation argument rather than a new one.

    Candidates whose catchment runs off the edge of the population grid are
    dropped, for the same reason a lattice sweep drops them: a catchment cut
    off by the download looks uncontested because nothing beyond it was fetched.
    """
    pts = load_amenity_points(root)
    if not pts:
        return {"candidates": [], "amenities": 0, "nodes": 0,
                "error": "No amenity data on this install. Fetch your region "
                         "first — the same fetch that loads population."}

    nodes = commercial_nodes(pts, cluster_km, min_amenities)
    kept, off_edge, too_close, unmapped = [], 0, 0, 0
    for n in nodes:
        if population is not None and not population.covers(
                n["lat"], n["lon"], catchment_km):
            off_edge += 1
            continue
        # AND outside the region competitors were fetched over. The population
        # grid is national, so it happily covers a town 45 km past where the
        # competitor download stopped — and that town then scores as
        # gloriously uncontested with zero rivals, because none were ever
        # looked for. Four such sites reached a shortlist before this check
        # existed. "No rivals" is a finding inside the fetched box and an
        # absence of data outside it.
        from .geo_sources import covers_competitors
        inside = covers_competitors(n["lat"], n["lon"], catchment_km, root)
        if inside is False:
            unmapped += 1
            continue
        if min_distance_km > 0 and exclude:
            near = min((haversine_km(n["lat"], n["lon"],
                                     float(s.get("lat", s.get("Latitude", 0))),
                                     float(s.get("lon", s.get("Longitude", 0))))
                        for s in exclude), default=None)
            if near is not None and near < min_distance_km:
                too_close += 1
                continue
        kept.append(n)
    return {"candidates": kept, "amenities": len(pts), "nodes": len(nodes),
            "off_edge": off_edge, "too_close": too_close,
            "outside_competitor_region": unmapped,
            "cluster_km": cluster_km, "min_amenities": min_amenities,
            "error": None}
