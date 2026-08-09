"""
Site scoring for greenfield location selection — interpretable, no model.

Answers "how good is this spot for a new store?" from geography the client can
check: how far the site is from their own stores and from competitors, and how
much of the surrounding demand a store there would plausibly capture.

WHY THERE IS NO ML HERE
    The expansion RandomForest that used to answer this was trained on
    ``np.random`` inputs against a hand-written target commented "GROUND TRUTH
    LOGIC (What we want the model to learn)". It therefore learned that formula
    and nothing about retail — a rules engine wearing a 13 MB coat, twenty
    times the size of the whole release. It leaks nothing, but it predicts
    nothing either, and a client asking "how was this trained" deserves a
    better answer. The scoring below is the same geography, stated plainly.

    A real model becomes possible once a chain has opened sites ON OASIS and
    their performance is known. That is a supervised problem with actual
    labels, and it should clear the same beat-the-baseline gate the store-GNN
    work uses before it is allowed to move a number.

THE MODEL
    Huff (1964) gravity share, the standard retail catchment model:

        P(site) = A_site / d_site^2  ÷  Σ over all stores ( A_k / d_k^2 )

    where A is attractiveness (floor area, weighted up for a big-box
    competitor) and d is distance. Every term is inspectable and every
    component is returned alongside the score, because a site decision is
    argued, not asserted.

Distances are great-circle. Straight-line understates real travel and does so
CONSISTENTLY, which is what matters when ranking candidates against each other;
``travel_friction`` carries the correction where a caller has one.
"""

from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Sequence

#: Beyond this a store is not competing for the same trip.
CATCHMENT_KM = 10.0
#: Distance floor, so a site on top of an existing store cannot divide by zero.
MIN_DISTANCE_KM = 0.1
#: Assumed floor area when a store record does not carry one.
DEFAULT_SIZE_SQFT = 10_000.0
#: Big-box competitors pull disproportionately for their footprint.
BIG_BOX_WEIGHT = 1.5
BIG_BOX_CHAINS = ("naivas", "carrefour", "quickmart")
#: A competitor closer than this is direct, head-on competition.
DIRECT_COMPETITION_KM = 2.0
#: Own store closer than this and the new site mostly eats its own trade.
CANNIBALISATION_KM = 3.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres (pure)."""
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = (math.sin(dp / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    return 2 * r * math.asin(min(1.0, math.sqrt(a)))


def attractiveness(size_sqft: float, chain: str = "") -> float:
    """Huff's A term: pull, in thousands of square feet (pure)."""
    weight = (BIG_BOX_WEIGHT
              if any(c in (chain or "").lower() for c in BIG_BOX_CHAINS)
              else 1.0)
    return (max(float(size_sqft or 0), 0.0) / 1000.0) * weight


def _utility(size_sqft: float, distance_km: float, chain: str = "") -> float:
    d = max(float(distance_km), MIN_DISTANCE_KM)
    return attractiveness(size_sqft, chain) / (d * d)


def _coords(rec: Dict[str, Any]) -> Optional[tuple]:
    for lat_key, lon_key in (("lat", "lon"), ("latitude", "longitude"),
                             ("Latitude", "Longitude")):
        if rec.get(lat_key) is not None and rec.get(lon_key) is not None:
            try:
                return float(rec[lat_key]), float(rec[lon_key])
            except (TypeError, ValueError):
                return None
    return None


#: Demand is sampled on a ring around the site, not AT it. Placing the demand
#: point on the candidate gives it distance ~0 and therefore near-infinite
#: utility — the site then beats every competitor by construction, an empty
#: desert scores 100%, and the ranking is meaningless. (The console's expansion
#: engine does exactly this: `candidates.append({'S': ..., 'dist': 0.1})`.)
#: Sampling where the shoppers actually are is what makes competitor proximity
#: bite.
SAMPLE_RING_KM = (1.0, 2.5, 5.0)
SAMPLE_BEARINGS = 8
#: Degrees of latitude per kilometre — good enough at city scale.
_DEG_PER_KM = 1.0 / 111.0


def _ring_points(lat: float, lon: float) -> List[tuple]:
    """Demand sample points around a site, on concentric rings."""
    pts = []
    cos_lat = max(math.cos(math.radians(lat)), 1e-6)
    for radius in SAMPLE_RING_KM:
        for i in range(SAMPLE_BEARINGS):
            theta = 2 * math.pi * i / SAMPLE_BEARINGS
            dlat = radius * _DEG_PER_KM * math.cos(theta)
            dlon = radius * _DEG_PER_KM * math.sin(theta) / cos_lat
            pts.append((lat + dlat, lon + dlon))
    return pts


def score_site(lat: float, lon: float,
               own_stores: Sequence[Dict[str, Any]],
               competitors: Sequence[Dict[str, Any]] = (),
               size_sqft: float = DEFAULT_SIZE_SQFT,
               catchment_km: float = CATCHMENT_KM,
               travel_friction: float = 0.0) -> Dict[str, Any]:
    """Score one candidate location. Pure — no I/O, no model, no globals.

    Capture is the Huff share the new store would take **averaged over demand
    points around it**, not at it. Every component is returned alongside,
    because the number alone is not an argument. ``cannibalisation`` is the part
    of that capture taken from the client's OWN nearby stores: a site can score
    well and still be a poor decision if the trade merely moves across town.

    NOTE ON WHAT THIS CANNOT DO. Demand is assumed uniform across the ring
    because OASIS has no population or footfall data. So this ranks how
    CONTESTED a catchment is, not how many people live in it — an underserved
    suburb and an empty field look alike. Treat it as "who else is competing
    here", and bring a demand estimate to the decision.
    """
    own_in_catchment = 0
    comp_within_direct = 0
    nearest_own = None
    nearest_comp = None

    own_pts, comp_pts = [], []
    for s in own_stores or []:
        c = _coords(s)
        if not c:
            continue
        d = haversine_km(lat, lon, c[0], c[1])
        if nearest_own is None or d < nearest_own:
            nearest_own = d
        if d <= catchment_km:
            own_in_catchment += 1
            own_pts.append((c[0], c[1], float(s.get("size_sqft",
                                                    DEFAULT_SIZE_SQFT)), ""))

    for k in competitors or []:
        c = _coords(k)
        if not c:
            continue
        d = haversine_km(lat, lon, c[0], c[1])
        if nearest_comp is None or d < nearest_comp:
            nearest_comp = d
        if d <= catchment_km:
            comp_pts.append((c[0], c[1], float(k.get("size_sqft", 15_000.0)),
                             str(k.get("Chain") or k.get("chain") or "")))
            if d <= DIRECT_COMPETITION_KM:
                comp_within_direct += 1

    site_shares, own_shares = [], []
    for plat, plon in _ring_points(lat, lon):
        u_site = _utility(size_sqft, haversine_km(plat, plon, lat, lon))
        u_own = sum(_utility(sz, haversine_km(plat, plon, slat, slon))
                    for slat, slon, sz, _c in own_pts)
        u_comp = sum(_utility(sz, haversine_km(plat, plon, klat, klon), ch)
                     for klat, klon, sz, ch in comp_pts)
        total = u_site + u_own + u_comp
        if total <= 0:
            continue
        site_shares.append(u_site / total)
        own_shares.append(u_own / total)

    capture = (sum(site_shares) / len(site_shares)) if site_shares else 0.0
    own_share = (sum(own_shares) / len(own_shares)) if own_shares else 0.0
    # Of the trade this site wins, how much is taken from our own network?
    cannibalisation = (own_share / (own_share + capture)
                       if (own_share + capture) > 0 else 0.0)

    # Friction is a correction a caller supplies (0 = free-flowing). It scales
    # capture down because a hard-to-reach site captures less of its catchment.
    friction = max(0.0, min(1.0, float(travel_friction or 0.0)))
    adjusted = capture * (1.0 - 0.5 * friction)

    return {
        "lat": lat, "lon": lon, "size_sqft": float(size_sqft),
        "capture_pct": round(capture * 100, 2),
        "adjusted_capture_pct": round(adjusted * 100, 2),
        "cannibalisation_pct": round(cannibalisation * 100, 2),
        "own_stores_in_catchment": own_in_catchment,
        "competitors_within_2km": comp_within_direct,
        "nearest_own_km": None if nearest_own is None else round(nearest_own, 2),
        "nearest_competitor_km": (None if nearest_comp is None
                                  else round(nearest_comp, 2)),
        "travel_friction": round(friction, 3),
        "isolated": not own_pts and not comp_pts,
        "verdict": verdict(adjusted, cannibalisation, nearest_own,
                           isolated=not own_pts and not comp_pts,
                           catchment_km=catchment_km),
    }


def verdict(capture: float, cannibalisation: float,
            nearest_own_km: Optional[float], isolated: bool = False,
            catchment_km: float = CATCHMENT_KM) -> str:
    """A sentence a buyer can argue with — not a score to defer to."""
    if isolated:
        # 100% of an empty catchment is still 100%, and it means nothing. Say
        # so rather than let the arithmetic imply an opportunity: OASIS has no
        # population data and cannot tell an underserved suburb from a field.
        return (f"Nothing within {catchment_km:.0f} km — no competition, but no "
                "evidence of demand either. Needs a catchment estimate.")
    if nearest_own_km is not None and nearest_own_km < CANNIBALISATION_KM:
        return ("Too close to your own store — most of this trade would move, "
                "not grow")
    if capture < 0.10:
        return "Crowded catchment; a store here would struggle for share"
    if cannibalisation > 0.5:
        return "Decent share, but over half of it comes from your own network"
    if capture >= 0.30:
        return "Strong share against the competition already here"
    return "Workable: moderate share against existing competition"


#: Store formats, smallest first. Chosen by the share a site can win — a small
#: catchment cannot carry a hypermarket's fixed cost.
STORE_FORMATS = (
    (0.10, "Express / Neighbourhood"),
    (0.20, "Medium Anchor"),
    (0.35, "Hyper / Flagship"),
)


def recommend_format(capture_pct: float) -> str:
    """Largest format the captured share can plausibly support."""
    share = float(capture_pct or 0) / 100.0
    fmt = "Unsuitable — too little share to justify a site"
    for threshold, name in STORE_FORMATS:
        if share >= threshold:
            fmt = name
    return fmt


def rank_sites(candidates: Sequence[Dict[str, Any]],
               own_stores: Sequence[Dict[str, Any]],
               competitors: Sequence[Dict[str, Any]] = (),
               size_sqft: float = DEFAULT_SIZE_SQFT) -> List[Dict[str, Any]]:
    """Score and rank candidate sites, best adjusted capture first."""
    out: List[Dict[str, Any]] = []
    for c in candidates or []:
        coords = _coords(c)
        if not coords:
            continue
        s = score_site(coords[0], coords[1], own_stores, competitors,
                       size_sqft=float(c.get("size_sqft", size_sqft)),
                       travel_friction=float(c.get("travel_friction", 0.0) or 0))
        s["name"] = c.get("name") or f"{coords[0]:.4f}, {coords[1]:.4f}"
        # An isolated site's share is 100% of nothing — recommending a format
        # off it would dress an empty field as a flagship.
        s["format"] = ("Unknown — no catchment context"
                       if s["isolated"]
                       else recommend_format(s["adjusted_capture_pct"]))
        out.append(s)
    out.sort(key=lambda s: -s["adjusted_capture_pct"])
    return out
