"""
Catchment population — the denominator site scoring never had.

WHY THIS EXISTS
    ``site_scoring`` returns a Huff SHARE: the fraction of surrounding demand a
    store here would take. A share has no size. It cannot tell an underserved
    suburb from an empty field, because both are uncontested and both score
    highly; the module's own docstring said so, and ``site_capital`` had to
    refuse isolated sites outright to stop the arithmetic dressing a desert as
    a flagship.

    Population supplies the missing term. Once each demand sample carries a
    number of people, the same Huff maths returns an ABSOLUTE quantity:

        captured_population = SUM over sample points of  share_p * people_p

    That converts every downstream claim from relative to real. In particular
    ``site_capital`` can stop calibrating on ``revenue / capture`` (KES per unit
    of share, which is not transferable between locations and varied 2x across
    a real estate) and calibrate on ``revenue / captured_population`` instead —
    spend per person in catchment, which is an economic constant a retailer
    already has intuitions about and can argue with.

WHERE THE DATA COMES FROM
    The client's, for the client's region — the same rule ``geo_sources`` keeps
    for competitors, for the same reasons. OASIS ships no population file.
    Practical open sources, all requiring attribution and none requiring us to
    redistribute anything:

      * WorldPop gridded population        (CC BY 4.0)
      * GHS-POP, European Commission       (CC BY 4.0)
      * Meta / HDX high-resolution density (CC BY 4.0)
      * A national census by ward or sub-location — in Kenya, KNBS 2019

    All of them export to the same shape: points with a population count. So
    the loader takes a CSV of ``latitude, longitude, population`` under any of
    the usual column spellings, and carries whatever attribution the client's
    source requires.

    This is an engineering summary of licence terms, not legal advice. A
    commercial release should confirm the attribution string for the source the
    client actually loads.

HOW THE WEIGHTS ARE BUILT
    Every population cell within the catchment is assigned to the NEAREST
    demand sample point, so each cell is counted exactly once and no area
    assumption is needed. A ring point's weight is the population that is
    closer to it than to any other ring point — an exact partition of the
    catchment, rather than an inferred annulus area.

    Cells beyond the outermost ring but inside the catchment attach to the
    outer ring, which scores them at the outer ring's distance. That slightly
    understates competition for the far edge of a catchment, and is stated
    rather than hidden.
"""

from __future__ import annotations

import csv
import math
import os
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

#: Where a client's own population extract lives. Never shipped, never
#: redistributed — the client's copy of public data, like the competitor set.
CACHE_FILE = "population_grid.csv"

#: Attribution strings per known source key, shown wherever a derived result
#: is displayed. An unrecognised source falls back to the generic line, because
#: the obligation to attribute does not disappear when we cannot name it.
ATTRIBUTIONS = {
    "worldpop": "Population data © WorldPop (CC BY 4.0)",
    "ghs-pop": "Population data © European Commission GHSL, GHS-POP (CC BY 4.0)",
    "ghsl": "Population data © European Commission GHSL, GHS-POP (CC BY 4.0)",
    "meta": "Population data © Meta / HDX High Resolution Population Density "
            "(CC BY 4.0)",
    "hdx": "Population data © Meta / HDX High Resolution Population Density "
           "(CC BY 4.0)",
    "knbs": "Population data © Kenya National Bureau of Statistics, 2019 Census",
    "census": "Population data from the client's national census",
}
GENERIC_ATTRIBUTION = "Population data from the source loaded by this install"

#: Column spellings accepted, in priority order. Exports differ by tool and a
#: client should not have to rename columns to be understood.
_LAT_KEYS = ("latitude", "lat", "y", "ycoord", "y_coord")
_LON_KEYS = ("longitude", "lon", "lng", "long", "x", "xcoord", "x_coord")
_POP_KEYS = ("population", "pop", "people", "value", "z", "pop_count",
             "population_count", "persons", "residents")
_SRC_KEYS = ("source", "dataset", "provider")

#: Index bucket size in degrees (~11 km at the equator). Big enough that a
#: 10 km catchment touches only a handful of buckets, small enough that a
#: national grid does not collapse into one.
_BUCKET_DEG = 0.1

_EARTH_KM = 6371.0


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in kilometres (pure)."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = math.radians(lat2 - lat1)
    dl = math.radians(lon2 - lon1)
    a = (math.sin(dp / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2)
    return 2 * _EARTH_KM * math.asin(min(1.0, math.sqrt(a)))


def _pick(row: Dict[str, Any], keys: Sequence[str]) -> Optional[Any]:
    low = {str(k).strip().lower(): v for k, v in row.items()}
    for k in keys:
        if k in low and low[k] not in (None, ""):
            return low[k]
    return None


def parse_rows(rows: Iterable[Dict[str, Any]]) -> Tuple[List[tuple], List[str]]:
    """(cells, sources) from raw CSV rows. Pure.

    A cell is ``(lat, lon, people)``. Rows without all three, or with a
    non-positive population, are dropped rather than guessed at: a zero cell
    contributes nothing and a malformed one would contribute nonsense.
    """
    cells: List[tuple] = []
    sources: List[str] = []
    for r in rows or []:
        lat, lon, pop = (_pick(r, _LAT_KEYS), _pick(r, _LON_KEYS),
                         _pick(r, _POP_KEYS))
        if lat is None or lon is None or pop is None:
            continue
        try:
            flat, flon, fpop = float(lat), float(lon), float(pop)
        except (TypeError, ValueError):
            continue
        if fpop <= 0 or abs(flat) > 90 or abs(flon) > 180:
            continue
        cells.append((flat, flon, fpop))
        src = _pick(r, _SRC_KEYS)
        if src:
            sources.append(str(src).strip())
    return cells, sources


def attribution_for(sources: Sequence[str]) -> str:
    """The attribution line a surface must show for these cells."""
    for s in sources or []:
        key = str(s).strip().lower()
        for known, text in ATTRIBUTIONS.items():
            if known in key:
                return text
    return GENERIC_ATTRIBUTION


class PopulationGrid:
    """A client's population cells, indexed for repeated catchment lookups.

    Site scoring calls ``near`` once per candidate and up to once per rung of
    the size ladder, so a linear scan of a national grid would dominate the
    console. The index buckets cells on a coarse lat/lon lattice; ``near``
    visits only the buckets a catchment can touch.
    """

    def __init__(self, cells: Sequence[tuple] = (), attribution: str = "",
                 source: str = ""):
        self.cells: List[tuple] = [tuple(c) for c in cells or []]
        self.attribution = attribution or GENERIC_ATTRIBUTION
        self.source = source
        self.total_people = sum(c[2] for c in self.cells)
        self._index: Dict[Tuple[int, int], List[tuple]] = {}
        for c in self.cells:
            self._index.setdefault(self._bucket(c[0], c[1]), []).append(c)

    def __bool__(self) -> bool:
        return bool(self.cells)

    def __len__(self) -> int:
        return len(self.cells)

    @staticmethod
    def _bucket(lat: float, lon: float) -> Tuple[int, int]:
        return (int(math.floor(lat / _BUCKET_DEG)),
                int(math.floor(lon / _BUCKET_DEG)))

    def near(self, lat: float, lon: float, radius_km: float) -> List[tuple]:
        """Cells within ``radius_km`` of a point."""
        if not self.cells:
            return []
        # Degrees of longitude shrink with latitude; a fixed bucket span would
        # miss cells near the poles and waste work near the equator.
        dlat = radius_km / 111.0
        cos_lat = max(math.cos(math.radians(lat)), 1e-6)
        dlon = radius_km / (111.0 * cos_lat)
        blat0, blon0 = self._bucket(lat - dlat, lon - dlon)
        blat1, blon1 = self._bucket(lat + dlat, lon + dlon)

        out: List[tuple] = []
        for bl in range(blat0, blat1 + 1):
            for bo in range(blon0, blon1 + 1):
                for c in self._index.get((bl, bo), ()):
                    if haversine_km(lat, lon, c[0], c[1]) <= radius_km:
                        out.append(c)
        return out

    def population_within(self, lat: float, lon: float,
                          radius_km: float) -> float:
        """Total people within a radius — the size of the prize, before any
        competition is considered."""
        return sum(c[2] for c in self.near(lat, lon, radius_km))


def weights_for_points(grid: Optional[PopulationGrid],
                       site_lat: float, site_lon: float,
                       points: Sequence[tuple],
                       catchment_km: float) -> List[float]:
    """People represented by each demand sample point (pure given the grid).

    Each catchment cell is assigned to the nearest sample point, so the
    partition is exact: every person is counted once, and the weights sum to
    the catchment population.

    With no grid, every point weighs 1.0 — which reproduces the unweighted
    mean the scorer used before, so population is a strict extension and its
    absence changes no existing number.
    """
    pts = list(points or [])
    if not pts:
        return []
    if not grid:
        return [1.0] * len(pts)

    weights = [0.0] * len(pts)
    for clat, clon, people in grid.near(site_lat, site_lon, catchment_km):
        best, best_d = 0, float("inf")
        for i, (plat, plon) in enumerate(pts):
            d = haversine_km(clat, clon, plat, plon)
            if d < best_d:
                best, best_d = i, d
        weights[best] += people
    return weights


# ── loading, the geo_sources way ────────────────────────────────────────────
def cache_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", CACHE_FILE)


def load_population(root: Optional[str] = None) -> Dict[str, Any]:
    """This install's population grid.

    An absent file is NOT an error — it is a client who has not loaded one, and
    every caller must keep working without it. Site scoring then falls back to
    an unweighted catchment, which measures how contested an area is rather
    than how populated, and says so.
    """
    path = cache_path(root)
    if not os.path.exists(path):
        return {"grid": PopulationGrid(), "rows": 0, "people": 0.0,
                "source": None, "attribution": None,
                "error": "No population data on this install yet. Site scores "
                         "measure how contested a catchment is, not how many "
                         "people live in it."}
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            raw = list(csv.DictReader(f))
    except OSError as e:
        return {"grid": PopulationGrid(), "rows": 0, "people": 0.0,
                "source": None, "attribution": None, "error": str(e)[:200]}

    cells, sources = parse_rows(raw)
    if not cells:
        return {"grid": PopulationGrid(), "rows": 0, "people": 0.0,
                "source": None, "attribution": None,
                "error": (f"{len(raw):,} rows read but none carried a usable "
                          "latitude, longitude and population.")}

    attribution = attribution_for(sources)
    source = sources[0] if sources else "client-supplied"
    grid = PopulationGrid(cells, attribution=attribution, source=source)
    return {"grid": grid, "rows": len(cells), "people": grid.total_people,
            "source": source, "attribution": attribution, "error": None}


def summarise(loaded: Dict[str, Any]) -> str:
    """One ASCII line for a console or a status panel."""
    if loaded.get("error"):
        return f"population: none loaded - {loaded['error']}"
    return (f"population: {loaded['rows']:,} cells, "
            f"{loaded['people']:,.0f} people, source {loaded['source']}")
