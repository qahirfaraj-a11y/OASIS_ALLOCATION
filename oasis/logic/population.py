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
#: Length of one degree of latitude. Longitude shrinks by cos(lat).
KM_PER_DEGREE = 111.32

#: WorldPop publishes UN-adjusted 1 km density per country as an ASCII XYZ zip.
#: One file per country per year, so a client fetches only their own country.
WORLDPOP_URL = ("https://data.worldpop.org/GIS/Population_Density/"
                "Global_2000_2020_1km_UNadj/{year}/{iso3}/"
                "{iso3_lower}_pd_{year}_1km_UNadj_ASCII_XYZ.zip")
WORLDPOP_ATTRIBUTION = ATTRIBUTIONS["worldpop"]

#: Overpass and WorldPop both reject the default urllib/requests agent with
#: HTTP 406, which is silent from the caller's side and looks like "no data in
#: your region". Identify the client properly.
USER_AGENT = "OASIS-Retail-Intelligence/1.0 (site selection; open data client)"


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


# ── fetching a real grid ────────────────────────────────────────────────────
def cell_area_km2(lat: float, step_deg: float) -> float:
    """Ground area of one grid cell at this latitude (pure).

    WorldPop's "1km" grid is really 30 arc-seconds, which is 0.9277 km at the
    equator and narrows with the cosine of latitude going north or south.
    """
    ns = step_deg * KM_PER_DEGREE
    ew = step_deg * KM_PER_DEGREE * math.cos(math.radians(lat))
    return abs(ns * ew)


def density_to_count(density: float, lat: float, step_deg: float) -> float:
    """Persons per square kilometre -> persons in this cell (pure).

    THE CORRECTION THAT MATTERS. WorldPop's Z column is a DENSITY. Reading it
    as a headcount overstates a country by the reciprocal of the cell area:
    summed raw, Kenya comes to 63.1M against a UN estimate of 53.8M (+17%).
    Multiplied by the true cell area it comes to 54.2M, within 0.9%. Everything
    downstream of this module deals in people, never in density, so the
    conversion happens once here and the stored grid is unambiguous.
    """
    return max(0.0, float(density)) * cell_area_km2(lat, step_deg)


def infer_step_deg(rows: Sequence[Dict[str, Any]]) -> float:
    """Grid spacing in degrees, read off the data rather than assumed."""
    lons = []
    for r in rows[:64]:
        v = _pick(r, _LON_KEYS)
        try:
            lons.append(float(v))
        except (TypeError, ValueError):
            continue
    gaps = sorted({round(abs(b - a), 10) for a, b in zip(lons, lons[1:])
                   if abs(b - a) > 1e-9})
    return gaps[0] if gaps else 1.0 / 120.0      # 30 arc-seconds


def fetch_worldpop(iso3: str, bbox: Tuple[float, float, float, float],
                   year: int = 2020, root: Optional[str] = None,
                   timeout: int = 300,
                   write: bool = True) -> Dict[str, Any]:
    """Fetch one country's WorldPop density grid, clip it, and cache it here.

    ``bbox`` is ``(south, west, north, east)`` — the same order Overpass uses in
    ``geo_sources``, so an operator supplies one region for both fetches.

    Nothing is bundled with OASIS. This writes the CLIENT's own copy of public
    CC BY 4.0 data on the CLIENT's machine, for the CLIENT's region: the rule
    ``geo_sources`` keeps for competitors, kept here for the same reason.

    The clip matters practically as well as legally — Kenya is 680,000 cells
    and a metropolitan catchment needs a few thousand.
    """
    try:
        import requests
    except Exception as e:
        return {"rows": 0, "written": 0, "error": f"requests unavailable: {e}"}

    iso = str(iso3 or "").strip().upper()
    url = WORLDPOP_URL.format(year=int(year), iso3=iso, iso3_lower=iso.lower())
    south, west, north, east = (float(b) for b in bbox)

    try:
        resp = requests.get(url, headers={"User-Agent": USER_AGENT},
                            timeout=timeout)
        resp.raise_for_status()
    except Exception as e:
        return {"rows": 0, "written": 0, "url": url,
                "error": f"could not fetch {iso} {year}: {str(e)[:160]}"}

    import io as _io
    import zipfile
    try:
        zf = zipfile.ZipFile(_io.BytesIO(resp.content))
        member = next(n for n in zf.namelist() if n.lower().endswith(".csv"))
        with zf.open(member) as fh:
            text = _io.TextIOWrapper(fh, encoding="utf-8", errors="replace")
            raw = list(csv.DictReader(text))
    except Exception as e:
        return {"rows": 0, "written": 0, "url": url,
                "error": f"could not read the archive: {str(e)[:160]}"}

    step = infer_step_deg(raw)
    cells: List[tuple] = []
    for r in raw:
        lat, lon, dens = (_pick(r, _LAT_KEYS), _pick(r, _LON_KEYS),
                          _pick(r, _POP_KEYS))
        try:
            flat, flon, fdens = float(lat), float(lon), float(dens)
        except (TypeError, ValueError):
            continue
        if not (south <= flat <= north and west <= flon <= east):
            continue
        people = density_to_count(fdens, flat, step)
        if people > 0:
            cells.append((flat, flon, people))

    result = {"rows": len(cells), "written": 0, "step_deg": step,
              "people": sum(c[2] for c in cells), "url": url,
              "source": f"WorldPop {year} 1km UN-adjusted",
              "attribution": WORLDPOP_ATTRIBUTION, "error": None}
    if not cells:
        result["error"] = ("no cells inside that bounding box — check the "
                           "order is (south, west, north, east)")
        return result
    if not write:
        return result

    path = cache_path(root)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["latitude", "longitude",
                                              "population", "source"])
            w.writeheader()
            for lat, lon, people in cells:
                w.writerow({"latitude": f"{lat:.6f}", "longitude": f"{lon:.6f}",
                            "population": f"{people:.2f}",
                            "source": result["source"]})
    except OSError as e:
        result["error"] = str(e)[:200]
        return result

    result["written"] = len(cells)
    return result


def summarise(loaded: Dict[str, Any]) -> str:
    """One ASCII line for a console or a status panel."""
    if loaded.get("error"):
        return f"population: none loaded - {loaded['error']}"
    return (f"population: {loaded['rows']:,} cells, "
            f"{loaded['people']:,.0f} people, source {loaded['source']}")
