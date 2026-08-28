"""
Catchment affluence — why the same person is worth more in one suburb.

WHY THIS EXISTS
    With population loaded, site capital predicts revenue as
    ``captured_population x spend_per_person``. Measured on a real estate that
    failed, because spend per person is not a constant: across five Nairobi
    branches it ran from 24 to 99 shillings a head, a 4x range, and the
    highest was the SPARSEST catchment. Affluent, low-density suburbs spend
    several times more per person than dense low-income ones. A single median
    cannot carry that, so the population basis lost its own validation gate.

    Affluence is the term that closes it: spend per person becomes a function
    of the catchment rather than one number for the chain.

WHAT THIS MEASURES, AND WHAT IT DOES NOT
    Not income. There is no commercially usable, spatially granular income
    surface for Kenya: Meta's Relative Wealth Index is the obvious candidate
    and is **CC BY-NC 4.0**, which forbids use in a product that is sold; the
    CC0 alternatives (MPI, KIHBS poverty rates) are county-level, and Nairobi
    is a single county, so they carry no within-city variation at all.

    So this uses a PROXY: the density of discretionary amenities per person,
    from OpenStreetMap. Banks, cafes, restaurants, malls and department stores
    appear where people have money to spend beyond necessities; kiosks,
    convenience shops and butchers appear wherever people simply live.

    PER PERSON is the whole trick. Raw counts measure how urban a place is,
    not how rich — Eastlands has far more shops than Karen and far less money
    per head. Dividing by catchment population inverts that correctly.

THE CONFOUND, STATED UP FRONT
    OpenStreetMap is not uniformly mapped. Wealthier and more commercial areas
    tend to be mapped more thoroughly, so some of this signal is mapping effort
    rather than money. That bias runs in the SAME direction as the effect we
    want, which makes it dangerous: it can look like it works while measuring
    something else.

    The defence is the one used everywhere else here. This index does not get
    to set a budget because it is plausible. It enters ``site_capital`` as one
    more predictor in the same leave-one-out gate, and must beat floor area and
    the estate median on the client's own trading before a shilling moves.

WHAT HAPPENED WHEN IT WAS MEASURED (2026-08-28, five real Nairobi catchments)
    Two findings, both negative, both worth keeping.

    1. THE SIGN CAME OUT BACKWARDS. Fitted against observed spend per person,
       the coefficient was NEGATIVE: b = -0.66. More discretionary amenities
       per head went with LESS spend captured per head. The catchment thinnest
       in amenities (0.59 per thousand) had the HIGHEST spend at 99; the
       thickest (2.19 per thousand) had the lowest at 24.

       That is not a broken proxy, it is a mislabelled one. The high-index
       catchments sit in a commercial core, dense with banks and restaurants
       serving workers and visitors rather than residents, so a resident there
       has many places to spend. The low-index one is residential, so a single
       supermarket captures far more of each resident's grocery budget. This
       index measures RETAIL COMPETITION INTENSITY, not wealth. Read as
       affluence it ranks catchments exactly wrong.

    2. IT FAILED ITS OWN GATE, AND THE IN-SAMPLE FIT HID THAT. R2 was 0.61,
       which reads as respectable. Under leave-one-out the same model came
       LAST of five predictors at 46.8% median error, against 34.9% for a flat
       spend per person and 24.9% for floor area alone. Two parameters fitted
       on four points memorise; they do not generalise. MIN_STORES_FOR_SLOPE
       was raised from 3 to 6 as a direct result.

    So this layer is BUILT, WIRED AND GATED OFF. It is kept because the
    plumbing is correct and because a chain with enough stores may yet clear
    the bar: the finding is "five stores cannot support a two-parameter spend
    model", not "the catchment does not matter". What it must never do is set
    a budget on the strength of an in-sample R2.

    The honest conclusion for a product that is sold: there is no commercially
    usable, spatially granular affluence surface for this market today. Until
    a client brings one, spend per person stays a single measured number.
"""

from __future__ import annotations

import csv
import math
import os
from typing import Any, Dict, List, Optional, Sequence, Tuple

from .population import USER_AGENT, PopulationGrid, haversine_km

#: The client's own extract, for the client's region. Never shipped.
CACHE_FILE = "amenity_poi.csv"

#: OSM is published under the Open Database License. A score computed FROM the
#: data is a Produced Work and may be licensed freely; it still needs
#: attribution. See oasis.logic.geo_sources for the full reasoning.
OSM_ATTRIBUTION = "Amenity locations © OpenStreetMap contributors (ODbL)"

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

#: Appear where there is money to spend beyond necessities.
DISCRETIONARY = ("bank", "cafe", "restaurant", "mall", "department_store",
                 "bar", "pub", "cinema", "gym", "fitness_centre",
                 "beauty", "jewelry", "optician", "car_dealer", "hotel")

#: Appear wherever people live, at roughly population rate. Counted only to
#: describe the mix — they carry no affluence signal on their own.
STAPLE = ("convenience", "butcher", "kiosk", "greengrocer", "marketplace",
          "pharmacy", "fuel", "hairdresser", "supermarket")

#: Overpass selectors. Kept as data so the taxonomy above is the only thing
#: anyone has to argue with.
_QUERY_KEYS = (("amenity", DISCRETIONARY + STAPLE),
               ("shop", DISCRETIONARY + STAPLE),
               ("leisure", ("fitness_centre",)),
               ("tourism", ("hotel",)))

#: A catchment thinner than this cannot support a rate per thousand — the
#: denominator is too small and the index explodes.
MIN_PEOPLE_FOR_INDEX = 500.0

#: Fitting a slope needs at least this many stores with DISTINCT affluence.
#: Below it, spend falls back to the estate median.
#:
#: WHY SIX AND NOT THREE. This began at 3, which permits a two-parameter fit on
#: three points — one degree of freedom. Measured on a five-store estate the
#: consequence was stark: the fit reported an in-sample R2 of 0.61, and under
#: leave-one-out the same model predicted revenue at 46.8% median error, the
#: WORST of the five predictors and nearly twice the error of floor area alone
#: (24.9%). The R2 was memorisation. Six keeps at least three degrees of
#: freedom in every leave-one-out fold, which is the least that can distinguish
#: a slope from noise.
MIN_STORES_FOR_SLOPE = 6

_BUCKET_DEG = 0.1


def classify(tags: Dict[str, Any]) -> Optional[str]:
    """``discretionary`` | ``staple`` | None for one OSM element (pure)."""
    values = {str(tags.get(k) or "").strip().lower()
              for k in ("amenity", "shop", "leisure", "tourism")}
    values.discard("")
    if values & set(DISCRETIONARY):
        return "discretionary"
    if values & set(STAPLE):
        return "staple"
    return None


class AffluenceGrid:
    """Classified POIs, indexed for repeated catchment lookups.

    A POI is ``(lat, lon, kind)`` where kind is ``discretionary`` or
    ``staple``.
    """

    def __init__(self, pois: Sequence[tuple] = (), attribution: str = "",
                 source: str = ""):
        self.pois: List[tuple] = [tuple(p) for p in pois or []]
        self.attribution = attribution or OSM_ATTRIBUTION
        self.source = source or "OpenStreetMap"
        self._index: Dict[Tuple[int, int], List[tuple]] = {}
        for p in self.pois:
            self._index.setdefault(self._bucket(p[0], p[1]), []).append(p)

    def __bool__(self) -> bool:
        return bool(self.pois)

    def __len__(self) -> int:
        return len(self.pois)

    @staticmethod
    def _bucket(lat: float, lon: float) -> Tuple[int, int]:
        return (int(math.floor(lat / _BUCKET_DEG)),
                int(math.floor(lon / _BUCKET_DEG)))

    def near(self, lat: float, lon: float, radius_km: float) -> List[tuple]:
        if not self.pois:
            return []
        dlat = radius_km / 111.0
        cos_lat = max(math.cos(math.radians(lat)), 1e-6)
        dlon = radius_km / (111.0 * cos_lat)
        b0 = self._bucket(lat - dlat, lon - dlon)
        b1 = self._bucket(lat + dlat, lon + dlon)
        out = []
        for bl in range(b0[0], b1[0] + 1):
            for bo in range(b0[1], b1[1] + 1):
                for p in self._index.get((bl, bo), ()):
                    if haversine_km(lat, lon, p[0], p[1]) <= radius_km:
                        out.append(p)
        return out

    def index_at(self, lat: float, lon: float,
                 population: Optional[PopulationGrid] = None,
                 radius_km: float = 5.0) -> Dict[str, Any]:
        """The affluence reading for one catchment.

        ``index`` is discretionary amenities per thousand people. It is
        ``None`` — never zero — when it cannot be computed, so nothing
        downstream can read "unknown" as "poor".
        """
        found = self.near(lat, lon, radius_km)
        disc = sum(1 for p in found if p[2] == "discretionary")
        staple = sum(1 for p in found if p[2] == "staple")
        people = (population.population_within(lat, lon, radius_km)
                  if population else 0.0)

        index = None
        if people >= MIN_PEOPLE_FOR_INDEX:
            index = disc / (people / 1000.0)

        return {
            "discretionary": disc,
            "staple": staple,
            "people": round(people, 1),
            # Per THOUSAND people: raw counts measure how urban a place is,
            # not how rich, and would rank a dense low-income suburb above an
            # affluent sparse one.
            "index": index,
            # Secondary, population-free view of the same idea: what fraction
            # of the local trade is discretionary rather than staple.
            "composition": (disc / (disc + staple)
                            if (disc + staple) > 0 else None),
            "radius_km": radius_km,
        }


# ── the spend model ─────────────────────────────────────────────────────────
def fit_spend_model(pairs: Sequence[Tuple[float, float]]) -> Optional[Dict[str, float]]:
    """Least-squares fit of ``log(spend) = a + b * affluence`` (pure).

    Log-linear because spend is positive and responds multiplicatively: a
    catchment twice as affluent does not spend two shillings more per head, it
    spends some multiple more. Returns ``None`` when there is not enough
    distinct affluence to support a slope — a fit through one x value is a
    horizontal line pretending to be a model.
    """
    pts = [(float(a), float(s)) for a, s in pairs
           if a is not None and s is not None and s > 0]
    if len(pts) < MIN_STORES_FOR_SLOPE:
        return None
    if len({round(a, 6) for a, _ in pts}) < 2:
        return None

    n = float(len(pts))
    xs = [a for a, _ in pts]
    ys = [math.log(s) for _, s in pts]
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx <= 1e-12:
        return None
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    a = my - b * mx

    ss_tot = sum((y - my) ** 2 for y in ys)
    ss_res = sum((y - (a + b * x)) ** 2 for x, y in zip(xs, ys))
    r2 = 1.0 - (ss_res / ss_tot) if ss_tot > 1e-12 else 0.0
    return {"a": a, "b": b, "r2": r2, "n": len(pts)}


def predict_spend(model: Optional[Dict[str, float]], affluence: Optional[float],
                  fallback: float = 0.0) -> float:
    """Spend per person implied by a catchment's affluence (pure)."""
    if not model or affluence is None:
        return max(0.0, float(fallback))
    try:
        return math.exp(model["a"] + model["b"] * float(affluence))
    except (OverflowError, ValueError):
        return max(0.0, float(fallback))


# ── fetching ────────────────────────────────────────────────────────────────
def cache_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", CACHE_FILE)


def _overpass_query(bbox: Tuple[float, float, float, float]) -> str:
    box = ",".join(f"{float(b)}" for b in bbox)
    parts = []
    for key, values in _QUERY_KEYS:
        pattern = "|".join(sorted(set(values)))
        parts.append(f'node["{key}"~"^({pattern})$"]({box});')
    return "[out:json][timeout:120];(" + "".join(parts) + ");out center;"


def fetch_amenities(bbox: Tuple[float, float, float, float],
                    root: Optional[str] = None, timeout: int = 180,
                    write: bool = True) -> Dict[str, Any]:
    """Fetch a region's amenity POIs from Overpass and cache them locally.

    ``bbox`` is ``(south, west, north, east)`` — the order Overpass and the
    population fetcher both use, so one region serves every layer.
    """
    try:
        import requests
    except Exception as e:
        return {"rows": 0, "written": 0, "error": f"requests unavailable: {e}"}

    try:
        resp = requests.get(OVERPASS_URL,
                            params={"data": _overpass_query(bbox)},
                            headers={"User-Agent": USER_AGENT}, timeout=timeout)
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
    except Exception as e:
        return {"rows": 0, "written": 0, "error": str(e)[:200]}

    pois: List[tuple] = []
    for el in elements:
        lat = el.get("lat", (el.get("center") or {}).get("lat"))
        lon = el.get("lon", (el.get("center") or {}).get("lon"))
        kind = classify(el.get("tags") or {})
        if lat is None or lon is None or kind is None:
            continue
        pois.append((float(lat), float(lon), kind))

    result = {"rows": len(pois), "written": 0,
              "discretionary": sum(1 for p in pois if p[2] == "discretionary"),
              "staple": sum(1 for p in pois if p[2] == "staple"),
              "source": "OpenStreetMap (Overpass)",
              "attribution": OSM_ATTRIBUTION, "error": None}
    if not pois:
        result["error"] = "no amenities found in that bounding box"
        return result
    if not write:
        return result

    path = cache_path(root)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["latitude", "longitude", "kind"])
            w.writeheader()
            for lat, lon, kind in pois:
                w.writerow({"latitude": f"{lat:.6f}",
                            "longitude": f"{lon:.6f}", "kind": kind})
    except OSError as e:
        result["error"] = str(e)[:200]
        return result

    result["written"] = len(pois)
    return result


def load_affluence(root: Optional[str] = None) -> Dict[str, Any]:
    """This install's amenity extract. Absent is not an error."""
    path = cache_path(root)
    if not os.path.exists(path):
        return {"grid": AffluenceGrid(), "rows": 0, "source": None,
                "attribution": None,
                "error": "No amenity data on this install yet. Spend per "
                         "person is one number for the whole chain until a "
                         "region is fetched."}
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as f:
            rows = list(csv.DictReader(f))
    except OSError as e:
        return {"grid": AffluenceGrid(), "rows": 0, "source": None,
                "attribution": None, "error": str(e)[:200]}

    pois = []
    for r in rows:
        low = {str(k).strip().lower(): v for k, v in r.items()}
        try:
            lat = float(low.get("latitude") or low.get("lat"))
            lon = float(low.get("longitude") or low.get("lon"))
        except (TypeError, ValueError):
            continue
        kind = str(low.get("kind") or "").strip().lower()
        if kind in ("discretionary", "staple"):
            pois.append((lat, lon, kind))

    if not pois:
        return {"grid": AffluenceGrid(), "rows": 0, "source": None,
                "attribution": None,
                "error": f"{len(rows):,} rows read but none were usable."}

    grid = AffluenceGrid(pois, source="OpenStreetMap (Overpass)")
    return {"grid": grid, "rows": len(pois),
            "discretionary": sum(1 for p in pois if p[2] == "discretionary"),
            "staple": sum(1 for p in pois if p[2] == "staple"),
            "source": grid.source, "attribution": grid.attribution,
            "error": None}


def summarise(loaded: Dict[str, Any]) -> str:
    """One ASCII line for a console."""
    if loaded.get("error"):
        return f"affluence: none loaded - {loaded['error']}"
    return (f"affluence: {loaded['rows']:,} POIs "
            f"({loaded.get('discretionary', 0):,} discretionary / "
            f"{loaded.get('staple', 0):,} staple), source {loaded['source']}")
