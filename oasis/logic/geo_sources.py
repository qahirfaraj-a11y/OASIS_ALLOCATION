"""
Third-party geographic data: what OASIS may use, and on what terms.

The competitor set behind site selection comes from OpenStreetMap via the
Overpass API. OSM data is published under the **Open Database License 1.0
(ODbL)**, which is permissive but not unconditional, and the distinction that
matters to a commercial product is between:

  * a **Derivative Database** — an extract, a filtered copy, our
    ``competitor_network.csv``. Redistributing one obliges us to license *that
    database* under ODbL and to attribute.
  * a **Produced Work** — a map, a score, a ranked site list computed FROM the
    data. This may be licensed however we like, and needs only attribution.

So OASIS does not ship the extract. A client fetches their own region at run
time (``fetch_competitors``), which avoids redistributing an OSM database
altogether AND is the better product answer: a retailer in Mombasa should be
scored against Mombasa's competitors, not Nairobi's.

What OASIS *does* ship is the scoring, and any surface that displays a result
derived from OSM must carry ``OSM_ATTRIBUTION``.

Reviewed 2026-08-08. This is an engineering summary of a licence, not legal
advice; confirm before a commercial release.
"""

from __future__ import annotations

import csv
import json
import math
import os
import re
from typing import Any, Dict, List, Optional

from .population import KM_PER_DEG_LAT, KM_PER_DEG_LON

#: Required wherever OSM-derived output is shown. ODbL §4.3.
OSM_ATTRIBUTION = "Competitor locations © OpenStreetMap contributors (ODbL)"

#: Public Overpass endpoint. Rate-limited and best-effort; a client install
#: fetches once and caches, it does not query per page view.
OVERPASS_URL = "https://overpass-api.de/api/interpreter"

#: Overpass rejects the default ``python-requests/x.y`` agent with HTTP 406,
#: which surfaces to the operator as "no competitors in your region" rather
#: than as a failure. Measured against the live endpoint 2026-08-28: identical
#: query, 406 without this header and 200 with it. Identify the client.
USER_AGENT = "OASIS-Retail-Intelligence/1.0 (site selection; open data client)"

#: Where a fetched extract is cached, per install. Never shipped, never
#: redistributed — it is the client's own copy of public data.
CACHE_FILE = "competitor_network.csv"


#: Per-chain floor areas, in square feet. Huff attractiveness is PROPORTIONAL
#: to floor area, so without this every rival pulls identically — a kiosk
#: competes exactly as hard as a hypermarket. OSM carries no floor areas, so
#: the operator supplies what they know.
SIZES_FILE = "competitor_sizes.json"

#: Used only where a chain has no entry. Deliberately a mid-size supermarket:
#: it is wrong for both ends of the range and should be replaced by anyone who
#: cares about the ranking.
DEFAULT_COMPETITOR_SQFT = 15_000.0


#: Spellings that mean the same chain. OSM records what a mapper typed, so one
#: retailer appears as "Quickmart", "Quick Mart" and "QuickMart" — three
#: chains as far as a floor-area table or a store count is concerned.
_CHAIN_ALIASES = {
    "quick mart": "Quickmart",
    "quickmart": "Quickmart",
    "clean shelf": "Cleanshelf",
    "cleanshelf": "Cleanshelf",
    "food plus": "Foodplus",
    "foodplus": "Foodplus",
}


def match_chain(store_name: str, brands: List[str]) -> Optional[str]:
    """Which of ``brands`` this OSM name belongs to, canonically (pure).

    Matched on a normalised substring so "Naivas Supermarket Ngong Road" and
    "NAIVAS - Ruaka" both land on the same chain.
    """
    text = " ".join(str(store_name or "").lower().split())
    if not text:
        return None
    best = None
    for brand in brands or []:
        key = " ".join(str(brand).lower().split())
        if not key or key not in text:
            continue
        # Prefer the longest brand that matches, so "Quick Mart" is not
        # shadowed by a shorter entry that happens to be a substring.
        if best is None or len(key) > len(best[0]):
            best = (key, str(brand).strip())
    if best is None:
        return None
    return _CHAIN_ALIASES.get(best[0], best[1])


def cache_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", CACHE_FILE)


#: The client's own chain, excluded from THEIR competitor set. Per install.
OWN_CHAIN_FILE = "own_chain.json"


def own_chain_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", OWN_CHAIN_FILE)


def load_own_chain(root: Optional[str] = None) -> List[str]:
    """Name fragments identifying the operator's own banner(s)."""
    path = own_chain_path(root)
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except (OSError, ValueError):
        return []
    names = data.get("names") if isinstance(data, dict) else data
    return [str(n).strip() for n in (names or []) if str(n).strip()]


def save_own_chain(names: List[str],
                   root: Optional[str] = None) -> Dict[str, object]:
    """Record which banner belongs to the operator.

    WHY THIS EXISTS. A market matrix should hold every chain in the region,
    including the operator's — that is what makes it reusable and what lets a
    second client score against the first. But a retailer must never appear in
    their OWN competitor set: their branches already enter the Huff denominator
    as "own stores" from the POS, so counting them again has every store
    competing with itself.

    Measured on the reference estate, that double-count was not cosmetic: it
    held the capture model at 40.8% median error against floor area's 24.9%.
    Excluding the own banner took it to 23.7% and the geography cleared its
    validation gate for the first time.
    """
    clean = [str(n).strip() for n in (names or []) if str(n).strip()]
    path = own_chain_path(root)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"names": clean}, f, indent=2)
    except OSError as e:
        return {"saved": False, "error": str(e)[:200]}
    return {"saved": True, "names": clean, "error": None}


def drop_own_chain(rows: List[dict], own: List[str]) -> List[dict]:
    """Remove the operator's own banner from a competitor set (pure)."""
    keys = [" ".join(str(n).lower().split()) for n in (own or []) if n]
    if not keys:
        return list(rows or [])
    out = []
    for r in rows or []:
        hay = " ".join(f"{r.get('Chain') or r.get('chain') or ''} "
                       f"{r.get('Store_Name') or ''}".lower().split())
        if any(k in hay for k in keys):
            continue
        out.append(r)
    return out


def legacy_cache_path(root: Optional[str] = None) -> str:
    """Where the pre-``oasis/data`` console kept the same extract.

    The Streamlit expansion engine read ``competitor_network.csv`` from the
    working directory. An install carried over from that console has the file
    there, and reading only the canonical path made it look as though the
    client had no competitors at all — which scores every site as uncontested.
    """
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, CACHE_FILE)


def sizes_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", SIZES_FILE)


def load_sizes(root: Optional[str] = None) -> Dict[str, float]:
    """``{chain_lowercase: sqft}`` — empty when the operator has set none."""
    path = sizes_path(root)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except (OSError, ValueError):
        return {}
    out: Dict[str, float] = {}
    for chain, sqft in (data.get("chains") or data).items():
        try:
            v = float(sqft)
        except (TypeError, ValueError):
            continue
        if v > 0:
            out[str(chain).strip().lower()] = v
    return out


def save_sizes(sizes: Dict[str, float],
               root: Optional[str] = None) -> Dict[str, object]:
    """Record per-chain floor areas. Validates before writing."""
    clean: Dict[str, float] = {}
    for chain, sqft in (sizes or {}).items():
        name = str(chain).strip()
        if not name:
            continue
        try:
            v = float(sqft)
        except (TypeError, ValueError):
            return {"saved": False,
                    "error": f"{name}: floor area must be a number."}
        if v <= 0:
            return {"saved": False,
                    "error": f"{name}: floor area must be greater than zero."}
        clean[name.lower()] = v

    path = sizes_path(root)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"chains": clean}, f, indent=2)
    except OSError as e:
        return {"saved": False, "error": str(e)[:200]}
    return {"saved": True, "chains": len(clean), "error": None}


#: Per-chain profile: everything the Huff term needs about a rival that is not
#: its coordinates. Richer than the flat sizes file, which it supersedes and
#: still reads for back-compatibility.
CHAINS_FILE = "competitor_chains.json"

#: Square feet per square metre.
_SQFT_PER_SQM = 10.7639

#: A polygon smaller than this is a kiosk outline or a mapping error, not a
#: supermarket footprint.
MIN_FOOTPRINT_SQM = 20.0


def chains_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", CHAINS_FILE)


def _polygon_area_sqm(geometry: List[dict]) -> float:
    """Ground area of an OSM way, by shoelace on a local projection (pure).

    Equirectangular about the ring's own latitude: at the scale of one building
    the distortion is far below the precision of the outline itself.
    """
    pts = [p for p in (geometry or [])
           if p.get("lat") is not None and p.get("lon") is not None]
    if len(pts) < 3:
        return 0.0
    k = math.cos(math.radians(float(pts[0]["lat"])))
    xy = [(float(p["lon"]) * KM_PER_DEG_LON * 1000.0 * k,
           float(p["lat"]) * KM_PER_DEG_LAT * 1000.0)
          for p in pts]
    total = 0.0
    for i in range(len(xy)):
        x1, y1 = xy[i]
        x2, y2 = xy[(i + 1) % len(xy)]
        total += x1 * y2 - x2 * y1
    return abs(total) / 2.0


def footprints_from_elements(elements: List[dict],
                             brands: List[str]) -> Dict[str, List[float]]:
    """Measured footprints in square feet, per chain (pure).

    Only ways and relations carry an outline; a store mapped as a single point
    contributes nothing, which is why coverage is partial and reported.
    """
    out: Dict[str, List[float]] = {}
    for el in elements or []:
        name = (el.get("tags") or {}).get("name")
        chain = match_chain(name, brands)
        if not chain:
            continue
        sqm = _polygon_area_sqm(el.get("geometry") or [])
        if sqm < MIN_FOOTPRINT_SQM:
            continue
        out.setdefault(chain, []).append(sqm * _SQFT_PER_SQM)
    return out


def _median(xs: List[float]) -> float:
    v = sorted(xs)
    n = len(v)
    if not n:
        return 0.0
    m = n // 2
    return v[m] if n % 2 else (v[m - 1] + v[m]) / 2.0


def measure_footprints(brands: List[str], bbox: str,
                       root: Optional[str] = None,
                       timeout: int = 180) -> Dict[str, object]:
    """Measure real store footprints from OSM building outlines.

    A floor area entered by guesswork is the weakest term in the Huff model,
    because pull is PROPORTIONAL to it. Where a branch is mapped as a polygon
    rather than a point, its ground area is a real measurement and costs
    nothing to take.

    Coverage is partial and stays partial: on the reference region 14 of 111
    branches carried an outline. So this reports ``n_measured`` per chain and
    the caller must decide whether one or two outlines is enough to speak for a
    banner — it is a starting point for the operator, not a substitute for
    what they know.
    """
    try:
        import requests
    except Exception as e:
        return {"chains": {}, "error": f"requests unavailable: {e}"}

    names = [str(b).strip() for b in (brands or []) if str(b).strip()]
    if not names:
        return {"chains": {}, "error": "no chain names to search for"}

    pattern = "|".join(re.escape(n) for n in names)
    selectors = "".join(f'{el}["shop"]["name"~"{pattern}",i]({bbox});'
                        for el in ("way", "relation"))
    query = f"[out:json][timeout:{timeout}];({selectors});out geom;"
    try:
        resp = requests.get(OVERPASS_URL, params={"data": query},
                            headers={"User-Agent": USER_AGENT}, timeout=timeout)
        if resp.status_code == 429:
            return {"chains": {}, "error": "Overpass is rate-limiting this "
                                           "address. Wait a minute."}
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
    except Exception as e:
        return {"chains": {}, "error": str(e)[:200]}

    measured = footprints_from_elements(elements, names)
    chains = {
        chain: {"size_sqft": round(_median(v), 0), "n_measured": len(v),
                "min_sqft": round(min(v), 0), "max_sqft": round(max(v), 0),
                "source": "osm-footprint"}
        for chain, v in measured.items()
    }
    return {"chains": chains, "polygons_seen": len(elements), "error": None}


def load_chain_profiles(root: Optional[str] = None) -> Dict[str, dict]:
    """``{chain_lowercase: {size_sqft, pull, source, n_measured}}``.

    The flat sizes file is folded in and WINS, because it is what the operator
    typed and they know their market better than a building outline does.
    """
    profiles: Dict[str, dict] = {}
    path = chains_path(root)
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            for chain, rec in (data.get("chains") or {}).items():
                if not isinstance(rec, dict):
                    continue
                try:
                    size = float(rec.get("size_sqft") or 0)
                except (TypeError, ValueError):
                    continue
                if size <= 0:
                    continue
                try:
                    pull = float(rec.get("pull", 1.0) or 1.0)
                except (TypeError, ValueError):
                    pull = 1.0
                profiles[str(chain).strip().lower()] = {
                    "size_sqft": size, "pull": max(0.0, pull),
                    "source": str(rec.get("source") or "unknown"),
                    "n_measured": int(rec.get("n_measured") or 0)}
        except (OSError, ValueError):
            pass

    for chain, size in load_sizes(root).items():
        profiles[chain] = {"size_sqft": size, "pull": 1.0,
                           "source": "operator", "n_measured": 0}
    return profiles


def save_chain_profiles(profiles: Dict[str, dict],
                        root: Optional[str] = None) -> Dict[str, object]:
    """Record per-chain size, pull and provenance."""
    clean: Dict[str, dict] = {}
    for chain, rec in (profiles or {}).items():
        name = str(chain).strip()
        if not name or not isinstance(rec, dict):
            continue
        try:
            size = float(rec.get("size_sqft") or 0)
        except (TypeError, ValueError):
            return {"saved": False, "error": f"{name}: size must be a number."}
        if size <= 0:
            return {"saved": False,
                    "error": f"{name}: size must be greater than zero."}
        try:
            pull = float(rec.get("pull", 1.0) or 1.0)
        except (TypeError, ValueError):
            return {"saved": False, "error": f"{name}: pull must be a number."}
        clean[name.lower()] = {
            "size_sqft": size, "pull": max(0.0, pull),
            "source": str(rec.get("source") or "operator"),
            "n_measured": int(rec.get("n_measured") or 0)}

    path = chains_path(root)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"chains": clean}, f, indent=2)
    except OSError as e:
        return {"saved": False, "error": str(e)[:200]}
    return {"saved": True, "chains": len(clean), "error": None}


def apply_sizes(rows: List[dict], sizes: Dict[str, float]) -> List[dict]:
    """Attach a floor area to each competitor row (pure).

    Matched on the chain name, case-insensitively, falling back to a substring
    match so "Naivas Supermarket" picks up an entry for "naivas".
    """
    out = []
    for r in rows or []:
        rec = dict(r)
        chain = str(rec.get("Chain") or rec.get("chain") or "").strip().lower()
        profile = _lookup(chain, sizes)
        if profile is None:
            rec["size_sqft"] = DEFAULT_COMPETITOR_SQFT
            rec["size_is_default"] = True
            rec["size_source"] = "default"
            rec["n_measured"] = 0
            rec["pull"] = None            # fall back to the name heuristic
        else:
            rec["size_sqft"] = float(profile["size_sqft"])
            rec["size_is_default"] = False
            rec["size_source"] = profile.get("source", "operator")
            rec["n_measured"] = int(profile.get("n_measured") or 0)
            rec["pull"] = float(profile.get("pull", 1.0))
        out.append(rec)
    return out


def _lookup(chain: str, profiles: Dict[str, Any]) -> Optional[dict]:
    """Profile for a chain, matched exactly then by substring (pure).

    Accepts either the rich profile map or the flat ``{chain: sqft}`` one, so a
    caller that still passes plain sizes keeps working.
    """
    def _norm(v):
        if isinstance(v, dict):
            return v
        try:
            return {"size_sqft": float(v), "pull": 1.0, "source": "operator",
                    "n_measured": 0}
        except (TypeError, ValueError):
            return None

    if not chain or not profiles:
        return None
    if chain in profiles:
        return _norm(profiles[chain])
    # LONGEST WINS, matching match_chain. Taking the first substring hit meant
    # dict order decided: a profile keyed "quick" silently claimed "Quickmart"
    # and handed it that chain's floor area. Two functions doing the same job
    # must not disagree about which match is the right one.
    best = None
    for known, v in profiles.items():
        if known and (known in chain or chain in known):
            if best is None or len(known) > len(best[0]):
                best = (known, v)
    return _norm(best[1]) if best else None


def chains_in(rows: List[dict]) -> List[str]:
    """Distinct chain names present, so the UI can offer them for sizing."""
    seen = []
    for r in rows or []:
        chain = str(r.get("Chain") or r.get("chain") or "").strip()
        if chain and chain not in seen:
            seen.append(chain)
    return sorted(seen)


def load_competitors(root: Optional[str] = None) -> Dict[str, object]:
    """The cached competitor set for this install, with floor areas applied.

    ``{rows, source, attribution, error}``. Absent cache is not an error — it
    is a client who has not fetched yet, and the caller should say so rather
    than pretend there is no competition.
    """
    path = cache_path(root)
    legacy = False
    if not os.path.exists(path):
        alt = legacy_cache_path(root)
        if os.path.exists(alt):
            path, legacy = alt, True
        else:
            return {"rows": [], "source": None,
                    "attribution": OSM_ATTRIBUTION, "chains": [],
                    "sized": 0, "unsized": 0,
                    "error": "No competitor data on this install yet. Fetch it "
                             "for your region first."}
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            rows = [r for r in csv.DictReader(f)]
    except OSError as e:
        return {"rows": [], "source": None, "attribution": OSM_ATTRIBUTION,
                "chains": [], "sized": 0, "unsized": 0, "error": str(e)[:200]}

    # The matrix holds every chain in the region; a client's OWN banner is
    # removed here rather than at fetch time, so one extract serves any client.
    own = load_own_chain(root)
    in_matrix = len(rows)
    rows = drop_own_chain(rows, own)
    profiles = load_chain_profiles(root)
    rows = apply_sizes(rows, profiles)
    return {"rows": rows,
            "in_matrix": in_matrix,
            "own_chain": own,
            "own_excluded": in_matrix - len(rows),
            "source": ("OpenStreetMap (Overpass, legacy console location)"
                       if legacy else "OpenStreetMap (Overpass)"),
            "legacy_path": legacy,
            "chains": chains_in(rows),
            "sized": sum(1 for r in rows if not r["size_is_default"]),
            "unsized": sum(1 for r in rows if r["size_is_default"]),
            "attribution": OSM_ATTRIBUTION, "error": None}


def fetch_competitors(brands: List[str], bbox: str,
                      root: Optional[str] = None,
                      timeout: int = 65) -> Dict[str, object]:
    """Fetch a region's competitor set from Overpass and cache it locally.

    ``bbox`` is Overpass's ``south,west,north,east``. Nothing is bundled with
    OASIS: this writes the CLIENT's own copy on the CLIENT's machine, so no OSM
    database is ever redistributed by us.
    """
    try:
        import requests
    except Exception as e:
        return {"rows": [], "written": 0, "error": f"requests unavailable: {e}"}

    names = [str(b).strip() for b in (brands or []) if str(b).strip()]
    if not names:
        return {"rows": [], "written": 0, "error": "no chain names to search for"}

    # ONE request, not one per brand. Overpass rate-limits the public endpoint
    # hard: six sequential brand queries returned 200, 200, 429, 429, 429, 504
    # — and the old loop reported that as "<chain>: <error>", which reads as
    # "that chain has no stores" rather than "we were throttled".
    #
    # Requiring a shop tag is what makes the result a retail matrix: a bare
    # name match on "Naivas" also returns 93 bus stops, car parks and a petrol
    # station named after the supermarket.
    pattern = "|".join(re.escape(n) for n in names)
    selectors = "".join(
        f'{el}["shop"]["name"~"{pattern}",i]({bbox});'
        for el in ("node", "way", "relation"))
    query = f"[out:json][timeout:{timeout}];({selectors});out center;"

    try:
        resp = requests.get(OVERPASS_URL, params={"data": query},
                            headers={"User-Agent": USER_AGENT},
                            timeout=timeout)
        if resp.status_code == 429:
            return {"rows": [], "written": 0,
                    "error": "Overpass is rate-limiting this address. Wait a "
                             "minute and try again — this is throttling, not "
                             "an empty region."}
        resp.raise_for_status()
        elements = resp.json().get("elements", [])
    except Exception as e:
        return {"rows": [], "written": 0, "error": str(e)[:200]}

    rows: List[dict] = []
    for el in elements:
        centre = el.get("center") or {}
        lat = el.get("lat", centre.get("lat"))
        lon = el.get("lon", centre.get("lon"))
        tags = el.get("tags") or {}
        name = tags.get("name") or ""
        if lat is None or lon is None:
            continue
        chain = match_chain(name, names)
        if not chain:
            continue
        rows.append({"Store_Name": name, "Latitude": lat, "Longitude": lon,
                     "Chain": chain, "Source": "OSM_Overpass"})

    path = cache_path(root)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Store_Name", "Latitude",
                                              "Longitude", "Chain", "Source"])
            w.writeheader()
            w.writerows(rows)
    except OSError as e:
        return {"rows": rows, "written": 0, "error": str(e)[:200]}

    return {"rows": rows, "written": len(rows),
            "attribution": OSM_ATTRIBUTION, "error": None}
