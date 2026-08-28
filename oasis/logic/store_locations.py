"""
Where the client's own stores are.

``ORGANIZATION_MST`` carries an address but no coordinates, and OASIS will not
guess: geocoding a free-text address silently puts a store in the wrong place,
and every site score downstream is a distance from these points. So the
operator supplies them once, per install, and they are stored next to the
store — never shipped, because a chain's estate map is theirs.

The record is keyed by ORG_CD and merged onto the live organisation list, so a
store added to the POS later shows up here as "needs a location" rather than
silently vanishing from the network map.

The reference customer's own ``store_coords.json`` was previously read from the
repo root. That file is their estate and is not part of any release.
"""

from __future__ import annotations

import json
import os
from typing import Any, Dict, List, Optional

#: Per-install, per-client. Machine state, excluded from the release.
LOCATIONS_FILE = "store_locations.json"

#: Assumed floor area when the operator has not given one.
DEFAULT_SIZE_SQFT = 10_000.0


def locations_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", LOCATIONS_FILE)


def load_locations(root: Optional[str] = None) -> Dict[str, dict]:
    """``{org_cd: {lat, lon, size_sqft}}`` — empty when nothing is set yet."""
    path = locations_path(root)
    if not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
    except (OSError, ValueError):
        return {}
    out: Dict[str, dict] = {}
    for org, rec in (data.get("stores") or data).items():
        try:
            out[str(org)] = {
                "lat": float(rec["lat"]), "lon": float(rec["lon"]),
                "size_sqft": float(rec.get("size_sqft", DEFAULT_SIZE_SQFT)),
            }
        except (KeyError, TypeError, ValueError):
            continue
    return out


def save_location(org_cd: str, lat: float, lon: float,
                  size_sqft: float = DEFAULT_SIZE_SQFT,
                  root: Optional[str] = None) -> Dict[str, Any]:
    """Record one store's position. Validates before writing."""
    try:
        lat_f, lon_f = float(lat), float(lon)
    except (TypeError, ValueError):
        return {"saved": False, "error": "Latitude and longitude must be numbers."}
    if not (-90.0 <= lat_f <= 90.0) or not (-180.0 <= lon_f <= 180.0):
        return {"saved": False,
                "error": "Coordinates out of range (lat -90..90, lon -180..180)."}

    current = load_locations(root)
    current[str(org_cd)] = {"lat": lat_f, "lon": lon_f,
                            "size_sqft": float(size_sqft or DEFAULT_SIZE_SQFT)}
    path = locations_path(root)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump({"stores": current}, f, indent=2)
    except OSError as e:
        return {"saved": False, "error": str(e)[:200]}
    return {"saved": True, "org_cd": str(org_cd), "error": None}


#: Column spellings accepted by the bulk importer, in priority order. An
#: operator exporting from their own systems should not have to rename headers.
_ORG_KEYS = ("org_cd", "org", "store", "store_code", "code", "branch", "id")
_LAT_KEYS = ("lat", "latitude", "y")
_LON_KEYS = ("lon", "lng", "long", "longitude", "x")
_SIZE_KEYS = ("size_sqft", "sqft", "size", "floor_area", "area", "sq_ft")

#: What a template hands the operator to fill in.
IMPORT_HEADER = "org_cd,lat,lon,size_sqft"


def _cell(row: Dict[str, Any], keys) -> Optional[str]:
    low = {str(k).strip().lower().replace(" ", "_"): v for k, v in row.items()
           if k is not None}
    for k in keys:
        v = low.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return None


def parse_locations(text: str) -> Dict[str, Any]:
    """Parse pasted or file CSV into placements. Pure — writes nothing.

    Typing one store at a time is fine for five and unusable for thirty, and
    thirty is roughly where the estate becomes big enough to validate anything.
    Accepts a header row in any of the usual spellings; a headerless file is
    read positionally as ``org_cd, lat, lon, size_sqft``.

    Every rejected row is returned with its line number and the reason, because
    a silent partial import is how half an estate ends up in the wrong place.
    """
    import csv as _csv
    import io as _io

    lines = [ln for ln in (text or "").splitlines() if ln.strip()]
    if not lines:
        return {"rows": [], "errors": [], "error": "nothing to import"}

    # Detect a header by whether the row is DATA, not by whether it contains
    # header-ish words: this POS codes stores "ORG001", so a substring test for
    # "org" swallowed the first store of every headerless file.
    first_cells = [c.strip() for c in lines[0].split(",")]

    def _numeric(cell: str) -> bool:
        try:
            float(cell)
            return True
        except (TypeError, ValueError):
            return False

    has_header = not (len(first_cells) >= 3
                      and _numeric(first_cells[1]) and _numeric(first_cells[2]))
    reader = (_csv.DictReader(_io.StringIO("\n".join(lines))) if has_header
              else _csv.DictReader(_io.StringIO("\n".join(lines)),
                                   fieldnames=["org_cd", "lat", "lon",
                                               "size_sqft"]))

    rows: List[dict] = []
    errors: List[dict] = []
    seen = set()
    for i, raw in enumerate(reader, start=2 if has_header else 1):
        org = _cell(raw, _ORG_KEYS)
        lat_s, lon_s = _cell(raw, _LAT_KEYS), _cell(raw, _LON_KEYS)
        size_s = _cell(raw, _SIZE_KEYS)
        if not org:
            errors.append({"line": i, "reason": "no store code"})
            continue
        try:
            lat, lon = float(lat_s), float(lon_s)
        except (TypeError, ValueError):
            errors.append({"line": i, "org_cd": org,
                           "reason": "latitude/longitude not numeric"})
            continue
        if not (-90.0 <= lat <= 90.0) or not (-180.0 <= lon <= 180.0):
            errors.append({"line": i, "org_cd": org,
                           "reason": "coordinates out of range"})
            continue
        try:
            size = float(size_s) if size_s else DEFAULT_SIZE_SQFT
        except (TypeError, ValueError):
            size = DEFAULT_SIZE_SQFT
        if size <= 0:
            size = DEFAULT_SIZE_SQFT
        if org in seen:
            errors.append({"line": i, "org_cd": org,
                           "reason": "duplicate store code in this file"})
            continue
        seen.add(org)
        rows.append({"org_cd": org, "lat": lat, "lon": lon, "size_sqft": size})

    return {"rows": rows, "errors": errors, "error": None}


def import_locations(text: str, known_orgs: Optional[List[str]] = None,
                     root: Optional[str] = None) -> Dict[str, Any]:
    """Parse and save many placements at once.

    ``known_orgs`` — when given, a code the POS does not recognise is REJECTED
    rather than written. A location saved against a typo is invisible: the
    store still reads as "needs a location" while a phantom point quietly
    joins every catchment calculation.
    """
    parsed = parse_locations(text)
    if parsed.get("error"):
        return {"saved": 0, "rows": [], "errors": parsed["errors"],
                "error": parsed["error"]}

    known = {str(o) for o in (known_orgs or [])}
    rows, errors = [], list(parsed["errors"])
    for r in parsed["rows"]:
        if known and r["org_cd"] not in known:
            errors.append({"org_cd": r["org_cd"],
                           "reason": "not a store code in this POS"})
            continue
        rows.append(r)

    current = load_locations(root)
    for r in rows:
        current[r["org_cd"]] = {"lat": r["lat"], "lon": r["lon"],
                                "size_sqft": r["size_sqft"]}
    if rows:
        path = locations_path(root)
        try:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump({"stores": current}, f, indent=2)
        except OSError as e:
            return {"saved": 0, "rows": rows, "errors": errors,
                    "error": str(e)[:200]}

    return {"saved": len(rows), "rows": rows, "errors": errors, "error": None}


def export_template(stores: List[dict], root: Optional[str] = None) -> str:
    """A CSV the operator can fill in, pre-filled with what is already known."""
    placed = load_locations(root)
    out = [IMPORT_HEADER]
    for s in stores or []:
        org = str(s.get("org_cd") or s.get("ORG_CD") or "")
        if not org:
            continue
        rec = placed.get(org)
        if rec:
            out.append(f"{org},{rec['lat']:.6f},{rec['lon']:.6f},"
                       f"{rec['size_sqft']:.0f}")
        else:
            out.append(f"{org},,,")
    return "\n".join(out)


def merge_with_stores(stores: List[dict],
                      root: Optional[str] = None) -> Dict[str, Any]:
    """Join saved coordinates onto the live organisation list.

    ``{located, missing, error}``. A store the POS knows about but which has no
    coordinates appears in ``missing`` — the network map must say "you have not
    placed this store" rather than quietly leave it out of every calculation.
    """
    saved = load_locations(root)
    located, missing = [], []
    for s in stores or []:
        org = str(s.get("org_cd") or "")
        rec = saved.get(org)
        if rec:
            located.append({"org_cd": org, "name": s.get("name") or org,
                            "lat": rec["lat"], "lon": rec["lon"],
                            "size_sqft": rec["size_sqft"]})
        else:
            missing.append({"org_cd": org, "name": s.get("name") or org})

    # Coordinates on file for stores the POS has never mentioned. Reporting
    # this separately matters: with an unreachable POS the estate reads as
    # "nothing placed", which sends the operator back to place stores they
    # have already placed, when the real fault is the connection.
    orphaned = sorted(set(saved) - {str(s.get("org_cd") or "")
                                    for s in (stores or [])})
    return {"located": located, "missing": missing, "orphaned": orphaned,
            "saved_total": len(saved), "error": None}
