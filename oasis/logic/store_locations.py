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
    return {"located": located, "missing": missing, "error": None}
