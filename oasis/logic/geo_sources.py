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
import os
from typing import Dict, List, Optional

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


def cache_path(root: Optional[str] = None) -> str:
    base = root or os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))
    return os.path.join(base, "oasis", "data", CACHE_FILE)


def load_competitors(root: Optional[str] = None) -> Dict[str, object]:
    """The cached competitor set for this install.

    ``{rows, source, attribution, error}``. Absent cache is not an error — it
    is a client who has not fetched yet, and the caller should say so rather
    than pretend there is no competition.
    """
    path = cache_path(root)
    if not os.path.exists(path):
        return {"rows": [], "source": None, "attribution": OSM_ATTRIBUTION,
                "error": "No competitor data on this install yet. Fetch it for "
                         "your region first."}
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as f:
            rows = [r for r in csv.DictReader(f)]
    except OSError as e:
        return {"rows": [], "source": None, "attribution": OSM_ATTRIBUTION,
                "error": str(e)[:200]}
    return {"rows": rows, "source": "OpenStreetMap (Overpass)",
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

    rows: List[dict] = []
    for brand in brands:
        query = (f'[out:json][timeout:{timeout}];'
                 f'node["name"~"{brand}",i]({bbox});out center;')
        try:
            resp = requests.get(OVERPASS_URL, params={"data": query},
                                headers={"User-Agent": USER_AGENT},
                                timeout=timeout)
            resp.raise_for_status()
            for el in resp.json().get("elements", []):
                lat, lon = el.get("lat"), el.get("lon")
                if lat is None or lon is None:
                    continue
                rows.append({"Store_Name": (el.get("tags") or {}).get("name", brand),
                             "Latitude": lat, "Longitude": lon,
                             "Chain": brand, "Source": "OSM_Overpass"})
        except Exception as e:
            return {"rows": rows, "written": 0,
                    "error": f"{brand}: {str(e)[:160]}"}

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
