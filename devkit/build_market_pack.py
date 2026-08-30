"""Build the shipped national market matrix.

DEV TOOLING — never ships. The PACK it produces does.

WHY A PACK AT ALL
    Site selection is only as honest as its competitive field. Measured by
    dropping stores at random from the real matrix, a retailer who knows only
    30% of their rivals sees every site overstated by 2.26x, and the bias runs
    one way — toward opportunity. That is the same failure that produced a
    phantom "open corridor" earlier in this work.

    A retailer entering rivals from memory lands somewhere near that 30%. So
    the competitive field cannot be their job to assemble.

WHY NOT JUST FETCH IT ON FIRST RUN
    Because the public Overpass endpoint is not reliable enough to stand at the
    front of a first-run experience. Across one working session roughly a third
    of calls failed on first attempt — HTTP 429 rate limits and 504 timeouts —
    and a whole-country query times out outright. This script therefore sweeps
    in latitude bands and retries, which is fine for us to run occasionally and
    would be a miserable way to meet the product.

    The client keeps the fetcher for REFRESH and for regions the pack does not
    cover. It stops being setup and becomes maintenance.

LICENSING — READ BEFORE SHIPPING A NEW PACK
    An extract of OSM is a DERIVATIVE DATABASE. Redistributing one obliges us
    to license THAT DATABASE under ODbL and to attribute; the scoring code
    remains a Produced Work and is unaffected. So the pack is written with its
    licence beside it and ``oasis.logic.geo_sources`` refuses to load a pack
    whose notice is missing.

    This is an engineering summary of a licence, not legal advice. Confirm
    before a commercial release — the same caveat geo_sources has carried since
    the competitor fetcher was written.

    python devkit/build_market_pack.py --country KEN
"""

from __future__ import annotations

import argparse
import csv
import os
import sys
import time

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)

#: Latitude bands to sweep. A single national query returns 504 from the public
#: endpoint; bands of a few degrees come back in seconds.
_BANDS = {
    "KEN": [(-4.9, -1.6), (-1.6, 0.2), (0.2, 2.0), (2.0, 5.5)],
}
_LON = {"KEN": (33.8, 42.0)}

#: Chains worth carrying nationally.
#:
#: The first build of this pack erred wide on the theory that a name matching
#: nothing costs nothing. It does not. "Greenspan" and "Naivasha" are PLACES,
#: and because ``match_chain`` resolves ties longest-name-wins, the 9-letter
#: "Greenspan" beat the 6-letter "Naivas" and filed a real Naivas branch under
#: a shopping mall — while a tyre shop in the same mall joined it. A junk brand
#: does not merely add rows, it STEALS them from the right chain.
#:
#: So: supermarket banners only, and check what a new name actually caught
#: before keeping it.
_CHAINS = {
    "KEN": ["Naivas", "Carrefour", "Chandarana", "Jaza", "Quickmart",
            "Quick Mart", "Cleanshelf", "Clean Shelf", "Tuskys", "Eastmatt",
            "Magunas", "Society Stores", "Zucchini", "Choppies"],
}

#: Two OSM elements for one shop — a node the shopfront and a way the building
#: — survive an exact-coordinate dedupe and land as two competitors. Same
#: chain within this distance is the same store.
_SAME_STORE_M = 120.0

RETRIES = 3
BACKOFF_S = 25


def sweep(country: str) -> list:
    import requests

    from oasis.logic.geo_sources import USER_AGENT, match_chain
    names = _CHAINS[country]
    pattern = "|".join(n.replace(" ", r"\\s*") for n in names)
    west, east = _LON[country]

    rows, seen = [], set()
    for i, (south, north) in enumerate(_BANDS[country], 1):
        box = f"{south},{west},{north},{east}"
        sel = "".join(f'{el}["shop"]["name"~"{pattern}",i]({box});'
                      for el in ("node", "way"))
        query = f"[out:json][timeout:150];({sel});out center;"
        for attempt in range(1, RETRIES + 1):
            try:
                r = requests.get(
                    "https://overpass-api.de/api/interpreter",
                    params={"data": query},
                    headers={"User-Agent": USER_AGENT}, timeout=240)
                if r.status_code != 200:
                    print(f"  band {i} attempt {attempt}: HTTP {r.status_code}")
                    time.sleep(BACKOFF_S)
                    continue
                els = r.json().get("elements", [])
            except Exception as e:
                print(f"  band {i} attempt {attempt}: {str(e)[:70]}")
                time.sleep(BACKOFF_S)
                continue

            kept = 0
            for el in els:
                centre = el.get("center") or {}
                lat = el.get("lat", centre.get("lat"))
                lon = el.get("lon", centre.get("lon"))
                name = (el.get("tags") or {}).get("name") or ""
                chain = match_chain(name, names)
                if lat is None or lon is None or not chain:
                    continue
                key = (round(float(lat), 5), round(float(lon), 5))
                if key in seen:
                    continue
                seen.add(key)
                rows.append({"Store_Name": name, "Latitude": round(float(lat), 6),
                             "Longitude": round(float(lon), 6), "Chain": chain,
                             "Source": "OSM_Overpass"})
                kept += 1
            print(f"  band {i} ({south} to {north}): {len(els)} elements, {kept} kept")
            break
        else:
            print(f"  band {i}: FAILED after {RETRIES} attempts — pack is short")
        time.sleep(12)
    return _dedupe_nearby(rows)


def _dedupe_nearby(rows: list) -> list:
    """Collapse same-chain stores within ``_SAME_STORE_M`` (keeps the first).

    OSM maps one shop as both a node and a building way often enough that the
    raw sweep returned "Zucchini Greengrocers Limited - ABC Place" twice, a few
    metres apart. Two rows is two competitors to the scorer, and a phantom
    rival suppresses a real site.
    """
    import math
    kept = []
    for r in rows:
        lat, lon = r["Latitude"], r["Longitude"]
        dup = False
        for k in kept:
            if k["Chain"] != r["Chain"]:
                continue
            dlat = (lat - k["Latitude"]) * 110_574
            dlon = ((lon - k["Longitude"]) * 111_320
                    * math.cos(math.radians(lat)))
            if math.hypot(dlat, dlon) <= _SAME_STORE_M:
                dup = True
                break
        if not dup:
            kept.append(r)
    if len(kept) != len(rows):
        print(f"\n  deduped {len(rows) - len(kept)} co-located same-chain rows")
    return kept


def main(country: str) -> None:
    from oasis.logic.geo_sources import (OSM_ATTRIBUTION, pack_licence_path,
                                         pack_path)
    print(f"  sweeping {country} in {len(_BANDS[country])} bands\n")
    rows = sweep(country)
    if not rows:
        print("  nothing fetched — pack NOT written")
        return

    out = pack_path(country, root=_ROOT)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Store_Name", "Latitude", "Longitude",
                                          "Chain", "Source"])
        w.writeheader()
        w.writerows(sorted(rows, key=lambda r: (r["Chain"], r["Latitude"])))

    # The licence travels WITH the data, and geo_sources refuses to load a pack
    # without it. A derivative database that loses its notice in transit is the
    # exact failure ODbL section 4.3 is about.
    with open(pack_licence_path(country, root=_ROOT), "w",
              encoding="utf-8") as f:
        f.write(
            "OASIS market matrix — " + country + "\n"
            "=====================================\n\n"
            "This file is a DERIVATIVE DATABASE extracted from OpenStreetMap.\n\n"
            "It is made available under the Open Database License (ODbL) v1.0:\n"
            "    https://opendatacommons.org/licenses/odbl/1-0/\n\n"
            + OSM_ATTRIBUTION + "\n\n"
            "Any redistribution of this file, or of a database derived from it,\n"
            "must carry the same licence and attribution. Scores, rankings and\n"
            "reports COMPUTED from it are Produced Works and are not bound by\n"
            "these terms — only the data is.\n\n"
            "Rebuild with: python devkit/build_market_pack.py --country "
            + country + "\n")

    import collections
    counts = collections.Counter(r["Chain"] for r in rows)
    print(f"\n  written {out}")
    print(f"  {len(rows)} stores, {round(os.path.getsize(out)/1024,1)} KB")
    for c, n in counts.most_common():
        print(f"     {c:<22}{n:>4}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--country", default="KEN", choices=sorted(_BANDS))
    main(ap.parse_args().country)
