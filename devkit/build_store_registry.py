"""Consolidate store identity into one canonical registry.

DEV TOOLING — devkit/, never ships.

THE PROBLEM
store_coords.json carries 44 entries for roughly 29 physical sites, under two
different code schemes that were never reconciled:

    '003'      "Mobil Branch"                        legacy numeric
    'CFP-011'  "Chandarana Mobil Plaza Muthaiga"     CFP scheme

Fifteen coordinate positions hold two codes. That is not cosmetic:

  * a distance lookup resolves to whichever code it happens to be given, so the
    same store can measure as two different nodes;
  * the transfer engine treats codes as identity, so one physical shop can
    appear as both donor and recipient of the same move — a store transferring
    to itself, which no guard catches because the codes differ;
  * any per-store roll-up double-counts it.

THE FIX
Deduplicate by position. One canonical code per site, every other code kept as
an alias so historical data still resolves. The numeric code is preferred as
canonical because it is what the POS emits; the CFP entry usually carries the
better display name, so the name is taken from whichever entry has one.
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SRC = os.path.join(ROOT, "store_coords.json")
DEFAULT_OUT = os.path.join(ROOT, "oasis", "data", "store_registry.json")

#: positions closer than this are the same shop recorded twice
SAME_SITE_DEGREES = 0.0006          # roughly 65 m


def build(src: str, out: str) -> None:
    with open(src, "r", encoding="utf-8") as fh:
        coords = json.load(fh)

    groups = defaultdict(list)
    skipped = []
    for code, info in coords.items():
        if not isinstance(info, dict) or info.get("lat") is None:
            skipped.append(code)
            continue
        key = (round(float(info["lat"]) / SAME_SITE_DEGREES),
               round(float(info["lon"]) / SAME_SITE_DEGREES))
        groups[key].append((code, info))

    sites = []
    for members in groups.values():
        # numeric code wins as canonical — it is what the POS emits
        members.sort(key=lambda kv: (not str(kv[0]).isdigit(), str(kv[0])))
        canon_code, canon_info = members[0]
        # prefer the longest name across the group: the CFP entries carry the
        # full "Chandarana ..." form while the numeric ones are abbreviated
        name = max((str(i.get("name") or "") for _, i in members), key=len)
        is_hub = any(bool(i.get("is_warehouse_hub")) for _, i in members)
        sites.append({
            "org_cd": str(canon_code),
            "name": name or str(canon_code),
            "aliases": sorted(str(c) for c, _ in members[1:]),
            "lat": float(canon_info["lat"]),
            "lon": float(canon_info["lon"]),
            "is_warehouse_hub": is_hub,
        })
    sites.sort(key=lambda s: (not s["is_warehouse_hub"], s["org_cd"]))

    merged = [s for s in sites if s["aliases"]]
    hubs = [s for s in sites if s["is_warehouse_hub"]]
    print(f"input entries        {len(coords)}")
    print(f"distinct sites       {len(sites)}")
    print(f"sites with aliases   {len(merged)}  (duplicate identities collapsed)")
    print(f"warehouse hubs       {len(hubs)}  -> {[h['name'] for h in hubs]}")
    if skipped:
        print(f"skipped, no position {len(skipped)}: {skipped[:6]}")
    print()
    for s in merged[:8]:
        print(f"  {s['org_cd']:<8} {s['name'][:38]:<38} aliases {s['aliases']}")

    index = {}
    for s in sites:
        for code in [s["org_cd"], *s["aliases"]]:
            index[code] = s["org_cd"]
            # the ORG-prefixed form the adapters emit
            index[f"ORG{code.zfill(3)}" if code.isdigit() else f"ORG{code}"] = s["org_cd"]

    with open(out, "w", encoding="utf-8") as fh:
        json.dump({"sites": sites, "alias_index": index}, fh, indent=2)
    print(f"\nwrote {out}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--src", default=DEFAULT_SRC)
    p.add_argument("--out", default=DEFAULT_OUT)
    a = p.parse_args()
    build(a.src, a.out)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
