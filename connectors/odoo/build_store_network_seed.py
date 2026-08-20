"""Distil stores_network.json into a compact, Odoo-seedable network profile.

WHY A BUILD STEP
----------------
``stores_network.json`` is 85 MB: 14 Chandarana outlets x 23,511 SKUs, each
carrying a per-store ``qty`` and ``ads_scaled``. Loading that on every seed run
is slow, and pushing all 329k store-SKU pairs over XML-RPC is hours of work for
a test depot. This produces a ~3,000-SKU slice ONCE, so ``seed_store_network.py``
consumes a small file and stays re-runnable.

WHAT THE SLICE HAS TO PRESERVE
------------------------------
The point of seeding 14 stores is to exercise the transfer engine on real
multi-store data, so the slice is chosen to keep the things the engine reacts to
rather than simply the biggest SKUs:

  * **Value** — the top SKUs by network stock value, because that is what the
    risk_kes weighting ranks on and what an operator would recognise.
  * **Department breadth** — at least one SKU from every department present, so
    the fresh/KG rules and department grouping see their real variety instead of
    a grocery monoculture.
  * **Velocity spread** — a stratified sample across ADS bands INCLUDING zero.
    Dead stock (ads=0, stock>0) is fully excess by definition and is what feeds
    the PUSH pass; a value-ranked slice alone would be nearly all fast movers
    and the cold-node half of the engine would never fire.

Selection is deterministic — ties break on SKU name — so the same slice comes
out on every machine and the depot can be rebuilt identically.

Usage:
    python build_store_network_seed.py                 # 3000 SKUs (default)
    python build_store_network_seed.py --skus 500      # smaller/faster
    python build_store_network_seed.py --skus all      # everything (slow)
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
SOURCE = os.path.join(REPO, "stores_network.json")
OUT = os.path.join(HERE, "store_network_seed.json")

#: Odoo's stock.warehouse.code is size=5, and "CFP-003" is seven characters.
#: Truncating silently would collide ("CFP-0"), so the numeric part carries the
#: identity: CFP-003 -> C003. The full store id stays in the warehouse NAME and
#: in this mapping, which the seeder prints and writes into the seed file.
def short_code(store_id: str) -> str:
    digits = "".join(ch for ch in str(store_id) if ch.isdigit())
    return ("C" + digits.zfill(3))[:5]


#: velocity bands, so the slice keeps cold stock as well as fast movers.
#: (label, predicate, share of the stratified remainder)
BANDS = (
    ("dead", lambda a: a <= 0.0, 0.20),
    ("slow", lambda a: 0.0 < a <= 1.0, 0.30),
    ("medium", lambda a: 1.0 < a <= 5.0, 0.30),
    ("fast", lambda a: a > 5.0, 0.20),
)


def choose_skus(stores, limit):
    """Pick the SKU slice: value-ranked, department-complete, velocity-spread."""
    value = defaultdict(float)
    ads_max = defaultdict(float)
    dept_of = {}
    for st in stores:
        for row in st["stock_profile"]:
            sku = row["sku"]
            value[sku] += float(row.get("qty") or 0) * float(row.get("cost") or 0)
            ads_max[sku] = max(ads_max[sku], float(row.get("ads_scaled") or 0))
            dept_of.setdefault(sku, str(row.get("department") or "GENERAL"))

    all_skus = sorted(value, key=lambda s: (-value[s], s))
    if limit is None or limit >= len(all_skus):
        return all_skus, {"strategy": "all", "departments": len(set(dept_of.values()))}

    chosen, seen_dept = [], set()

    # 1. value core — two thirds of the budget
    core_n = int(limit * 0.66)
    for sku in all_skus[:core_n]:
        chosen.append(sku)
        seen_dept.add(dept_of[sku])

    # 2. one SKU from every department still unrepresented (highest value first)
    picked = set(chosen)
    for sku in all_skus:
        if len(chosen) >= limit:
            break
        d = dept_of[sku]
        if d not in seen_dept and sku not in picked:
            chosen.append(sku)
            picked.add(sku)
            seen_dept.add(d)

    # 3. stratified velocity fill for whatever budget is left
    remaining = limit - len(chosen)
    if remaining > 0:
        pool = [s for s in all_skus if s not in picked]
        for label, pred, share in BANDS:
            want = int(remaining * share)
            band = [s for s in pool if pred(ads_max[s])]
            for sku in band[:want]:
                if sku not in picked:
                    chosen.append(sku)
                    picked.add(sku)
        for sku in pool:                      # top up any rounding shortfall
            if len(chosen) >= limit:
                break
            if sku not in picked:
                chosen.append(sku)
                picked.add(sku)

    bands = {label: sum(1 for s in chosen if pred(ads_max[s]))
             for label, pred, _ in BANDS}
    return chosen, {"strategy": "value+department+velocity",
                    "departments": len(seen_dept), "bands": bands}


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--skus", default="3000",
                   help="how many SKUs to keep, or 'all' (default: 3000)")
    p.add_argument("--source", default=SOURCE)
    p.add_argument("--out", default=OUT)
    args = p.parse_args(argv)

    limit = None if str(args.skus).lower() == "all" else int(args.skus)

    if not os.path.exists(args.source):
        print(f"! source not found: {args.source}")
        return 1
    size_mb = os.path.getsize(args.source) / 1e6
    print(f"-> reading {os.path.basename(args.source)} ({size_mb:.0f} MB)...")
    with open(args.source, "r", encoding="utf-8") as f:
        net = json.load(f)
    stores = net["stores"]
    print(f"   {len(stores)} stores x {len(stores[0]['stock_profile']):,} SKUs")

    chosen, how = choose_skus(stores, limit)
    order = {s: i for i, s in enumerate(chosen)}
    print(f"-> slice: {len(chosen):,} SKUs ({how['strategy']}), "
          f"{how['departments']} departments")
    if "bands" in how:
        print(f"   velocity bands: " +
              ", ".join(f"{k}={v}" for k, v in how["bands"].items()))

    # catalogue: one entry per SKU, from whichever store has the richest row
    catalogue = {}
    out_stores = []
    for st in stores:
        rows = []
        for row in st["stock_profile"]:
            sku = row["sku"]
            if sku not in order:
                continue
            rows.append({
                "sku": sku,
                "qty": float(row.get("qty") or 0),
                "ads": round(float(row.get("ads_scaled") or 0), 4),
            })
            if sku not in catalogue:
                catalogue[sku] = {
                    "sku": sku,
                    "price": float(row.get("price") or 0),
                    "cost": float(row.get("cost") or 0),
                    "supplier": str(row.get("supplier") or "Unknown"),
                    "department": str(row.get("department") or "GENERAL"),
                }
        rows.sort(key=lambda r: order[r["sku"]])
        out_stores.append({
            "store_id": st["store_id"],
            "code": short_code(st["store_id"]),
            "name": st["name"],
            "latitude": st.get("latitude"),
            "longitude": st.get("longitude"),
            "region": st.get("region", ""),
            "demand_scale_factor": st.get("demand_scale_factor", 1.0),
            "sales_rank": st.get("sales_rank", 0),
            "safety_days": st.get("safety_days", 10),
            "reorder_frequency_days": st.get("reorder_frequency_days", 1),
            # Physical and catchment attributes. These are what
            # differentiate_store_network.py builds each outlet's demand mix
            # from — without them every store is a scaled copy of every other,
            # which is exactly the condition under which the transfer engine
            # has nothing to find.
            "floor_area_sqft": st.get("floor_area_sqft", 0),
            "store_category": st.get("store_category", ""),
            "catchment_affluence_index": st.get("catchment_affluence_index", 3.0),
            "cold_chain_capable": st.get("cold_chain_capable", True),
            "footfall_rank": st.get("footfall_rank", 5.0),
            "max_skus": st.get("max_skus", 23000),
            "stock_profile": rows,
        })
        print(f"   {st['store_id']} -> {short_code(st['store_id'])}  "
              f"{st['name'][:34]:<34} {len(rows):>5,} SKUs")

    seed = {
        "source": os.path.basename(args.source),
        "network_name": net.get("network_name", ""),
        "sku_count": len(chosen),
        "store_count": len(out_stores),
        "selection": how,
        "code_map": {s["store_id"]: s["code"] for s in out_stores},
        "catalogue": [catalogue[s] for s in chosen if s in catalogue],
        "stores": out_stores,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(seed, f, indent=1)
    mb = os.path.getsize(args.out) / 1e6
    print(f"-> wrote {os.path.relpath(args.out, REPO)} ({mb:.1f} MB), "
          f"{len(seed['catalogue']):,} catalogue entries, "
          f"{sum(len(s['stock_profile']) for s in out_stores):,} store-SKU pairs")
    return 0


if __name__ == "__main__":
    sys.exit(main())
