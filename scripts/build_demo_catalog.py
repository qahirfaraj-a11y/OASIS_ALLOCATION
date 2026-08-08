"""
Generate the shipped SAMPLE catalogue from the hot (selling) product set.

DEV-TIME ONLY. This script reads sources that never ship — the allocation
scorecard and the Obsidian vault — and emits a small de-identified JSON that
does. Run it when the sample catalogue needs regenerating; clients never run it.

What "hot" means here is the operator's definition, not the codebase's
``hot_node_days`` (which is about days of cover): a hot node SELLS, a cold node
has zero velocity. Nearly the whole catalogue sells at least a little, so the
useful cut is the velocity tier — A (Staple), B (Core) and C (Filler) are the
live assortment; D (Risk) is 16,668 lines of near-dead weight a sample store
should not carry.

WHAT IS DELIBERATELY NOT COPIED
    revenue, margin, gross profit, GMROI, supplier terms, and the real
    per-SKU ADS. Those are the retailer's book. Shelf price and the product's
    identity are publicly observable — you can read both off a shelf — and the
    velocity TIER is a coarse band, not a figure. Demand is regenerated from
    the tier at build time, so the sample behaves realistically without
    exporting anyone's sales.

Usage:
    python scripts/build_demo_catalog.py [--tiers A,B,C] [--max-skus 4000]
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import random
import re
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

SCORECARD = "Full_Product_Allocation_Scorecard_v7.csv"
OUT = os.path.join("oasis", "data", "demo_catalog.json.gz")

#: Units/day sampled per tier — a realistic spread, not the client's figures.
TIER_ADS = {"A": (6.0, 40.0), "B": (1.5, 6.0), "C": (0.2, 1.5)}
#: Days of cover a sample store opens with, per tier.
TIER_COVER = {"A": (7, 21), "B": (10, 30), "C": (14, 45)}


def _tier(value: str) -> str:
    m = re.match(r"\s*([ABCD])", str(value or ""))
    return m.group(1) if m else "D"


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tiers", default="A,B,C",
                    help="velocity tiers to carry (default A,B,C — excludes D)")
    ap.add_argument("--max-skus", type=int, default=4000,
                    help="cap the catalogue so the release stays small")
    ap.add_argument("--seed", type=int, default=42)
    ap.add_argument("--out", default=OUT)
    args = ap.parse_args()

    import pandas as pd
    if not os.path.exists(SCORECARD):
        print(f"[FAIL] {SCORECARD} not found — this is a dev-only script.")
        return 1

    keep = {t.strip().upper() for t in args.tiers.split(",") if t.strip()}
    rng = random.Random(args.seed)

    df = pd.read_csv(SCORECARD)
    df["tier_band"] = df.get("Velocity_Tier", "").map(_tier)
    df = df[df["tier_band"].isin(keep)].copy()
    df["ads_sort"] = pd.to_numeric(df.get("Avg_Daily_Sales"), errors="coerce").fillna(0)

    # Rank WITHIN the file only to choose which lines to carry, then throw the
    # figure away. Selection is not disclosure; the ordering would be.
    df = df.sort_values("ads_sort", ascending=False).head(args.max_skus)

    rows, seen = [], set()
    for i, r in enumerate(df.to_dict("records"), start=1):
        name = str(r.get("Product", "") or "").strip()
        if not name or name.upper() in seen:
            continue
        seen.add(name.upper())
        tier = r["tier_band"]
        price = float(r.get("Unit_Price", 0) or 0)
        if price <= 0:
            continue
        lo, hi = TIER_ADS.get(tier, (0.2, 1.0))
        ads = round(rng.uniform(lo, hi), 2)
        c_lo, c_hi = TIER_COVER.get(tier, (14, 30))
        rows.append({
            "itm_cd": f"DEMO{i:05d}",
            "name": name,
            "dept": str(r.get("Department", "") or "GENERAL").strip().title(),
            "vendor": str(r.get("Supplier", "") or "Unknown").strip().title(),
            "price": round(price, 2),
            "stock": round(ads * rng.uniform(c_lo, c_hi), 0),
            "tier": tier,
        })

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    # gzip: 4,000 lines is 607 KB raw against a 0.6 MB release, 112 KB packed.
    with gzip.open(args.out, "wt", encoding="utf-8") as f:
        json.dump({"generated_by": "scripts/build_demo_catalog.py",
                   "tiers": sorted(keep), "skus": len(rows),
                   "note": "Synthetic demand; no revenue, margin or real ADS.",
                   "rows": rows}, f, separators=(",", ":"))

    size_kb = os.path.getsize(args.out) / 1024
    by_tier: dict = {}
    for r in rows:
        by_tier[r["tier"]] = by_tier.get(r["tier"], 0) + 1
    print(f"[OK] {len(rows):,} hot SKUs -> {args.out} ({size_kb:,.0f} KB)")
    print(f"     tiers: {by_tier}")
    print(f"     departments: {len({r['dept'] for r in rows})}, "
          f"suppliers: {len({r['vendor'] for r in rows})}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
