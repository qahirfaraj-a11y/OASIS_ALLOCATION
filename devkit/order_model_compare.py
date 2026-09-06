"""Classic vs derived, through the PRODUCTION entry point, on the real book.

WHY THIS AND NOT ANOTHER ABLATION
    The existing sweeps call ``ou.recommend()`` directly, so they only ever
    exercise the derived path. They cannot answer the question that matters
    operationally, which is not "is the derived model self-consistent" but
    "does what we are about to ship differ from what a customer runs today".

    That fork is one ``if`` in ``SimulationOrderUtil.calculate_order_quantity``
    (see simulation_bridge.py, ``if _ou.is_enabled()``), and the ONLY way to
    measure it honestly is to call that method. So this harness does.

WHAT IS HELD FIXED
    Enrichment runs ONCE and both arms score the same enriched book -- deep
    copied per arm, because calculate_order_quantity mutates its input. That
    matters more than it sounds: the classic path takes
    ``target_coverage_days`` straight out of enrichment, which is where the
    velocity multiplier (intelligence_mixin.py, target_days *= v(d)) is
    applied. Running with skip_enrichment=True would quietly delete the very
    mechanism under test and flatter the classic arm.

    Same products, same stock, same shared trigger. The difference is the
    quantity decision, which is what we want to attribute it to.

WHAT THE INPUTS ARE
    The real shelf (stock_snapshot_dept.json), the real margin/vendor book
    (build_margin) and the ADS that reconciles unit-for-unit against the six
    cash extracts (corrected_ads_from_pos.json). No simulator, no generated
    demand -- so there is no demand law here to be circular about.

USAGE
    python devkit/order_model_compare.py [stock_multiplier ...]
"""
from __future__ import annotations

import copy
import json
import os
import statistics
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import build_margin                       # noqa: E402
from devkit.amit_return import norm                   # noqa: E402
from oasis.logic import order_up_to as ou             # noqa: E402

DATA_DIR = str(ROOT / "oasis" / "data")

#: Velocity bands, chosen to straddle the old multiplier's breakpoints so the
#: comparison can say WHERE the two models disagree, not just by how much.
BANDS = ((0.0, 1.0, "<=1/day"), (1.0, 5.0, "1-5/day"),
         (5.0, 10.0, "5-10/day"), (10.0, 1e9, ">10/day"))


def band_of(d: float) -> str:
    for lo, hi, name in BANDS:
        if lo < d <= hi or (lo == 0.0 and d <= hi):
            return name
    return ">10/day"


def build_book():
    """The real shelf, priced and demanded. Same construction as the sweep."""
    stock = json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json")
                       .read_text(encoding="utf-8"))
    mar = {norm(k): v for k, v in build_margin.load_or_derive()[0].items()}
    ads = {norm(k): v for k, v in json.loads(
        (ROOT / "oasis" / "data" / "corrected_ads_from_pos.json")
        .read_text(encoding="utf-8")).items()}

    book = []
    for k, sv in stock.items():
        m = mar.get(k)
        av = ads.get(k) or {}
        d = float(av.get("new_ads") or av.get("old_ads") or 0)
        if not m or d <= 0:
            continue
        book.append({
            "sku": k,
            "item_code": k,
            "itm_cd": k,
            "product_name": k,
            "department": " ".join(str(sv.get("dept") or "").upper().split()),
            "supplier_name": " ".join(str(m.get("vendor") or "").upper().split()),
            "avg_daily_sales": d,
            "current_stock": max(0.0, float(sv["stock"])),
            "unit_cost": float(m["unit_cost"]),
            "cost_price": float(m["unit_cost"]),
            "gross_profit_per_unit": float(m["gross_profit_per_unit"]),
            "selling_price": float(m["unit_cost"]) + float(m["gross_profit_per_unit"]),
            "on_order_qty": 0.0,
        })
    return book


def score(enriched, util, mult):
    """One arm, at one stock position. Returns the metrics and the per-SKU Q."""
    arm = copy.deepcopy(enriched)
    for p in arm:
        p["current_stock"] = p["_base_stock"] * mult
    recs = util.calculate_order_quantity(arm, use_real_date=True)
    recs = util.finalize_orders(recs)

    by_sku, per_band = {}, defaultdict(lambda: {"lines": 0, "units": 0.0, "kes": 0.0})
    lines = suppressed = 0
    kes = units = 0.0
    cover = []
    for r in recs:
        q = float(r.get("recommended_quantity") or 0)
        sku = r.get("sku") or r.get("item_code")
        by_sku[sku] = q
        d = float(r.get("avg_daily_sales") or 0)
        if r.get("auto_order_suppressed"):
            suppressed += 1
        if q > 0:
            lines += 1
            units += q
            kes += q * float(r.get("unit_cost") or 0)
            b = per_band[band_of(d)]
            b["lines"] += 1
            b["units"] += q
            b["kes"] += q * float(r.get("unit_cost") or 0)
        if d > 0:
            cover.append((float(r.get("current_stock") or 0) + q) / d)
    return {
        "lines": lines, "kes": kes, "units": units, "suppressed": suppressed,
        "cover": statistics.median(cover) if cover else 0.0,
        "by_sku": by_sku, "bands": dict(per_band),
    }


def main(argv) -> int:
    mults = [float(a) for a in argv[1:]] or [0.0, 0.5, 1.0, 2.0]

    print("Building the real book…")
    book = build_book()
    print(f"  {len(book):,} SKUs priced, demanded and on the shelf")

    from oasis.logic.simulation_bridge import SimulationOrderUtil
    util = SimulationOrderUtil(DATA_DIR)

    print("Enriching once, shared by both arms "
          "(this is where the velocity multiplier lands)…")
    enriched = util.prepare_sku_data(copy.deepcopy(book))

    # enrich_product_data OVERWRITES current_stock from its own databases and
    # writes 0 on a miss, so the caller's shelf is silently discarded: 12,000
    # of 15,037 rows went in with stock and every one came out at zero. Reading
    # the base stock off the ENRICHED rows therefore measured a cold start at
    # every multiplier -- four identical rows, which is what gave it away.
    # Take the shelf from the book, which is the real snapshot.
    base_stock = {p["sku"]: float(p.get("current_stock") or 0) for p in book}
    for p in enriched:
        p["_base_stock"] = base_stock.get(p.get("sku") or p.get("item_code"), 0.0)
    held = sum(1 for p in enriched if p["_base_stock"] > 0)
    print(f"  {len(enriched):,} enriched, {held:,} carrying real stock")
    assert held > 0.5 * len(enriched), \
        "the real shelf did not survive enrichment — the arms would be cold starts"

    # Prove the fork actually moved, rather than trusting the env var.
    arms = {}
    for name, val in (("classic", "classic"), ("derived", "order_up_to")):
        os.environ["OASIS_ORDER_MODEL"] = val
        assert ou.is_enabled() is (val == "order_up_to"), \
            f"the {name} arm did not take: is_enabled()={ou.is_enabled()}"
        arms[name] = {m: score(enriched, util, m) for m in mults}
    os.environ.pop("OASIS_ORDER_MODEL", None)

    print(f"\n{'stock':>7} {'model':>8} {'lines':>8} {'order KES':>14} "
          f"{'units':>12} {'suppressed':>11} {'med cover':>10}")
    for m in mults:
        for name in ("classic", "derived"):
            r = arms[name][m]
            print(f"{m:>6.2f}x {name:>8} {r['lines']:>8,} {r['kes']:>14,.0f} "
                  f"{r['units']:>12,.0f} {r['suppressed']:>11,} "
                  f"{r['cover']:>9.1f}d")
        c, d_ = arms["classic"][m], arms["derived"][m]
        dk = d_["kes"] - c["kes"]
        pct = (100 * dk / c["kes"]) if c["kes"] else 0.0
        print(f"{'':>7} {'delta':>8} {d_['lines']-c['lines']:>+8,} "
              f"{dk:>+14,.0f} {d_['units']-c['units']:>+12,.0f} "
              f"{d_['suppressed']-c['suppressed']:>+11,} "
              f"{d_['cover']-c['cover']:>+9.1f}d   ({pct:+.1f}% spend)")

    # Where they disagree, by velocity. The cv fix is a claim ABOUT the slow
    # tail specifically, so a comparison that only reports totals cannot
    # confirm or refute it.
    print("\nAt 1.0x (today's shelf), by velocity band — the cv claim is "
          "specifically about the slow tail:")
    m = 1.0 if 1.0 in mults else mults[-1]
    print(f"  {'band':>10} {'classic KES':>14} {'derived KES':>14} "
          f"{'delta':>14} {'cls lines':>10} {'drv lines':>10}")
    for _, _, name in BANDS:
        cb = arms["classic"][m]["bands"].get(name, {"kes": 0, "lines": 0})
        db = arms["derived"][m]["bands"].get(name, {"kes": 0, "lines": 0})
        print(f"  {name:>10} {cb['kes']:>14,.0f} {db['kes']:>14,.0f} "
              f"{db['kes']-cb['kes']:>+14,.0f} {cb['lines']:>10,} "
              f"{db['lines']:>10,}")

    # Agreement, so the totals cannot hide two large offsetting moves.
    cs, ds = arms["classic"][m]["by_sku"], arms["derived"][m]["by_sku"]
    both = set(cs) & set(ds)
    same = sum(1 for k in both if abs(cs[k] - ds[k]) < 1e-9)
    only_c = sum(1 for k in both if cs[k] > 0 and ds[k] <= 0)
    only_d = sum(1 for k in both if ds[k] > 0 and cs[k] <= 0)
    print(f"\n  identical Q on {same:,}/{len(both):,} SKUs "
          f"({100*same/max(1,len(both)):.1f}%)")
    print(f"  classic orders where derived does not: {only_c:,}")
    print(f"  derived orders where classic does not: {only_d:,}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
