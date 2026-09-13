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


def pipeline_ads():
    """ADS exactly as the shipping pipeline measures it, by normalised name.

    WHY THIS REPLACED corrected_ads_from_pos.json

    That file is a different measurement of the same shop, and nothing said
    so. Reconciled against the pipeline on the 15,037 SKUs they share: same
    catalogue (100% of this book is in the POS catalogue), same shelf (98.4%
    of stock agrees exactly), and demand agreeing on 3.9% of lines -- median
    ratio 1.079, p10 0.830, p90 1.500. Aggregate within 6%, per line nowhere
    near.

    The cause is not a formula. They are different vintages over different
    windows: the pipeline derives ADS from the seven 2025 cash extracts
    (jan, mar, may, jun, jul, sep, oct) over a global 7 x 30.4 days, while
    corrected_ads_from_pos.json is dated 2026-02-11 and divides each SKU's
    units by its own months_active, capped at 6.

    Which makes it more than stale. A run measuring AS OF 2025-12-09 that
    takes demand from a window ending February 2026 is using information from
    after the date it claims to stand on. The model verdict survived it --
    both arms shared the same demand, so the classic-vs-derived delta was
    still attributable to the quantity decision -- but every absolute figure
    it printed belonged to a book nothing else in the repo reads.
    """
    # get_adapter is what generate_smart_orders calls, so this reads the store
    # the pipeline reads, resolved the way the pipeline resolves it. The first
    # version of this function rebuilt that resolution by hand and promptly
    # diverged from it -- which is the whole failure this change exists to
    # end. Point it with OASIS_POS_DB_URL, exactly as the shipped run is
    # pointed.
    from oasis.desktop.data import get_adapter
    from oasis.logic.simulation_bridge import SimulationOrderUtil

    adapter = get_adapter(str(ROOT))
    orgs = adapter.fetch_all_organizations() or []
    if not orgs:
        raise SystemExit(
            "the pipeline's adapter returned no organizations. It resolves the "
            "POS the way the shipped run does, so set OASIS_POS_DB_URL to the "
            "store you mean -- otherwise it reads whatever the install wizard "
            "recorded, which on this machine is a SQL Server that needs pyodbc.")
    org = orgs[0]["ORG_CD"]
    util = SimulationOrderUtil(DATA_DIR)
    out = {}
    for p in util.prepare_sku_data(adapter.fetch_enriched_products(org)):
        n = norm(p.get("product_name") or "")
        # First row wins. 18 names carry more than one item code, which is the
        # cost of joining on a name at all -- recorded, not hidden.
        if n and n not in out:
            out[n] = float(p.get("avg_daily_sales") or 0)
    return out


def build_book(demand: str = "snapshot"):
    """The real shelf, priced and demanded. Same construction as the sweep.

    `demand` defaults to the SNAPSHOT deliberately, even though the pipeline's
    own measurement is the better basis and is what this module's own main()
    asks for. multistore_run, ordering_audit and offline_engine_check all call
    build_book() with no arguments, and the last of those ends by printing "no
    POS connection was opened at any point in the above". Defaulting to
    "pipeline" opens one -- so that line became false the moment the default
    changed, and it is a bare print, not a check, so nothing caught it.

    Those three still read corrected_ads_from_pos.json and so still measure a
    different shop from the pipeline. That is worth fixing; it is not worth
    fixing by falsifying an offline guarantee on the way past.
    """
    stock = json.loads((ROOT / "oasis" / "data" / "stock_snapshot_dept.json")
                       .read_text(encoding="utf-8"))
    mar = {norm(k): v for k, v in build_margin.load_or_derive()[0].items()}
    if demand == "pipeline":
        _p = pipeline_ads()
        ads = {k: {"new_ads": v} for k, v in _p.items()}
    else:
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
    args = [a for a in argv[1:]]
    # Default is the pipeline's own demand. The legacy file stays reachable
    # for a deliberate cross-check, never by accident.
    # THIS module measures against the pipeline. The shared build_book keeps
    # the snapshot default for the callers that must stay offline.
    demand = "snapshot" if "--demand=snapshot" in args else "pipeline"
    mults = [float(a) for a in args if not a.startswith("--")] or [0.0, 0.5, 1.0, 2.0]

    print("Building the real book…")
    book = build_book(demand)
    # SAY WHICH BOOK. Two measurements of one shop lived in this repo with
    # nothing in either output naming the source, and the same silence over
    # two POS databases cost an afternoon: identical log lines, a funnel that
    # moved by 7,589 SKUs, and no way to tell which file produced which.
    src = ("the pipeline (POS-weighted, as the shipped run measures it)"
           if demand == "pipeline"
           else "corrected_ads_from_pos.json (2026-02-11, months_active<=6)")
    print(f"  demand from {src}")
    if demand == "snapshot":
        print("  !! that window ENDS AFTER a 2025-12-09 as-of date; absolute "
              "figures below are not the shipped pipeline's")
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
