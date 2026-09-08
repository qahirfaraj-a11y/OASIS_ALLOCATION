"""Every policy constant in the ordering path, perturbed one at a time.

WHY
    The order-up-to MATHEMATICS is scale-free: S = d(R+L) + z*sigma_P, and the
    feasibility test cancels d from both sides, so a store at 0.3x and one at
    2.5x get proportional answers from the formula. The size dependence that
    costs small stores five points of service therefore lives in the POLICY
    layer wrapped around it -- the constants that decide whether to ACT on a
    quantity the formula already produced.

    Naming that layer is not the same as measuring it. This perturbs each
    constant one at a time and reports what it actually moves, so the fix goes
    to whichever one carries the effect rather than to whichever is easiest to
    argue about. Two changes made on argument alone moved 12 lines of 1,196.

WHAT IS MEASURED
    Lines that end up on a purchase order at a SMALL store (0.3x) and a LARGE
    one (2.5x), and the gap between them. A constant that lifts the small
    store without lifting the large one is closing the size gap; one that
    lifts both is a general loosening and should be argued on its own merits;
    one that moves neither is inert and should stop being discussed.

    Order-book effects only -- no simulation. A constant that cannot change
    what gets ordered cannot change service, so this is the cheap filter that
    says which constants are worth a 90-day run.

USAGE
    python devkit/policy_sensitivity.py
"""
from __future__ import annotations

import copy
import importlib
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.order_model_compare import build_book, DATA_DIR   # noqa: E402
from oasis.logic import order_up_to as ou                     # noqa: E402

SMALL, LARGE = 0.3, 2.5

#: (label, env var, value) -- module constants, read at import, so the module
#: is reloaded between cases.
ENV_CASES = [
    ("MAX_AUTO_ORDER_COVER_DAYS 60 -> 120", "OASIS_MAX_AUTO_COVER", "120"),
    ("MAX_AUTO_ORDER_COVER_DAYS 60 -> 365", "OASIS_MAX_AUTO_COVER", "365"),
    ("AUTO_ORDER_COVER_CYCLES 2 -> 6", "OASIS_MAX_AUTO_COVER_CYCLES", "6"),
    ("service level 0.90 -> 0.95 (z 1.28->1.64)", "OASIS_SERVICE_LEVEL", "0.95"),
    ("service level 0.90 -> 0.85 (z 1.28->1.04)", "OASIS_SERVICE_LEVEL", "0.85"),
    ("phi 0.40 -> 0.20", "OASIS_DEMAND_OVERDISPERSION", "0.20"),
    ("phi 0.40 -> 0.80", "OASIS_DEMAND_OVERDISPERSION", "0.80"),
]

#: (label, threshold key, value) -- the MOQ/MOP/MOT gate's own dials.
THRESHOLD_CASES = [
    ("SKU MOP dry 100 -> 0", "min_item_value_dry_kes", 0.0),
    ("SKU MOP fresh 200 -> 0", "min_item_value_fresh_kes", 0.0),
    ("supplier MOT units 10 -> 0", "min_order_units", 0),
    ("supplier MOT value 5,000 -> 0", "min_order_value_kes", 0),
    ("key SKU boost 0.20 -> 0", "key_sku_boost_pct", 0.0),
]


def po_lines(book, enriched, scale, thresholds=None):
    """PO lines and value at one store size, through the production path."""
    from oasis.logic.simulation_bridge import SimulationOrderUtil
    util = SimulationOrderUtil(DATA_DIR, thresholds=thresholds)
    base = {p["sku"]: float(p.get("current_stock") or 0) for p in book}
    cost = {p["sku"]: p["unit_cost"] for p in book}
    a = copy.deepcopy(enriched)
    for p in a:
        p["avg_daily_sales"] = float(p.get("avg_daily_sales") or 0) * scale
        p["current_stock"] = base.get(p.get("sku"), 0.0) * scale
    os.environ["OASIS_ORDER_MODEL"] = "order_up_to"
    recs = util.finalize_orders(util.calculate_order_quantity(a, use_real_date=True))
    gate = util.apply_minimum_order_gate(recs)
    os.environ.pop("OASIS_ORDER_MODEL", None)
    po = gate.get("po_recs") or []
    return len(po), sum(float(r.get("recommended_quantity") or 0) *
                        cost.get(r.get("sku") or r.get("item_code"), 0.0) for r in po)


def main() -> int:
    from oasis.logic.simulation_bridge import SimulationOrderUtil
    book = build_book()
    util = SimulationOrderUtil(DATA_DIR)
    enriched = util.prepare_sku_data(copy.deepcopy(book))
    print(f"{len(book):,} SKUs | small = {SMALL}x | large = {LARGE}x\n")

    bs, bsk = po_lines(book, enriched, SMALL)
    bl, blk = po_lines(book, enriched, LARGE)
    print(f"BASELINE   small {bs:,} lines / KES {bsk:,.0f}"
          f"   large {bl:,} lines / KES {blk:,.0f}")
    print(f"           size gap {bl-bs:,} lines\n")

    rows = []
    for label, var, val in ENV_CASES:
        os.environ[var] = val
        importlib.reload(ou)
        try:
            s, _ = po_lines(book, enriched, SMALL)
            l, _ = po_lines(book, enriched, LARGE)
        finally:
            os.environ.pop(var, None)
            importlib.reload(ou)
        rows.append((label, s - bs, l - bl, (l - s) - (bl - bs)))
        print(f"  done: {label}")

    for label, key, val in THRESHOLD_CASES:
        th = {key: val}
        s, _ = po_lines(book, enriched, SMALL, thresholds=th)
        l, _ = po_lines(book, enriched, LARGE, thresholds=th)
        rows.append((label, s - bs, l - bl, (l - s) - (bl - bs)))
        print(f"  done: {label}")

    print("\n" + "=" * 92)
    print("WHAT EACH POLICY CONSTANT MOVES  (change in PO lines vs baseline)")
    print("=" * 92)
    print(f"  {'constant':<44}{'small 0.3x':>12}{'large 2.5x':>12}{'size gap':>12}")
    for label, ds, dl, dgap in sorted(rows, key=lambda r: -abs(r[1])):
        print(f"  {label:<44}{ds:>+12,}{dl:>+12,}{dgap:>+12,}")

    print("\n  size gap column: negative = the gap between a large and a small")
    print("  store NARROWS, which is the only column that speaks to the")
    print("  small-store service deficit. A constant that lifts both stores")
    print("  equally leaves the gap untouched.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
