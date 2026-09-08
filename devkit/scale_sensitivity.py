"""Which constants stop adapting when the store gets smaller?

THE OBSERVATION TO EXPLAIN
    Service rises monotonically with store size, 89.76% at 0.3x to 94.91% at
    2.5x, and dead-stock suppression runs at 20.8% of the book on the smallest
    stores against 9.9% on average. The formula is identical at every size, so
    the size dependence has to come from something in the path that is NOT
    expressed per-store.

THE FRAME
    A threshold in DAYS is scale-free: 60 days of cover means the same thing
    at any d. A threshold in UNITS or SHILLINGS is not: a 0.3x store generates
    0.3x the order quantity and 0.3x the order value from the same SKU, so a
    fixed KES 100 minimum is roughly three times as likely to reject it.

    Some absolute limits are physical and must stay absolute -- a supplier's
    pack size is a fact about the pallet, not a policy. The ones worth finding
    are the POLICY thresholds that were set at one store's scale and then
    applied to all of them.

WHAT THIS COUNTS
    Per store scale, how many lines and how much money each gate removes, so
    the size dependence can be attributed to a named constant rather than to
    "discreteness" in general.

USAGE
    python devkit/scale_sensitivity.py
"""
from __future__ import annotations

import copy
import os
import re
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.order_model_compare import build_book, DATA_DIR   # noqa: E402
from oasis.logic import order_up_to as ou                     # noqa: E402

SCALES = (0.3, 0.5, 1.0, 2.0, 2.5)


def classify(rec) -> str:
    """Which gate removed this line, from the engine's own reasoning."""
    why = str(rec.get("reasoning") or "")
    if rec.get("auto_order_suppressed"):
        return "60-day one-pack rule (MAX_AUTO_ORDER_COVER_DAYS)"
    if "MOQ" in why and "units <" in why:
        return "SKU MOQ (pack size / moq_floor)  [physical]"
    if "MOP" in why or "value KES" in why:
        return "SKU MOP (KES 100 dry / 200 fresh)  [policy]"
    if "Supplier" in why and ("MOT" in why or "batch" in why.lower()):
        return "supplier MOT (10 units / KES 5,000)  [policy]"
    if "GUARD:" in why:
        m = re.search(r"\[GUARD: ([^(\]]+)", why)
        return "guard: " + (m.group(1).strip() if m else "?")
    return "other"


def main() -> int:
    from oasis.logic.simulation_bridge import SimulationOrderUtil

    book = build_book()
    util = SimulationOrderUtil(DATA_DIR)
    enriched = util.prepare_sku_data(copy.deepcopy(book))
    base = {p["sku"]: float(p.get("current_stock") or 0) for p in book}
    cost = {p["sku"]: p["unit_cost"] for p in book}
    print(f"{len(book):,} SKUs\n")

    # R distribution, for the "never reviewed in a month" question.
    os.environ["OASIS_ORDER_MODEL"] = "order_up_to"
    arm = copy.deepcopy(enriched)
    for p in arm:
        p["current_stock"] = base.get(p.get("sku"), 0.0)
    probe = util.finalize_orders(util.calculate_order_quantity(arm, use_real_date=True))
    os.environ.pop("OASIS_ORDER_MODEL", None)
    rsrc = defaultdict(int)
    long_r = []
    for r in probe:
        t = r.get("order_up_to_terms") or {}
        R = float(t.get("R") or 0)
        if R > 28:
            long_r.append((r.get("sku"), R, r.get("supplier_name")))
            rsrc[ou._r_source(r.get("supplier_name") or "", util._review_schedule)] += 1
    print(f"lines with R > 28 days: {len(long_r):,}")
    print(f"  where that R came from: {dict(rsrc)}")
    if long_r:
        import statistics
        print(f"  median R {statistics.median(x[1] for x in long_r):.1f}d, "
              f"max {max(x[1] for x in long_r):.1f}d")
    print()

    rows = {}
    for scale in SCALES:
        a = copy.deepcopy(enriched)
        for p in a:
            p["avg_daily_sales"] = float(p.get("avg_daily_sales") or 0) * scale
            p["current_stock"] = base.get(p.get("sku"), 0.0) * scale
        os.environ["OASIS_ORDER_MODEL"] = "order_up_to"
        recs = util.finalize_orders(util.calculate_order_quantity(a, use_real_date=True))
        gate = util.apply_minimum_order_gate(recs)
        os.environ.pop("OASIS_ORDER_MODEL", None)

        counts = defaultdict(lambda: [0, 0.0])
        for r in recs:
            if r.get("auto_order_suppressed"):
                counts[classify(r)][0] += 1
        for r in gate.get("transfer_recs") or []:
            k = classify(r)
            counts[k][0] += 1
            counts[k][1] += float(r.get("recommended_quantity") or 0) * \
                cost.get(r.get("sku") or r.get("item_code"), 0.0)
        rows[scale] = {
            "counts": counts,
            "po": len(gate.get("po_recs") or []),
            "dropped": len(gate.get("transfer_recs") or []),
        }
        print(f"  scale {scale:<5} PO lines {rows[scale]['po']:>6,}   "
              f"dropped by a gate {rows[scale]['dropped']:>6,}")

    keys = sorted({k for s in SCALES for k in rows[s]["counts"]})
    print("\n" + "=" * 90)
    print("LINES REMOVED BY EACH GATE, BY STORE SIZE")
    print("=" * 90)
    print(f"  {'gate':<50}" + "".join(f"{s:>8}x" for s in SCALES))
    for k in keys:
        print(f"  {k[:50]:<50}" +
              "".join(f"{rows[s]['counts'][k][0]:>9,}" for s in SCALES))

    print("\n  as a share of the 15,037-line book:")
    for k in keys:
        print(f"  {k[:50]:<50}" +
              "".join(f"{100*rows[s]['counts'][k][0]/len(book):>8.1f}%" for s in SCALES))

    print("\n" + "=" * 90)
    print("READING IT")
    print("=" * 90)
    print("  A gate whose row FALLS as the store grows is scale-dependent: the")
    print("  same SKU passes at 2.5x and is refused at 0.3x. A flat row is")
    print("  scale-free and is not part of the small-store gap.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
