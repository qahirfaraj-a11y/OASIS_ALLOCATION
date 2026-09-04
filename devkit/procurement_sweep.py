"""The procurement team, run as agents, across the whole network.

WHAT THIS IS
    One agent per store, each running the real ordering engine over the whole
    SKU universe, under a grid of stock conditions it might actually find. Every
    decision it makes — quantity, and every term the quantity was built from —
    is written to the decision ledger.

    That is not a simulation of demand. Nothing here pretends to know what will
    sell. It is a systematic interrogation of what the ENGINE decides, which is
    a different and more answerable question: given this line, this supplier and
    this stock position, what does OASIS buy, and does that answer behave the
    way an order policy is supposed to behave?

WHY A GRID RATHER THAN A HISTORY
    Replaying history only ever visits the stock positions the business happened
    to be in. The interesting failures live at the edges — a line at zero with a
    long lead time, a line already carrying twice its order-up-to level, a fresh
    item whose shelf life clamps below its cycle stock. A grid visits them on
    purpose.

WHAT IS OBSERVED AND WHAT IS NOT
    d          observed  ADS from six months of POS
    L, sigma_L observed  per vendor, PO date to GRN date
    R          observed  the client's declared calendar, cleaned
    sigma_d    ASSUMED   no per-period sales series exists, so cv falls back to
                         the engine default. Every decision records it, so
                         nothing downstream can mistake it for measured.

The ledger rows this writes are what Loop B attributes against.
"""
from __future__ import annotations

import argparse
import json
import os
import random
import sys
import tempfile
import time
from collections import Counter, defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import stock_ledger as SL                       # noqa: E402
from devkit.methodology.traps import join_match_rate        # noqa: E402
from oasis.logic import order_up_to as OU                   # noqa: E402
from oasis.logic.decision_ledger import DecisionLedger      # noqa: E402

PATTERNS = ROOT / "oasis" / "data" / "supplier_lead_patterns.json"
ESTATE = ROOT / "stores_network.json"

#: Stock positions worth visiting, as multiples of the order-up-to level.
STOCK_GRID = (0.0, 0.25, 0.5, 0.75, 1.0, 1.5, 2.0)
ON_ORDER_GRID = (0.0, 0.5)
DEFAULT_CV = 0.4


def load_estate(limit=None):
    raw = json.loads(ESTATE.read_text(encoding="utf-8")).get("stores", [])
    out = [{"id": s.get("store_id"), "name": s.get("name"),
            "scale": float(s.get("demand_scale_factor") or 1.0),
            "category": s.get("store_category", "")} for s in raw]
    return out[:limit] if limit else out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=None)
    ap.add_argument("--skus", default="400", help="count, or 'all'")
    ap.add_argument("--stores", type=int, default=5)
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--db", default=None)
    ap.add_argument("--sigma-source", choices=("observed", "default"),
                    default="observed")
    a = ap.parse_args(argv)
    random.seed(a.seed)

    led = SL.load(local_copy=Path(a.xlsx) if a.xlsx else None)
    patterns = (json.loads(PATTERNS.read_text(encoding="utf-8"))
                if (PATTERNS.exists() and a.sigma_source == "observed") else {})
    schedule = OU.load_review_schedule(str(ROOT))
    estate = load_estate(a.stores)

    # The universe: a line we can price the demand of AND name the supplier for.
    vendor_of = {}
    for item, rs in led.receipts.items():
        if rs:
            vendor_of[item] = Counter(r.vendor for r in rs).most_common(1)[0][0]
    universe = sorted(set(led.ads) & set(vendor_of))
    if a.skus != "all":
        n = min(int(a.skus), len(universe))
        universe = random.sample(universe, n)

    print(f"  universe {len(universe):,} SKUs · {len(estate)} stores · "
          f"{len(STOCK_GRID)}x{len(ON_ORDER_GRID)} stock conditions")
    print(f"  sigma_L source: {a.sigma_source} "
          f"({len(patterns):,} vendor patterns loaded)")
    print(f"  review schedule: {len(schedule):,} suppliers with a declared day")

    # The engine keys patterns and schedule on the supplier NAME. Assert both
    # joins before running twenty thousand decisions through them — a lookup
    # that silently misses turns this whole sweep into a measurement of the
    # default constant.
    keys = sorted({(v.split(" - ", 1)[1] if " - " in v else v).upper().strip()
                   for v in vendor_of.values()})
    if patterns:
        print("  " + str(join_match_rate(keys, list(patterns),
                                         "supplier -> lead pattern", floor=0.50)))
    print("  " + str(join_match_rate(keys, list(schedule),
                                     "supplier -> review schedule", floor=0.30)))

    # The agents write to a local staging ledger and the run is merged into the
    # durable one at the end: one transaction instead of twenty thousand round
    # trips across a network mount.
    target = a.db
    stage = Path(tempfile.gettempdir()) / f"oasis_sweep_{os.getpid()}.db"
    if stage.exists():
        stage.unlink()
    ledger = DecisionLedger(stage)
    t0 = time.time()
    rows, stats = [], Counter()
    for store in estate:
        for item in universe:
            d0 = led.ads.get(item) or 0.0
            if d0 <= 0:
                continue
            vendor = vendor_of[item]
            # The supplier name the schedule keys on is the part after the code.
            sup = vendor.split(" - ", 1)[1] if " - " in vendor else vendor
            product = {
                "avg_daily_sales": d0 * store["scale"],
                "demand_cv": DEFAULT_CV,
                "supplier_name": sup,
                "lead_time_days": (patterns.get(sup.upper().strip(), {})
                                   .get("lead_time_days") or 3),
                "current_stock": 0.0, "on_order_qty": 0.0, "pack_size": 1,
            }
            base = OU.recommend(product, schedule, patterns)
            S = float(base.get("S") or 0)
            if S <= 0:
                stats["zero_S"] += 1
                continue
            for f in STOCK_GRID:
                for g in ON_ORDER_GRID:
                    product["current_stock"] = S * f
                    product["on_order_qty"] = S * g
                    t = OU.recommend(product, schedule, patterns)
                    rows.append({
                        "org": store["id"], "entity": item,
                        "quantity": t["quantity"],
                        "inputs": {"store": store["name"],
                                   "scale": store["scale"],
                                   "vendor": vendor, "supplier_key": sup,
                                   "on_hand_mult": f, "on_order_mult": g,
                                   "on_hand": product["current_stock"],
                                   "on_order": product["on_order_qty"],
                                   "d": t["d"], "sigma_d": t["sigma_d"],
                                   "cv_is_assumed": True,
                                   "S": t["S"], "S_unclamped": t["S_unclamped"],
                                   "R": t["R"], "L": t["L"], "P": t["P"],
                                   "sigma_lead": t["sigma_lead"], "z": t["z"],
                                   "cycle_stock": t["cycle_stock"],
                                   "safety_stock": t["safety_stock"],
                                   "clamped": t["clamped"],
                                   "cover_days": t["cover_days"]},
                    })
                    stats["decisions"] += 1
                    if t["quantity"] <= 0:
                        stats["zero_qty"] += 1

    params = {"model": "order_up_to", "z": OU.z_score(),
              "service_level": OU.service_level(),
              "default_cv": DEFAULT_CV, "sigma_source": a.sigma_source,
              "stock_grid": list(STOCK_GRID), "on_order_grid": list(ON_ORDER_GRID),
              "schedule_suppliers": len(schedule),
              "vendor_patterns": len(patterns)}
    run_id = ledger.log_many("order", rows, params, source="procurement_sweep",
                             engine_version="order_up_to")
    ledger.close()
    durable = DecisionLedger(target)
    merged = durable.merge_from(stage)
    try:
        stage.unlink()
    except OSError:
        pass
    ledger = durable
    dt = time.time() - t0
    print(f"  {stats['decisions']:,} decisions written in {dt:.1f}s "
          f"({merged:,} merged into the durable ledger) "
          f"({stats['zero_qty']:,} were 'order nothing', "
          f"{stats['zero_S']:,} SKUs had no order-up-to level)")
    print(f"  ledger: {ledger.path}")
    print(json.dumps({"run": run_id[0][:12] if run_id else None,
                      "decisions": stats["decisions"],
                      "summary": ledger.summary()}, default=str)[:400])
    ledger.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
