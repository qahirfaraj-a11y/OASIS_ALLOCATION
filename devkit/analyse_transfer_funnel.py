"""Where do 42,000 store-SKU pairs turn into 215 transfer lines?

A 14-store x 3,000-SKU network producing a couple of hundred single-unit
recommendations is not a plausible retail answer, and there are two candidate
explanations that must be told apart before either is acted on:

  DATA   the seeded network genuinely has no imbalance to find, or
  METHOD the gates in the engine eliminate imbalance that is really there.

This walks the SAME gates the engine walks, in the same order, counting what
survives each one. Whatever kills the volume shows up as the step where the
count collapses.

It also checks THRESHOLD COHERENCE, which the funnel alone cannot show: the
engine holds several independent numbers that only make sense relative to each
other -- the deficit trigger, the fill target, the safety floor, the donor
overstock gate, and the donor eligibility ratio. If the donor bar sits above
what the plan ever stocks, no amount of good data produces a donor.

Read-only. Runs against whatever the OdooAdapter can see.

Usage:
    python devkit/analyse_transfer_funnel.py
    python devkit/analyse_transfer_funnel.py --source seed   # skip Odoo, use the file
"""

from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, ".."))
sys.path.insert(0, os.path.join(REPO, "connectors", "odoo"))

from oasis.logic.consolidated_transfer_service import (          # noqa: E402
    ConsolidatedTransferService as CTS)
from oasis.logic.fulfillment_decider import _is_fresh_department  # noqa: E402

SEED = os.path.join(REPO, "connectors", "odoo", "store_network_seed.json")


def bar(n, total, width=34):
    filled = int(width * n / total) if total else 0
    return "#" * filled + "." * (width - filled)


def step(label, n, total, note=""):
    pct = 100.0 * n / total if total else 0.0
    print(f"  {bar(n, total)}  {n:>7,}  {pct:5.1f}%  {label}")
    if note:
        print(f"  {' ' * 34}                   ^ {note}")


def load_from_odoo():
    from oasis.logic.odoo_adapter import OdooAdapter
    from verify_store_network import fetch_with_retry
    seed = json.load(open(SEED, encoding="utf-8"))
    a = OdooAdapter(url=os.getenv("ODOO_URL", "http://localhost:8069"),
                    db=os.getenv("ODOO_DB", "oasis"),
                    user=os.getenv("ODOO_USER", "admin"),
                    password=os.getenv("ODOO_PASSWORD", "admin"))
    data = {s["code"]: fetch_with_retry(a, s["code"]) for s in seed["stores"]}
    return seed, data


def load_from_seed():
    """Reconstruct the adapter's product shape from the seed file + depletion."""
    # the SAME age rule the seeder uses, imported rather than restated, or this
    # analysis would describe a network that is not the one in Odoo
    from seed_store_network import receipt_age, REPLENISHMENT_CYCLE_DAYS
    seed = json.load(open(SEED, encoding="utf-8"))
    cat = {c["sku"]: c for c in seed["catalogue"]}
    data = {}
    for st in seed["stores"]:
        rows = []
        for r in st["stock_profile"]:
            c = cat.get(r["sku"], {})
            ads, plan = float(r["ads"]), float(r["qty"])
            age = receipt_age(st["code"], r["sku"], ads, plan,
                              REPLENISHMENT_CYCLE_DAYS)
            if ads > 0 and plan > 0:
                stock = float(int(round(max(0.0, plan - ads * age))))
            else:
                stock = plan
            rows.append({
                "item_code": r["sku"], "product_name": r["sku"],
                "current_stocks": stock, "avg_daily_sales": ads,
                "selling_price": c.get("price", 0.0), "cost_price": c.get("cost", 0.0),
                "department": c.get("department", "GENERAL"),
                "supplier_name": c.get("supplier", ""), "estimated_delivery_days": 7,
                "uom": "EA", "is_fresh": _is_fresh_department(c.get("department", "")),
                "last_days_since_last_delivery": age,
            })
        data[st["code"]] = rows
    return seed, data


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--source", choices=("odoo", "seed"), default="odoo")
    p.add_argument("--pull-deficit-days", type=float, default=7.0)
    args = p.parse_args(argv)

    seed, data = load_from_odoo() if args.source == "odoo" else load_from_seed()
    names = {s["code"]: s["name"] for s in seed["stores"]}
    coords = {s["code"]: {"lat": s["latitude"], "lon": s["longitude"]}
              for s in seed["stores"]}
    cts = CTS(org_names=names, stock_data=data, distance_map=coords)
    nmap = cts.network_map

    print("=" * 74)
    print(f"TRANSFER FUNNEL  ({args.source}, {len(data)} stores)")
    print("=" * 74)

    pairs = [(org, p) for org, prods in data.items() for p in prods
             if str(p.get("item_code"))]
    total = len(pairs)

    # ── PULL side ─────────────────────────────────────────────────────────
    print("\nPULL -- a store is short and someone else has spare\n")
    step("store-SKU pairs in the network", total, total)

    selling = [(o, p) for o, p in pairs if float(p["avg_daily_sales"] or 0) > 0]
    step("with any demand (ADS > 0)", len(selling), total,
         "no demand = never a deficit, and fully 'excess' as a donor")

    def cover(p):
        ads = float(p["avg_daily_sales"] or 0)
        return (float(p["current_stocks"] or 0) / ads) if ads > 0 else 999.0

    deficits = [(o, p) for o, p in selling if cover(p) < args.pull_deficit_days]
    step(f"below the deficit trigger ({args.pull_deficit_days:g}d cover)",
         len(deficits), total, "these are the ONLY stores the PULL pass serves")

    # shortfall against the fill target
    tgt = cts.target_cover_days
    with_short = []
    for o, p in deficits:
        ads = float(p["avg_daily_sales"] or 0)
        short = ads * tgt - float(p["current_stocks"] or 0)
        if short >= 0.1:
            with_short.append((o, p, short))
    step(f"with a real shortfall vs the {tgt:g}d target",
         len(with_short), total)

    # is the SKU held anywhere else at all?
    has_other = [x for x in with_short if len(nmap._index.get(x[1]["item_code"], [])) > 1]
    step("SKU exists at another store", len(has_other), total)

    # donor eligibility, gate by gate
    any_excess = donor_ok = pool_ok = 0
    blocked_by_ratio = blocked_by_excess = blocked_by_pool = 0
    for o, p, short in has_other:
        itm = p["item_code"]
        states = [s for s in nmap._index.get(itm, []) if s.org_cd != o]
        if any(s.excess > 0 for s in states):
            any_excess += 1
        else:
            blocked_by_excess += 1
            continue
        elig = []
        for s in states:
            if s.excess <= 0:
                continue
            ratio = 2.0
            if s.avg_daily_sales > 5.0:
                ratio = 1.5
            elif s.avg_daily_sales <= 1.0:
                ratio = 2.5
            if s.current_stock >= s.safety_stock * ratio:
                elig.append(s)
        if elig:
            donor_ok += 1
        else:
            blocked_by_ratio += 1
            continue
        if any(s.excess * cts.RELEASE_FRACTION >= 1.0 for s in elig):
            pool_ok += 1
        else:
            blocked_by_pool += 1

    step("some other store has excess > 0", any_excess, total,
         f"{blocked_by_excess:,} lost here -- nobody is over the overstock gate")
    step("a donor also clears the eligibility ratio", donor_ok, total,
         f"{blocked_by_ratio:,} lost here -- stock < safety x 1.5/2.0/2.5")
    step("that donor can release a WHOLE unit", pool_ok, total,
         f"{blocked_by_pool:,} lost here -- pool = excess x "
         f"{cts.RELEASE_FRACTION} is under 1 unit")

    scan = cts.scan_network_opportunities(pull_deficit_days=args.pull_deficit_days)
    pull = [o for o in scan.opportunities if o.type == "PULL"]
    push = [o for o in scan.opportunities if o.type == "PUSH"]
    step("PULL lines the engine actually emits", len(pull), total,
         "above the previous line only because rounding CEILS sub-unit shares")

    # ── donor supply side ─────────────────────────────────────────────────
    print("\nDONOR SUPPLY -- who is allowed to give\n")
    states = [s for lst in nmap._index.values() for s in lst]
    n = len(states)
    step("store-SKU states indexed", n, n)
    over30 = [s for s in states if s.avg_daily_sales > 0
              and s.current_stock / s.avg_daily_sales > 30]
    step("cover > 30d (the overstock gate for dry goods)", len(over30), n)
    exc = [s for s in states if s.excess > 0]
    step("excess > 0", len(exc), n)
    whole = [s for s in exc if s.excess * cts.RELEASE_FRACTION >= 1.0]
    step("releasable pool >= 1 whole unit", len(whole), n,
         "PUSH skips anything below this outright")
    print(f"\n  PUSH lines emitted: {len(push):,}")

    # ── threshold coherence ───────────────────────────────────────────────
    print("\n" + "=" * 74)
    print("THRESHOLD COHERENCE -- do the constants agree with each other?")
    print("=" * 74)
    covers = sorted(cover(p) for o, p in selling)
    med = covers[len(covers) // 2]
    p90 = covers[int(len(covers) * 0.9)]
    print(f"\n  network cover:  median {med:.1f}d   p90 {p90:.1f}d   "
          f"max {covers[-1]:.0f}d")
    print(f"""
  deficit trigger      < {args.pull_deficit_days:g}d      a store is 'short' below this
  fill target          = {tgt:g}d      restore to here
  safety floor         = 14d     excess only counts above this
  overstock gate       > 30d     donor must ALSO be above this (dry goods)
  buffer above safety  + 7d      and this much again before any excess counts
  eligibility ratio    x 1.5-2.5 stock >= safety x ratio, i.e. 21-35d cover

  EFFECTIVE DONOR BAR: a store must hold ~30d of cover to give anything away,
  while the plan only ever stocks {tgt:g}d and the trigger fires at
  {args.pull_deficit_days:g}d. Anything between {args.pull_deficit_days:g}d and 30d
  neither asks nor gives -- it is inert. On this network that dead band holds
  {sum(1 for c in covers if args.pull_deficit_days <= c <= 30) / len(covers) * 100:.0f}%
  of all selling store-SKU pairs.""")

    print("\n  Read: for a donor to exist at all, some store must hold more than")
    print("  TWICE the cover the plan targets. A network stocked to its own plan")
    print("  cannot produce donors -- only one stocked well past it can.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
