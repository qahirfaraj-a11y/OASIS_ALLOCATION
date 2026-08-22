"""Seed a throwaway Odoo with REAL purchase-order -> goods-receipt history.

DEV TOOLING - devkit/, never ships.

The 14-store depot cannot exercise the supplier-rhythm derivation: its receipts
were written as bare stock moves, so no picking, no partner, and nothing that
says who delivered. That is a faithful reproduction of one kind of customer and
a useless test of the derivation.

So this builds the other kind: suppliers with genuine purchase orders, confirmed
and received on a chosen cadence, with the receipt dates backdated so a rhythm
actually exists to measure. Then `derive()` runs against a real Odoo rather than
a fake, which is the only way to find out whether the field shapes assumed by
the derivation - partner_id and purchase_id on stock.picking,
picking_type_id.warehouse_id - are what Odoo really provides.

Run against a SCRATCH database only. It creates suppliers, products, purchase
orders and validated receipts.
"""

from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from oasis.logic.odoo_adapter import OdooAdapter          # noqa: E402

#: supplier -> (cadence in days, how many deliveries, stated lead time)
SUPPLIERS = {
    "Rhythm Dairy": (7, 10, 2),        # weekly, reliable
    "Rhythm Bakery": (1, 20, 1),       # daily
    "Rhythm Dry Goods": (14, 8, 5),    # fortnightly
}


def _ex(a, model, method, args, kw=None):
    return a._ex(model, method, args, kw or {})


def seed(a, start_days_ago=400):
    warehouses = _ex(a, "stock.warehouse", "search_read", [[]],
                     {"fields": ["code", "in_type_id", "lot_stock_id"]})
    if not warehouses:
        raise SystemExit("no warehouses in this database")
    print(f"  warehouses: {[w['code'] for w in warehouses]}")

    made = 0
    for name, (cadence, count, lead) in SUPPLIERS.items():
        partner = _ex(a, "res.partner", "create",
                      [{"name": name, "supplier_rank": 1}])
        product = _ex(a, "product.product", "create",
                      [{"name": f"{name} Item", "default_code": f"RH-{made}",
                        "type": "product", "purchase_ok": True,
                        "list_price": 100.0, "standard_price": 60.0,
                        "seller_ids": [(0, 0, {"partner_id": partner,
                                               "delay": lead})]}])
        # deliver into a DIFFERENT warehouse per supplier where possible, so
        # per-store cadence has something to distinguish
        wh = warehouses[made % len(warehouses)]
        print(f"  {name}: every {cadence}d x{count} into {wh['code']}")

        for i in range(count):
            when = datetime.now() - timedelta(days=start_days_ago - i * cadence)
            if when > datetime.now():
                break
            ordered = when - timedelta(days=lead)
            po = _ex(a, "purchase.order", "create", [{
                "partner_id": partner,
                "date_order": ordered.strftime("%Y-%m-%d %H:%M:%S"),
                "picking_type_id": wh["in_type_id"][0],
                "order_line": [(0, 0, {"product_id": product,
                                       "name": "seeded",
                                       "product_qty": 10,
                                       "price_unit": 60.0,
                                       "date_planned": when.strftime("%Y-%m-%d %H:%M:%S")})],
            }])
            _ex(a, "purchase.order", "button_confirm", [[po]])
            picks = _ex(a, "stock.picking", "search",
                        [[["purchase_id", "=", po]]])
            for pid in picks:
                moves = _ex(a, "stock.move", "search", [[["picking_id", "=", pid]]])
                for m in moves:
                    # setting quantity_done avoids Odoo 16's immediate-transfer
                    # wizard, which cannot be driven over XML-RPC
                    _ex(a, "stock.move", "write", [[m], {"quantity_done": 10}])
                _ex(a, "stock.picking", "button_validate", [[pid]])
                # backdate the completion so a cadence exists to measure
                _ex(a, "stock.picking", "write",
                    [[pid], {"date_done": when.strftime("%Y-%m-%d %H:%M:%S")}])
            made += 1
    return made


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--db", required=True, help="SCRATCH database only")
    p.add_argument("--url", default=os.getenv("ODOO_URL", "http://localhost:8069"))
    args = p.parse_args(argv)

    if args.db == "oasis":
        raise SystemExit("refusing to seed the depot database 'oasis' — "
                         "use a scratch database")

    a = OdooAdapter(url=args.url, db=args.db, user="admin", password="admin")
    print(f"seeding purchase->receipt history into {args.db}")
    n = seed(a)
    print(f"  created {n} received purchase orders")

    from oasis.logic.odoo_supplier_rhythm import derive, format_report
    print(format_report(derive(adapter=a, days=730, write=False)))

    report = derive(adapter=a, days=730, write=False)
    print("  DERIVED CADENCE vs WHAT WAS SEEDED")
    ok = True
    for name, (cadence, _count, _lead) in SUPPLIERS.items():
        rec = report["patterns"].get(name.lower())
        got = rec["median_gap_days"] if rec else None
        flag = "ok " if got == cadence else "MISMATCH"
        if got != cadence:
            ok = False
        print(f"    [{flag}] {name:<20} seeded {cadence:>3}d   derived "
              f"{got if got is not None else '-':>3}"
              + (f"   lead {rec['estimated_delivery_days']} "
                 f"({rec['lead_time_source']})" if rec else ""))
    for site, sup in sorted(report["per_store"].items()):
        print(f"    per-store {site}: "
              + ", ".join(f"{k}={v['median_gap_days']}d" for k, v in sorted(sup.items())))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
