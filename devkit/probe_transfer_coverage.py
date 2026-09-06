"""Measured probe: at what donor stock level does the REAL transfer engine
actually move a unit, for the near-zero-ADS population named in gap 2/3
(MARTELL XXO 0.03/day, CHIVAS REGAL 12YO 0.17/day, HENNESSY VS 0.31/day)?

Uses the real ConsolidatedTransferService / FulfillmentDecider classes,
unmodified, against a minimal synthetic 2-store network built only to
exercise the eligibility arithmetic (_excess_units, _releasable_transfer_qty,
the <3d rescue pass, find_donors). This is NOT a claim about real store
stock levels -- those are not observable from the data in this repo for
these SKUs. It answers a narrower, answerable question: "given a donor
holding N units of a SKU this slow, does the engine's own math ever
release a transfer?"

Read-only: writes nothing to any engine file or the registry.
"""
from __future__ import annotations
import os, sys, json

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService

SKUS = [
    ("MARTELL XXO COGNAC", 0.03),
    ("CHIVAS REGAL 12YO", 0.17),
    ("HENNESSY VS", 0.31),
]

RECIPIENT = "ORG_A"
DONOR = "ORG_B"


def make_stock(ads: float, donor_stock: float, recipient_stock: float = 0.0, name="SKU"):
    return {
        RECIPIENT: [{
            "itm_cd": name, "item_code": name, "product_name": name,
            "avg_daily_sales": ads, "current_stocks": recipient_stock,
            "selling_price": 3000.0, "cost_price": 2200.0,
            "department": "SPIRITS", "supplier_name": "TEST SUPPLIER",
            "estimated_delivery_days": 3, "is_fresh": False,
        }],
        DONOR: [{
            "itm_cd": name, "item_code": name, "product_name": name,
            "avg_daily_sales": ads, "current_stocks": donor_stock,
            "selling_price": 3000.0, "cost_price": 2200.0,
            "department": "SPIRITS", "supplier_name": "TEST SUPPLIER",
            "estimated_delivery_days": 3, "is_fresh": False,
        }],
    }


def main():
    results = []
    for name, ads in SKUS:
        row = {"sku": name, "ads_per_day": ads}
        for donor_stock in [1, 2, 3, 4, 5, 8, 12]:
            stock = make_stock(ads, donor_stock, recipient_stock=0.0, name=name)
            svc = ConsolidatedTransferService(
                org_names={RECIPIENT: "Store A (out of stock)", DONOR: "Store B (donor)"},
                stock_data=stock,
                registry_path=None,
                data_dir=None,
            )
            plan = svc.optimize_network(store_orders={RECIPIENT: [], DONOR: []})
            moved_qty = sum(t.qty for t in plan.transfers if t.itm_cd == name)
            n_shortfalls_seen = sum(1 for d in plan.decisions)
            row[f"donor_stock_{donor_stock}"] = {
                "transfer_qty": moved_qty,
                "n_decisions": n_shortfalls_seen,
                "decisions": [d.decision for d in plan.decisions],
            }
        results.append(row)

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    main()
