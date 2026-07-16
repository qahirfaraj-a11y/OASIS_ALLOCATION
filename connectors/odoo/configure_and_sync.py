"""
Point the Odoo connector at the hub, seed a little real Odoo activity, and run
the first sync — so the supplier portal shows live Odoo-sourced movement.

Runs on the HOST against Odoo's XML-RPC API (localhost:8069). It:
  1. writes the addon settings (hub URL, ingest token, store code, enable) as
     ir.config_parameter — the same values the Settings -> OASIS Connector UI sets;
  2. seeds a Coca-Cola supplier + a few products and posts real done stock.moves
     to a customer location (sell-through) over the last two weeks;
  3. triggers oasis.sync.run_sync() inside Odoo, which maps + pushes to the hub.

    python configure_and_sync.py            # localhost defaults

Idempotent: products/partner are found-or-created; movements use fresh refs each
run (the hub dedups by source_ref, so re-runs just add new days safely).
Stdlib only (xmlrpc.client).
"""

import argparse
import json
import os
import sys
import xmlrpc.client

HERE = os.path.dirname(os.path.abspath(__file__))
STATE = os.path.join(HERE, ".hub_state.json")

# Odoo reaches the hub by its compose service name, NOT localhost.
HUB_URL_IN_NETWORK = "http://oasis-hub:8700"

PRODUCTS = [
    ("COKE_500", "Coke 500ml", 55.0),
    ("COKE_1L", "Coke 1L", 95.0),
    ("SPRITE_500", "Sprite 500ml", 55.0),
    ("FANTA_500", "Fanta 500ml", 55.0),
]
SUPPLIER_REF = "SUP_COKE"          # must match the hub ownership rule
DEPARTMENT = "Beverages"


def connect(url, db, user, password):
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, user, password, {})
    if not uid:
        raise PermissionError("Odoo auth failed — check db/user/password.")
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

    def ex(model, method, args, kw=None):
        return models.execute_kw(db, uid, password, model, method, args, kw or {})
    return ex, uid


def _find_or_create(ex, model, domain, vals):
    ids = ex(model, "search", [domain], {"limit": 1})
    if ids:
        return ids[0]
    return ex(model, "create", [vals])


def set_addon_config(ex, token, store_code):
    params = {
        "oasis.enabled": "True",
        "oasis.hub_url": HUB_URL_IN_NETWORK,
        "oasis.ingest_token": token,
        "oasis.store_code": store_code,
        "oasis.send_sales": "True",
        "oasis.send_receipts": "True",
    }
    for k, v in params.items():
        ex("ir.config_parameter", "set_param", [k, v])
    print(f"  addon configured -> hub {HUB_URL_IN_NETWORK}, store {store_code}")


def seed_movements(ex):
    # supplier partner whose ref is the ownership key
    partner = _find_or_create(ex, "res.partner", [["ref", "=", SUPPLIER_REF]],
                              {"name": "Coca-Cola Beverages", "ref": SUPPLIER_REF,
                               "supplier_rank": 1})
    categ = _find_or_create(ex, "product.category", [["name", "=", DEPARTMENT]],
                            {"name": DEPARTMENT})
    # locations: internal stock -> customer (a sale / sell-through)
    src = ex("stock.location", "search", [[["usage", "=", "internal"]]], {"limit": 1})
    dst = ex("stock.location", "search", [[["usage", "=", "customer"]]], {"limit": 1})
    if not src or not dst:
        print("  ! could not find internal/customer stock locations — skipping seed")
        return 0
    src, dst = src[0], dst[0]

    prod_ids = {}
    for code, name, price in PRODUCTS:
        pid = ex("product.product", "search", [[["default_code", "=", code]]], {"limit": 1})
        if pid:
            prod_ids[code] = pid[0]
            continue
        tmpl_vals = {"name": name, "default_code": code, "type": "product",
                     "list_price": price, "categ_id": categ}
        pid = ex("product.product", "create", [tmpl_vals])
        prod_ids[code] = pid
        # link the supplier so the connector maps supplier_cd = SUP_COKE
        tmpl = ex("product.product", "read", [[pid], ["product_tmpl_id"]])[0]["product_tmpl_id"][0]
        ex("product.supplierinfo", "create", [{"partner_id": partner,
                                               "product_tmpl_id": tmpl, "price": price}])

    uom = ex("product.product", "read", [[list(prod_ids.values())[0]], ["uom_id"]])[0]["uom_id"][0]
    made = 0
    import itertools
    for day, (code, name, price) in itertools.product(range(1, 15), PRODUCTS):
        qty = 2 + (day * 7 + len(code)) % 38          # deterministic pseudo-spread
        mv = ex("stock.move", "create", [{
            "name": f"OASIS demo {code}",
            "product_id": prod_ids[code],
            "product_uom_qty": qty,
            "product_uom": uom,
            "location_id": src,
            "location_dest_id": dst,
            "date": f"2026-07-{day:02d} 14:30:00",
        }])
        ex("stock.move", "write", [[mv], {"state": "done"}])
        made += 1
    print(f"  seeded {made} done stock moves (sell-through) across 4 SKUs × 14 days")
    return made


def main(argv=None):
    p = argparse.ArgumentParser(description="Configure Odoo connector + seed + sync")
    p.add_argument("--odoo", default=os.getenv("ODOO_URL", "http://localhost:8069"))
    p.add_argument("--db", default=os.getenv("ODOO_DB", "oasis"))
    p.add_argument("--user", default=os.getenv("ODOO_USER", "admin"))
    p.add_argument("--password", default=os.getenv("ODOO_PASSWORD", "admin"))
    p.add_argument("--no-seed", action="store_true")
    args = p.parse_args(argv)

    if not os.path.exists(STATE):
        print("! .hub_state.json not found — run bootstrap_hub.py first.")
        return 1
    state = json.load(open(STATE))
    token, store_code = state["ingest_token"], state["store_code"]

    print(f"-> connecting to Odoo at {args.odoo} (db={args.db})")
    ex, uid = connect(args.odoo, args.db, args.user, args.password)
    print(f"  authenticated as uid {uid}")

    set_addon_config(ex, token, store_code)
    if not args.no_seed:
        seed_movements(ex)

    print("-> running oasis.sync inside Odoo…")
    result = ex("oasis.sync", "run_sync", [])
    print(f"  sync result: {result}")

    portal = state["hub"] + "/portal-app/"
    print("\n" + "=" * 56)
    print(f"  Live. Open the portal:  {portal}")
    print(f"  Login:  {state['supplier_code']} / demo123")
    print("=" * 56)
    return 0


if __name__ == "__main__":
    sys.exit(main())
