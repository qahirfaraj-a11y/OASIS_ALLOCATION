"""
Standalone Odoo → OASIS backfill via the Odoo External API (XML-RPC).

Use this to seed history from an Odoo instance WITHOUT installing the addon
(evaluations, one-off backfills, or servers where you can't add modules). It
authenticates over Odoo's public JSON/XML-RPC endpoint and reuses the exact same
pure ``mapping`` + ``push_client`` code the addon uses, so the payloads are
identical.

    python -m connectors.odoo.xmlrpc_sync \
        --odoo-url https://my.odoo.example --db mydb \
        --user admin --password *** \
        --hub-url https://hub.oasis.example --ingest-token oist_xxx \
        --since 2026-06-01

The Odoo transport is injectable (``models_proxy``) so tests exercise the whole
collect→map→push path against fakes with no network.
"""

import argparse
import logging
import xmlrpc.client
from typing import Callable, List, Optional

from connectors.odoo.oasis_telemetry import mapping, push_client

logger = logging.getLogger("OASIS.Connector.Odoo.XMLRPC")

# execute_kw(model, method, args, kwargs) -> result
ExecKw = Callable[..., object]


def connect(odoo_url: str, db: str, user: str, password: str) -> ExecKw:
    """Authenticate and return a bound execute_kw(model, method, args, kw=...)."""
    common = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/common")
    uid = common.authenticate(db, user, password, {})
    if not uid:
        raise PermissionError("Odoo authentication failed — check db/user/password.")
    models = xmlrpc.client.ServerProxy(f"{odoo_url}/xmlrpc/2/object")

    def execute_kw(model, method, args, kw=None):
        return models.execute_kw(db, uid, password, model, method, args, kw or {})

    return execute_kw


def _build_product_map(execute_kw: ExecKw, product_ids: List[int]) -> dict:
    if not product_ids:
        return {}
    ids = list(set(product_ids))
    fields = ["id", "default_code", "display_name", "categ_id", "list_price",
              "product_brand_id", "seller_ids"]
    # product_brand_id / seller_ids may not exist on all installs; read defensively
    records = execute_kw("product.product", "read", [ids],
                         {"fields": fields}) or []
    out = {}
    for r in records:
        r["name"] = r.get("display_name")
        # resolve a supplier ref from the first supplierinfo, if any
        seller_ids = r.get("seller_ids") or []
        if seller_ids:
            info = execute_kw("product.supplierinfo", "read", [seller_ids[:1]],
                              {"fields": ["partner_id"]}) or []
            if info:
                r["_main_supplier"] = info[0].get("partner_id")
        out[r["id"]] = r
    return out


def collect_sales(execute_kw: ExecKw, since: Optional[str]) -> List[dict]:
    domain = [("qty", "!=", 0)]
    if since:
        domain.append(("write_date", ">", since))
    lines = execute_kw("pos.order.line", "search_read", [domain],
                       {"fields": ["id", "qty", "price_unit", "product_id",
                                   "order_id", "write_date"]}) or []
    pmap = _build_product_map(execute_kw, [l["product_id"][0] for l in lines
                                           if l.get("product_id")])
    movements = []
    for ln in lines:
        if not ln.get("product_id"):
            continue
        prod = pmap.get(ln["product_id"][0])
        if not prod:
            continue
        movements.append(mapping.map_pos_order_line(
            ln, prod, order_date=ln.get("write_date")))
    return movements


def collect_receipts(execute_kw: ExecKw, since: Optional[str]) -> List[dict]:
    domain = [("state", "=", "done")]
    if since:
        domain.append(("write_date", ">", since))
    moves = execute_kw("stock.move", "search_read", [domain],
                       {"fields": ["id", "product_qty", "date", "product_id",
                                   "location_id", "location_dest_id"]}) or []
    # usage lives on stock.location; fetch it for the referenced locations
    loc_ids = set()
    for m in moves:
        for key in ("location_id", "location_dest_id"):
            if m.get(key):
                loc_ids.add(m[key][0])
    usage = {}
    if loc_ids:
        for loc in execute_kw("stock.location", "read", [list(loc_ids)],
                              {"fields": ["id", "usage"]}) or []:
            usage[loc["id"]] = loc.get("usage")
    pmap = _build_product_map(execute_kw, [m["product_id"][0] for m in moves
                                           if m.get("product_id")])
    movements = []
    for mv in moves:
        if not mv.get("product_id"):
            continue
        prod = pmap.get(mv["product_id"][0])
        if not prod:
            continue
        mv = dict(mv)
        mv["location_usage"] = usage.get(mv["location_id"][0]) if mv.get("location_id") else None
        mv["location_dest_usage"] = usage.get(mv["location_dest_id"][0]) if mv.get("location_dest_id") else None
        movements.append(mapping.map_stock_move(mv, prod))
    return movements


def backfill(execute_kw: ExecKw, client: push_client.HubPushClient, *,
             since: Optional[str] = None, sales=True, receipts=True) -> dict:
    movements: List[dict] = []
    if sales:
        movements += collect_sales(execute_kw, since)
    if receipts:
        movements += collect_receipts(execute_kw, since)
    if not movements:
        logger.info("No movements to backfill.")
        return {"accepted": 0, "duplicates": 0, "batches": 0}
    logger.info("Backfilling %d movement(s) to the hub…", len(movements))
    return client.push(movements)


def main(argv=None):
    logging.basicConfig(level=logging.INFO)
    p = argparse.ArgumentParser(description="OASIS Odoo XML-RPC backfill")
    p.add_argument("--odoo-url", required=True)
    p.add_argument("--db", required=True)
    p.add_argument("--user", required=True)
    p.add_argument("--password", required=True)
    p.add_argument("--hub-url", required=True)
    p.add_argument("--ingest-token", required=True)
    p.add_argument("--since", default=None, help="ISO date, e.g. 2026-06-01")
    p.add_argument("--no-sales", action="store_true")
    p.add_argument("--no-receipts", action="store_true")
    args = p.parse_args(argv)

    execute_kw = connect(args.odoo_url, args.db, args.user, args.password)
    client = push_client.HubPushClient(args.hub_url, args.ingest_token)
    result = backfill(execute_kw, client, since=args.since,
                      sales=not args.no_sales, receipts=not args.no_receipts)
    logger.info("Backfill complete: %s", result)
    return result


if __name__ == "__main__":
    main()
