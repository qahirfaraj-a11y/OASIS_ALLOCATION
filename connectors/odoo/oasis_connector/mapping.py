"""
Odoo → OASIS Hub field mapping.

PURE Python: this module imports nothing from ``odoo`` so it can be unit-tested
outside a running Odoo instance and validated directly against the hub's
``MovementIn`` schema. The Odoo-side model code (models/oasis_sync.py) and the
standalone XML-RPC backfill both feed record dicts through here.

Odoo represents many-to-one fields as ``[id, "Display Name"]`` pairs in
read()/XML-RPC results. We accept that shape and also tolerate plain scalars.

Movement type derivation:
  * pos.order.line          → 'sale'         (retail sell-through)
  * stock.move (from vendor)→ 'receipt'      (GRN / goods in)
  * stock.move (internal±)  → 'adjustment'   (counts, wastage, internal xfer)
  * stock.quant snapshot    → 'stock_on_hand'
"""

from datetime import datetime
from typing import Optional

# hub schema's accepted movement types (kept in sync with oasis_hub.schemas)
SALE = "sale"
RECEIPT = "receipt"
STOCK_ON_HAND = "stock_on_hand"
ADJUSTMENT = "adjustment"


# ── helpers ──────────────────────────────────────────────────────────────
def _m2o_id(val):
    """Odoo many2one → id. Accepts [id, name], id, or falsy."""
    if isinstance(val, (list, tuple)) and val:
        return val[0]
    if isinstance(val, (int, str)) and val not in ("", False):
        return val
    return None


def _m2o_name(val) -> Optional[str]:
    """Odoo many2one → display name. Accepts [id, name], str, or falsy."""
    if isinstance(val, (list, tuple)) and len(val) >= 2:
        return val[1]
    if isinstance(val, str) and val:
        return val
    return None


def _to_iso(val) -> str:
    """Normalise an Odoo datetime/date to ISO-8601 string.

    Odoo returns naive 'YYYY-MM-DD HH:MM:SS' (UTC) strings or datetime/date
    objects. We emit ISO so pydantic parses it; None → epoch-safe raise.
    """
    if isinstance(val, datetime):
        return val.isoformat()
    if hasattr(val, "isoformat"):          # date
        return val.isoformat()
    if isinstance(val, str) and val:
        return val.replace(" ", "T", 1)
    raise ValueError("movement is missing an occurred_at timestamp")


def _num(val, default=None):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def product_info(product: dict) -> dict:
    """Extract the ownership/identity fields from a product.product record.

    Expects keys as returned by read(): default_code, name, categ_id,
    seller_ids/main supplier resolved to ``supplier_ref``, product_brand_id,
    list_price. Missing keys degrade gracefully to None.
    """
    return {
        "sku_code": product.get("default_code") or str(product.get("id") or ""),
        "sku_name": product.get("name") or _m2o_name(product.get("display_name")),
        "department": _m2o_name(product.get("categ_id")),
        # supplier_cd is the ownership key the hub matches on. Prefer an explicit
        # vendor ref the addon resolves; fall back to the brand-less seller name.
        "supplier_cd": product.get("supplier_ref")
        or _m2o_name(product.get("_main_supplier")),
        "brand": _m2o_name(product.get("product_brand_id")),
        "list_price": _num(product.get("list_price")),
    }


# ── movement mappers ─────────────────────────────────────────────────────
def map_pos_order_line(line: dict, product: dict, *, order_date=None) -> dict:
    """pos.order.line → hub 'sale' movement.

    line: {id, qty, price_unit, order_id:[id,name] or with .date_order}
    """
    info = product_info(product)
    occurred = order_date or line.get("order_date") or line.get("write_date")
    return {
        "sku_code": info["sku_code"],
        "sku_name": info["sku_name"],
        "supplier_cd": info["supplier_cd"],
        "brand": info["brand"],
        "department": info["department"],
        "movement_type": SALE,
        "qty": _num(line.get("qty"), 0.0),
        "unit_price": _num(line.get("price_unit"), info["list_price"]),
        "occurred_at": _to_iso(occurred),
        "source_ref": f"odoo:pos.order.line:{line['id']}",
    }


def map_stock_move(move: dict, product: dict) -> dict:
    """stock.move → hub 'sale', 'receipt', or 'adjustment' movement.

    Type is inferred from source/dest location usage:
      * → customer            → 'sale'     (sell-through; POS deliveries land here
                                            too, so this covers POS and non-POS Odoo)
      * supplier → internal   → 'receipt'  (goods in)
      * anything else internal→ 'adjustment'(counts, wastage, internal transfer)
    move: {id, product_qty, date, location_id, location_dest_id,
           location_usage, location_dest_usage, price_unit?}
    """
    info = product_info(product)
    src_usage = move.get("location_usage")
    dest_usage = move.get("location_dest_usage")
    if dest_usage == "customer":
        mtype = SALE
    elif src_usage == "supplier" and dest_usage in ("internal", None):
        mtype = RECEIPT
    else:
        mtype = ADJUSTMENT
    return {
        "sku_code": info["sku_code"],
        "sku_name": info["sku_name"],
        "supplier_cd": info["supplier_cd"],
        "brand": info["brand"],
        "department": info["department"],
        "movement_type": mtype,
        "qty": _num(move.get("product_qty"), 0.0),
        "unit_price": _num(move.get("price_unit"), info["list_price"]),
        "occurred_at": _to_iso(move.get("date")),
        "source_ref": f"odoo:stock.move:{move['id']}",
    }


def map_stock_quant(quant: dict, product: dict, *, as_of=None) -> dict:
    """stock.quant → hub 'stock_on_hand' snapshot.

    quant: {id, quantity, in_date/write_date}
    """
    info = product_info(product)
    occurred = as_of or quant.get("in_date") or quant.get("write_date")
    qty = _num(quant.get("quantity"), 0.0)
    return {
        "sku_code": info["sku_code"],
        "sku_name": info["sku_name"],
        "supplier_cd": info["supplier_cd"],
        "brand": info["brand"],
        "department": info["department"],
        "movement_type": STOCK_ON_HAND,
        "qty": qty,
        "on_hand": qty,
        "unit_price": info["list_price"],
        "occurred_at": _to_iso(occurred),
        # snapshots are keyed by day so a daily cron is idempotent per quant/day
        "source_ref": f"odoo:stock.quant:{quant['id']}:{_to_iso(occurred)[:10]}",
    }
