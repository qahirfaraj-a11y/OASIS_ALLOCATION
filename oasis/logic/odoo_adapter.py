"""Odoo ERP adapter — the same contract as PosErpAdapter, over XML-RPC.

WHY THIS EXISTS
---------------
OASIS's ordering intelligence was reachable only through ``PosErpAdapter``,
which requires DIRECT DATABASE ACCESS to the client's POS. That is fine for an
on-prem SQL Server (RXL) and impossible for most modern ERPs. The Cloud Hub is
deliberately supplier-facing — it holds a privacy-filtered slice and, by design,
carries no cost price and no item master — so it cannot feed ordering either.

This adapter closes that gap: any Odoo install can be read over XML-RPC with no
schema bridge, no views, and no database credentials to the underlying Postgres.

WHAT ODOO GIVES US THAT THE HUB CANNOT
--------------------------------------
  * ``standard_price``  — real cost price (the hub excludes cost on purpose)
  * item master         — UoM, barcode, category, active flag
  * ``stock.move``      — genuine RECEIPT dates, so ``days_since_delivery`` is
                          real. On RXL that field did not exist, defaulted to 0,
                          and silently disabled the dead-stock guard.

CONTRACT
--------
Mirrors the subset of PosErpAdapter the ordering path actually uses::

    fetch_enriched_products   fetch_all_organizations   fetch_sales_history
    push_purchase_order       fetch_pending_pos         update_po_status
    fetch_pending_po_by_sku

Still declared on PosErpAdapter but NOT implemented here: ``fetch_transfers``
and ``push_transfer_request`` (inter-store movement).

Product dicts are emitted in exactly the shape PosErpAdapter produces, so the
engine is unchanged — the source is swapped, the intelligence is not.

WRITES
------
Everything this adapter writes is reversible and stops short of committing
money: purchase orders are created DRAFT, approval never confirms them, and a
confirmed order is refused rather than edited. OASIS proposes; a human presses
the button in Odoo.
"""

from __future__ import annotations

import logging
import os
import xmlrpc.client
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("OdooAdapter")

#: departments treated as perishable, mirroring PosErpAdapter's rule
FRESH_DEPARTMENTS = ("DAIRY", "FRESH PRODUCE", "BUTCHERY", "BAKERY", "FRESH")


def _name_of(m2o) -> str:
    """Odoo many2one reads as ``[id, "name"]``; anything else degrades to ''."""
    if isinstance(m2o, (list, tuple)) and len(m2o) > 1:
        return str(m2o[1])
    return ""


def _id_of(m2o):
    if isinstance(m2o, (list, tuple)) and m2o:
        return m2o[0]
    return m2o or None


def split_hierarchy(complete_name: str) -> Dict[str, str]:
    """Odoo's category path -> Department / Category / Sub-Category.

    ``product.category`` is a TREE and Odoo reads it back as a path:
    ``"All / Saleable / Office Furniture"``. Passing that whole string through as
    ``department`` (what this adapter did first) is worse than useless — every
    product in a different leaf looks like a different department, so grouping,
    budget allocation and department reporting all fragment.

    "All" is Odoo's synthetic root and carries no meaning, so it is dropped.
    Levels are then filled forward: a 1-level category is its own category and
    sub-category, which keeps every consumer able to group at any depth without
    null-checking.
    """
    parts = [p.strip() for p in str(complete_name or "").split("/")]
    parts = [p for p in parts if p and p.upper() != "ALL"]
    if not parts:
        return {"department": "GROCERY", "category": "GROCERY",
                "sub_category": "GROCERY", "department_path": ""}
    dept = parts[0]
    cat = parts[1] if len(parts) > 1 else dept
    sub = parts[2] if len(parts) > 2 else cat
    return {"department": dept, "category": cat, "sub_category": sub,
            "department_path": " / ".join(parts)}


class OdooAdapter:
    """Reads an Odoo instance over XML-RPC and speaks PosErpAdapter's dialect."""

    def __init__(self, url: Optional[str] = None, db: Optional[str] = None,
                 user: Optional[str] = None, password: Optional[str] = None,
                 store_connector=None):
        self.url = (url or os.getenv("ODOO_URL", "http://localhost:8069")).rstrip("/")
        self.db = db or os.getenv("ODOO_DB", "oasis")
        self.user = user or os.getenv("ODOO_USER", "admin")
        self.password = password or os.getenv("ODOO_PASSWORD", "admin")
        # OASIS's OWN store (POs, audit, users) stays where it always was — this
        # adapter never writes OASIS bookkeeping into the client's ERP.
        self.store_connector = store_connector
        self.store_engine = getattr(store_connector, "engine", None)
        self._uid = None
        self._models = None
        self._wh_cache: Dict[str, Optional[dict]] = {}

    # ── connection ────────────────────────────────────────────────────────
    def _connect(self):
        if self._uid is not None:
            return
        common = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/common", allow_none=True)
        self._uid = common.authenticate(self.db, self.user, self.password, {})
        if not self._uid:
            raise ConnectionError(
                f"Odoo authentication failed for user '{self.user}' on db '{self.db}'")
        self._models = xmlrpc.client.ServerProxy(f"{self.url}/xmlrpc/2/object",
                                                 allow_none=True)
        logger.info("Odoo connected: %s db=%s uid=%s", self.url, self.db, self._uid)

    def _ex(self, model: str, method: str, args: list, kwargs: dict = None):
        self._connect()
        return self._models.execute_kw(self.db, self._uid, self.password,
                                       model, method, args, kwargs or {})

    def health_check(self) -> dict:
        try:
            t0 = datetime.now()
            n = self._ex("product.product", "search_count", [[]])
            ms = (datetime.now() - t0).total_seconds() * 1000
            return {"connected": True, "latency_ms": round(ms, 1), "tables_found": n}
        except Exception as e:
            return {"connected": False, "error": str(e)[:200], "tables_found": 0}

    # ── organisations ─────────────────────────────────────────────────────
    def fetch_all_organizations(self) -> List[dict]:
        """Odoo warehouses are OASIS's 'organisations' (stores)."""
        try:
            whs = self._ex("stock.warehouse", "search_read", [[]],
                           {"fields": ["name", "code"]}) or []
            return [{"ORG_CD": str(w.get("code") or w["id"]),
                     "ORG_NAME": w.get("name") or "", "ACTIVE_FLAG": "Y"} for w in whs]
        except Exception as e:
            logger.error("fetch_all_organizations failed: %s", e)
            return []

    # ── internal reads ────────────────────────────────────────────────────
    def _warehouse(self, org_cd: Optional[str]) -> Optional[dict]:
        """The warehouse behind an OASIS org code, or None for company-wide.

        Cached per adapter because one ``fetch_enriched_products`` resolves the
        site three times over (stock, receipts, demand) and the answer cannot
        change mid-call.

        ``in_type_id`` is carried alongside the location because the WRITE side
        needs it: a purchase order is aimed at a site by its receipt operation
        type, not by a location.
        """
        if not org_cd:
            return None
        key = str(org_cd)
        if key in self._wh_cache:
            return self._wh_cache[key]
        try:
            whs = self._ex("stock.warehouse", "search_read",
                           [["|", ["code", "=", key], ["name", "=", key]]],
                           {"fields": ["name", "code", "view_location_id", "in_type_id"],
                            "limit": 1}) or []
        except Exception as e:
            # Reached if this Odoo version does not carry one of those fields.
            # Callers sit inside fetch_enriched_products' try/except, so letting
            # this propagate would blank the whole CATALOGUE and report it as
            # "no products" — a schema problem disguised as an empty store.
            logger.error("warehouse lookup failed for %r (%s) — reading "
                         "company-wide; site scoping is NOT in effect",
                         org_cd, str(e)[:120])
            self._wh_cache[key] = None
            return None
        wh = whs[0] if whs else None
        if wh is None:
            logger.warning("warehouse %r not found — falling back to company-wide",
                           org_cd)
        self._wh_cache[key] = wh
        return wh

    def _warehouse_scope(self, org_cd: Optional[str]) -> Optional[int]:
        """The warehouse's ROOT view location, or None for company-wide.

        Multi-site is the normal case for a retail chain, and every read below
        must be scoped to one site or each store sees the whole company's stock
        and orders as though it held it. Odoo models this as a location TREE per
        warehouse, so `child_of view_location_id` is the correct filter — not
        `lot_stock_id`, which is only the default stock location and misses
        input/quality/output sub-locations.
        """
        wh = self._warehouse(org_cd)
        return _id_of(wh.get("view_location_id")) if wh else None

    def _po_site_domain(self, org_cd: Optional[str]) -> list:
        """Domain fragment restricting purchase orders to one site.

        A PO belongs to the site it will be RECEIVED at, which Odoo records as
        the order's picking type. Reading POs company-wide is not a cosmetic
        problem for ``fetch_pending_po_by_sku``: that feeds ``on_order_qty``, so
        stock inbound to one store would suppress ordering at every other store
        in the chain — the same silent under-ordering the read path had.
        """
        wh = self._warehouse(org_cd)
        if not wh:
            return []
        return [["order_id.picking_type_id.warehouse_id", "=", wh["id"]]]

    def _on_hand(self, org_cd: Optional[str] = None) -> Dict[int, float]:
        """product_id -> quantity in INTERNAL locations, scoped to one site.

        Excluding non-internal usages matters: customer/supplier/inventory-loss
        locations also hold quants, and counting them inflates on-hand.
        """
        dom = [["location_id.usage", "=", "internal"]]
        scope = self._warehouse_scope(org_cd)
        if scope:
            dom.append(["location_id", "child_of", scope])
        rows = self._ex("stock.quant", "read_group", [dom],
                        {"fields": ["quantity"], "groupby": ["product_id"]}) or []
        return {_id_of(r["product_id"]): float(r.get("quantity") or 0.0)
                for r in rows if r.get("product_id")}

    def _last_receipt(self, org_cd: Optional[str] = None) -> Dict[int, datetime]:
        """product_id -> most recent completed INCOMING move date.

        This is what feeds days_since_delivery, which gates the dead-stock and
        stale-fresh rules. Sourcing it properly is the whole reason those guards
        can work here — on a POS without a receipt date they fail OPEN.
        """
        dom = [["state", "=", "done"],
               ["location_id.usage", "in", ["supplier", "inventory", "production"]]]
        scope = self._warehouse_scope(org_cd)
        if scope:            # goods received INTO this site only
            dom.append(["location_dest_id", "child_of", scope])
        rows = self._ex("stock.move", "search_read", [dom],
                        {"fields": ["product_id", "date"], "order": "date desc",
                         "limit": 20000}) or []
        out: Dict[int, datetime] = {}
        for r in rows:
            pid = _id_of(r.get("product_id"))
            if pid is None or pid in out:
                continue
            try:
                out[pid] = datetime.strptime(str(r["date"])[:19], "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                continue
        return out

    def _sales_by_product(self, days: int = 90,
                          org_cd: Optional[str] = None) -> Dict[int, Dict[str, float]]:
        """product_id -> {units, revenue} of goods sold in the window.

        Two sources, unioned, because Odoo installs differ in how they sell:

          1. ``pos.order.line``  — the till, when Point of Sale is installed.
          2. outgoing ``stock.move`` to a CUSTOMER location — how every Odoo
             sale depletes inventory regardless of the front end (POS, Sales,
             eCommerce, manual delivery).

        Relying on POS alone would report zero demand for any client selling
        through Sales or eCommerce — the engine would then see a live catalogue
        with no velocity and behave as if nothing moves.
        """
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
        agg: Dict[int, Dict[str, float]] = defaultdict(
            lambda: {"units": 0.0, "revenue": 0.0})

        try:
            rows = self._ex("pos.order.line", "search_read",
                            [[["order_id.date_order", ">=", since]]],
                            {"fields": ["product_id", "qty", "price_subtotal_incl"],
                             "limit": 200000}) or []
            for r in rows:
                pid = _id_of(r.get("product_id"))
                if pid is None:
                    continue
                agg[pid]["units"] += float(r.get("qty") or 0.0)
                agg[pid]["revenue"] += float(r.get("price_subtotal_incl") or 0.0)
        except Exception as e:                       # POS not installed — fine
            logger.info("POS lines unavailable (%s); using stock moves", str(e)[:60])

        try:
            dom = [["state", "=", "done"], ["date", ">=", since],
                   ["location_dest_id.usage", "=", "customer"]]
            scope = self._warehouse_scope(org_cd)
            if scope:        # goods that left THIS site
                dom.append(["location_id", "child_of", scope])
            moves = self._ex("stock.move", "search_read", [dom],
                             {"fields": ["product_id", "product_uom_qty"],
                              "limit": 200000}) or []
            for m in moves:
                pid = _id_of(m.get("product_id"))
                if pid is None:
                    continue
                agg[pid]["units"] += float(m.get("product_uom_qty") or 0.0)
        except Exception as e:
            logger.warning("outgoing stock moves unavailable: %s", str(e)[:80])

        return dict(agg)

    def _supplier_of(self, product_ids: List[int]) -> Dict[int, dict]:
        """product_id -> {code, name, lead_time} from the first supplierinfo."""
        out: Dict[int, dict] = {}
        if not product_ids:
            return out
        rows = self._ex("product.supplierinfo", "search_read",
                        [[["product_tmpl_id.product_variant_ids", "in", product_ids]]],
                        {"fields": ["partner_id", "delay", "product_tmpl_id"],
                         "limit": 20000}) or []
        by_tmpl: Dict[int, dict] = {}
        for r in rows:
            t = _id_of(r.get("product_tmpl_id"))
            if t is not None and t not in by_tmpl:
                by_tmpl[t] = {"name": _name_of(r.get("partner_id")),
                              "code": str(_id_of(r.get("partner_id")) or ""),
                              "lead": int(r.get("delay") or 7)}
        return by_tmpl

    # ── the main read ─────────────────────────────────────────────────────
    def fetch_enriched_products(self, org_cd: str = None,
                                sales_days: int = 90) -> List[dict]:
        """Products in PosErpAdapter's exact dict shape, so the engine is unchanged."""
        try:
            prods = self._ex("product.product", "search_read",
                             [[["active", "=", True], ["type", "in", ["product", "consu"]]]],
                             {"fields": ["default_code", "display_name", "categ_id",
                                         "list_price", "standard_price", "uom_id",
                                         "barcode", "product_tmpl_id"],
                              "limit": 100000}) or []
        except Exception as e:
            logger.error("fetch_enriched_products failed: %s", e)
            return []

        on_hand = self._on_hand(org_cd)
        receipts = self._last_receipt(org_cd)
        sales = self._sales_by_product(sales_days, org_cd)
        suppliers = self._supplier_of([p["id"] for p in prods])
        now = datetime.now()

        out: List[dict] = []
        for p in prods:
            pid = p["id"]
            h = split_hierarchy(_name_of(p.get("categ_id")))
            dept = h["department"]
            sup = suppliers.get(_id_of(p.get("product_tmpl_id")), {})
            s = sales.get(pid, {})
            units = float(s.get("units") or 0.0)
            last_recv = receipts.get(pid)
            days_since = (now - last_recv).days if last_recv else 0

            out.append({
                "item_code": str(p.get("default_code") or pid),
                "product_name": str(p.get("display_name") or ""),
                "barcode": str(p.get("barcode") or ""),
                "current_stocks": float(on_hand.get(pid, 0.0)),
                "selling_price": float(p.get("list_price") or 0.0),
                # Odoo's standard_price IS the cost — the field the hub cannot carry
                "cost_price": float(p.get("standard_price") or 0.0),
                "mrp": float(p.get("list_price") or 0.0),
                "supplier_name": sup.get("name") or "Unknown",
                "supplier_cd": sup.get("code") or "",
                # 3-level hierarchy, not a flat label. The engine groups on
                # `department`; `category`/`sub_category` let budget allocation
                # and reporting drill without a second query.
                "department": dept,
                "category": h["category"],
                "sub_category": h["sub_category"],
                "department_path": h["department_path"],
                "uom": _name_of(p.get("uom_id")) or "EA",
                "is_fresh": any(f in dept.upper() for f in FRESH_DEPARTMENTS),
                "last_days_since_last_delivery": days_since,
                "days_since_delivery": days_since,
                "blocked_open_for_order": "open",
                "pack_size": 1,
                "estimated_delivery_days": int(sup.get("lead") or 7),
                "supplier_reliability": 0.9,
                "supplier_frequency": "weekly",
                "avg_daily_sales": round(units / max(1, sales_days), 4),
                "total_units_sold_last_90d": units,
                "estimated_daily_sales": round(units / max(1, sales_days), 4),
                "units_sold_last_month": round(units / max(1, sales_days) * 30, 2),
                "on_order_qty": 0.0,
            })
        logger.info("Odoo: enriched %d products (org=%s)", len(out), org_cd)
        return out

    def fetch_sales_history(self, org_cd: str = None, days: int = 90) -> List[dict]:
        sales = self._sales_by_product(days, org_cd)
        return [{"item_code": str(pid), "units": v["units"], "revenue": v["revenue"]}
                for pid, v in sales.items()]

    # ── writes: purchase orders go back INTO Odoo ─────────────────────────
    def push_purchase_order(self, org_cd: str, recommendations: List[dict]) -> int:
        """Create a draft purchase.order per supplier from OASIS recommendations.

        Draft, never confirmed: OASIS proposes, a human approves in Odoo. Writing
        a confirmed PO would commit a client's money without review.

        The order is aimed at ``org_cd``'s own receipt operation type. Without
        that Odoo silently applies the DEFAULT warehouse's, so a PO computed
        from one store's stock and demand is received into another — goods
        physically arriving at the wrong site, with nothing in the run to show
        it. The read path is scoped per site, so the write path must be too.
        """
        by_supplier: Dict[Any, List[dict]] = defaultdict(list)
        for rec in recommendations:
            if float(rec.get("recommended_quantity") or 0) <= 0:
                continue
            by_supplier[rec.get("supplier_cd") or ""].append(rec)
        if not by_supplier:
            return 0

        wh = self._warehouse(org_cd)
        picking_type = _id_of(wh.get("in_type_id")) if wh else None
        if org_cd and picking_type is None:
            logger.warning("no receipt operation type for site %r — Odoo will use "
                           "the default warehouse for these POs", org_cd)

        codes = [r.get("item_code") for recs in by_supplier.values() for r in recs]
        found = self._ex("product.product", "search_read",
                         [[["default_code", "in", [str(c) for c in codes if c]]]],
                         {"fields": ["default_code"], "limit": 100000}) or []
        pid_of = {str(f["default_code"]): f["id"] for f in found}

        written = 0
        for supplier_cd, recs in by_supplier.items():
            lines = []
            for r in recs:
                pid = pid_of.get(str(r.get("item_code")))
                if not pid:
                    continue
                lines.append((0, 0, {
                    "product_id": pid,
                    "product_qty": float(r.get("recommended_quantity") or 0),
                    "price_unit": float(r.get("cost_price") or 0.0),
                    "name": str(r.get("product_name") or "")[:200],
                    "date_planned": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                }))
            if not lines:
                continue
            try:
                partner = int(supplier_cd) if str(supplier_cd).isdigit() else None
                if partner is None:
                    partner = (self._ex("res.partner", "search",
                                        [[["supplier_rank", ">", 0]]], {"limit": 1}) or [None])[0]
                if partner is None:
                    logger.warning("no vendor partner available — skipping %s", supplier_cd)
                    continue
                vals = {"partner_id": partner, "order_line": lines}
                if picking_type:
                    vals["picking_type_id"] = picking_type
                self._ex("purchase.order", "create", [vals])
                written += len(lines)
            except Exception as e:
                logger.error("push_purchase_order failed for %s: %s", supplier_cd, str(e)[:160])
        logger.info("Odoo: wrote %d PO lines as DRAFT", written)
        return written

    #: what the consoles actually send (oasis/ui/shell.py, smart_ordering_tab).
    #: An unrecognised status is refused rather than guessed at — defaulting to
    #: "approve" on a typo is not a failure mode worth having.
    _APPROVE = frozenset({"APPROVED", "APPROVE", "ACCEPTED"})
    _REJECT = frozenset({"REJECTED", "REJECT", "DECLINED", "CANCELLED"})

    def _post_note(self, order_id: int, body: str) -> None:
        """Leave OASIS's decision on the order's chatter. Never fatal.

        The point is that whoever confirms in Odoo can see what OASIS decided
        and who decided it, without having to open OASIS.
        """
        try:
            self._ex("purchase.order", "message_post", [[order_id]],
                     {"body": body})
        except Exception as e:
            logger.warning("could not post note to PO %s: %s", order_id, str(e)[:100])

    def update_po_status(self, po_id: int, status: str, approved_by: str,
                         new_quantity: Optional[float] = None,
                         reason: Optional[str] = None) -> bool:
        """Apply an OASIS approval decision to a pending Odoo order line.

        ``po_id`` is a **purchase.order.line** id — that is what
        ``fetch_pending_pos`` hands the console, and what the console hands
        back. It is NOT PosErpAdapter's ``INTEGRATION_PURCHASE_ORDERS.PO_ID``;
        the two are different id spaces over the same parameter name, so this
        must never fall back to the store table on a miss. It would find an
        unrelated row with a coincidentally equal id and report success.

        APPROVED applies any quantity override and records the decision. It
        deliberately does **not** call ``button_confirm``: OASIS proposes and a
        human confirms in Odoo, and confirming here would let the desktop UI
        commit a client's money without anyone opening the ERP.

        REJECTED drops the line from the order, and cancels the order if that
        emptied it — an empty draft PO is litter that looks like a real one.

        Only draft/sent orders are touched. A confirmed order is somebody's
        committed spend and is refused, not silently edited.
        """
        decision = (status or "").strip().upper()
        if decision not in self._APPROVE and decision not in self._REJECT:
            logger.error("update_po_status: unknown status %r for line %s — "
                         "refusing (expected one of %s)", status, po_id,
                         sorted(self._APPROVE | self._REJECT))
            return False

        try:
            rows = self._ex("purchase.order.line", "read", [[int(po_id)]],
                            {"fields": ["product_id", "product_qty", "order_id",
                                        "state"]}) or []
        except Exception as e:
            logger.error("update_po_status: could not read line %s: %s",
                         po_id, str(e)[:140])
            return False
        if not rows:
            logger.error("update_po_status: purchase.order.line %s not found "
                         "(is this a PosErpAdapter PO_ID?)", po_id)
            return False

        line = rows[0]
        order_id = _id_of(line.get("order_id"))
        order_name = _name_of(line.get("order_id"))
        state = line.get("state")
        product = _name_of(line.get("product_id")) or f"line {po_id}"

        if state not in ("draft", "sent"):
            logger.error("update_po_status: %s is in state %r — refusing to "
                         "modify a committed order", order_name, state)
            return False

        who = approved_by or "OASIS"
        tail = f"<br/>Reason: {reason}" if reason else ""

        try:
            if decision in self._APPROVE:
                old_qty = float(line.get("product_qty") or 0)
                changed = ""
                if new_quantity is not None and float(new_quantity) > 0:
                    self._ex("purchase.order.line", "write",
                             [[int(po_id)], {"product_qty": float(new_quantity)}])
                    # ASCII arrow deliberately: this string goes to the logger
                    # as well as to Odoo, and OASIS logs to a Windows console
                    # whose cp1252 codec cannot encode U+2192.
                    changed = f" Quantity {old_qty:g} -> {float(new_quantity):g}."
                self._post_note(order_id,
                                f"<b>OASIS: approved</b> by {who}.{changed} "
                                f"Line: {product}. Left as {state} for "
                                f"confirmation in Odoo.{tail}")
                logger.info("Odoo: line %s (%s) APPROVED by %s%s",
                            po_id, order_name, who, changed)
                return True

            # ── rejection ────────────────────────────────────────────────
            self._post_note(order_id,
                            f"<b>OASIS: rejected</b> by {who}. "
                            f"Removed line: {product}.{tail}")
            self._ex("purchase.order.line", "unlink", [[int(po_id)]])
            remaining = self._ex("purchase.order.line", "search_count",
                                 [[["order_id", "=", order_id]]])
            if not remaining:
                self._cancel_order(order_id, order_name)
            logger.info("Odoo: line %s (%s) REJECTED by %s — %d lines left",
                        po_id, order_name, who, remaining)
            return True
        except Exception as e:
            logger.error("update_po_status failed for line %s: %s",
                         po_id, str(e)[:160])
            return False

    def _cancel_order(self, order_id: int, order_name: str = "") -> None:
        """Cancel an order left empty by rejection.

        ``button_cancel`` returns None and Odoo's XML-RPC endpoint serialises
        with allow_none=False, so a SUCCESSFUL cancel raises "cannot marshal
        None". Setting allow_none on our proxy does not help — the server is
        the one serialising. So the exception is swallowed and the outcome is
        confirmed by READING THE STATE BACK, never inferred from the return.
        """
        try:
            self._ex("purchase.order", "button_cancel", [[order_id]])
        except xmlrpc.client.Fault as e:
            if "cannot marshal None" not in str(e):
                raise
        state = (self._ex("purchase.order", "read", [[order_id]],
                          {"fields": ["state"]}) or [{}])[0].get("state")
        if state == "cancel":
            logger.info("Odoo: %s had no lines left — cancelled", order_name)
        else:
            logger.warning("Odoo: %s is empty but state is %r, not cancelled",
                           order_name, state)

    def fetch_pending_pos(self, org_cd: str = None):
        """Draft/sent purchase orders for one site — OASIS's 'pending approval'."""
        try:
            dom = [["order_id.state", "in", ["draft", "sent"]]]
            dom += self._po_site_domain(org_cd)
            rows = self._ex("purchase.order.line", "search_read", [dom],
                            {"fields": ["product_id", "product_qty", "price_unit",
                                        "order_id"], "limit": 5000}) or []
        except Exception as e:
            logger.warning("fetch_pending_pos failed: %s", str(e)[:120])
            return []
        return [{"PO_ID": r["id"], "ITM_CD": _name_of(r.get("product_id")),
                 "PRODUCT_NAME": _name_of(r.get("product_id")),
                 "QUANTITY": float(r.get("product_qty") or 0),
                 "UNIT_COST": float(r.get("price_unit") or 0),
                 "STATUS": "PENDING"} for r in rows]

    def diagnose(self, org_cd: str = None) -> Dict[str, Any]:
        """Human-readable picture of what this adapter can actually see.

        Exists because an adapter is otherwise a black box: if ordering returns
        nothing you cannot tell whether the connection failed, the catalogue is
        empty, demand is missing, or costs are unset — and each has a completely
        different fix. Every number here is READ-ONLY.
        """
        out: Dict[str, Any] = {"url": self.url, "db": self.db, "user": self.user}
        h = self.health_check()
        out["connected"] = h.get("connected")
        out["latency_ms"] = h.get("latency_ms")
        if not h.get("connected"):
            out["error"] = h.get("error")
            return out

        prods = self.fetch_enriched_products(org_cd)
        out["products"] = len(prods)
        out["with_stock"] = sum(1 for p in prods if float(p.get("current_stocks") or 0) > 0)
        out["negative_stock"] = sum(1 for p in prods if float(p.get("current_stocks") or 0) < 0)
        out["with_demand"] = sum(1 for p in prods if float(p.get("avg_daily_sales") or 0) > 0)
        out["with_cost"] = sum(1 for p in prods if float(p.get("cost_price") or 0) > 0)
        out["with_price"] = sum(1 for p in prods if float(p.get("selling_price") or 0) > 0)
        out["with_supplier"] = sum(1 for p in prods
                                   if (p.get("supplier_name") or "Unknown") != "Unknown")
        out["with_receipt_date"] = sum(1 for p in prods
                                       if int(p.get("days_since_delivery") or 0) > 0)
        out["departments"] = len({p.get("department") for p in prods})
        out["categories"] = len({p.get("category") for p in prods})
        out["organisations"] = [o.get("ORG_CD") for o in self.fetch_all_organizations()]
        try:
            out["open_po_lines"] = len(self.fetch_pending_pos(org_cd))
        except Exception:
            out["open_po_lines"] = None

        # the checks that silently break ordering — each maps to a real failure
        warnings = []
        if out["products"] and not out["with_demand"]:
            warnings.append("NO DEMAND: every ADS is zero — the engine will "
                            "recommend nothing. Check sales/outgoing moves exist.")
        if out["negative_stock"]:
            warnings.append(f"{out['negative_stock']} products have NEGATIVE stock — "
                            "order quantities will be inflated.")
        if out["products"] and out["with_cost"] < out["products"] * 0.5:
            warnings.append(f"only {out['with_cost']}/{out['products']} have a cost "
                            "price — order VALUE and budget gating will be wrong.")
        if out["products"] and not out["with_receipt_date"]:
            warnings.append("NO receipt dates — days_since_delivery is 0 for all, "
                            "so the dead-stock guard cannot fire (fails OPEN).")
        if out["products"] and out["departments"] <= 1:
            warnings.append("all products in ONE department — check the category "
                            "hierarchy is being read.")
        out["warnings"] = warnings
        return out

    def fetch_pending_po_by_sku(self, org_cd: str = None) -> Dict[str, dict]:
        """SKU -> outstanding qty, so the engine does not re-order what is inbound.

        Without this, day 2 re-orders everything still in transit — the exact
        failure mode that on_order_qty exists to prevent.
        """
        out: Dict[str, dict] = {}
        try:
            dom = [["order_id.state", "in", ["draft", "sent", "purchase"]]]
            dom += self._po_site_domain(org_cd)
            rows = self._ex("purchase.order.line", "search_read", [dom],
                            {"fields": ["product_id", "product_qty", "qty_received"],
                             "limit": 20000}) or []
        except Exception:
            return out
        pids = [_id_of(r.get("product_id")) for r in rows if r.get("product_id")]
        codes = {}
        if pids:
            for p in (self._ex("product.product", "read", [list(set(pids))],
                               {"fields": ["default_code"]}) or []):
                codes[p["id"]] = str(p.get("default_code") or p["id"])
        for r in rows:
            code = codes.get(_id_of(r.get("product_id")))
            if not code:
                continue
            outstanding = float(r.get("product_qty") or 0) - float(r.get("qty_received") or 0)
            if outstanding > 0:
                e = out.setdefault(code, {"qty": 0.0, "eta_days": 7})
                e["qty"] += outstanding
        return out
