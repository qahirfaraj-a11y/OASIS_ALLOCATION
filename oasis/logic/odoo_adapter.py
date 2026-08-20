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
    fetch_pending_po_by_sku   fetch_transfers           push_transfer_request
    update_transfer_status

The contract is now fully covered for the ordering and transfer paths.

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

from oasis.logic import erp_contract as _contract

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


class OdooAdapter(_contract.ErpAdapter):
    """Reads an Odoo instance over XML-RPC and speaks PosErpAdapter's dialect."""

    ERP_NAME = "odoo"
    #: Everything, and all of it live-verified against Odoo 16 — Odoo supplies
    #: cost price and genuine receipt dates natively, which the hub excludes by
    #: design and RXL cannot supply at all.
    CAPABILITIES = frozenset({
        _contract.READ_CATALOGUE, _contract.READ_STOCK, _contract.READ_DEMAND,
        _contract.READ_COST, _contract.READ_RECEIPTS, _contract.READ_SUPPLIERS,
        _contract.READ_OPEN_POS, _contract.MULTI_SITE,
        _contract.WRITE_PO, _contract.WRITE_PO_STATUS,
        _contract.READ_TRANSFERS, _contract.WRITE_TRANSFER,
        _contract.WRITE_TRANSFER_STATUS,
    })

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

        The write side needs more than the location, so the whole set is
        fetched once: ``in_type_id`` aims a purchase order at a site (a PO is
        routed by receipt operation type, not by location), and
        ``lot_stock_id`` / ``int_type_id`` are the source and destination of an
        inter-store transfer.
        """
        if not org_cd:
            return None
        key = str(org_cd)
        if key in self._wh_cache:
            return self._wh_cache[key]
        try:
            whs = self._ex("stock.warehouse", "search_read",
                           [["|", ["code", "=", key], ["name", "=", key]]],
                           {"fields": ["name", "code", "view_location_id",
                                       "in_type_id", "lot_stock_id", "int_type_id",
                                       "company_id"],
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

    #: How many incoming moves the receipt lookup reads. The read is ordered
    #: newest-first, so this is a WINDOW, and what falls outside it is older
    #: than everything inside — see _last_receipt.
    RECEIPT_READ_LIMIT = 20000

    def _last_receipt(self, org_cd: Optional[str] = None):
        """(product_id -> last incoming move date, horizon, truncated).

        This feeds days_since_delivery, which gates the dead-stock and
        stale-fresh rules. Sourcing it properly is the whole reason those guards
        can work here — on a POS without a receipt date they fail OPEN.

        WHY THE HORIZON IS RETURNED
        ---------------------------
        The read is capped and ordered NEWEST FIRST, so on a busy chain it
        covers only recent history. A product whose last receipt predates the
        window is simply absent from the result — and the caller used to read
        that absence as ``days_since_delivery = 0``, i.e. "delivered today".

        That inverted the dead-stock rule exactly: is_dead needs an age of 90
        days or more, so the STALEST stock in the business — the stock whose
        last receipt is furthest back, and therefore least likely to be inside
        the window — was the stock guaranteed never to be flagged. The rule
        failed hardest on the cases it exists for.

        The oldest date actually read is a sound LOWER BOUND for everything
        missing: nothing absent can be newer than it, or it would have been in
        the result. Returning it lets the caller say "at least this old" rather
        than inventing "today".
        """
        dom = [["state", "=", "done"],
               ["location_id.usage", "in", ["supplier", "inventory", "production"]]]
        scope = self._warehouse_scope(org_cd)
        if scope:            # goods received INTO this site only
            dom.append(["location_dest_id", "child_of", scope])
        rows = self._ex("stock.move", "search_read", [dom],
                        {"fields": ["product_id", "date"], "order": "date desc",
                         "limit": self.RECEIPT_READ_LIMIT}) or []
        out: Dict[int, datetime] = {}
        oldest = None
        for r in rows:
            try:
                when = datetime.strptime(str(r["date"])[:19], "%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError):
                continue
            if oldest is None or when < oldest:
                oldest = when
            pid = _id_of(r.get("product_id"))
            if pid is not None and pid not in out:
                out[pid] = when

        truncated = len(rows) >= self.RECEIPT_READ_LIMIT
        if truncated:
            logger.warning(
                "receipt history TRUNCATED at %s moves for site %r (oldest read "
                "%s). Products with no receipt since then are aged from that "
                "date as a lower bound, not treated as freshly delivered.",
                self.RECEIPT_READ_LIMIT, org_cd or "company-wide",
                oldest.date() if oldest else "?")
        return out, oldest, truncated

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

        #: whether the till was readable. Decides below whether the matching
        #: stock moves would be a second count of the same sale.
        pos_counted = False
        try:
            rows = self._ex("pos.order.line", "search_read",
                            [[["order_id.date_order", ">=", since]]],
                            {"fields": ["product_id", "qty", "price_subtotal_incl"],
                             "limit": 200000}) or []
            pos_counted = True
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

            # DO NOT COUNT A POS SALE TWICE.
            #
            # Closing a POS order creates a stock.picking (linked by
            # pos_order_id) whose moves go to a customer location. Those moves
            # describe the SAME units the pos.order.line above already counted,
            # so a POS install was having every till sale doubled — and ADS is
            # the input to every horizon, reorder point and transfer quantity
            # in the system. Nothing errors; the numbers are simply twice what
            # the shop sold.
            #
            # The exclusion is only correct when the POS read actually
            # succeeded. If Point of Sale is not installed there is no
            # pos.order.line to double up with, and the customer moves ARE the
            # only record of the sale.
            if pos_counted:
                try:
                    self._ex("stock.picking", "fields_get",
                             [["pos_order_id"], ["type"]])
                    # KEEP MOVES THAT HAVE NO PICKING. A dotted domain walks
                    # the relation, so `picking_id.pos_order_id = False` silently
                    # drops every move whose picking_id is NULL — the join has
                    # nothing to walk. Those are ordinary sales, and excluding
                    # them zeroed demand outright: measured, 240,962 of 240,966
                    # customer moves vanished and every store reported ADS 0.
                    dom += ["|", ["picking_id", "=", False],
                            ["picking_id.pos_order_id", "=", False]]
                except Exception:
                    logger.debug("no pos_order_id on stock.picking; counting "
                                 "customer moves unfiltered")

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
                                         "barcode", "product_tmpl_id", "create_date"],
                              "limit": 100000}) or []
        except Exception as e:
            logger.error("fetch_enriched_products failed: %s", e)
            return []

        on_hand = self._on_hand(org_cd)
        receipts, receipts_from, receipts_truncated = self._last_receipt(org_cd)
        sales = self._sales_by_product(sales_days, org_cd)
        suppliers = self._supplier_of([p["id"] for p in prods])
        # Stock already bought and on its way. This used to be hardcoded to
        # zero here while fetch_pending_po_by_sku sat unused by this path, so
        # a store with a delivery landing tomorrow read as being just as short
        # as one with nothing coming — and the transfer engine moved stock to
        # it. The ordering path has always applied this; the read every other
        # consumer shares did not.
        try:
            on_order = self.fetch_pending_po_by_sku(org_cd)
        except Exception as e:
            logger.warning("pending PO lookup failed for %r (%s) — on_order_qty "
                           "reads 0, so inbound stock is invisible to ordering "
                           "and transfers", org_cd, str(e)[:120])
            on_order = {}
        now = datetime.now()

        out: List[dict] = []
        for p in prods:
            pid = p["id"]
            h = split_hierarchy(_name_of(p.get("categ_id")))
            dept = h["department"]
            sup = suppliers.get(_id_of(p.get("product_tmpl_id")), {})
            s = sales.get(pid, {})
            units = float(s.get("units") or 0.0)
            # AGE OF THE STOCK, and how confident we are in it.
            #
            # Absence from the receipt read is not evidence of a delivery
            # today — it is evidence of the opposite. Three cases, in
            # descending order of confidence:
            #   1. a receipt was found            -> exact
            #   2. the read was truncated         -> at least as old as the
            #      oldest record we did read (nothing missing can be newer)
            #   3. the read was complete and this product still has none ->
            #      never received here; the stock cannot predate the product
            #      record, so its creation date is the floor
            last_recv = receipts.get(pid)
            estimated = False
            if last_recv:
                days_since = (now - last_recv).days
            else:
                estimated = True
                floor = receipts_from if receipts_truncated else None
                if floor is None:
                    try:
                        floor = datetime.strptime(
                            str(p.get("create_date"))[:19], "%Y-%m-%d %H:%M:%S")
                    except (ValueError, TypeError):
                        floor = None
                days_since = max(0, (now - floor).days) if floor else 0

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
                #: True when no receipt was found and the age is a LOWER BOUND
                #: rather than a fact. Consumers that act on staleness should
                #: know the difference between measured and inferred.
                "days_since_delivery_estimated": estimated,
                "blocked_open_for_order": "open",
                "pack_size": 1,
                "estimated_delivery_days": int(sup.get("lead") or 7),
                "supplier_reliability": 0.9,
                "supplier_frequency": "weekly",
                "avg_daily_sales": round(units / max(1, sales_days), 4),
                "total_units_sold_last_90d": units,
                "estimated_daily_sales": round(units / max(1, sales_days), 4),
                "units_sold_last_month": round(units / max(1, sales_days) * 30, 2),
                "on_order_qty": float(
                    on_order.get(str(p.get("default_code") or pid), {}).get("qty", 0.0)),
                "on_order_eta_days": float(
                    on_order.get(str(p.get("default_code") or pid), {}).get("eta_days", 999.0)),
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

        ``button_cancel`` returns None, which raises over XML-RPC even though
        the cancel committed — see ``_call_ignoring_none``. The outcome is
        confirmed by READING THE STATE BACK, never inferred from the return.
        """
        self._call_ignoring_none("purchase.order", "button_cancel", [[order_id]])
        state = (self._ex("purchase.order", "read", [[order_id]],
                          {"fields": ["state"]}) or [{}])[0].get("state")
        if state == "cancel":
            logger.info("Odoo: %s had no lines left — cancelled", order_name)
        else:
            logger.warning("Odoo: %s is empty but state is %r, not cancelled",
                           order_name, state)

    # ── inter-store transfers ─────────────────────────────────────────────
    #: Odoo picking state -> the ladder OASIS's console renders
    #: (REQUESTED -> IN_TRANSIT -> RECEIVED). Odoo has six states to our three:
    #: everything Odoo has CONFIRMED but not finished is one bucket, because
    #: from the operator's side the goods are committed to move either way.
    _PICKING_STATUS = {
        "draft": "REQUESTED",
        "waiting": "IN_TRANSIT",
        "confirmed": "IN_TRANSIT",
        "assigned": "IN_TRANSIT",
        "done": "RECEIVED",
        "cancel": "CANCELLED",
    }

    #: transfer columns PosErpAdapter's INTEGRATION_TRANSFER_ORDERS produces,
    #: which transfer_intel_tab reads by name. An empty DataFrame must still
    #: carry them or `df[df['TO_ORG_CD'] == x]` raises instead of returning
    #: nothing (notification_service does exactly that).
    TRANSFER_COLUMNS = ("TRANSFER_ID", "FROM_ORG_CD", "TO_ORG_CD", "ITM_CD",
                        "PRODUCT_NAME", "QUANTITY", "VALUE_KES", "STATUS",
                        "URGENCY", "REQUESTED_BY", "CREATED_DT", "COMPLETED_DT")

    def can_transfer(self, from_org: str, to_org: str) -> Dict[str, Any]:
        """Whether Odoo would accept an internal transfer between these sites.

        Odoo refuses to CONFIRM a picking whose source and destination belong
        to different companies ("Incompatible companies on records") — but the
        CREATE succeeds, so an unchecked write leaves a draft that shows as
        REQUESTED forever and fails every attempt to advance it. Moving stock
        between legal entities is a sale/purchase pair in Odoo, not a transfer.
        """
        src, dst = self._warehouse(from_org), self._warehouse(to_org)
        missing = [o for o, w in ((from_org, src), (to_org, dst)) if not w]
        if missing:
            return {"ok": False,
                    "reason": f"unknown warehouse: {', '.join(map(str, missing))}"}
        if not (_id_of(src.get("lot_stock_id")) and _id_of(dst.get("lot_stock_id"))
                and _id_of(src.get("int_type_id"))):
            return {"ok": False,
                    "reason": f"{from_org} or {to_org} has no stock location or "
                              f"internal operation type"}
        src_co, dst_co = _id_of(src.get("company_id")), _id_of(dst.get("company_id"))
        if src_co != dst_co:
            return {"ok": False,
                    "reason": f"different companies "
                              f"({_name_of(src.get('company_id')) or src_co} vs "
                              f"{_name_of(dst.get('company_id')) or dst_co}) — "
                              f"Odoo cannot confirm an internal transfer across "
                              f"them; this is a sale/purchase pair, not a move"}
        return {"ok": True, "reason": ""}

    def push_transfer_request(self, from_org: str, to_org: str,
                              items: List[dict]) -> bool:
        """Create ONE draft internal picking moving `items` from_org -> to_org.

        Odoo models an inter-store move as a `stock.picking` over the source
        warehouse's internal operation type, with the destination set to the
        other warehouse's stock location. Left in DRAFT, which is what maps to
        OASIS's REQUESTED — nothing physically reserves or moves until someone
        advances it.

        One picking per request, not one per line: Odoo's unit of work is the
        picking, and splitting a single shipment into N pickings would make the
        warehouse pick, pack and validate N times for one van.
        """
        # ONE implementation of the rule: the writer asks the same question the
        # console asks before it recommends anything. Repeating the check here
        # is how the read side and write side drifted apart in the first place.
        verdict = self.can_transfer(from_org, to_org)
        if not verdict["ok"]:
            logger.error("push_transfer_request refused (%s -> %s): %s",
                         from_org, to_org, verdict["reason"])
            return False

        src, dst = self._warehouse(from_org), self._warehouse(to_org)
        src_loc, dst_loc = _id_of(src.get("lot_stock_id")), _id_of(dst.get("lot_stock_id"))
        ptype = _id_of(src.get("int_type_id"))

        codes = [str(i.get("item_code")) for i in items if i.get("item_code")]
        if not codes:
            logger.warning("push_transfer_request: no item codes in payload")
            return False
        try:
            prods = self._ex("product.product", "search_read",
                             [[["default_code", "in", codes]]],
                             {"fields": ["default_code", "display_name", "uom_id"],
                              "limit": 10000}) or []
        except Exception as e:
            logger.error("push_transfer_request: product lookup failed: %s",
                         str(e)[:140])
            return False
        by_code = {str(p["default_code"]): p for p in prods}

        moves, missing = [], []
        for it in items:
            p = by_code.get(str(it.get("item_code")))
            qty = float(it.get("transfer_qty") or 0)
            if p is None:
                missing.append(it.get("item_code"))
                continue
            if qty <= 0:
                continue
            moves.append((0, 0, {
                "name": str(it.get("product_name") or p["display_name"])[:200],
                "product_id": p["id"],
                "product_uom_qty": qty,
                # required on stock.move, and NOT defaulted when the move is
                # created through the picking's one2many
                "product_uom": _id_of(p.get("uom_id")),
                "location_id": src_loc,
                "location_dest_id": dst_loc,
            }))
        if missing:
            logger.warning("push_transfer_request: %d item(s) not in Odoo, "
                           "skipped: %s", len(missing), missing[:5])
        if not moves:
            logger.error("push_transfer_request: nothing transferable in payload")
            return False

        urgent = any(str(i.get("urgency", "")).upper() in ("HIGH", "URGENT")
                     for i in items)
        who = next((i.get("requested_by") for i in items if i.get("requested_by")),
                   None) or "OASIS"
        try:
            pid = self._ex("stock.picking", "create", [{
                "picking_type_id": ptype,
                "location_id": src_loc,
                "location_dest_id": dst_loc,
                "origin": f"OASIS transfer {from_org}->{to_org} ({who})",
                "priority": "1" if urgent else "0",
                "move_ids": moves,
            }])
        except Exception as e:
            logger.error("push_transfer_request failed (%s -> %s): %s",
                         from_org, to_org, str(e)[:160])
            return False
        logger.info("Odoo: transfer picking %s created DRAFT, %d lines, %s -> %s",
                    pid, len(moves), from_org, to_org)
        return True

    def fetch_transfers(self, org_cd: Optional[str] = None):
        """Transfer history as a DataFrame in PosErpAdapter's column shape.

        ONE ROW PER MOVED ITEM, because that is what the console's table
        renders — but ``TRANSFER_ID`` is the **picking** id, so the lines of one
        shipment share it. That is deliberate: Odoo's unit of work is the
        picking, and ``update_transfer_status`` advances a whole shipment. It
        differs from PosErpAdapter, where every item row has its own id.

        ``org_cd`` matches the store as EITHER sender or receiver, mirroring
        PosErpAdapter's `FROM_ORG_CD = :org OR TO_ORG_CD = :org`.
        """
        import pandas as pd

        empty = pd.DataFrame(columns=list(self.TRANSFER_COLUMNS))
        dom = [["picking_type_id.code", "=", "internal"]]
        if org_cd:
            wh = self._warehouse(org_cd)
            if not wh:
                return empty
            root = _id_of(wh.get("view_location_id"))
            dom += ["|", ["location_id", "child_of", root],
                    ["location_dest_id", "child_of", root]]
        try:
            picks = self._ex("stock.picking", "search_read", [dom],
                             {"fields": ["name", "state", "priority", "origin",
                                         "location_id", "location_dest_id",
                                         "create_date", "date_done", "create_uid",
                                         "move_ids"],
                              "order": "create_date desc", "limit": 2000}) or []
        except Exception as e:
            logger.error("fetch_transfers failed: %s", str(e)[:140])
            return empty
        if not picks:
            return empty

        move_ids = [m for p in picks for m in (p.get("move_ids") or [])]
        moves: Dict[int, dict] = {}
        if move_ids:
            try:
                for m in (self._ex("stock.move", "read", [move_ids],
                                   {"fields": ["product_id", "product_uom_qty",
                                               "quantity_done"]}) or []):
                    moves[m["id"]] = m
            except Exception as e:
                logger.warning("fetch_transfers: move read failed: %s", str(e)[:120])

        pids = {_id_of(m.get("product_id")) for m in moves.values()}
        pids.discard(None)
        meta: Dict[int, dict] = {}
        if pids:
            try:
                for p in (self._ex("product.product", "read", [sorted(pids)],
                                   {"fields": ["default_code", "display_name",
                                               "standard_price"]}) or []):
                    meta[p["id"]] = p
            except Exception as e:
                logger.warning("fetch_transfers: product read failed: %s", str(e)[:120])

        # location -> warehouse CODE, so rows carry org codes not location names
        wh_of_loc: Dict[int, str] = {}
        locs = {_id_of(p.get(k)) for p in picks
                for k in ("location_id", "location_dest_id")}
        locs.discard(None)
        if locs:
            try:
                for l in (self._ex("stock.location", "read", [sorted(locs)],
                                   {"fields": ["warehouse_id"]}) or []):
                    wh_of_loc[l["id"]] = _name_of(l.get("warehouse_id"))
            except Exception as e:
                logger.warning("fetch_transfers: location read failed: %s", str(e)[:120])
        # warehouse_id reads as [id, name]; the console wants the CODE
        code_of_name = {}
        try:
            for w in (self._ex("stock.warehouse", "search_read", [[]],
                               {"fields": ["name", "code"]}) or []):
                code_of_name[w["name"]] = w.get("code") or w["name"]
        except Exception:
            pass

        def org_of(loc) -> str:
            name = wh_of_loc.get(_id_of(loc), "")
            return code_of_name.get(name, name)

        rows = []
        for p in picks:
            status = self._PICKING_STATUS.get(p.get("state"), str(p.get("state") or ""))
            frm, to = org_of(p.get("location_id")), org_of(p.get("location_dest_id"))
            for mid in (p.get("move_ids") or []):
                m = moves.get(mid)
                if not m:
                    continue
                pm = meta.get(_id_of(m.get("product_id")), {})
                qty = float(m.get("product_uom_qty") or 0)
                rows.append({
                    "TRANSFER_ID": p["id"],
                    "FROM_ORG_CD": frm,
                    "TO_ORG_CD": to,
                    "ITM_CD": str(pm.get("default_code")
                                  or _id_of(m.get("product_id")) or ""),
                    "PRODUCT_NAME": pm.get("display_name")
                    or _name_of(m.get("product_id")),
                    "QUANTITY": qty,
                    # PosErpAdapter stores a value passed in by the caller; here
                    # it is derived at COST, which is what a transfer actually
                    # moves off one store's books and onto another's.
                    "VALUE_KES": round(qty * float(pm.get("standard_price") or 0), 2),
                    "STATUS": status,
                    "URGENCY": "HIGH" if str(p.get("priority")) == "1" else "NORMAL",
                    "REQUESTED_BY": _name_of(p.get("create_uid")),
                    "CREATED_DT": str(p.get("create_date") or ""),
                    "COMPLETED_DT": str(p.get("date_done") or ""),
                })
        return pd.DataFrame(rows, columns=list(self.TRANSFER_COLUMNS)) if rows else empty

    def update_transfer_status(self, transfer_id: int, status: str) -> bool:
        """Advance a transfer picking. ``transfer_id`` is a stock.picking id.

        IN_TRANSIT confirms and reserves — Odoo's `confirmed`/`assigned`.
        RECEIVED validates, which is a REAL stock movement between the client's
        own stores. That is the one genuinely irreversible step here, so it is
        taken only on an explicit operator action and the result is confirmed by
        reading the state back.

        Included alongside the two transfer methods because without it the
        console's transfer table renders rows it cannot advance.
        """
        want = (status or "").strip().upper()
        if want not in ("IN_TRANSIT", "RECEIVED"):
            logger.error("update_transfer_status: unknown status %r "
                         "(expected IN_TRANSIT or RECEIVED)", status)
            return False
        try:
            cur = self._ex("stock.picking", "read", [[int(transfer_id)]],
                           {"fields": ["name", "state", "move_ids"]}) or []
        except Exception as e:
            logger.error("update_transfer_status: cannot read picking %s: %s",
                         transfer_id, str(e)[:140])
            return False
        if not cur:
            logger.error("update_transfer_status: stock.picking %s not found",
                         transfer_id)
            return False
        pick, name, state = cur[0], cur[0].get("name"), cur[0].get("state")
        if state in ("done", "cancel"):
            logger.error("update_transfer_status: %s is already %r", name, state)
            return False

        try:
            if state == "draft":
                self._call_ignoring_none("stock.picking", "action_confirm",
                                         [[int(transfer_id)]])
            self._call_ignoring_none("stock.picking", "action_assign",
                                     [[int(transfer_id)]])
            if want == "RECEIVED":
                # Without a done quantity Odoo answers button_validate with an
                # "Immediate Transfer" WIZARD (a dict), not a validation — the
                # picking would stay open while the call looked successful.
                for mid in (pick.get("move_ids") or []):
                    m = (self._ex("stock.move", "read", [[mid]],
                                  {"fields": ["product_uom_qty"]}) or [{}])[0]
                    self._ex("stock.move", "write",
                             [[mid], {"quantity_done": float(m.get("product_uom_qty") or 0)}])
                self._call_ignoring_none("stock.picking", "button_validate",
                                         [[int(transfer_id)]])
        except Exception as e:
            logger.error("update_transfer_status(%s -> %s) failed: %s",
                         name, want, str(e)[:160])
            return False

        now = (self._ex("stock.picking", "read", [[int(transfer_id)]],
                        {"fields": ["state"]}) or [{}])[0].get("state")
        mapped = self._PICKING_STATUS.get(now, now)
        if mapped != want:
            logger.error("update_transfer_status: %s asked for %s but Odoo is "
                         "%r (%s) — reporting failure rather than a status the "
                         "warehouse does not have", name, want, now, mapped)
            return False
        logger.info("Odoo: transfer %s -> %s (picking state %s)", name, want, now)
        return True

    def _call_ignoring_none(self, model: str, method: str, args: list):
        """Call an Odoo action method whose return value may be None.

        Odoo's XML-RPC endpoint serialises with allow_none=False, so a method
        returning None raises "cannot marshal None" AFTER the write has already
        committed. Setting allow_none on our proxy does not help — the server
        is the one serialising. Callers must confirm by reading state back.
        """
        try:
            return self._ex(model, method, args)
        except xmlrpc.client.Fault as e:
            if "cannot marshal None" not in str(e):
                raise
            return None

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


_contract.register("odoo", OdooAdapter)
