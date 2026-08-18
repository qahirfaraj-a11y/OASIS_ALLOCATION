"""Zoho Inventory adapter — the ERP contract over Zoho's REST API.

STATUS: **NOT YET LIVE-VERIFIED.** Every field name below is taken from Zoho's
published API reference, not from a running organisation. That is precisely the
condition under which this codebase has repeatedly shipped code that agreed with
itself and disagreed with reality, so nothing here should be trusted until
``tests/test_erp_conformance.py`` runs green with ``OASIS_TEST_ZOHO`` set.
Treat the capability declaration as a claim awaiting evidence.

WHY ZOHO FIRST
--------------
Of the candidate backends it is the closest analogue to Odoo — real purchase
orders, real transfer orders, per-location stock and a genuine cost price — so
building it is the second proof that the eight-method contract is swappable.
Shopify and Square would be quicker to obtain credentials for and would prove
less: neither has a purchase order object to write into at all.

LOCATIONS, NOT WAREHOUSES
-------------------------
Zoho migrated multi-warehouse to "locations". ``/locations`` is the current
endpoint and ``location_id`` the current field, while transfer orders still
accept the legacy ``from_warehouse_id``/``to_warehouse_id`` alongside
``from_location_id``/``to_location_id``. OASIS's ``org_cd`` maps to a LOCATION,
and the legacy names are never written — a mixed write is how an integration
ends up half-migrated.

WHAT IS DELIBERATELY NOT CLAIMED
--------------------------------
``READ_RECEIPTS`` is **not** declared. Zoho has a purchase-receives concept, but
it has not been verified here, and a falsely declared receipt date is the exact
shape of the RXL defect that silently disabled the dead-stock guard and let
KES 10.4M of dead stock through. Better to declare it absent — the guard is
then known-inert and ``diagnose()`` says so — than to declare it present and be
wrong.
"""

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

from oasis.logic import erp_contract as _contract

logger = logging.getLogger("ZohoAdapter")

#: Zoho is region-partitioned; a token minted in one data centre is invalid in
#: another, and the failure reads as a bad token rather than a wrong host.
DATA_CENTRES = {
    "com": ("https://www.zohoapis.com", "https://accounts.zoho.com"),
    "eu": ("https://www.zohoapis.eu", "https://accounts.zoho.eu"),
    "in": ("https://www.zohoapis.in", "https://accounts.zoho.in"),
    "au": ("https://www.zohoapis.com.au", "https://accounts.zoho.com.au"),
    "jp": ("https://www.zohoapis.jp", "https://accounts.zoho.jp"),
    "ca": ("https://www.zohoapis.ca", "https://accounts.zohocloud.ca"),
}

#: Zoho caps a page at 200. A catalogue read that stops there looks like a small
#: store rather than a truncated one, so every list call paginates and the cap
#: below exists only to bound a runaway loop — hitting it is logged as an ERROR,
#: never passed off as a complete result.
PAGE_SIZE = 200
MAX_PAGES = 500

FRESH_DEPARTMENTS = ("DAIRY", "FRESH PRODUCE", "BUTCHERY", "BAKERY", "FRESH")

#: What OASIS calls a department when Zoho has not categorised the item. NOT
#: the item's own name — see _department_of.
UNCATEGORISED = "UNCATEGORISED"


def _department_of(item: dict) -> str:
    """Zoho's category, which is NOT ``group_name``.

    ``group_name`` is the ITEM-GROUP (variant group) name, and Zoho defaults it
    to the item's own name — verified live: an item created as "OASIS Probe
    Item" came back with ``group_name: 'OASIS Probe Item'`` and
    ``category_name: ''``. Reading it as the department gives every product a
    department of one, which fragments grouping, budget allocation and
    department reporting exactly as the Odoo category-path bug did.

    An uncategorised item is labelled as such rather than silently bucketed
    into a real department, so "nothing is categorised in Zoho" is visible in
    the console instead of looking like a grocery-only store.
    """
    cat = str(item.get("category_name") or "").strip()
    return cat.upper() if cat else UNCATEGORISED


class ZohoAdapter(_contract.ErpAdapter):
    """Reads a Zoho Inventory organisation over REST and speaks OASIS's dialect."""

    ERP_NAME = "zoho"
    #: Claimed, pending live conformance. READ_RECEIPTS is deliberately absent
    #: (see module docstring) — do not add it without evidence that
    #: days_since_delivery is genuinely populated.
    CAPABILITIES = frozenset({
        _contract.READ_CATALOGUE, _contract.READ_STOCK, _contract.READ_DEMAND,
        _contract.READ_COST, _contract.READ_SUPPLIERS, _contract.READ_OPEN_POS,
        _contract.MULTI_SITE,
        _contract.WRITE_PO, _contract.READ_TRANSFERS,
        _contract.WRITE_TRANSFER, _contract.WRITE_TRANSFER_STATUS,
    })

    def __init__(self, client_id: Optional[str] = None,
                 client_secret: Optional[str] = None,
                 refresh_token: Optional[str] = None,
                 organization_id: Optional[str] = None,
                 data_centre: Optional[str] = None,
                 store_connector=None):
        self.client_id = client_id or os.getenv("ZOHO_CLIENT_ID", "")
        self.client_secret = client_secret or os.getenv("ZOHO_CLIENT_SECRET", "")
        self.refresh_token = refresh_token or os.getenv("ZOHO_REFRESH_TOKEN", "")
        self.org_id = organization_id or os.getenv("ZOHO_ORG_ID", "")
        dc = (data_centre or os.getenv("ZOHO_DC", "com")).strip().lower()
        if dc not in DATA_CENTRES:
            raise ValueError(f"unknown Zoho data centre {dc!r}; "
                             f"expected one of {sorted(DATA_CENTRES)}")
        self.api_base, self.accounts_base = DATA_CENTRES[dc]
        self.data_centre = dc

        # OASIS's own store stays local — the client's ERP is read, never used
        # as OASIS's bookkeeping database.
        self.store_connector = store_connector
        self.store_engine = getattr(store_connector, "engine", None)

        self._token: Optional[str] = None
        self._token_expires_at: float = 0.0
        self._loc_cache: Dict[str, Optional[dict]] = {}

    # ── auth ─────────────────────────────────────────────────────────────
    def _token_cache_path(self) -> str:
        import hashlib
        tag = hashlib.sha256(
            f"{self.data_centre}:{self.org_id}".encode()).hexdigest()[:12]
        return os.path.join(os.path.expanduser("~"), f".oasis_zoho_token_{tag}")

    def _load_cached_token(self) -> None:
        """Reuse a still-valid access token across PROCESSES.

        Zoho rate-limits the token endpoint, and an in-memory cache does
        nothing when each CLI run, test run and console launch is a fresh
        process — a handful of runs in quick succession is enough to be
        refused, which then looks like a bad credential rather than a quota.
        """
        try:
            with open(self._token_cache_path(), "r", encoding="utf-8") as fh:
                tok, _, exp = fh.read().partition("\n")
            if tok and float(exp) > time.time():
                self._token, self._token_expires_at = tok, float(exp)
        except Exception:
            pass

    def _save_cached_token(self) -> None:
        try:
            path = self._token_cache_path()
            with open(path, "w", encoding="utf-8") as fh:
                fh.write(f"{self._token}\n{self._token_expires_at}")
            try:
                os.chmod(path, 0o600)
            except OSError:
                pass                      # best effort; Windows ACLs differ
        except Exception as e:
            logger.debug("could not cache Zoho token: %s", e)

    def _access_token(self) -> str:
        """Exchange the refresh token, cached until shortly before expiry.

        Zoho access tokens last an hour; refreshing on every call burns the
        (rate-limited) token endpoint, and refreshing only on 401 means every
        expiry costs a failed request.
        """
        if not self._token:
            self._load_cached_token()
        if self._token and time.time() < self._token_expires_at:
            return self._token
        missing = [n for n, v in (("ZOHO_CLIENT_ID", self.client_id),
                                  ("ZOHO_CLIENT_SECRET", self.client_secret),
                                  ("ZOHO_REFRESH_TOKEN", self.refresh_token),
                                  ("ZOHO_ORG_ID", self.org_id)) if not v]
        if missing:
            raise ConnectionError(
                f"Zoho credentials missing: {', '.join(missing)}. "
                f"An OAuth self-client from api-console.zoho.{self.data_centre} "
                f"with scope ZohoInventory.FullAccess.all supplies all four."
            )
        import requests
        # Credentials go in the POST BODY, never the query string. requests puts
        # the full URL into HTTPError, so query-string secrets end up in any
        # traceback, log file or terminal scrollback that captures the failure.
        # raise_for_status() is deliberately NOT used here for the same reason.
        r = requests.post(f"{self.accounts_base}/oauth/v2/token", data={
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
        }, timeout=30)
        try:
            body = r.json()
        except ValueError:
            body = {}
        token = body.get("access_token")
        if not token:
            # Zoho answers a refused refresh with HTTP 200 and an "error" key,
            # so status alone would let this through as success.
            err = body.get("error") or f"HTTP {r.status_code}"
            hint = ""
            if str(err) == "Access Denied":
                hint = (" — Zoho rate-limits token refreshes; wait a few "
                        "minutes rather than retrying immediately")
            raise ConnectionError(f"Zoho refused the refresh token: {err}{hint}")
        self._token = token
        self._token_expires_at = time.time() + int(body.get("expires_in", 3600)) - 120
        self._save_cached_token()
        return self._token

    # ── transport ────────────────────────────────────────────────────────
    def _call(self, method: str, path: str, params: Optional[dict] = None,
              json_body: Optional[dict] = None, _retry: int = 0) -> dict:
        import requests
        p = dict(params or {})
        p["organization_id"] = self.org_id
        r = requests.request(
            method, f"{self.api_base}/inventory/v1{path}",
            headers={"Authorization": f"Zoho-oauthtoken {self._access_token()}"},
            params=p, json=json_body, timeout=60,
        )
        if r.status_code == 401 and _retry == 0:
            self._token = None                       # force a refresh, retry once
            return self._call(method, path, params, json_body, _retry=1)
        if r.status_code == 429 and _retry < 3:
            wait = min(2 ** _retry, 8)
            logger.warning("Zoho rate-limited; retrying %s in %ss", path, wait)
            time.sleep(wait)
            return self._call(method, path, params, json_body, _retry=_retry + 1)
        r.raise_for_status()
        body = r.json()
        # Zoho returns HTTP 200 with a non-zero "code" for business errors.
        if isinstance(body, dict) and body.get("code") not in (0, None):
            raise RuntimeError(f"Zoho error {body.get('code')} on {path}: "
                               f"{body.get('message')}")
        return body

    def _paged(self, path: str, key: str, params: Optional[dict] = None) -> List[dict]:
        """Every record, not the first page.

        Silent truncation at 200 is the failure this exists to prevent: a
        4,000-SKU catalogue returning 200 rows reads as a small store, and the
        engine would order for a shop that does not exist.
        """
        out: List[dict] = []
        page = 1
        while page <= MAX_PAGES:
            body = self._call("GET", path,
                              {**(params or {}), "page": page, "per_page": PAGE_SIZE})
            rows = body.get(key) or []
            out.extend(rows)
            ctx = body.get("page_context") or {}
            if not ctx.get("has_more_page") or not rows:
                return out
            page += 1
        logger.error("%s: stopped at %d pages (%d rows) — result is TRUNCATED, "
                     "not complete", path, MAX_PAGES, len(out))
        return out

    # ── sites ────────────────────────────────────────────────────────────
    def _location(self, org_cd: Optional[str]) -> Optional[dict]:
        """Resolve an OASIS org code to a Zoho location, by id or by name."""
        if not org_cd:
            return None
        key = str(org_cd)
        if key in self._loc_cache:
            return self._loc_cache[key]
        try:
            locs = self._paged("/locations", "locations")
        except Exception as e:
            logger.error("location lookup failed for %r (%s) — reading "
                         "org-wide; site scoping is NOT in effect", org_cd, e)
            self._loc_cache[key] = None
            return None
        match = next((l for l in locs
                      if str(l.get("location_id")) == key
                      or str(l.get("location_name", "")).strip() == key), None)
        if match is None:
            logger.warning("Zoho location %r not found — falling back to "
                           "organisation-wide", org_cd)
        self._loc_cache[key] = match
        return match

    def _location_id(self, org_cd: Optional[str]) -> Optional[str]:
        loc = self._location(org_cd)
        return str(loc["location_id"]) if loc else None

    def health_check(self) -> Dict[str, Any]:
        try:
            t0 = time.time()
            body = self._call("GET", "/locations", {"per_page": 1})
            return {"connected": True,
                    "latency_ms": round((time.time() - t0) * 1000, 1),
                    "tables_found": len(body.get("locations") or [])}
        except Exception as e:
            return {"connected": False, "error": str(e)[:200], "tables_found": 0}

    def fetch_all_organizations(self) -> List[dict]:
        try:
            locs = self._paged("/locations", "locations")
        except Exception as e:
            logger.error("fetch_all_organizations failed: %s", e)
            return []
        return [{"ORG_CD": str(l.get("location_id")),
                 "ORG_NAME": l.get("location_name") or str(l.get("location_id")),
                 "ACTIVE_FLAG": "Y" if str(l.get("status", "active")).lower()
                                == "active" else "N"}
                for l in locs]

    # ── reads ────────────────────────────────────────────────────────────
    def _vendor_names(self) -> Dict[str, dict]:
        """vendor_id -> {name}. Zoho keeps vendors in contacts."""
        try:
            rows = self._paged("/contacts", "contacts",
                               {"contact_type": "vendor"})
        except Exception as e:
            logger.warning("vendor lookup failed: %s", str(e)[:120])
            return {}
        return {str(c.get("contact_id")): {
            "name": c.get("contact_name") or c.get("company_name") or "Unknown"}
            for c in rows}

    def _demand(self, org_cd: Optional[str] = None,
                days: int = 90) -> Dict[str, Dict[str, float]]:
        """item_id -> {units, revenue} from sales orders inside the window.

        Zoho's list endpoint exposes no documented date filter, so the window is
        applied client-side. Pages are walked newest-first and the walk stops
        once a page falls entirely outside the window — otherwise a long-lived
        organisation would be read in full on every run.
        """
        since = (datetime.now() - timedelta(days=days)).date()
        agg: Dict[str, Dict[str, float]] = {}
        params: Dict[str, Any] = {"sort_column": "date", "sort_order": "D"}
        loc = self._location_id(org_cd)
        if loc:
            params["location_id"] = loc

        page, stop = 1, False
        while page <= MAX_PAGES and not stop:
            try:
                body = self._call("GET", "/salesorders",
                                  {**params, "page": page, "per_page": PAGE_SIZE})
            except Exception as e:
                logger.warning("sales orders unavailable: %s", str(e)[:120])
                break
            orders = body.get("salesorders") or []
            if not orders:
                break
            in_window = 0
            for o in orders:
                try:
                    d = datetime.strptime(str(o.get("date"))[:10], "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    continue
                if d < since:
                    continue
                in_window += 1
                for li in (o.get("line_items") or []):
                    iid = str(li.get("item_id") or "")
                    if not iid:
                        continue
                    e = agg.setdefault(iid, {"units": 0.0, "revenue": 0.0})
                    qty = float(li.get("quantity") or 0)
                    e["units"] += qty
                    e["revenue"] += qty * float(li.get("rate") or 0)
            # sorted newest-first, so a page with nothing in-window ends the walk
            stop = in_window == 0
            if not (body.get("page_context") or {}).get("has_more_page"):
                break
            page += 1
        return agg

    def fetch_enriched_products(self, org_cd: Optional[str] = None,
                                sales_days: int = 90) -> List[dict]:
        params: Dict[str, Any] = {}
        loc = self._location_id(org_cd)
        if loc:
            # scopes location_stock_on_hand to this site; without it every site
            # reports the organisation's whole position and orders as though it
            # held it
            params["location_id"] = loc
        try:
            items = self._paged("/items", "items", params)
        except Exception as e:
            logger.error("fetch_enriched_products failed: %s", e)
            return []

        demand = self._demand(org_cd, sales_days)
        vendors = self._vendor_names()
        out: List[dict] = []
        for it in items:
            if str(it.get("status", "active")).lower() != "active":
                continue
            iid = str(it.get("item_id") or "")
            dept = _department_of(it)
            sold = demand.get(iid, {"units": 0.0, "revenue": 0.0})
            vid = str(it.get("vendor_id") or "")
            # location_stock_on_hand is present when the call is location-scoped;
            # stock_on_hand is the organisation-wide figure
            stock = it.get("location_stock_on_hand")
            if stock is None:
                stock = it.get("stock_on_hand")
            out.append({
                "item_code": str(it.get("sku") or iid),
                "zoho_item_id": iid,
                "product_name": it.get("name") or "",
                "barcode": str(it.get("ean") or it.get("upc") or ""),
                "current_stocks": float(stock or 0),
                "selling_price": float(it.get("rate") or 0),
                "cost_price": float(it.get("purchase_rate") or 0),
                "department": dept,
                "category": dept,
                "sub_category": dept,
                "uom": it.get("unit") or "EA",
                "is_fresh": any(f in dept for f in FRESH_DEPARTMENTS),
                "supplier_cd": vid,
                "supplier_name": vendors.get(vid, {}).get("name", "Unknown"),
                "estimated_delivery_days": 7,
                "supplier_reliability": 0.9,
                "pack_size": 1,
                "blocked_open_for_order": "open",
                "avg_daily_sales": round(sold["units"] / max(sales_days, 1), 4),
                "estimated_daily_sales": round(sold["units"] / max(sales_days, 1), 4),
                "units_sold_last_month": round(sold["units"] / max(sales_days, 1) * 30, 2),
                # NOT supplied by this backend — see the module docstring. Left
                # at 0 knowingly, and diagnose() reports the guard as inert.
                "days_since_delivery": 0,
                "last_days_since_last_delivery": 0,
            })
        logger.info("Zoho: enriched %d products (org=%s)", len(out), org_cd)
        return out

    def fetch_sales_history(self, org_cd: Optional[str] = None,
                            days: int = 90) -> List[dict]:
        return [{"item_code": k, "units": v["units"], "revenue": v["revenue"]}
                for k, v in self._demand(org_cd, days).items()]

    def _open_po_lines(self, org_cd: Optional[str] = None) -> List[dict]:
        params: Dict[str, Any] = {"status": "open"}
        loc = self._location_id(org_cd)
        if loc:
            params["location_id"] = loc
        try:
            return self._paged("/purchaseorders", "purchaseorders", params)
        except Exception as e:
            logger.warning("fetch of open purchase orders failed: %s", str(e)[:120])
            return []

    def fetch_pending_pos(self, org_cd: Optional[str] = None):
        rows = []
        for po in self._open_po_lines(org_cd):
            for li in (po.get("line_items") or []):
                rows.append({
                    "PO_ID": li.get("line_item_id") or po.get("purchaseorder_id"),
                    "ITM_CD": str(li.get("sku") or li.get("item_id") or ""),
                    "PRODUCT_NAME": li.get("name") or "",
                    "QUANTITY": float(li.get("quantity") or 0),
                    "UNIT_COST": float(li.get("rate") or li.get("purchase_rate") or 0),
                    "STATUS": "PENDING",
                })
        return rows

    def fetch_pending_po_by_sku(self, org_cd: Optional[str] = None) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for po in self._open_po_lines(org_cd):
            for li in (po.get("line_items") or []):
                code = str(li.get("sku") or li.get("item_id") or "")
                if not code:
                    continue
                outstanding = (float(li.get("quantity") or 0)
                               - float(li.get("quantity_received") or 0))
                if outstanding > 0:
                    e = out.setdefault(code, {"qty": 0.0, "eta_days": 7})
                    e["qty"] += outstanding
        return out

    # ── writes: proposals only ───────────────────────────────────────────
    def push_purchase_order(self, org_cd: str,
                            recommendations: List[dict]) -> int:
        """One DRAFT purchase order per vendor. Never submitted for approval.

        Zoho creates a purchase order in ``draft`` by default and only moves it
        on via its own endpoints, which this adapter does not call: OASIS
        proposes, a human approves in Zoho.
        """
        from collections import defaultdict
        by_vendor: Dict[str, List[dict]] = defaultdict(list)
        for rec in recommendations:
            if float(rec.get("recommended_quantity") or 0) <= 0:
                continue
            by_vendor[str(rec.get("supplier_cd") or "")].append(rec)
        if not by_vendor:
            return 0

        loc = self._location_id(org_cd)
        if org_cd and not loc:
            logger.warning("no Zoho location for %r — the order will land at "
                           "the organisation default", org_cd)

        written = 0
        for vendor_id, recs in by_vendor.items():
            if not vendor_id:
                logger.warning("skipping %d line(s) with no vendor — Zoho "
                               "requires vendor_id on a purchase order",
                               len(recs))
                continue
            lines = [{
                "item_id": r.get("zoho_item_id") or r.get("item_id"),
                "quantity": float(r.get("recommended_quantity") or 0),
                "purchase_rate": float(r.get("cost_price") or 0),
                "name": str(r.get("product_name") or "")[:200],
            } for r in recs if (r.get("zoho_item_id") or r.get("item_id"))]
            if not lines:
                continue
            payload: Dict[str, Any] = {"vendor_id": vendor_id, "line_items": lines}
            if loc:
                payload["location_id"] = loc
            try:
                self._call("POST", "/purchaseorders", json_body=payload)
                written += len(lines)
            except Exception as e:
                logger.error("push_purchase_order failed for vendor %s: %s",
                             vendor_id, str(e)[:160])
        logger.info("Zoho: wrote %d PO lines as DRAFT", written)
        return written

    # ── transfers ────────────────────────────────────────────────────────
    _TRANSFER_STATUS = {
        "draft": "REQUESTED",
        "pending_approval": "REQUESTED",
        "approved": "IN_TRANSIT",
        "in_transit": "IN_TRANSIT",
        "partially_transferred": "IN_TRANSIT",
        "transferred": "RECEIVED",
        "void": "CANCELLED",
    }

    TRANSFER_COLUMNS = ("TRANSFER_ID", "FROM_ORG_CD", "TO_ORG_CD", "ITM_CD",
                        "PRODUCT_NAME", "QUANTITY", "VALUE_KES", "STATUS",
                        "URGENCY", "REQUESTED_BY", "CREATED_DT", "COMPLETED_DT")

    def fetch_transfers(self, org_cd: Optional[str] = None):
        import pandas as pd
        empty = pd.DataFrame(columns=list(self.TRANSFER_COLUMNS))
        try:
            orders = self._paged("/transferorders", "transfer_orders")
        except Exception as e:
            logger.error("fetch_transfers failed: %s", str(e)[:140])
            return empty

        loc = self._location_id(org_cd)
        rows = []
        for t in orders:
            frm = str(t.get("from_location_id") or t.get("from_warehouse_id") or "")
            to = str(t.get("to_location_id") or t.get("to_warehouse_id") or "")
            if loc and loc not in (frm, to):     # sender OR receiver, as the contract says
                continue
            status = self._TRANSFER_STATUS.get(
                str(t.get("status", "")).lower(), str(t.get("status") or ""))
            for li in (t.get("line_items") or []):
                qty = float(li.get("quantity_transfer") or li.get("quantity") or 0)
                rows.append({
                    "TRANSFER_ID": t.get("transfer_order_id"),
                    "FROM_ORG_CD": frm, "TO_ORG_CD": to,
                    "ITM_CD": str(li.get("sku") or li.get("item_id") or ""),
                    "PRODUCT_NAME": li.get("name") or "",
                    "QUANTITY": qty,
                    "VALUE_KES": round(qty * float(li.get("rate") or 0), 2),
                    "STATUS": status,
                    "URGENCY": "NORMAL",
                    "REQUESTED_BY": t.get("created_by_name") or "",
                    "CREATED_DT": str(t.get("date") or ""),
                    "COMPLETED_DT": str(t.get("transferred_date") or ""),
                })
        return pd.DataFrame(rows, columns=list(self.TRANSFER_COLUMNS)) if rows else empty

    def push_transfer_request(self, from_org: str, to_org: str,
                              items: List[dict]) -> bool:
        src, dst = self._location_id(from_org), self._location_id(to_org)
        if not src or not dst:
            logger.error("push_transfer_request: unknown location (%s -> %s)",
                         from_org, to_org)
            return False
        lines = [{
            "item_id": i.get("zoho_item_id") or i.get("item_id"),
            "name": str(i.get("product_name") or "")[:200],
            "quantity_transfer": float(i.get("transfer_qty") or 0),
        } for i in items
            if (i.get("zoho_item_id") or i.get("item_id"))
            and float(i.get("transfer_qty") or 0) > 0]
        if not lines:
            logger.error("push_transfer_request: nothing transferable in payload")
            return False
        try:
            self._call("POST", "/transferorders", json_body={
                "from_location_id": src, "to_location_id": dst,
                "line_items": lines,
            })
        except Exception as e:
            logger.error("push_transfer_request failed (%s -> %s): %s",
                         from_org, to_org, str(e)[:160])
            return False
        logger.info("Zoho: transfer order created DRAFT, %d lines, %s -> %s",
                    len(lines), from_org, to_org)
        return True

    def update_transfer_status(self, transfer_id: int, status: str) -> bool:
        """Only RECEIVED is actionable — Zoho exposes ``markastransferred``.

        IN_TRANSIT is refused rather than faked: Zoho moves a transfer there
        through its own approval flow, and pretending otherwise would report a
        status the warehouse does not have.
        """
        want = (status or "").strip().upper()
        if want == "IN_TRANSIT":
            raise _contract.Unsupported(
                self.ERP_NAME, _contract.WRITE_TRANSFER_STATUS,
                "Zoho advances a transfer to in-transit through its own "
                "approval flow; only 'RECEIVED' can be set from outside")
        if want != "RECEIVED":
            logger.error("update_transfer_status: unknown status %r", status)
            return False
        try:
            self._call("POST", f"/transferorders/{int(transfer_id)}/markastransferred",
                       params={"date": datetime.now().strftime("%Y-%m-%d")})
        except Exception as e:
            logger.error("update_transfer_status(%s) failed: %s",
                         transfer_id, str(e)[:160])
            return False
        # confirm by reading back rather than trusting the call
        try:
            body = self._call("GET", f"/transferorders/{int(transfer_id)}")
            state = str((body.get("transfer_order") or {}).get("status", "")).lower()
        except Exception:
            logger.warning("could not confirm transfer %s — reporting failure",
                           transfer_id)
            return False
        if self._TRANSFER_STATUS.get(state) != "RECEIVED":
            logger.error("transfer %s asked for RECEIVED but Zoho is %r",
                         transfer_id, state)
            return False
        return True

    # ── observability ────────────────────────────────────────────────────
    def diagnose(self, org_cd: Optional[str] = None) -> Dict[str, Any]:
        out: Dict[str, Any] = {"url": self.api_base, "db": self.org_id,
                               "user": f"dc={self.data_centre}"}
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
        out["with_receipt_date"] = 0
        out["departments"] = len({p.get("department") for p in prods})
        out["organisations"] = [o.get("ORG_CD") for o in self.fetch_all_organizations()]
        out["open_po_lines"] = len(self.fetch_pending_pos(org_cd))

        warnings = ["NO receipt dates: Zoho purchase-receives are not read by "
                    "this adapter, so days_since_delivery is 0 for every "
                    "product and the dead-stock guard cannot fire (fails OPEN)."]
        if out["products"] and not out["with_demand"]:
            warnings.append("NO DEMAND: every ADS is zero — the engine will "
                            "recommend nothing.")
        if out["negative_stock"]:
            warnings.append(f"{out['negative_stock']} products have NEGATIVE "
                            "stock — order quantities will be inflated.")
        if out["products"] and out["with_cost"] < out["products"] * 0.5:
            warnings.append(f"only {out['with_cost']}/{out['products']} have a "
                            "cost price — order VALUE and budget gating "
                            "will be wrong.")
        uncategorised = sum(1 for p in prods
                            if p.get("department") == UNCATEGORISED)
        out["uncategorised"] = uncategorised
        if out["products"] and uncategorised == out["products"]:
            warnings.append(
                "NO categories: every item is UNCATEGORISED. Zoho's "
                "category_name is empty across the catalogue, so department "
                "grouping, budget allocation and department reporting all "
                "collapse to one bucket.")
        elif out["products"] and out["departments"] <= 1:
            warnings.append("all products in ONE department — check "
                            "category_name (NOT group_name, which Zoho "
                            "defaults to the item's own name).")
        out["warnings"] = warnings
        return out


_contract.register("zoho", ZohoAdapter)
