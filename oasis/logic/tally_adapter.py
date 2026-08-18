"""Tally Prime / ERP 9 adapter — the ERP contract over Tally's XML gateway.

STATUS: **NOT LIVE-VERIFIED.** Written from Tally's integration architecture,
which is stable and well known, but the exact TDL tag names below have not been
checked against a running company. The Zoho build proved that even WITH the
vendor's own documentation open, one field (``group_name``) was still wrong in a
way no unit test could catch. Treat every tag here as a claim until
``tests/test_erp_conformance.py`` runs green with ``OASIS_TEST_TALLY`` set.

WHY TALLY MATTERS FOR THIS MARKET
---------------------------------
It is the dominant SMB accounting/inventory package across East Africa and
India, and unlike the other on-prem targets it SHIPS an integration surface.
Sage 50, Pastel and RXL all require going through their database or their
vendor; Tally answers XML on a socket out of the box. The pattern built here is
the one that will serve the rest of that family.

HOW IT WORKS
------------
Tally runs an HTTP server (default port 9000) once "Act as Server" is enabled.
You POST an XML ``<ENVELOPE>`` and get XML back. There is no REST, no JSON, no
OAuth, and — importantly — **no authentication of any kind**.

THE SECURITY PROPERTY THAT SHAPES THIS ADAPTER
----------------------------------------------
Tally's gateway is unauthenticated. Anyone who can reach the port can read the
client's entire books and post vouchers into them. That is a property of Tally,
not of OASIS, but OASIS must not make it worse: this adapter refuses a
non-loopback host unless ``TALLY_ALLOW_REMOTE=1`` is set deliberately, so the
default configuration cannot quietly ship a client's ledger across their LAN.

PROPOSE, DO NOT COMMIT
----------------------
Tally has no "draft" state, but it does have OPTIONAL vouchers
(``<ISOPTIONAL>Yes</ISOPTIONAL>``) — a voucher that is recorded but does not
post to books or affect stock until a human marks it regular. That is the exact
analogue of the Odoo draft purchase order, and it is what this adapter writes.
"""

from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from xml.etree import ElementTree as ET

from oasis.logic import erp_contract as _contract

logger = logging.getLogger("TallyAdapter")

LOOPBACK = ("localhost", "127.0.0.1", "::1", "[::1]")

FRESH_KEYS = ("DAIRY", "FRESH", "BUTCHERY", "BAKERY", "MILK", "MEAT", "BREAD")

#: Tally quantities read back as "12.000 Nos" / "-3 pcs"; amounts as "1234.50".
_NUM = re.compile(r"-?\d+(?:\.\d+)?")


def _num(text: Any) -> float:
    """First number in a Tally value, or 0.0.

    Tally interleaves the unit with the quantity and uses a leading minus for
    outward balances, so ``float()`` on the raw string throws on almost every
    real record.
    """
    if text is None:
        return 0.0
    m = _NUM.search(str(text))
    return float(m.group()) if m else 0.0


def _text(node: Optional[ET.Element], tag: str, default: str = "") -> str:
    if node is None:
        return default
    el = node.find(tag)
    return (el.text or default).strip() if el is not None and el.text else default


class TallyAdapter(_contract.ErpAdapter):
    """Reads a Tally company over the XML gateway and speaks OASIS's dialect."""

    ERP_NAME = "tally"
    #: Claimed, pending live conformance.
    #:
    #: READ_RECEIPTS is NOT declared: receipt dates would come from Receipt Note
    #: / Purchase vouchers per item, which is unverified here, and a falsely
    #: declared receipt date silently disables the dead-stock guard — the RXL
    #: defect that let KES 10.4M through.
    #:
    #: Transfers are NOT declared either. Tally moves stock between godowns with
    #: a Stock Journal voucher, which is a real posting with no optional-voucher
    #: equivalent that still moves stock. Until that is verified, refusing is
    #: safer than a write that silently does nothing or silently does too much.
    CAPABILITIES = frozenset({
        _contract.READ_CATALOGUE, _contract.READ_STOCK, _contract.READ_DEMAND,
        _contract.READ_COST, _contract.READ_SUPPLIERS, _contract.MULTI_SITE,
        _contract.WRITE_PO,
    })

    def __init__(self, host: Optional[str] = None, port: Optional[int] = None,
                 company: Optional[str] = None, store_connector=None):
        self.host = (host or os.getenv("TALLY_HOST", "localhost")).strip()
        self.port = int(port or os.getenv("TALLY_PORT", "9000"))
        self.company = company or os.getenv("TALLY_COMPANY", "")

        if self.host.lower() not in LOOPBACK and \
                os.getenv("TALLY_ALLOW_REMOTE", "").strip() not in ("1", "true", "yes"):
            raise ValueError(
                f"Tally's XML gateway has NO AUTHENTICATION — anyone who can "
                f"reach {self.host}:{self.port} can read the client's books and "
                f"post vouchers into them. Refusing a non-loopback host by "
                f"default. Set TALLY_ALLOW_REMOTE=1 only on a network where "
                f"that is genuinely acceptable, and prefer an SSH tunnel."
            )

        self.store_connector = store_connector
        self.store_engine = getattr(store_connector, "engine", None)
        self._godown_cache: Optional[List[dict]] = None

    @property
    def url(self) -> str:
        return f"http://{self.host}:{self.port}"

    # ── transport ────────────────────────────────────────────────────────
    def _envelope(self, collection: str, fetch: List[str],
                  extra_filter: str = "") -> str:
        """An Export/Collection request for one Tally collection.

        The TDL block defines a collection on the fly rather than relying on a
        report existing in the client's company — a client with customised TDL
        would otherwise return a different shape, or nothing.
        """
        company = (f"<SVCURRENTCOMPANY>{self.company}</SVCURRENTCOMPANY>"
                   if self.company else "")
        fetch_tags = "".join(f"<FETCH>{f}</FETCH>" for f in fetch)
        return f"""<ENVELOPE>
 <HEADER>
  <VERSION>1</VERSION>
  <TALLYREQUEST>Export</TALLYREQUEST>
  <TYPE>Collection</TYPE>
  <ID>OASIS{collection}</ID>
 </HEADER>
 <BODY>
  <DESC>
   <STATICVARIABLES>
    <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
    {company}
   </STATICVARIABLES>
   <TDL><TDLMESSAGE>
    <COLLECTION NAME="OASIS{collection}" ISMODIFY="No">
     <TYPE>{collection}</TYPE>
     {fetch_tags}
     {extra_filter}
    </COLLECTION>
   </TDLMESSAGE></TDL>
  </DESC>
 </BODY>
</ENVELOPE>"""

    def _post(self, xml: str, timeout: int = 120) -> ET.Element:
        import requests
        r = requests.post(self.url, data=xml.encode("utf-8"),
                          headers={"Content-Type": "text/xml;charset=utf-8"},
                          timeout=timeout)
        r.raise_for_status()
        body = r.text
        # Tally answers errors with HTTP 200 and a <LINEERROR> in the body, so
        # the status code alone reports success on a failed request.
        if "<LINEERROR>" in body:
            err = re.search(r"<LINEERROR>(.*?)</LINEERROR>", body, re.S)
            raise RuntimeError(
                f"Tally rejected the request: "
                f"{(err.group(1) if err else body[:200]).strip()}")
        try:
            # Tally emits raw & and stray control chars that break strict XML.
            cleaned = body.replace("&#4;", "").replace("&", "&amp;")
            cleaned = re.sub(r"&amp;(amp|lt|gt|quot|apos|#\d+);", r"&\1;", cleaned)
            return ET.fromstring(cleaned)
        except ET.ParseError as e:
            raise RuntimeError(f"Tally returned unparseable XML: {e}") from e

    def health_check(self) -> Dict[str, Any]:
        try:
            t0 = datetime.now()
            root = self._post(self._envelope("Company", ["NAME"]))
            ms = (datetime.now() - t0).total_seconds() * 1000
            names = [c.get("NAME") for c in root.iter("COMPANY")]
            return {"connected": True, "latency_ms": round(ms, 1),
                    "tables_found": len(names), "companies": names}
        except Exception as e:
            return {"connected": False, "error": str(e)[:200], "tables_found": 0}

    # ── sites: godowns are OASIS organisations ───────────────────────────
    def _godowns(self) -> List[dict]:
        if self._godown_cache is not None:
            return self._godown_cache
        try:
            root = self._post(self._envelope("Godown", ["NAME", "PARENT"]))
            out = [{"name": g.get("NAME") or _text(g, "NAME"),
                    "parent": _text(g, "PARENT")}
                   for g in root.iter("GODOWN")]
        except Exception as e:
            logger.error("godown lookup failed (%s) — site scoping is NOT in "
                         "effect", str(e)[:140])
            out = []
        self._godown_cache = out
        return out

    def fetch_all_organizations(self) -> List[dict]:
        return [{"ORG_CD": g["name"], "ORG_NAME": g["name"], "ACTIVE_FLAG": "Y"}
                for g in self._godowns() if g.get("name")]

    # ── catalogue ────────────────────────────────────────────────────────
    def _stock_items(self) -> List[ET.Element]:
        root = self._post(self._envelope("StockItem", [
            "NAME", "PARENT", "BASEUNITS", "CLOSINGBALANCE", "CLOSINGVALUE",
            "STANDARDCOSTLIST", "STANDARDPRICELIST", "BATCHALLOCATIONS.LIST",
            "OPENINGBALANCE",
        ]))
        return list(root.iter("STOCKITEM"))

    def _stock_for_godown(self, item: ET.Element,
                          godown: Optional[str]) -> Optional[float]:
        """Closing balance for one godown, from the batch allocations.

        Tally reports a stock item's total closing balance at the top level and
        breaks it down per godown underneath. Reading only the top level would
        give every site the whole company's stock and order as though it held it
        — the defect the contract's conformance battery exists to catch.
        """
        if not godown:
            return None
        total = None
        for b in item.iter("BATCHALLOCATIONS.LIST"):
            if _text(b, "GODOWNNAME").strip().lower() == godown.strip().lower():
                total = (total or 0.0) + _num(_text(b, "CLOSINGBALANCE"))
        return total

    def fetch_enriched_products(self, org_cd: Optional[str] = None,
                                sales_days: int = 90) -> List[dict]:
        try:
            items = self._stock_items()
        except Exception as e:
            logger.error("fetch_enriched_products failed: %s", str(e)[:160])
            return []

        demand = self._demand(org_cd, sales_days)
        out: List[dict] = []
        for it in items:
            name = it.get("NAME") or _text(it, "NAME")
            if not name:
                continue
            dept = (_text(it, "PARENT") or "UNCATEGORISED").upper()
            scoped = self._stock_for_godown(it, org_cd)
            qty = scoped if scoped is not None else _num(_text(it, "CLOSINGBALANCE"))
            value = _num(_text(it, "CLOSINGVALUE"))
            # Tally does not carry a unit cost field directly; the weighted
            # average falls out of closing value over closing quantity.
            total_qty = _num(_text(it, "CLOSINGBALANCE"))
            cost = round(value / total_qty, 4) if total_qty else 0.0
            sold = demand.get(name, {"units": 0.0, "revenue": 0.0})
            ads = sold["units"] / max(sales_days, 1)
            out.append({
                "item_code": name,
                "product_name": name,
                "barcode": "",
                "current_stocks": qty,
                "cost_price": cost,
                "selling_price": _num(_text(it, "STANDARDPRICELIST")) or 0.0,
                "department": dept,
                "category": dept,
                "sub_category": dept,
                "uom": _text(it, "BASEUNITS") or "EA",
                "is_fresh": any(k in dept for k in FRESH_KEYS),
                "supplier_cd": "",
                "supplier_name": "Unknown",
                "estimated_delivery_days": 7,
                "supplier_reliability": 0.9,
                "pack_size": 1,
                "blocked_open_for_order": "open",
                "avg_daily_sales": round(ads, 4),
                "estimated_daily_sales": round(ads, 4),
                "units_sold_last_month": round(ads * 30, 2),
                # NOT supplied — see the capability note on the class.
                "days_since_delivery": 0,
                "last_days_since_last_delivery": 0,
            })
        logger.info("Tally: enriched %d stock items (godown=%s)", len(out), org_cd)
        return out

    # ── demand ───────────────────────────────────────────────────────────
    def _demand(self, org_cd: Optional[str] = None,
                days: int = 90) -> Dict[str, Dict[str, float]]:
        """item name -> {units, revenue} from Sales vouchers in the window."""
        since = (datetime.now() - timedelta(days=days)).strftime("%Y%m%d")
        until = datetime.now().strftime("%Y%m%d")
        company = (f"<SVCURRENTCOMPANY>{self.company}</SVCURRENTCOMPANY>"
                   if self.company else "")
        xml = f"""<ENVELOPE>
 <HEADER><VERSION>1</VERSION><TALLYREQUEST>Export</TALLYREQUEST>
  <TYPE>Collection</TYPE><ID>OASISSales</ID></HEADER>
 <BODY><DESC><STATICVARIABLES>
   <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>
   <SVFROMDATE>{since}</SVFROMDATE><SVTODATE>{until}</SVTODATE>
   {company}
  </STATICVARIABLES>
  <TDL><TDLMESSAGE>
   <COLLECTION NAME="OASISSales" ISMODIFY="No">
    <TYPE>Voucher</TYPE>
    <FILTERS>OASISIsSales</FILTERS>
    <FETCH>DATE</FETCH><FETCH>VOUCHERTYPENAME</FETCH>
    <FETCH>ALLINVENTORYENTRIES.LIST</FETCH>
   </COLLECTION>
   <SYSTEM TYPE="Formulae" NAME="OASISIsSales">
    $VOUCHERTYPENAME = "Sales"
   </SYSTEM>
  </TDLMESSAGE></TDL>
 </DESC></BODY></ENVELOPE>"""
        agg: Dict[str, Dict[str, float]] = {}
        try:
            root = self._post(xml)
        except Exception as e:
            logger.warning("sales vouchers unavailable: %s", str(e)[:140])
            return agg
        for v in root.iter("VOUCHER"):
            for entry in v.iter("ALLINVENTORYENTRIES.LIST"):
                name = _text(entry, "STOCKITEMNAME")
                if not name:
                    continue
                if org_cd:
                    godowns = [_text(b, "GODOWNNAME")
                               for b in entry.iter("BATCHALLOCATIONS.LIST")]
                    if godowns and not any(
                            g.strip().lower() == org_cd.strip().lower()
                            for g in godowns):
                        continue
                e = agg.setdefault(name, {"units": 0.0, "revenue": 0.0})
                # sales are outward, so Tally signs the quantity negative
                e["units"] += abs(_num(_text(entry, "ACTUALQTY")))
                e["revenue"] += abs(_num(_text(entry, "AMOUNT")))
        return agg

    def fetch_sales_history(self, org_cd: Optional[str] = None,
                            days: int = 90) -> List[dict]:
        return [{"item_code": k, "units": v["units"], "revenue": v["revenue"]}
                for k, v in self._demand(org_cd, days).items()]

    # ── suppliers ────────────────────────────────────────────────────────
    def _vendors(self) -> List[str]:
        """Ledgers under Sundry Creditors — Tally's vendors."""
        try:
            root = self._post(self._envelope(
                "Ledger", ["NAME", "PARENT"],
                "<FILTER>OASISIsCreditor</FILTER>"))
            return [l.get("NAME") or _text(l, "NAME") for l in root.iter("LEDGER")
                    if "creditor" in _text(l, "PARENT").lower()]
        except Exception as e:
            logger.warning("vendor ledgers unavailable: %s", str(e)[:140])
            return []

    # ── writes: OPTIONAL vouchers only ───────────────────────────────────
    def push_purchase_order(self, org_cd: str,
                            recommendations: List[dict]) -> int:
        """Post one OPTIONAL Purchase Order voucher per supplier.

        ``<ISOPTIONAL>Yes</ISOPTIONAL>`` is Tally's analogue of a draft: the
        voucher is recorded and visible, but does not post to books or affect
        stock until a human marks it regular. OASIS proposes; a person commits.
        Writing a regular voucher would commit a client's order without review.
        """
        from collections import defaultdict
        by_supplier: Dict[str, List[dict]] = defaultdict(list)
        for rec in recommendations:
            if float(rec.get("recommended_quantity") or 0) <= 0:
                continue
            by_supplier[str(rec.get("supplier_name")
                            or rec.get("supplier_cd") or "")].append(rec)
        if not by_supplier:
            return 0

        date = datetime.now().strftime("%Y%m%d")
        company = (f"<SVCURRENTCOMPANY>{self.company}</SVCURRENTCOMPANY>"
                   if self.company else "")
        written = 0
        for supplier, recs in by_supplier.items():
            if not supplier:
                logger.warning("skipping %d line(s) with no supplier — a Tally "
                               "purchase order needs a party ledger", len(recs))
                continue
            entries = ""
            for r in recs:
                qty = float(r.get("recommended_quantity") or 0)
                rate = float(r.get("cost_price") or 0)
                godown = (f"<GODOWNNAME>{org_cd}</GODOWNNAME>" if org_cd else "")
                entries += f"""
      <ALLINVENTORYENTRIES.LIST>
       <STOCKITEMNAME>{r.get('item_code')}</STOCKITEMNAME>
       <ISDEEMEDPOSITIVE>Yes</ISDEEMEDPOSITIVE>
       <ACTUALQTY>{qty}</ACTUALQTY>
       <BILLEDQTY>{qty}</BILLEDQTY>
       <RATE>{rate}</RATE>
       <AMOUNT>-{round(qty * rate, 2)}</AMOUNT>
       <BATCHALLOCATIONS.LIST>
        {godown}
        <BATCHNAME>Primary Batch</BATCHNAME>
        <ACTUALQTY>{qty}</ACTUALQTY>
        <BILLEDQTY>{qty}</BILLEDQTY>
       </BATCHALLOCATIONS.LIST>
      </ALLINVENTORYENTRIES.LIST>"""
            xml = f"""<ENVELOPE>
 <HEADER><VERSION>1</VERSION><TALLYREQUEST>Import</TALLYREQUEST>
  <TYPE>Data</TYPE><ID>Vouchers</ID></HEADER>
 <BODY><DESC><STATICVARIABLES>
   <SVEXPORTFORMAT>$$SysName:XML</SVEXPORTFORMAT>{company}
  </STATICVARIABLES></DESC>
  <DATA><TALLYMESSAGE>
   <VOUCHER VCHTYPE="Purchase Order" ACTION="Create">
    <DATE>{date}</DATE>
    <EFFECTIVEDATE>{date}</EFFECTIVEDATE>
    <VOUCHERTYPENAME>Purchase Order</VOUCHERTYPENAME>
    <PARTYLEDGERNAME>{supplier}</PARTYLEDGERNAME>
    <NARRATION>Proposed by OASIS. Optional voucher — mark regular to commit.</NARRATION>
    <ISOPTIONAL>Yes</ISOPTIONAL>
    <PERSISTEDVIEW>Invoice Voucher View</PERSISTEDVIEW>{entries}
   </VOUCHER>
  </TALLYMESSAGE></DATA>
 </BODY></ENVELOPE>"""
            try:
                root = self._post(xml)
                created = _num(_text(root, ".//CREATED"))
                if created <= 0:
                    logger.error("Tally accepted the request but created no "
                                 "voucher for %s — check the party ledger and "
                                 "stock item names exist exactly", supplier)
                    continue
                written += len(recs)
            except Exception as e:
                logger.error("push_purchase_order failed for %s: %s",
                             supplier, str(e)[:160])
        logger.info("Tally: wrote %d PO lines as OPTIONAL vouchers", written)
        return written

    # ── observability ────────────────────────────────────────────────────
    def diagnose(self, org_cd: Optional[str] = None) -> Dict[str, Any]:
        out: Dict[str, Any] = {"url": self.url, "db": self.company or "(default)",
                               "user": "no auth — Tally gateway is open"}
        h = self.health_check()
        out["connected"] = h.get("connected")
        out["latency_ms"] = h.get("latency_ms")
        if not h.get("connected"):
            out["error"] = h.get("error")
            return out
        out["companies"] = h.get("companies")

        prods = self.fetch_enriched_products(org_cd)
        out["products"] = len(prods)
        out["with_stock"] = sum(1 for p in prods if float(p.get("current_stocks") or 0) > 0)
        out["negative_stock"] = sum(1 for p in prods if float(p.get("current_stocks") or 0) < 0)
        out["with_demand"] = sum(1 for p in prods if float(p.get("avg_daily_sales") or 0) > 0)
        out["with_cost"] = sum(1 for p in prods if float(p.get("cost_price") or 0) > 0)
        out["with_price"] = sum(1 for p in prods if float(p.get("selling_price") or 0) > 0)
        out["with_supplier"] = 0
        out["with_receipt_date"] = 0
        out["departments"] = len({p.get("department") for p in prods})
        out["organisations"] = [o["ORG_CD"] for o in self.fetch_all_organizations()]
        out["open_po_lines"] = None

        warnings = [
            "NO receipt dates: this adapter does not read Receipt Notes, so "
            "days_since_delivery is 0 for every item and the dead-stock guard "
            "cannot fire (fails OPEN).",
            "NO supplier per item: Tally links a party to the VOUCHER, not to "
            "the stock item, so supplier-level ordering rules are inert until "
            "purchase history is mined for it.",
        ]
        if out["products"] and not out["with_demand"]:
            warnings.append("NO DEMAND: every ADS is zero — check that Sales "
                            "vouchers exist in the window and the voucher type "
                            "is named exactly 'Sales'.")
        if out["negative_stock"]:
            warnings.append(f"{out['negative_stock']} items have NEGATIVE stock "
                            "— order quantities will be inflated.")
        if not out["organisations"]:
            warnings.append("NO godowns: every read is company-wide, so a "
                            "multi-store client would order as though each "
                            "shop held the whole group's stock.")
        out["warnings"] = warnings
        return out


_contract.register("tally", TallyAdapter)
