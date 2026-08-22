"""Derive each supplier's delivery rhythm from Odoo's own goods receipts.

WHY THIS EXISTS
---------------
Every horizon in the transfer engine is derived from LATA's measured supplier
rhythm — the relief window a store must survive, and now sigma, the safety
floor. ``lata_shield`` does not produce that rhythm; it ENRICHES an existing
``supplier_patterns_2025.json`` with a variance multiplier. The only thing that
ever produced the file scanned ``po_*.xlsx`` off disk.

So a customer whose history lives in Odoo had nothing to enrich, and the engine
fell back to a flat 14 days for both the target and the floor. Measured on an
empty data dir: 0 suppliers, median relief None, sigma 14.0 — every constant
the derivation exists to remove, quietly restored. For fresh milk that is
worse than the 7 days a real rhythm gives.

WHAT A RECEIPT ACTUALLY TELLS US
--------------------------------
An incoming picking carries a partner (the supplier), a completion date, and a
warehouse. From a supplier's ordered receipt dates:

    gap        = days between consecutive receipts        -> the cadence
    lead       = date_done - order_date on the linked PO  -> the real lead time
    variance   = how much those gaps move                 -> LATA's multiplier

Lead time is taken from the purchase order where one is linked, because that is
the only place the PROMISE and the ARRIVAL can be compared. Falling back to
``product.supplierinfo.delay`` uses the supplier's own claim, which is exactly
the number LATA exists to distrust — so it is recorded as a fallback and marked.

PER STORE, NOT JUST PER SUPPLIER
--------------------------------
Odoo scopes receipts by warehouse, so the same read yields cadence per (store,
supplier) at no extra cost. A supplier that delivers daily to the flagship and
fortnightly to the forecourt is one supplier with two rhythms, and only the
per-store form can say so. Both files are written; the engine uses the
per-store one where it has enough evidence and falls back to the chain-wide
figure where it does not.

READ-ONLY against Odoo. It writes only into OASIS's own data directory.
"""

from __future__ import annotations

import json
import logging
import os
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

logger = logging.getLogger("OASIS.OdooRhythm")

#: A single gap is an anecdote. The median of two is still one number pretending
#: to be a distribution. Below this the rhythm is recorded but marked LOW, and
#: the engine's own fallbacks are the safer answer.
MIN_RECEIPTS_FOR_RHYTHM = 3
MIN_RECEIPTS_FOR_CONFIDENCE = 6

#: Ceiling on a single gap. A supplier that last delivered a year ago has not
#: got a 365-day cadence; it has a discontinued line or a data gap, and letting
#: that into a median poisons it.
MAX_SENSIBLE_GAP_DAYS = 180

#: How many receipts to read. Named and warned on, like every other capped read
#: in this codebase — a silent truncation here understates cadence for the
#: busiest suppliers, which are exactly the ones the engine leans on.
RECEIPT_HISTORY_LIMIT = 100000

PATTERNS_FILE = "supplier_patterns_2025.json"
GAPS_FILE = "supplier_delivery_gaps.json"
PER_STORE_FILE = "supplier_patterns_by_store.json"


def _as_date(value) -> Optional[datetime]:
    try:
        return datetime.strptime(str(value)[:19], "%Y-%m-%d %H:%M:%S")
    except (ValueError, TypeError):
        return None


def _gaps_from(dates: List[datetime]) -> List[int]:
    """Whole-day gaps between consecutive DISTINCT receipt days.

    Distinct days, because one delivery split across three pickings is one
    delivery — counting it as three arrivals a day apart would report a daily
    cadence for a weekly supplier.
    """
    days = sorted({d.date() for d in dates})
    gaps = []
    for i in range(1, len(days)):
        g = (days[i] - days[i - 1]).days
        if 0 < g <= MAX_SENSIBLE_GAP_DAYS:
            gaps.append(g)
    return gaps


def _summarise(dates: List[datetime], leads: List[float],
               stated_lead: Optional[float]) -> Optional[dict]:
    gaps = _gaps_from(dates)
    if len(gaps) < MIN_RECEIPTS_FOR_RHYTHM - 1:
        return None

    if leads:
        lead = round(statistics.median(leads), 1)
        lead_source = "measured"          # PO placed -> goods arrived
    elif stated_lead:
        lead = float(stated_lead)
        lead_source = "stated"            # the supplier's own claim
    else:
        lead = 0.0
        lead_source = "unknown"

    return {
        "median_gap_days": int(statistics.median(gaps)),
        "avg_gap_days": round(statistics.mean(gaps), 1),
        "average_gap_days": round(statistics.mean(gaps), 1),
        "estimated_delivery_days": lead,
        "lead_time_source": lead_source,
        "total_orders_2025": len(gaps) + 1,
        "receipt_count": len(gaps) + 1,
        "confidence": ("HIGH" if len(gaps) + 1 >= MIN_RECEIPTS_FOR_CONFIDENCE
                       else "LOW"),
        "derived_from": "odoo_goods_receipts",
    }


def derive(adapter=None, days: int = 730, data_dir: str = None,
           write: bool = True, force: bool = False) -> Dict[str, Any]:
    """Read Odoo's receipt history and write OASIS's supplier rhythm files."""
    from .odoo_adapter import OdooAdapter

    a = adapter or OdooAdapter()
    since = None
    if days:
        from datetime import timedelta
        since = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")

    dom = [["picking_type_id.code", "=", "incoming"],
           ["state", "=", "done"],
           ["partner_id", "!=", False]]
    if since:
        dom.append(["date_done", ">=", since])

    picks = a._ex("stock.picking", "search_read", [dom],
                  {"fields": ["partner_id", "date_done", "picking_type_id",
                              "purchase_id", "location_dest_id"],
                   "order": "date_done asc",
                   "limit": RECEIPT_HISTORY_LIMIT}) or []
    if hasattr(a, "_warn_if_truncated"):
        a._warn_if_truncated(
            picks, RECEIPT_HISTORY_LIMIT, "goods receipt history", None,
            "The busiest suppliers lose their oldest receipts, so their "
            "measured cadence is drawn from a shorter window than the rest.")

    logger.info("Read %s incoming pickings with a supplier", f"{len(picks):,}")

    # warehouse of a receipt, via its operation type
    types = {t["id"]: t for t in (a._ex(
        "stock.picking.type", "search_read", [[["code", "=", "incoming"]]],
        {"fields": ["warehouse_id"]}) or [])}
    wh_code = {w["id"]: (w.get("code") or "").strip() or str(w["id"])
               for w in (a._ex("stock.warehouse", "search_read", [[]],
                               {"fields": ["code"]}) or [])}

    # PO order dates, so lead time is MEASURED rather than claimed
    po_ids = [p["purchase_id"][0] for p in picks
              if isinstance(p.get("purchase_id"), (list, tuple)) and p["purchase_id"]]
    po_date: Dict[int, datetime] = {}
    if po_ids:
        for row in (a._ex("purchase.order", "search_read",
                          [[["id", "in", list(set(po_ids))]]],
                          {"fields": ["date_order"], "limit": len(set(po_ids))}) or []):
            d = _as_date(row.get("date_order"))
            if d:
                po_date[row["id"]] = d

    chain_dates: Dict[str, List[datetime]] = defaultdict(list)
    chain_leads: Dict[str, List[float]] = defaultdict(list)
    store_dates: Dict[tuple, List[datetime]] = defaultdict(list)
    store_leads: Dict[tuple, List[float]] = defaultdict(list)

    for p in picks:
        partner = p.get("partner_id")
        name = (partner[1] if isinstance(partner, (list, tuple)) and len(partner) > 1
                else "")
        name = str(name or "").strip()
        when = _as_date(p.get("date_done"))
        if not (name and when):
            continue
        key = name.lower()
        chain_dates[key].append(when)

        pt = p.get("picking_type_id")
        pt_id = pt[0] if isinstance(pt, (list, tuple)) and pt else None
        wh = types.get(pt_id, {}).get("warehouse_id")
        site = wh_code.get(wh[0]) if isinstance(wh, (list, tuple)) and wh else None
        if site:
            store_dates[(site, key)].append(when)

        po = p.get("purchase_id")
        po_id = po[0] if isinstance(po, (list, tuple)) and po else None
        ordered = po_date.get(po_id)
        if ordered:
            lead = (when - ordered).days
            if 0 <= lead <= MAX_SENSIBLE_GAP_DAYS:
                chain_leads[key].append(float(lead))
                if site:
                    store_leads[(site, key)].append(float(lead))

    stated = _stated_lead_times(a)

    patterns: Dict[str, dict] = {}
    raw_gaps: Dict[str, List[int]] = {}
    for key, dates in chain_dates.items():
        rec = _summarise(dates, chain_leads.get(key, []), stated.get(key))
        if rec:
            patterns[key] = rec
            raw_gaps[key] = _gaps_from(dates)

    per_store: Dict[str, Dict[str, dict]] = defaultdict(dict)
    for (site, key), dates in store_dates.items():
        rec = _summarise(dates, store_leads.get((site, key), []), stated.get(key))
        if rec:
            per_store[site][key] = rec

    result = {
        "suppliers": len(patterns),
        "receipts_read": len(picks),
        "stores_with_rhythm": len(per_store),
        "store_supplier_pairs": sum(len(v) for v in per_store.values()),
        "measured_lead_times": sum(1 for k in patterns
                                   if patterns[k]["lead_time_source"] == "measured"),
        "written": [],
    }
    if patterns:
        result["median_gap_across_suppliers"] = statistics.median(
            [p["median_gap_days"] for p in patterns.values()])

    if write and data_dir:
        ok, why = _may_replace(data_dir, patterns, force)
        result["refused"] = None if ok else why
        if not ok:
            logger.warning("REFUSED to write: %s", why)
        else:
            os.makedirs(data_dir, exist_ok=True)
            for fname, payload in ((PATTERNS_FILE, patterns),
                                   (GAPS_FILE, raw_gaps),
                                   (PER_STORE_FILE, dict(per_store))):
                _write_json(os.path.join(data_dir, fname), payload, result)
            logger.info("Wrote %s supplier rhythms and %s store/supplier pairs",
                        len(patterns), result["store_supplier_pairs"])

    result["patterns"] = patterns
    result["per_store"] = dict(per_store)
    return result


def _may_replace(data_dir: str, patterns: dict, force: bool):
    """Never let a thinner derivation quietly replace a richer one.

    This module wrote an EMPTY supplier_patterns_2025.json over a working
    599-supplier file the first time it was run, because the instance it read
    stores receipts as bare stock moves with no supplier on them. Nothing
    errored. The engine would simply have started answering 14 to every
    horizon, on a customer site, with the evidence gone.

    A derivation that finds less than what is already there is not a fact about
    the suppliers; it is a fact about the read. Refuse, say so, and let a human
    pass --force if they genuinely mean to replace it.
    """
    if not patterns:
        return False, ("nothing was derived, and writing that would erase any "
                       "rhythm already present. Receipts must come from "
                       "purchase orders so they carry a supplier.")
    existing = os.path.join(data_dir, PATTERNS_FILE)
    if os.path.exists(existing) and not force:
        try:
            with open(existing, "r", encoding="utf-8") as f:
                had = len(json.load(f) or {})
        except Exception:
            had = 0
        if had > len(patterns):
            return False, (
                f"the existing file holds {had:,} suppliers and this read "
                f"derived only {len(patterns):,}. Refusing to replace richer "
                f"data with thinner. Pass force=True if that is intended.")
    return True, None


def _write_json(path: str, payload, result: dict) -> None:
    """Write atomically, keeping one backup of whatever was there before.

    A half-written patterns file is worse than none: it parses as far as the
    truncation and the engine trusts it.
    """
    if os.path.exists(path):
        backup = path + ".bak"
        try:
            os.replace(path, backup)
            result.setdefault("backed_up", []).append(backup)
        except OSError as e:
            logger.debug("could not back up %s: %s", path, e)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True)
    os.replace(tmp, path)
    result["written"].append(path)

def _stated_lead_times(adapter) -> Dict[str, float]:
    """The supplier's OWN claim, used only where nothing was measured."""
    out: Dict[str, float] = {}
    try:
        rows = adapter._ex("product.supplierinfo", "search_read", [[]],
                           {"fields": ["partner_id", "delay"],
                            "limit": getattr(adapter, "SUPPLIERINFO_READ_LIMIT",
                                             20000)}) or []
    except Exception as e:
        logger.debug("supplierinfo unavailable: %s", e)
        return out
    for r in rows:
        p = r.get("partner_id")
        if isinstance(p, (list, tuple)) and len(p) > 1:
            name = str(p[1]).strip().lower()
            if name and name not in out:
                out[name] = float(r.get("delay") or 0)
    return out


def format_report(r: Dict[str, Any]) -> str:
    """ASCII only — this prints to a customer's Windows console."""
    w = ["",
         "O.A.S.I.S. - supplier rhythm derived from Odoo goods receipts",
         "=" * 66,
         f"  receipts read            {r.get('receipts_read', 0):,}",
         f"  suppliers with a rhythm  {r.get('suppliers', 0):,}",
         f"  of those, lead MEASURED  {r.get('measured_lead_times', 0):,}"
         f"  (the rest use the supplier's stated lead time)",
         f"  stores with a rhythm     {r.get('stores_with_rhythm', 0):,}",
         f"  store/supplier pairs     {r.get('store_supplier_pairs', 0):,}"]
    if "median_gap_across_suppliers" in r:
        w.append(f"  median gap               "
                 f"{r['median_gap_across_suppliers']:.0f} days")
    w.append("-" * 66)
    if not r.get("suppliers"):
        w.append("  NOTHING DERIVED. Receipts must be created from purchase")
        w.append("  orders so they carry a supplier; bare stock moves cannot")
        w.append("  say who delivered. The engine will fall back to a flat")
        w.append("  14-day horizon and say so on every scan.")
    else:
        for p in r.get("written", []):
            w.append(f"  wrote  {p}")
        w.append("  Now run lata_shield to add the variance multiplier:")
        w.append("    python -m oasis.logic.lata_shield --data-dir <data_dir>")
    w.append("")
    return "\n".join(w)
