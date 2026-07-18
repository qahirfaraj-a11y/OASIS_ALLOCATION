"""
Shape on-prem OASIS intelligence into supplier-safe insight cards.

This is the on-premise half of the Insight Push rail (SUPPLIER_PORTAL_PLAN.md
§4). The store's engine already computes the methodology-grade numbers
(mande_triage → SEI/trapped capital, supplier_scorecard → reliability class,
basket_affinity → halo Confidence/Lift). This module converts those results into
the small, DERIVED cards the hub accepts.

**The privacy rule this module enforces:** a supplier is shown only what is
theirs and only what they need to act. Goods-received (GRN) lines, unit cost
prices, credit terms, margins, other suppliers' figures and store-wide totals
are NEVER placed in a card — they stay on-premise. Every builder below returns
an explicit, hand-picked field set (allow-list, not "everything minus a few"),
so a new field upstream cannot silently start leaking.

Pure functions — no network, no DB. The caller pushes the returned dicts to
``POST /ingest/insights`` with the store's ingest token.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

# Kinds the hub accepts (mirrors oasis_hub.models.INSIGHT_KINDS).
KIND_VELOCITY = "velocity"
KIND_RELIABILITY = "reliability"
KIND_HALO = "halo"
KIND_REORDER = "reorder"
KIND_SEI = "sei"
KIND_QUALITY = "quality"
KIND_CANNIBALIZATION = "cannibalization"

#: Never allow these near a card, whatever an upstream dict happens to contain.
_FORBIDDEN_HINTS = (
    "grn", "goods_received", "receipt_line", "cost", "cogs", "buy_price",
    "margin", "credit", "terms", "invoice", "payable", "dio", "float",
)


def _assert_supplier_safe(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Guard: refuse to emit a card carrying store-private commercial data.

    Defence in depth — the builders already allow-list their fields; this stops
    a hand-assembled or future card from smuggling GRN/cost/credit values.
    """
    for key in payload:
        low = key.lower()
        for bad in _FORBIDDEN_HINTS:
            if bad in low:
                raise ValueError(
                    f"insight payload field '{key}' looks like store-private "
                    f"commercial data ({bad}) — suppliers receive derived "
                    f"metrics only, never GRN/cost/credit detail")
    return payload


def _card(supplier_code: str, kind: str, payload: Dict[str, Any],
          source_ref: Optional[str] = None, **extra) -> Dict[str, Any]:
    card = {"supplier_code": supplier_code, "kind": kind,
            "payload": _assert_supplier_safe(payload)}
    if source_ref:
        card["source_ref"] = source_ref
    card.update({k: v for k, v in extra.items() if v is not None})
    return card


# ── builders ─────────────────────────────────────────────────────────────
def reliability_card(supplier_code: str, row: Dict[str, Any],
                     source_ref: Optional[str] = None) -> Dict[str, Any]:
    """supplier_scorecard row → the supplier's OWN reliability standing.

    Showing a supplier their own class is the fair, actionable half of the
    methodology: they can fix lead-time drift before it costs them the listing.
    Spend/COGS figures in the row are deliberately dropped.
    """
    payload = {
        "reliability_class": row.get("classification") or row.get("class"),
        "lead_time_days": row.get("avg_lead_time") or row.get("lead_time_days"),
        "lead_time_variance_days": row.get("lead_time_variance"),
        "on_time_rate": row.get("on_time_rate"),
        "orders_observed": row.get("orders") or row.get("order_count"),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    return _card(supplier_code, KIND_RELIABILITY, payload, source_ref)


def velocity_card(supplier_code: str, sku_rows: List[Dict[str, Any]],
                  source_ref: Optional[str] = None, **extra) -> Dict[str, Any]:
    """Per-SKU sell-through for the supplier's OWN products."""
    items = [{
        "sku_code": r.get("sku_code"),
        "units": r.get("units"),
        "ads": r.get("ads"),
        "trend_pct": r.get("trend_pct"),
    } for r in sku_rows]
    return _card(supplier_code, KIND_VELOCITY, {"items": items}, source_ref, **extra)


def halo_card(supplier_code: str, pairs: List[Dict[str, Any]],
              source_ref: Optional[str] = None) -> Dict[str, Any]:
    """basket_affinity output → 'your anchor pulls these attachments'.

    Only pairs anchored on the supplier's own SKUs should be passed in; the
    attachment side carries no pricing or margin, just the relationship.
    """
    items = [{
        "anchor_sku": p.get("anchor_sku") or p.get("anchor"),
        "attachment_sku": p.get("attachment_sku") or p.get("attachment"),
        "confidence": p.get("confidence"),
        "lift": p.get("lift"),
    } for p in pairs]
    return _card(supplier_code, KIND_HALO, {"pairs": items}, source_ref)


def reorder_card(supplier_code: str, lines: List[Dict[str, Any]],
                 source_ref: Optional[str] = None) -> Dict[str, Any]:
    """Forward-looking 'ship X units by date' — units and dates only."""
    items = [{
        "sku_code": ln.get("sku_code"),
        "suggested_units": ln.get("suggested_units") or ln.get("qty"),
        "needed_by": ln.get("needed_by"),
        "days_of_cover": ln.get("days_of_cover"),
    } for ln in lines]
    return _card(supplier_code, KIND_REORDER, {"lines": items}, source_ref)


def sei_card(supplier_code: str, mande: Dict[str, Any],
             source_ref: Optional[str] = None) -> Dict[str, Any]:
    """mande_triage output → the retailer-gated efficiency standing.

    Retailer-gated by default (hub exposure is default-deny), because this is
    the store's negotiation position. Even so we emit only the supplier's own
    aggregate scores — no per-line GRN or cost detail, and nothing about any
    other supplier.
    """
    payload = {
        "sei": mande.get("sei"),
        "revenue_per_sku": mande.get("revenue_per_sku"),
        "sku_count": mande.get("sku_count"),
        "classification": mande.get("classification") or mande.get("kill_box"),
        "substitution_exposure": mande.get("substitution_exposure"),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    return _card(supplier_code, KIND_SEI, payload, source_ref)
