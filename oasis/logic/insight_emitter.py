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
KIND_BROKEN_HALO = "broken_halo"
KIND_ARCHETYPE = "archetype"
KIND_CAPITAL_EFFICIENCY = "capital_efficiency"
KIND_REORDER = "reorder"
KIND_SEI = "sei"
KIND_NCP = "ncp"
KIND_QUALITY = "quality"
KIND_CANNIBALIZATION = "cannibalization"

#: Never allow these near a card, whatever an upstream dict happens to contain.
_FORBIDDEN_HINTS = (
    "grn", "goods_received", "receipt_line", "cost", "cogs", "buy_price",
    "margin", "credit", "terms", "invoice", "payable", "dio", "float",
)


def _assert_supplier_safe(payload: Dict[str, Any],
                          allow: frozenset = frozenset()) -> Dict[str, Any]:
    """Guard: refuse to emit a card carrying store-private commercial data.

    Defence in depth — the builders already allow-list their fields; this stops
    a hand-assembled or future card from smuggling GRN/cost/credit values.

    ``allow`` is a narrow, per-builder exemption for fields that trip the
    keyword screen but are legitimately the SUPPLIER'S OWN position (e.g. the
    credit terms they themselves set). Every exemption must be justified in the
    calling builder's docstring; the default stays strict.
    """
    for key in payload:
        if key in allow:
            continue
        low = key.lower()
        for bad in _FORBIDDEN_HINTS:
            if bad in low:
                raise ValueError(
                    f"insight payload field '{key}' looks like store-private "
                    f"commercial data ({bad}) — suppliers receive derived "
                    f"metrics only, never GRN/cost/credit detail")
    return payload


def _card(supplier_code: str, kind: str, payload: Dict[str, Any],
          source_ref: Optional[str] = None, allow: frozenset = frozenset(),
          **extra) -> Dict[str, Any]:
    card = {"supplier_code": supplier_code, "kind": kind,
            "payload": _assert_supplier_safe(payload, allow=allow)}
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


def broken_halo_card(supplier_code: str, breaks: List[Dict[str, Any]],
                     source_ref: Optional[str] = None) -> Dict[str, Any]:
    """DHARAM detect_broken_halo() → 'your anchor stopped pulling its attachment'.

    Genuinely actionable for the supplier (their anchor is losing network effect,
    often an availability or placement problem) and carries no commercial data —
    just the pair, the confidence drop, and how long it has been decaying.
    """
    items = [{
        "anchor_sku": b.get("anchor_sku") or b.get("anchor"),
        "attachment_sku": b.get("attachment_sku") or b.get("attachment"),
        "confidence_before": b.get("confidence_before") or b.get("baseline_confidence"),
        "confidence_now": b.get("confidence_now") or b.get("current_confidence"),
        "drop_pct": b.get("drop_pct"),
        "days_broken": b.get("days_broken"),
    } for b in breaks]
    return _card(supplier_code, KIND_BROKEN_HALO, {"breaks": items}, source_ref)


def archetype_card(supplier_code: str, mix: List[Dict[str, Any]],
                   source_ref: Optional[str] = None) -> Dict[str, Any]:
    """Demand-shape archetype mix across the supplier's own SKUs.

    Tells a supplier what KIND of demand their portfolio actually has (steady
    staple vs spiky impulse vs seasonal), which shapes how they should ship —
    without exposing anything about the store's economics.
    """
    items = [{
        "archetype": m.get("archetype") or m.get("name"),
        "sku_count": m.get("sku_count") or m.get("count"),
        "share_pct": m.get("share_pct"),
        "example_sku": m.get("example_sku"),
    } for m in mix]
    return _card(supplier_code, KIND_ARCHETYPE, {"mix": items}, source_ref)


def capital_efficiency_card(supplier_code: str, stats: Dict[str, Any],
                            source_ref: Optional[str] = None) -> Dict[str, Any]:
    """Capital efficiency as a RELATIVE index — deliberately not GMROI itself.

    GMROI is gross-margin-over-inventory-cost: shipping it would hand the
    supplier the retailer's markup, which is exactly the store-private data this
    module exists to keep in. Instead we emit only where the supplier's products
    stand RELATIVE to their category (an index around 1.0 plus a band), which is
    what a supplier can actually act on — argue for more facings, fix a slow
    line — while the absolute margin never leaves the store.

    Callers must pass a pre-computed ratio; ``_assert_supplier_safe`` will reject
    any attempt to smuggle raw margin/cost fields alongside it.
    """
    payload = {
        "efficiency_index": stats.get("efficiency_index"),   # e.g. 1.3 = 1.3× median
        "band": stats.get("band"),                            # e.g. "top quartile"
        "category": stats.get("category"),
        "skus_compared": stats.get("skus_compared"),
        "basis": "relative to category median — absolute margin not shared",
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    return _card(supplier_code, KIND_CAPITAL_EFFICIENCY, payload, source_ref)


def efficiency_band(index: Optional[float]) -> Optional[str]:
    """Plain-English band for an efficiency index (keeps UI and tests aligned)."""
    if index is None:
        return None
    if index >= 1.25:
        return "top quartile"
    if index >= 1.0:
        return "above median"
    if index >= 0.75:
        return "below median"
    return "bottom quartile"


def ncp_card(supplier_code: str, ncp: Dict[str, Any],
             source_ref: Optional[str] = None) -> Dict[str, Any]:
    """Net Capital Position for THIS supplier's own account — the Flex (Ch.6.2).

    NCP = their credit days − how long their goods take to sell (DIO). The
    methodology's whole point is to put this in front of the supplier: "your
    14-day terms against a 45-day DIO means you are financing nothing — fix the
    terms." So it is meant to be shown, but only when the retailer chooses
    (hub exposure is default-deny and this kind is retailer-gated).

    Guard exemption, justified: ``credit_days`` and ``dio_days`` trip the
    keyword screen, but the credit terms are the SUPPLIER'S OWN (they set them)
    and the DIO is for their OWN products — both are their side of the account,
    not the store's book. Nothing about other suppliers, margins, or the store's
    overall liquidity is included.
    """
    payload = {
        "ncp_days": ncp.get("ncp_days"),
        "credit_days": ncp.get("credit_days"),
        "dio_days": ncp.get("dio_days"),
        "position": ncp.get("position"),          # "funding" | "neutral" | "draining"
        "skus_considered": ncp.get("skus_considered"),
    }
    payload = {k: v for k, v in payload.items() if v is not None}
    return _card(supplier_code, KIND_NCP, payload, source_ref,
                 allow=frozenset({"credit_days", "dio_days"}))


def ncp_position(ncp_days: Optional[float]) -> Optional[str]:
    """Plain-English reading of an NCP figure (shared by UI and tests)."""
    if ncp_days is None:
        return None
    if ncp_days > 0:
        return "funding"        # supplier credit outlasts the sell-through
    if ncp_days == 0:
        return "neutral"
    return "draining"           # goods outlive the credit — ties up store capital


def cannibalization_card(supplier_code: str, rows: List[Dict[str, Any]],
                         source_ref: Optional[str] = None) -> Dict[str, Any]:
    """Substitution/redundancy within the supplier's own range (Ch.9).

    Tells a supplier which of THEIR OWN SKUs are eating each other rather than
    adding incremental volume — a line-extension reality check. Carries rates
    and counts only, never the store's economics.
    """
    items = [{
        "sku_code": r.get("sku_code"),
        "cannibalization_rate": r.get("cannibalization_rate") or r.get("cr"),
        "substitutes_sku": r.get("substitutes_sku") or r.get("victim_sku"),
        "incremental_pct": r.get("incremental_pct"),
        "substitution_edges": r.get("substitution_edges"),
    } for r in rows]
    return _card(supplier_code, KIND_CANNIBALIZATION, {"items": items}, source_ref)


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
