"""
The privacy backbone of the Retail Central Intelligence portal.

A supplier may see a stock movement **iff** BOTH hold:
  1. OWNERSHIP  — the movement matches one of the supplier's ownership rules
                  (hub_supplier_brand: by supplier_cd / brand / department / sku).
  2. CONSENT    — the movement's store granted this supplier consent
                  (hub_store_consent.status == 'granted').

When a consenting store has reveal_identity=False, the store's name and city are
masked to a stable opaque handle so the supplier can still track a distinct
outlet over time without learning which physical store it is.

Everything in this module is default-deny: no ownership rules → sees nothing;
no granted consent → sees nothing. All portal reads MUST go through here — never
query hub_stock_movement directly from a request handler.
"""

import hashlib
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional

from sqlalchemy import or_
from sqlalchemy.orm import Session

from .models import (
    HubStockMovement, HubStoreConsent, HubStore, HubSupplierBrand,
)

_MASK_SALT = "oasis_hub_store_masking_v1"

# ownership match_type → the movement column it constrains
_OWNERSHIP_COLUMN = {
    "supplier_cd": HubStockMovement.supplier_cd,
    "brand": HubStockMovement.brand,
    "department": HubStockMovement.department,
    "sku": HubStockMovement.sku_code,
}


@dataclass
class VisibleMovement:
    """A movement as a supplier is permitted to see it (identity already resolved)."""
    movement_id: str
    store_handle: str          # real name, or "Store ####" when masked
    store_masked: bool
    city: Optional[str]
    sku_code: str
    sku_name: Optional[str]
    department: Optional[str]
    brand: Optional[str]
    movement_type: str
    qty: float
    unit_price: Optional[float]
    on_hand: Optional[float]
    occurred_at: datetime


def _mask_handle(store_id: str) -> str:
    """Stable opaque label for an identity-withheld store."""
    digest = hashlib.sha256((_MASK_SALT + store_id).encode()).hexdigest()
    return "Store #" + digest[:8].upper()


def _ownership_filter(rules: List[HubSupplierBrand]):
    """Build an OR-clause matching any of the supplier's ownership rules.

    Returns None when the supplier has no (recognised) rules — the caller must
    treat that as "sees nothing" rather than "matches everything".
    """
    clauses = []
    for r in rules:
        col = _OWNERSHIP_COLUMN.get(r.match_type)
        if col is not None and r.match_value:
            clauses.append(col == r.match_value)
    if not clauses:
        return None
    return or_(*clauses)


def _consent_map(db: Session, supplier_id: str) -> dict:
    """store_id → reveal_identity for every store that granted this supplier."""
    rows = (db.query(HubStoreConsent)
              .filter(HubStoreConsent.supplier_id == supplier_id,
                      HubStoreConsent.status == "granted")
              .all())
    return {c.store_id: bool(c.reveal_identity) for c in rows}


def visible_movements(
    db: Session,
    supplier_id: str,
    *,
    since: Optional[datetime] = None,
    until: Optional[datetime] = None,
    department: Optional[str] = None,
    limit: int = 500,
) -> List[VisibleMovement]:
    """All movements a supplier is entitled to see, identity resolved per consent.

    Default-deny: returns [] if the supplier has no ownership rules or no granted
    consent. `department` is an optional *narrowing* filter applied on top of the
    ownership+consent gate — it can never widen visibility.
    """
    rules = (db.query(HubSupplierBrand)
               .filter(HubSupplierBrand.supplier_id == supplier_id)
               .all())
    ownership = _ownership_filter(rules)
    if ownership is None:
        return []

    consent = _consent_map(db, supplier_id)
    if not consent:
        return []
    consented_store_ids = list(consent.keys())

    q = (db.query(HubStockMovement, HubStore)
           .join(HubStore, HubStore.id == HubStockMovement.store_id)
           .filter(HubStockMovement.store_id.in_(consented_store_ids))
           .filter(ownership))
    if since is not None:
        q = q.filter(HubStockMovement.occurred_at >= since)
    if until is not None:
        q = q.filter(HubStockMovement.occurred_at <= until)
    if department:
        q = q.filter(HubStockMovement.department == department)
    q = q.order_by(HubStockMovement.occurred_at.desc()).limit(limit)

    out: List[VisibleMovement] = []
    for mv, store in q.all():
        reveal = consent.get(mv.store_id, False)
        out.append(VisibleMovement(
            movement_id=mv.id,
            store_handle=store.store_name if reveal else _mask_handle(mv.store_id),
            store_masked=not reveal,
            city=store.city if reveal else None,
            sku_code=mv.sku_code,
            sku_name=mv.sku_name,
            department=mv.department,
            brand=mv.brand,
            movement_type=mv.movement_type,
            qty=mv.qty,
            unit_price=mv.unit_price,
            on_hand=mv.on_hand,
            occurred_at=mv.occurred_at,
        ))
    return out


def visible_insights(db: Session, supplier_id: str, *,
                     kind: Optional[str] = None, limit: int = 500) -> List[dict]:
    """Derived insight cards a supplier may read — DOUBLE default-deny.

    Two independent gates, both must pass:
      1. CONSENT  — the store granted this supplier (same gate as movements).
      2. EXPOSURE — the store set hub_insight_exposure.visible = True for that
                    (store, supplier, kind). No row → hidden.

    Store identity is masked exactly as it is for movements. The card body is
    whatever the store's engine derived; the hub never holds the GRN/cost/credit
    data behind it, so there is nothing raw here to leak.
    """
    import json as _json

    from .models import HubInsightExposure, HubSupplierInsight

    consent = _consent_map(db, supplier_id)          # granted stores only
    if not consent:
        return []

    exposed = {
        (e.store_id, e.kind)
        for e in db.query(HubInsightExposure)
        .filter(HubInsightExposure.supplier_id == supplier_id,
                HubInsightExposure.visible.is_(True))
        .all()
    }
    if not exposed:
        return []

    q = (db.query(HubSupplierInsight, HubStore)
           .join(HubStore, HubStore.id == HubSupplierInsight.store_id)
           .filter(HubSupplierInsight.supplier_id == supplier_id,
                   HubSupplierInsight.store_id.in_(list(consent.keys()))))
    if kind:
        q = q.filter(HubSupplierInsight.kind == kind)
    q = q.order_by(HubSupplierInsight.ingested_at.desc()).limit(limit)

    out: List[dict] = []
    for card, store in q.all():
        if (card.store_id, card.kind) not in exposed:
            continue                                  # not flipped on → hidden
        reveal = consent.get(card.store_id, False)
        try:
            payload = _json.loads(card.payload_json)
        except ValueError:
            payload = {}
        out.append({
            "insight_id": card.id,
            "kind": card.kind,
            "store_handle": store.store_name if reveal else _mask_handle(card.store_id),
            "store_masked": not reveal,
            "payload": payload,
            "period_start": card.period_start,
            "period_end": card.period_end,
            "computed_at": card.computed_at,
        })
    return out


def resolve_store_handle(db: Session, supplier_id: str,
                         handle: Optional[str]) -> Optional[str]:
    """Map a handle the supplier can see back to a store_id — consenting only.

    Suppliers address stores by the handle they were shown (a real name, or the
    masked "Store #XXXX"), never by internal id. Unknown or non-consenting
    handles resolve to None, so a supplier cannot aim an offer at a store that
    has not shared with them. With exactly one consenting store, the handle may
    be omitted.
    """
    consent = _consent_map(db, supplier_id)
    if not consent:
        return None
    if not handle:
        return list(consent)[0] if len(consent) == 1 else None
    for store_id, reveal in consent.items():
        store = db.query(HubStore).filter(HubStore.id == store_id).first()
        if not store:
            continue
        shown = store.store_name if reveal else _mask_handle(store_id)
        if shown == handle:
            return store_id
    return None


def _offer_row(offer, store, reveal: bool) -> dict:
    import json as _json
    try:
        terms = _json.loads(offer.terms_json)
    except ValueError:
        terms = {}
    return {
        "offer_id": offer.id,
        "store_handle": store.store_name if reveal else _mask_handle(offer.store_id),
        "store_masked": not reveal,
        "offer_type": offer.offer_type,
        "terms": terms,
        "message": offer.message,
        "status": offer.status,
        "retailer_note": offer.retailer_note,
        "created_at": offer.created_at,
        "responded_at": offer.responded_at,
    }


def supplier_offers(db: Session, supplier_id: str, limit: int = 200) -> List[dict]:
    """Offers this supplier has made — their own only, consenting stores only."""
    from .models import HubSupplierOffer

    consent = _consent_map(db, supplier_id)
    if not consent:
        return []
    rows = (db.query(HubSupplierOffer, HubStore)
              .join(HubStore, HubStore.id == HubSupplierOffer.store_id)
              .filter(HubSupplierOffer.supplier_id == supplier_id,
                      HubSupplierOffer.store_id.in_(list(consent)))
              .order_by(HubSupplierOffer.created_at.desc())
              .limit(limit).all())
    return [_offer_row(o, s, consent.get(o.store_id, False)) for o, s in rows]


def supplier_overview(db: Session, supplier_id: str, *,
                      window_days: int = 28, risk_days: float = 7.0) -> dict:
    """Hub-native Overview signals (velocity, days-of-cover, stockout risk).

    Runs the same ownership+consent gate as everything else, then hands the
    permitted movements to the pure ``analytics`` layer. No on-prem data needed.
    """
    from . import analytics
    movements = visible_movements(db, supplier_id, limit=200_000)
    return analytics.compute_overview(movements, window_days=window_days,
                                      risk_days=risk_days)


def supplier_store_summary(db: Session, supplier_id: str) -> List[dict]:
    """Per-store rollup a supplier may see: unit velocity + outlet handle.

    Same ownership+consent gate as visible_movements. Aggregation is done in
    Python (small result sets per supplier) to keep identity-masking in one place.
    """
    movements = visible_movements(db, supplier_id, limit=100_000)
    by_store: dict = {}
    for m in movements:
        agg = by_store.setdefault(m.store_handle, {
            "store_handle": m.store_handle,
            "store_masked": m.store_masked,
            "city": m.city,
            "units_sold": 0.0,
            "skus": set(),
            "last_seen": m.occurred_at,
        })
        if m.movement_type == "sale":
            agg["units_sold"] += m.qty or 0.0
        agg["skus"].add(m.sku_code)
        if m.occurred_at > agg["last_seen"]:
            agg["last_seen"] = m.occurred_at
    result = []
    for agg in by_store.values():
        agg["distinct_skus"] = len(agg.pop("skus"))
        result.append(agg)
    result.sort(key=lambda a: a["units_sold"], reverse=True)
    return result
