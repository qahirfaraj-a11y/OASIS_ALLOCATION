"""
Retail Central Intelligence portal API — the supplier-facing read surface.

A supplier logs in with their code + password and receives a signed session
token. Every read goes through ``visibility.visible_movements`` /
``supplier_store_summary``, so the ownership + store-consent gate is enforced in
exactly one place. There is no endpoint here that can bypass it.
"""

import json
import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..db import get_session
from ..security import require_supplier, verify_password
from .. import tokens, visibility
from ..models import HubSupplier, HubStore, HubSupplierOffer, OFFER_TYPES
from ..schemas import (
    SupplierLoginIn, SupplierTokenOut, MovementOut, StoreSummaryOut, InsightOut,
    OfferIn, OfferOut,
)

logger = logging.getLogger("OASIS.Hub.Portal")

router = APIRouter(prefix="/portal", tags=["portal"])

_SESSION_TTL = 8 * 3600


@router.post("/login", response_model=SupplierTokenOut)
def login(body: SupplierLoginIn, db: Session = Depends(get_session)):
    supplier = (db.query(HubSupplier)
                  .filter(HubSupplier.supplier_code == body.supplier_code)
                  .first())
    # constant-ish: verify against whatever hash we have (or None → False)
    ok = bool(supplier) and supplier.active and \
        verify_password(body.password, supplier.password_hash)
    if not ok:
        raise HTTPException(401, "invalid supplier code or password")
    token = tokens.sign(supplier.id, role="supplier", ttl_seconds=_SESSION_TTL,
                        code=supplier.supplier_code)
    return SupplierTokenOut(token=token, supplier_code=supplier.supplier_code,
                            expires_in=_SESSION_TTL)


@router.get("/movements", response_model=List[MovementOut])
def movements(
    identity: dict = Depends(require_supplier),
    db: Session = Depends(get_session),
    since: Optional[datetime] = Query(None),
    until: Optional[datetime] = Query(None),
    department: Optional[str] = Query(None),
    limit: int = Query(500, le=5000),
):
    rows = visibility.visible_movements(
        db, identity["supplier_id"], since=since, until=until,
        department=department, limit=limit,
    )
    return [MovementOut(**vars(m)) for m in rows]


@router.get("/overview")
def overview(
    identity: dict = Depends(require_supplier),
    db: Session = Depends(get_session),
    window_days: int = Query(28, ge=7, le=180),
    risk_days: float = Query(7.0, ge=1, le=60),
):
    """Hub-native velocity / days-of-cover / stockout-risk for this supplier."""
    return visibility.supplier_overview(
        db, identity["supplier_id"], window_days=window_days, risk_days=risk_days)


@router.get("/insights", response_model=List[InsightOut])
def insights(
    identity: dict = Depends(require_supplier),
    db: Session = Depends(get_session),
    kind: Optional[str] = Query(None),
    limit: int = Query(200, le=1000),
):
    """Derived insight cards this supplier is permitted to see.

    Double default-deny: the store must have granted consent AND flipped this
    kind visible. Everything else returns nothing.
    """
    return [InsightOut(**row) for row in
            visibility.visible_insights(db, identity["supplier_id"],
                                        kind=kind, limit=limit)]


@router.post("/offers", response_model=OfferOut)
def create_offer(
    body: OfferIn,
    identity: dict = Depends(require_supplier),
    db: Session = Depends(get_session),
):
    """Propose a commercial offer to a store that shares with this supplier.

    The supplier addresses the store by the handle they were shown, so they can
    never aim an offer at a store that has not consented.
    """
    if body.offer_type not in OFFER_TYPES:
        raise HTTPException(422, f"offer_type must be one of {list(OFFER_TYPES)}")
    supplier_id = identity["supplier_id"]
    store_id = visibility.resolve_store_handle(db, supplier_id, body.store_handle)
    if not store_id:
        raise HTTPException(404, "unknown store — specify a store_handle you "
                                 "have been shown")
    offer = HubSupplierOffer(
        store_id=store_id, supplier_id=supplier_id, offer_type=body.offer_type,
        terms_json=json.dumps(body.terms), message=body.message, status="pending",
    )
    db.add(offer)
    db.flush()
    store = db.query(HubStore).filter(HubStore.id == store_id).first()
    reveal = visibility._consent_map(db, supplier_id).get(store_id, False)
    return OfferOut(**visibility._offer_row(offer, store, reveal))


@router.get("/offers", response_model=List[OfferOut])
def list_offers(identity: dict = Depends(require_supplier),
                db: Session = Depends(get_session)):
    """This supplier's own offers and where they stand."""
    return [OfferOut(**row)
            for row in visibility.supplier_offers(db, identity["supplier_id"])]


@router.get("/stores", response_model=List[StoreSummaryOut])
def stores(
    identity: dict = Depends(require_supplier),
    db: Session = Depends(get_session),
):
    return [StoreSummaryOut(**row)
            for row in visibility.supplier_store_summary(db, identity["supplier_id"])]


@router.get("/me")
def me(identity: dict = Depends(require_supplier),
       db: Session = Depends(get_session)):
    supplier = db.query(HubSupplier).filter(
        HubSupplier.id == identity["supplier_id"]).first()
    if not supplier:
        raise HTTPException(404, "supplier not found")
    return {
        "supplier_code": supplier.supplier_code,
        "name": supplier.name,
        "ownership_rules": [
            {"match_type": r.match_type, "match_value": r.match_value}
            for r in supplier.brands
        ],
    }
