"""
Admin API — provisioning and license issuing.

Every route requires the admin key (X-Hub-Admin-Key). This is the operator
surface: register tenants/stores, mint store ingest tokens, create suppliers,
declare what each supplier owns, record store consent, and issue licenses.
"""

import json
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_session
from ..security import (
    require_admin, hash_password, new_ingest_token, hash_ingest_token,
)
from ..licensing import issue_license, revoke_tenant, LicensingError
from ..models import (
    HubTenant, HubStore, HubSupplier, HubSupplierBrand, HubStoreConsent,
    HubIngestToken, HubInsightExposure, HubSupplierOffer, INSIGHT_KINDS,
)
from .. import visibility
from ..schemas import (
    TenantIn, StoreIn, StoreOut, IngestTokenOut, SupplierIn, OwnershipRuleIn,
    ConsentIn, LicenseIssueIn, LicenseOut, InsightExposureIn,
    AdminOfferOut, OfferRespondIn,
)

router = APIRouter(prefix="/admin", tags=["admin"],
                   dependencies=[Depends(require_admin)])

_VALID_MATCH_TYPES = {"supplier_cd", "brand", "department", "sku"}
_VALID_CONSENT = {"granted", "revoked", "pending"}


def _get_supplier(db: Session, supplier_code: str) -> HubSupplier:
    s = db.query(HubSupplier).filter(HubSupplier.supplier_code == supplier_code).first()
    if not s:
        raise HTTPException(404, f"unknown supplier_code '{supplier_code}'")
    return s


# ── provisioning ─────────────────────────────────────────────────────────
@router.post("/tenants")
def create_tenant(body: TenantIn, db: Session = Depends(get_session)):
    if db.query(HubTenant).filter(HubTenant.tenant_id == body.tenant_id).first():
        raise HTTPException(409, f"tenant '{body.tenant_id}' already exists")
    t = HubTenant(tenant_id=body.tenant_id, name=body.name,
                  country=body.country, contact_email=body.contact_email)
    db.add(t)
    db.flush()
    return {"id": t.id, "tenant_id": t.tenant_id}


@router.post("/stores", response_model=StoreOut)
def create_store(body: StoreIn, db: Session = Depends(get_session)):
    tenant = db.query(HubTenant).filter(HubTenant.tenant_id == body.tenant_id).first()
    if not tenant:
        raise HTTPException(404, f"unknown tenant '{body.tenant_id}'")
    existing = (db.query(HubStore)
                  .filter(HubStore.tenant_pk == tenant.id,
                          HubStore.store_code == body.store_code)
                  .first())
    if existing:
        raise HTTPException(409, f"store '{body.store_code}' already exists for tenant")
    s = HubStore(tenant_pk=tenant.id, store_code=body.store_code,
                 store_name=body.store_name, city=body.city, region=body.region)
    db.add(s)
    db.flush()
    return StoreOut(id=s.id, store_code=s.store_code,
                    store_name=s.store_name, city=s.city)


@router.post("/stores/{store_id}/ingest-token", response_model=IngestTokenOut)
def mint_ingest_token(store_id: str, label: str = "default",
                      db: Session = Depends(get_session)):
    store = db.query(HubStore).filter(HubStore.id == store_id).first()
    if not store:
        raise HTTPException(404, f"unknown store id '{store_id}'")
    raw = new_ingest_token()
    tok = HubIngestToken(store_id=store_id, token_hash=hash_ingest_token(raw),
                         label=label)
    db.add(tok)
    db.flush()
    return IngestTokenOut(store_id=store_id, token=raw, label=label)


@router.post("/suppliers")
def create_supplier(body: SupplierIn, db: Session = Depends(get_session)):
    if db.query(HubSupplier).filter(HubSupplier.supplier_code == body.supplier_code).first():
        raise HTTPException(409, f"supplier '{body.supplier_code}' already exists")
    s = HubSupplier(
        supplier_code=body.supplier_code, name=body.name,
        contact_email=body.contact_email,
        password_hash=hash_password(body.password) if body.password else None,
    )
    db.add(s)
    db.flush()
    return {"id": s.id, "supplier_code": s.supplier_code}


@router.post("/suppliers/ownership")
def add_ownership_rule(body: OwnershipRuleIn, db: Session = Depends(get_session)):
    if body.match_type not in _VALID_MATCH_TYPES:
        raise HTTPException(422, f"match_type must be one of {sorted(_VALID_MATCH_TYPES)}")
    supplier = _get_supplier(db, body.supplier_code)
    existing = (db.query(HubSupplierBrand)
                  .filter(HubSupplierBrand.supplier_id == supplier.id,
                          HubSupplierBrand.match_type == body.match_type,
                          HubSupplierBrand.match_value == body.match_value)
                  .first())
    if existing:
        return {"id": existing.id, "status": "exists"}
    rule = HubSupplierBrand(supplier_id=supplier.id, match_type=body.match_type,
                            match_value=body.match_value)
    db.add(rule)
    db.flush()
    return {"id": rule.id, "status": "created"}


@router.post("/consent")
def set_consent(body: ConsentIn, db: Session = Depends(get_session)):
    if body.status not in _VALID_CONSENT:
        raise HTTPException(422, f"status must be one of {sorted(_VALID_CONSENT)}")
    store = db.query(HubStore).filter(HubStore.id == body.store_id).first()
    if not store:
        raise HTTPException(404, f"unknown store id '{body.store_id}'")
    supplier = _get_supplier(db, body.supplier_code)
    row = (db.query(HubStoreConsent)
             .filter(HubStoreConsent.store_id == body.store_id,
                     HubStoreConsent.supplier_id == supplier.id)
             .first())
    if not row:
        row = HubStoreConsent(store_id=body.store_id, supplier_id=supplier.id)
        db.add(row)
    row.status = body.status
    row.reveal_identity = body.reveal_identity
    if body.status == "granted" and row.granted_at is None:
        row.granted_at = datetime.utcnow()
    db.flush()
    return {"id": row.id, "status": row.status,
            "reveal_identity": row.reveal_identity}


@router.post("/insight-exposure")
def set_insight_exposure(body: InsightExposureIn, db: Session = Depends(get_session)):
    """Flip an insight kind on/off for one supplier at one store — the Flex.

    Default-deny: insights stay hidden until a row here says visible=True, so a
    store can compute and stage intelligence privately, then reveal exactly the
    metric it wants, exactly when it wants (e.g. before a negotiation).
    """
    if body.kind not in INSIGHT_KINDS:
        raise HTTPException(422, f"kind must be one of {list(INSIGHT_KINDS)}")
    store = db.query(HubStore).filter(HubStore.id == body.store_id).first()
    if not store:
        raise HTTPException(404, f"unknown store id '{body.store_id}'")
    supplier = _get_supplier(db, body.supplier_code)

    row = (db.query(HubInsightExposure)
             .filter(HubInsightExposure.store_id == body.store_id,
                     HubInsightExposure.supplier_id == supplier.id,
                     HubInsightExposure.kind == body.kind)
             .first())
    if not row:
        row = HubInsightExposure(store_id=body.store_id, supplier_id=supplier.id,
                                 kind=body.kind)
        db.add(row)
    row.visible = bool(body.visible)
    db.flush()
    return {"store_id": body.store_id, "supplier_code": body.supplier_code,
            "kind": body.kind, "visible": row.visible}


@router.get("/offers", response_model=List[AdminOfferOut])
def list_offers(db: Session = Depends(get_session),
                store_id: Optional[str] = None,
                status: Optional[str] = None):
    """Offers suppliers have proposed — the retailer's review queue."""
    q = (db.query(HubSupplierOffer, HubStore, HubSupplier)
           .join(HubStore, HubStore.id == HubSupplierOffer.store_id)
           .join(HubSupplier, HubSupplier.id == HubSupplierOffer.supplier_id))
    if store_id:
        q = q.filter(HubSupplierOffer.store_id == store_id)
    if status:
        q = q.filter(HubSupplierOffer.status == status)
    out = []
    for offer, store, supplier in q.order_by(HubSupplierOffer.created_at.desc()).all():
        # the retailer always sees their own store named + who is proposing
        row = visibility._offer_row(offer, store, reveal=True)
        row.update({"supplier_code": supplier.supplier_code,
                    "supplier_name": supplier.name})
        out.append(AdminOfferOut(**row))
    return out


@router.post("/offers/{offer_id}/respond", response_model=AdminOfferOut)
def respond_to_offer(offer_id: str, body: OfferRespondIn,
                     db: Session = Depends(get_session)):
    """Accept or decline a supplier's offer."""
    if body.status not in ("accepted", "declined"):
        raise HTTPException(422, "status must be 'accepted' or 'declined'")
    offer = (db.query(HubSupplierOffer)
               .filter(HubSupplierOffer.id == offer_id).first())
    if not offer:
        raise HTTPException(404, f"unknown offer '{offer_id}'")
    if offer.status != "pending":
        raise HTTPException(409, f"offer already {offer.status}")
    offer.status = body.status
    offer.retailer_note = body.retailer_note
    offer.responded_at = datetime.utcnow()
    db.flush()
    store = db.query(HubStore).filter(HubStore.id == offer.store_id).first()
    supplier = db.query(HubSupplier).filter(
        HubSupplier.id == offer.supplier_id).first()
    row = visibility._offer_row(offer, store, reveal=True)
    row.update({"supplier_code": supplier.supplier_code,
                "supplier_name": supplier.name})
    return AdminOfferOut(**row)


# ── licensing ────────────────────────────────────────────────────────────
@router.post("/licenses", response_model=LicenseOut)
def issue(body: LicenseIssueIn, db: Session = Depends(get_session)):
    try:
        rec = issue_license(db, body.tenant_id, body.expiry_date,
                            modules=body.modules, bundle=body.bundle)
    except LicensingError as e:
        raise HTTPException(422, str(e))
    return LicenseOut(
        license_id=rec.id, tenant_id=rec.tenant_id,
        modules=rec.modules.split(","), expiry_date=rec.expiry_date,
        key=json.loads(rec.key_json),
    )


@router.post("/licenses/revoke")
def revoke(tenant_id: str, db: Session = Depends(get_session)):
    n = revoke_tenant(db, tenant_id)
    return {"tenant_id": tenant_id, "revoked": n}
