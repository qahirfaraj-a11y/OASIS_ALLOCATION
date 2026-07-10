"""
Admin API — provisioning and license issuing.

Every route requires the admin key (X-Hub-Admin-Key). This is the operator
surface: register tenants/stores, mint store ingest tokens, create suppliers,
declare what each supplier owns, record store consent, and issue licenses.
"""

import json
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from ..db import get_session
from ..security import (
    require_admin, hash_password, new_ingest_token, hash_ingest_token,
)
from ..licensing import issue_license, revoke_tenant, LicensingError
from ..models import (
    HubTenant, HubStore, HubSupplier, HubSupplierBrand, HubStoreConsent,
    HubIngestToken,
)
from ..schemas import (
    TenantIn, StoreIn, StoreOut, IngestTokenOut, SupplierIn, OwnershipRuleIn,
    ConsentIn, LicenseIssueIn, LicenseOut,
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
