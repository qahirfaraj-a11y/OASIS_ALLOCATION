"""
Retail Central Intelligence portal API — the supplier-facing read surface.

A supplier logs in with their code + password and receives a signed session
token. Every read goes through ``visibility.visible_movements`` /
``supplier_store_summary``, so the ownership + store-consent gate is enforced in
exactly one place. There is no endpoint here that can bypass it.
"""

import logging
from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from ..db import get_session
from ..security import require_supplier, verify_password
from .. import tokens, visibility
from ..models import HubSupplier
from ..schemas import (
    SupplierLoginIn, SupplierTokenOut, MovementOut, StoreSummaryOut,
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
