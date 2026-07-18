"""
Ingestion API — stores push opt-in stock-movement telemetry.

Authenticated by a per-store ingest token (Authorization: Bearer). A store only
ever writes to its OWN store_id — the token resolves the store server-side, so a
compromised token cannot impersonate another outlet. Idempotent on
(store_id, source_ref): re-sending the same batch is safe.
"""

import json
import logging
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from ..db import get_session
from ..security import require_ingest
from ..models import (
    HubStore, HubStockMovement, HubSupplier, HubSupplierInsight, INSIGHT_KINDS,
)
from ..schemas import IngestBatchIn, IngestResult, InsightBatchIn

logger = logging.getLogger("OASIS.Hub.Ingest")

router = APIRouter(prefix="/ingest", tags=["ingest"])


@router.post("/movements", response_model=IngestResult)
def push_movements(
    batch: IngestBatchIn,
    identity: dict = Depends(require_ingest),
    db: Session = Depends(get_session),
):
    store_id = identity["store_id"]
    store = db.query(HubStore).filter(HubStore.id == store_id).first()
    tenant_pk_to_tenant_id = store.tenant.tenant_id if store and store.tenant else None

    # existing source_refs for this store → skip duplicates cheaply
    seen = {
        r[0] for r in db.query(HubStockMovement.source_ref)
        .filter(HubStockMovement.store_id == store_id,
                HubStockMovement.source_ref.isnot(None)).all()
    }
    accepted = duplicates = 0
    batch_refs: set = set()
    for m in batch.movements:
        if m.source_ref and (m.source_ref in seen or m.source_ref in batch_refs):
            duplicates += 1
            continue
        db.add(HubStockMovement(
            store_id=store_id,
            tenant_id=tenant_pk_to_tenant_id or "unknown",
            sku_code=m.sku_code, sku_name=m.sku_name,
            supplier_cd=m.supplier_cd, brand=m.brand, department=m.department,
            movement_type=m.movement_type, qty=m.qty, unit_price=m.unit_price,
            on_hand=m.on_hand, occurred_at=m.occurred_at,
            ingested_at=datetime.utcnow(), source_ref=m.source_ref,
        ))
        if m.source_ref:
            batch_refs.add(m.source_ref)
        accepted += 1

    try:
        db.flush()
    except IntegrityError:
        # racing writer inserted a matching source_ref between our read and flush
        db.rollback()
        logger.warning("Ingest race for store %s — retrying dedup", store_id)
        raise
    return IngestResult(accepted=accepted, duplicates=duplicates, store_id=store_id)


@router.post("/insights", response_model=IngestResult)
def push_insights(
    batch: InsightBatchIn,
    identity: dict = Depends(require_ingest),
    db: Session = Depends(get_session),
):
    """Push DERIVED supplier-scoped insight cards (P1 Insight Push rail).

    The store's engine computes these locally and sends only supplier-safe
    numbers — the hub never receives the GRN, cost, or credit data behind them.
    Pushing does NOT reveal anything: every card stays hidden until the store
    flips its kind on via /admin/insight-exposure (default-deny).
    Idempotent on (store, supplier, kind, source_ref).
    """
    store_id = identity["store_id"]
    accepted = duplicates = 0
    batch_keys: set = set()

    for card in batch.insights:
        if card.kind not in INSIGHT_KINDS:
            raise HTTPException(422, f"unknown insight kind '{card.kind}' "
                                     f"(expected one of {list(INSIGHT_KINDS)})")
        supplier = (db.query(HubSupplier)
                      .filter(HubSupplier.supplier_code == card.supplier_code)
                      .first())
        if not supplier:
            raise HTTPException(404, f"unknown supplier_code '{card.supplier_code}'")

        key = (supplier.id, card.kind, card.source_ref)
        if card.source_ref:
            if key in batch_keys:
                duplicates += 1
                continue
            exists = (db.query(HubSupplierInsight)
                        .filter(HubSupplierInsight.store_id == store_id,
                                HubSupplierInsight.supplier_id == supplier.id,
                                HubSupplierInsight.kind == card.kind,
                                HubSupplierInsight.source_ref == card.source_ref)
                        .first())
            if exists:
                duplicates += 1
                continue
            batch_keys.add(key)

        db.add(HubSupplierInsight(
            store_id=store_id, supplier_id=supplier.id, kind=card.kind,
            payload_json=json.dumps(card.payload),
            period_start=card.period_start, period_end=card.period_end,
            computed_at=card.computed_at or datetime.utcnow(),
            ingested_at=datetime.utcnow(), source_ref=card.source_ref,
        ))
        accepted += 1

    db.flush()
    logger.info("Insights ingested for store %s: %d accepted, %d duplicate",
                store_id, accepted, duplicates)
    return IngestResult(accepted=accepted, duplicates=duplicates, store_id=store_id)
