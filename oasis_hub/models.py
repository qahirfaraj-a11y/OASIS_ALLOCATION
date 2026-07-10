"""
OASIS Cloud Hub — ORM schema.

Design centres on ONE privacy contract: a supplier sees movement of only their
own products, and only in stores that explicitly opted in.

  hub_tenant ──< hub_store ──< hub_stock_movement
                     │
                     └──< hub_store_consent >── hub_supplier ──< hub_supplier_brand
                                                     │
  hub_license (per tenant)              hub_ingest_token (per store)

Ownership (hub_supplier_brand) answers "which movements are THIS supplier's?".
Consent (hub_store_consent) answers "which stores let this supplier see them?".
The intersection — enforced in oasis_hub.visibility — is all a supplier can see.
"""

import uuid
from datetime import datetime

from sqlalchemy import (
    Column, Text, Float, Boolean, DateTime, ForeignKey,
    UniqueConstraint, Index, MetaData,
)
from sqlalchemy.orm import declarative_base, relationship

metadata = MetaData()
Base = declarative_base(metadata=metadata)


def _uuid() -> str:
    return uuid.uuid4().hex


class HubTenant(Base):
    """A retailer organisation registered with the hub (matches a license tenant_id)."""
    __tablename__ = "hub_tenant"
    id = Column(Text, primary_key=True, default=_uuid)
    tenant_id = Column(Text, nullable=False, unique=True)   # == license tenant_id
    name = Column(Text, nullable=False)
    country = Column(Text, default="KE")
    contact_email = Column(Text)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    stores = relationship("HubStore", back_populates="tenant",
                          cascade="all, delete-orphan")


class HubStore(Base):
    """A physical store belonging to a tenant. Consent + movement are per-store."""
    __tablename__ = "hub_store"
    __table_args__ = (
        UniqueConstraint("tenant_pk", "store_code", name="uq_store_code_per_tenant"),
    )
    id = Column(Text, primary_key=True, default=_uuid)
    tenant_pk = Column(Text, ForeignKey("hub_tenant.id"), nullable=False)
    store_code = Column(Text, nullable=False)
    store_name = Column(Text, nullable=False)
    city = Column(Text)
    region = Column(Text)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    tenant = relationship("HubTenant", back_populates="stores")


class HubLicense(Base):
    """An issued license record. The signed key_json is what a client installs.

    The hub keeps the ledger; the signing salt never leaves the hub environment.
    """
    __tablename__ = "hub_license"
    id = Column(Text, primary_key=True, default=_uuid)
    tenant_id = Column(Text, nullable=False, index=True)
    modules = Column(Text, nullable=False)              # comma-separated SKUs
    expiry_date = Column(Text, nullable=False)          # YYYY-MM-DD
    key_json = Column(Text, nullable=False)             # the signed key file body
    issued_at = Column(DateTime, default=datetime.utcnow)
    issued_by = Column(Text)                            # admin identity
    revoked = Column(Boolean, default=False)
    revoked_at = Column(DateTime)


class HubSupplier(Base):
    """A supplier account for the Retail Central Intelligence portal."""
    __tablename__ = "hub_supplier"
    id = Column(Text, primary_key=True, default=_uuid)
    supplier_code = Column(Text, nullable=False, unique=True)
    name = Column(Text, nullable=False)
    contact_email = Column(Text)
    password_hash = Column(Text)          # bcrypt; null until first set
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    brands = relationship("HubSupplierBrand", back_populates="supplier",
                          cascade="all, delete-orphan")


class HubSupplierBrand(Base):
    """An OWNERSHIP rule: which catalog items belong to a supplier.

    match_type ∈ {supplier_cd, brand, department, sku}. A movement is "owned" by
    the supplier if it matches ANY of the supplier's rules. This is the sole
    definition of what a supplier is entitled to see (before consent is applied).
    """
    __tablename__ = "hub_supplier_brand"
    __table_args__ = (
        UniqueConstraint("supplier_id", "match_type", "match_value",
                         name="uq_supplier_ownership_rule"),
        Index("ix_ownership_lookup", "match_type", "match_value"),
    )
    id = Column(Text, primary_key=True, default=_uuid)
    supplier_id = Column(Text, ForeignKey("hub_supplier.id"), nullable=False)
    match_type = Column(Text, nullable=False)
    match_value = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    supplier = relationship("HubSupplier", back_populates="brands")


class HubStoreConsent(Base):
    """A store's opt-in decision for a specific supplier.

    Without a row here in status 'granted', the supplier sees NOTHING from the
    store — this is the default-deny gate. reveal_identity controls whether the
    store's name/city is shown or masked to an opaque handle.
    """
    __tablename__ = "hub_store_consent"
    __table_args__ = (
        UniqueConstraint("store_id", "supplier_id", name="uq_store_supplier_consent"),
    )
    id = Column(Text, primary_key=True, default=_uuid)
    store_id = Column(Text, ForeignKey("hub_store.id"), nullable=False)
    supplier_id = Column(Text, ForeignKey("hub_supplier.id"), nullable=False)
    status = Column(Text, nullable=False, default="pending")   # pending|granted|revoked
    reveal_identity = Column(Boolean, default=False)
    granted_at = Column(DateTime)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class HubStockMovement(Base):
    """Opt-in stock-movement telemetry pushed by a store.

    Ingestion stores rows verbatim; the read layer (visibility.py) filters them
    by ownership + consent. movement_type ∈ {sale, receipt, stock_on_hand,
    adjustment}. Money is optional (a store may share units but not prices).
    """
    __tablename__ = "hub_stock_movement"
    __table_args__ = (
        Index("ix_movement_store_time", "store_id", "occurred_at"),
        Index("ix_movement_owner", "supplier_cd", "brand", "department"),
        UniqueConstraint("store_id", "source_ref", name="uq_movement_source_ref"),
    )
    id = Column(Text, primary_key=True, default=_uuid)
    store_id = Column(Text, ForeignKey("hub_store.id"), nullable=False)
    tenant_id = Column(Text, nullable=False, index=True)
    sku_code = Column(Text, nullable=False)
    sku_name = Column(Text)
    supplier_cd = Column(Text)          # ownership match keys (mirrors ITEM_MST)
    brand = Column(Text)
    department = Column(Text)
    movement_type = Column(Text, nullable=False)
    qty = Column(Float, nullable=False, default=0.0)
    unit_price = Column(Float)          # nullable — store may withhold pricing
    on_hand = Column(Float)             # snapshot qty for stock_on_hand rows
    occurred_at = Column(DateTime, nullable=False)
    ingested_at = Column(DateTime, default=datetime.utcnow)
    source_ref = Column(Text)           # store-side idempotency key (dedup)


class HubIngestToken(Base):
    """A per-store push credential. Only the bcrypt hash is stored."""
    __tablename__ = "hub_ingest_token"
    id = Column(Text, primary_key=True, default=_uuid)
    store_id = Column(Text, ForeignKey("hub_store.id"), nullable=False, index=True)
    token_hash = Column(Text, nullable=False)
    label = Column(Text)
    active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    last_used_at = Column(DateTime)
