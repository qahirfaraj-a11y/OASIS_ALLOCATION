"""Pydantic request/response models for the hub API."""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field


# ── admin: provisioning ──────────────────────────────────────────────────
class TenantIn(BaseModel):
    tenant_id: str
    name: str
    country: str = "KE"
    contact_email: Optional[str] = None


class StoreIn(BaseModel):
    tenant_id: str
    store_code: str
    store_name: str
    city: Optional[str] = None
    region: Optional[str] = None


class StoreOut(BaseModel):
    id: str
    store_code: str
    store_name: str
    city: Optional[str] = None


class IngestTokenOut(BaseModel):
    store_id: str
    token: str = Field(..., description="Raw token — shown once, store it now.")
    label: Optional[str] = None


class SupplierIn(BaseModel):
    supplier_code: str
    name: str
    contact_email: Optional[str] = None
    password: Optional[str] = Field(None, description="Initial portal password.")


class OwnershipRuleIn(BaseModel):
    supplier_code: str
    match_type: str = Field(..., description="supplier_cd | brand | department | sku")
    match_value: str


class ConsentIn(BaseModel):
    store_id: str
    supplier_code: str
    status: str = Field("granted", description="granted | revoked | pending")
    reveal_identity: bool = False


# ── admin: licensing ─────────────────────────────────────────────────────
class LicenseIssueIn(BaseModel):
    tenant_id: str
    expiry_date: str = Field(..., description="YYYY-MM-DD")
    modules: Optional[List[str]] = None
    bundle: Optional[str] = Field(None, description="starter | pro | enterprise")


class LicenseOut(BaseModel):
    license_id: str
    tenant_id: str
    modules: List[str]
    expiry_date: str
    key: dict = Field(..., description="Signed key body — write to oasis_license.key")


# ── ingest ───────────────────────────────────────────────────────────────
class MovementIn(BaseModel):
    sku_code: str
    movement_type: str = Field(..., description="sale | receipt | stock_on_hand | adjustment")
    qty: float = 0.0
    occurred_at: datetime
    sku_name: Optional[str] = None
    supplier_cd: Optional[str] = None
    brand: Optional[str] = None
    department: Optional[str] = None
    unit_price: Optional[float] = None
    on_hand: Optional[float] = None
    source_ref: Optional[str] = Field(None, description="Store-side idempotency key.")


class IngestBatchIn(BaseModel):
    movements: List[MovementIn]


class IngestResult(BaseModel):
    accepted: int
    duplicates: int
    store_id: str


# ── insights (P1: the Insight Push rail) ─────────────────────────────────
class InsightIn(BaseModel):
    supplier_code: str
    kind: str = Field(..., description="velocity|reliability|halo|reorder|sei|"
                                       "quality|cannibalization")
    payload: dict = Field(..., description="DERIVED, supplier-safe numbers only "
                                           "— never GRN lines, cost, or credit terms.")
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    computed_at: Optional[datetime] = None
    source_ref: Optional[str] = Field(None, description="Store-side idempotency key.")


class InsightBatchIn(BaseModel):
    insights: List[InsightIn]


class InsightExposureIn(BaseModel):
    store_id: str
    supplier_code: str
    kind: str
    visible: bool = False


# ── offers (P3: the non-margin-revenue rail) ─────────────────────────────
class OfferIn(BaseModel):
    store_handle: Optional[str] = Field(
        None, description="Which shared store this is for. Omit when the "
                          "supplier has exactly one consenting store.")
    offer_type: str = Field(..., description="slotting|rebate|consignment|price_support")
    terms: dict = Field(..., description="e.g. {'rebate_pct': 5, 'volume_units': 10000}")
    message: Optional[str] = None


class OfferRespondIn(BaseModel):
    status: str = Field(..., description="accepted|declined")
    retailer_note: Optional[str] = None


class OfferOut(BaseModel):
    offer_id: str
    store_handle: str
    store_masked: bool
    offer_type: str
    terms: dict
    message: Optional[str] = None
    status: str
    retailer_note: Optional[str] = None
    created_at: Optional[datetime] = None
    responded_at: Optional[datetime] = None


class AdminOfferOut(OfferOut):
    supplier_code: str
    supplier_name: Optional[str] = None
    # Recorded on acceptance for downstream invoicing; no money moves here.
    commission_rate: Optional[float] = None
    commission_amount: Optional[float] = None
    commission_basis: Optional[str] = None


class InsightOut(BaseModel):
    insight_id: str
    kind: str
    store_handle: str
    store_masked: bool
    payload: dict
    period_start: Optional[datetime] = None
    period_end: Optional[datetime] = None
    computed_at: Optional[datetime] = None


# ── portal ───────────────────────────────────────────────────────────────
class SupplierLoginIn(BaseModel):
    supplier_code: str
    password: str


class SupplierTokenOut(BaseModel):
    token: str
    supplier_code: str
    expires_in: int


class MovementOut(BaseModel):
    movement_id: str
    store_handle: str
    store_masked: bool
    city: Optional[str] = None
    sku_code: str
    sku_name: Optional[str] = None
    department: Optional[str] = None
    brand: Optional[str] = None
    movement_type: str
    qty: float
    unit_price: Optional[float] = None
    on_hand: Optional[float] = None
    occurred_at: datetime


class StoreSummaryOut(BaseModel):
    store_handle: str
    store_masked: bool
    city: Optional[str] = None
    units_sold: float
    distinct_skus: int
    last_seen: datetime
