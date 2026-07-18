"""
P4: subscription tier gating, the NCP/cannibalization Flex kinds, and the
brokered-offer take-rate.

Two orthogonal gates are under test:
  * EXPOSURE — will the store share this? (retailer's call)
  * TIER     — has the supplier subscribed to receive it? (OASIS billing)
Retailer-gated Flex kinds are deliberately TIER-EXEMPT: a negotiation must never
sit behind a paywall.
"""

import importlib
import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import insight_emitter as IE
from oasis_hub.billing import compute_commission
from oasis_hub.models import (
    Base, HubTenant, HubStore, HubSupplier, HubStoreConsent,
    HubSupplierInsight, HubInsightExposure, kind_allowed_for_tier,
)
from oasis_hub.visibility import visible_insights

ADMIN_KEY = "test-admin-key"


# ── tier policy ──────────────────────────────────────────────────────────
def test_free_kinds_available_to_everyone():
    for kind in ("velocity", "reliability"):
        assert kind_allowed_for_tier(kind, "free")
        assert kind_allowed_for_tier(kind, "premium")


def test_premium_kinds_need_a_subscription():
    for kind in ("halo", "broken_halo", "archetype", "capital_efficiency", "reorder"):
        assert not kind_allowed_for_tier(kind, "free")
        assert kind_allowed_for_tier(kind, "premium")


def test_flex_kinds_are_never_paywalled():
    """The retailer chose to show these — billing must not block the conversation."""
    for kind in ("sei", "ncp", "quality", "cannibalization"):
        assert kind_allowed_for_tier(kind, "free")
        assert kind_allowed_for_tier(kind, "premium")


# ── tier enforcement in the read path ────────────────────────────────────
@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    s = sessionmaker(bind=engine, future=True, expire_on_commit=False)()
    s.add_all([
        HubTenant(id="t1", tenant_id="acme", name="Acme"),
        HubStore(id="sA", tenant_pk="t1", store_code="A", store_name="Acme Downtown"),
        HubSupplier(id="supX", supplier_code="COKE", name="Coca-Cola", tier="free"),
        HubStoreConsent(id="c1", store_id="sA", supplier_id="supX",
                        status="granted", reveal_identity=True),
        # one premium kind and one flex kind, both exposed by the retailer
        HubSupplierInsight(id="i1", store_id="sA", supplier_id="supX",
                           kind="halo", payload_json='{"pairs": []}'),
        HubSupplierInsight(id="i2", store_id="sA", supplier_id="supX",
                           kind="ncp", payload_json='{"ncp_days": -31}'),
        HubInsightExposure(id="e1", store_id="sA", supplier_id="supX",
                           kind="halo", visible=True),
        HubInsightExposure(id="e2", store_id="sA", supplier_id="supX",
                           kind="ncp", visible=True),
    ])
    s.commit()
    yield s
    s.close()


def test_free_supplier_sees_flex_but_not_premium(db):
    kinds = {r["kind"] for r in visible_insights(db, "supX")}
    assert kinds == {"ncp"}, "premium halo must be withheld, flex ncp must show"


def test_upgrading_to_premium_reveals_the_premium_kind(db):
    db.query(HubSupplier).filter_by(id="supX").update({"tier": "premium"})
    db.commit()
    kinds = {r["kind"] for r in visible_insights(db, "supX")}
    assert kinds == {"halo", "ncp"}


def test_tier_cannot_override_exposure(db):
    """Premium tier must not reveal something the retailer never exposed."""
    db.query(HubSupplier).filter_by(id="supX").update({"tier": "premium"})
    db.query(HubInsightExposure).filter_by(id="e1").update({"visible": False})
    db.commit()
    kinds = {r["kind"] for r in visible_insights(db, "supX")}
    assert "halo" not in kinds


# ── the new Flex card kinds ──────────────────────────────────────────────
def test_ncp_card_allows_own_terms_but_nothing_else():
    card = IE.ncp_card("COKE", {"ncp_days": -31, "credit_days": 14,
                                "dio_days": 45, "position": "draining",
                                "skus_considered": 36})
    p = card["payload"]
    assert p["ncp_days"] == -31 and p["credit_days"] == 14 and p["dio_days"] == 45
    # the vetted exemption is narrow — the store's own book still cannot ride along
    with pytest.raises(ValueError):
        IE._card("COKE", IE.KIND_NCP, {"ncp_days": -31, "store_float_kes": 4_000_000},
                 allow=frozenset({"credit_days", "dio_days"}))


def test_ncp_position_reading():
    assert IE.ncp_position(10) == "funding"
    assert IE.ncp_position(0) == "neutral"
    assert IE.ncp_position(-5) == "draining"
    assert IE.ncp_position(None) is None


def test_cannibalization_card_is_commercial_free():
    card = IE.cannibalization_card("COKE", [{
        "sku_code": "COKE_ZERO", "cannibalization_rate": 0.92,
        "substitutes_sku": "COKE_500", "incremental_pct": 8,
        "lost_margin_kes": 12000,          # store-private → dropped
    }])
    item = card["payload"]["items"][0]
    assert item["cannibalization_rate"] == 0.92
    assert "lost_margin_kes" not in item


# ── take-rate ────────────────────────────────────────────────────────────
def test_commission_disabled_by_default(monkeypatch):
    monkeypatch.delenv("OASIS_HUB_COMMISSION_RATE", raising=False)
    rate, amount, basis = compute_commission({"fee_amount": 50000})
    assert rate == 0.0 and amount is None


def test_commission_on_a_monetary_offer(monkeypatch):
    monkeypatch.setenv("OASIS_HUB_COMMISSION_RATE", "0.02")
    rate, amount, basis = compute_commission({"fee_amount": 50000})
    assert rate == 0.02 and amount == 1000.0 and basis == "fee_amount"


def test_percentage_offer_defers_the_amount(monkeypatch):
    """A 5% rebate on future volume has no knowable cash value yet — record the
    rate and settle later rather than inventing a number."""
    monkeypatch.setenv("OASIS_HUB_COMMISSION_RATE", "0.02")
    rate, amount, basis = compute_commission({"rebate_pct": 5})
    assert rate == 0.02 and amount is None and "actual volume" in basis


def test_commission_recorded_on_acceptance(monkeypatch, tmp_path):
    monkeypatch.setenv("OASIS_HUB_DB_URL", f"sqlite:///{(tmp_path/'b.db').as_posix()}")
    monkeypatch.setenv("OASIS_HUB_ADMIN_KEY", ADMIN_KEY)
    monkeypatch.setenv("OASIS_HUB_TOKEN_SECRET", "s")
    monkeypatch.setenv("OASIS_LICENSE_SALT", "salt")
    monkeypatch.setenv("OASIS_HUB_COMMISSION_RATE", "0.025")
    from fastapi.testclient import TestClient
    import oasis_hub.db as hubdb
    import oasis_hub.security as hubsec
    importlib.reload(hubdb)
    importlib.reload(hubsec)
    for name in ("oasis_hub.routers.admin", "oasis_hub.routers.ingest",
                 "oasis_hub.routers.portal", "oasis_hub.app"):
        if name in sys.modules:
            importlib.reload(sys.modules[name])
    app_mod = importlib.import_module("oasis_hub.app")
    importlib.reload(app_mod)

    with TestClient(app_mod.app) as c:
        ah = {"X-Hub-Admin-Key": ADMIN_KEY}
        c.post("/admin/tenants", json={"tenant_id": "acme", "name": "A"}, headers=ah)
        store = c.post("/admin/stores", json={"tenant_id": "acme", "store_code": "A01",
                                              "store_name": "Acme"}, headers=ah).json()
        c.post("/admin/suppliers", json={"supplier_code": "COKE", "name": "C",
                                         "password": "p"}, headers=ah)
        c.post("/admin/consent", json={"store_id": store["id"], "supplier_code": "COKE",
                                       "status": "granted", "reveal_identity": True},
               headers=ah)
        sess = c.post("/portal/login", json={"supplier_code": "COKE",
                                             "password": "p"}).json()["token"]
        offer = c.post("/portal/offers",
                       headers={"Authorization": f"Bearer {sess}"},
                       json={"store_handle": "Acme", "offer_type": "slotting",
                             "terms": {"fee_amount": 80000}}).json()
        resp = c.post(f"/admin/offers/{offer['offer_id']}/respond", headers=ah,
                      json={"status": "accepted"}).json()
        assert resp["commission_rate"] == 0.025
        assert resp["commission_amount"] == 2000.0
        assert resp["commission_basis"] == "fee_amount"
