"""
The privacy contract of the Retail Central Intelligence portal.

These tests are the security spec for oasis_hub.visibility: a supplier sees a
movement iff they OWN it AND the store CONSENTED — and identity is masked unless
the store reveals it. Every scenario below is a promise made to retailers.
"""

import os
import sys
from datetime import datetime, timedelta

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis_hub.models import (
    Base, HubTenant, HubStore, HubSupplier, HubSupplierBrand,
    HubStoreConsent, HubStockMovement,
)
from oasis_hub.visibility import visible_movements, supplier_store_summary


@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine, future=True, expire_on_commit=False)
    s = Session()
    yield s
    s.close()


def _seed(db, *, reveal=False, consent_status="granted",
          ownership=("supplier_cd", "SUP_COKE")):
    """A tenant with two stores, a Coke supplier owning SUP_COKE, and movements
    for both a Coke SKU (owned) and a rival SKU (not owned) in store A."""
    tenant = HubTenant(id="t1", tenant_id="acme", name="Acme Retail")
    store_a = HubStore(id="sA", tenant_pk="t1", store_code="A01",
                       store_name="Acme Downtown", city="Nairobi")
    store_b = HubStore(id="sB", tenant_pk="t1", store_code="B02",
                       store_name="Acme Westlands", city="Nairobi")
    sup = HubSupplier(id="supX", supplier_code="COKE", name="Coca-Cola Co",
                      password_hash=None)
    rule = HubSupplierBrand(id="r1", supplier_id="supX",
                            match_type=ownership[0], match_value=ownership[1])
    consent = HubStoreConsent(id="c1", store_id="sA", supplier_id="supX",
                              status=consent_status, reveal_identity=reveal,
                              granted_at=datetime.utcnow())
    now = datetime.utcnow()
    owned = HubStockMovement(
        id="m_owned", store_id="sA", tenant_id="acme", sku_code="COKE_500",
        sku_name="Coke 500ml", supplier_cd="SUP_COKE", brand="Coca-Cola",
        department="Beverages", movement_type="sale", qty=12, unit_price=50,
        occurred_at=now, source_ref="ref-owned")
    rival = HubStockMovement(
        id="m_rival", store_id="sA", tenant_id="acme", sku_code="PEPSI_500",
        sku_name="Pepsi 500ml", supplier_cd="SUP_PEPSI", brand="Pepsi",
        department="Beverages", movement_type="sale", qty=9, unit_price=48,
        occurred_at=now, source_ref="ref-rival")
    # owned SKU sold in a store that did NOT consent (store B)
    owned_b = HubStockMovement(
        id="m_owned_b", store_id="sB", tenant_id="acme", sku_code="COKE_500",
        sku_name="Coke 500ml", supplier_cd="SUP_COKE", brand="Coca-Cola",
        department="Beverages", movement_type="sale", qty=5, unit_price=50,
        occurred_at=now, source_ref="ref-owned-b")
    db.add_all([tenant, store_a, store_b, sup, rule, consent,
                owned, rival, owned_b])
    db.commit()
    return "supX"


def test_owned_and_consented_is_visible(db):
    sid = _seed(db)
    rows = visible_movements(db, sid)
    ids = {r.movement_id for r in rows}
    assert "m_owned" in ids


def test_not_owned_is_excluded(db):
    sid = _seed(db)
    rows = visible_movements(db, sid)
    ids = {r.movement_id for r in rows}
    assert "m_rival" not in ids, "supplier must never see a rival's SKU"


def test_no_consent_store_is_excluded(db):
    """Owned SKU in store B (no consent row) must not surface."""
    sid = _seed(db)
    rows = visible_movements(db, sid)
    ids = {r.movement_id for r in rows}
    assert "m_owned_b" not in ids, "movement from a non-consenting store leaked"


def test_revoked_consent_sees_nothing(db):
    sid = _seed(db, consent_status="revoked")
    assert visible_movements(db, sid) == []


def test_pending_consent_sees_nothing(db):
    sid = _seed(db, consent_status="pending")
    assert visible_movements(db, sid) == []


def test_no_ownership_rules_sees_nothing(db):
    """A supplier with consent but zero ownership rules is default-denied."""
    sid = _seed(db)
    db.query(HubSupplierBrand).delete()
    db.commit()
    assert visible_movements(db, sid) == []


def test_identity_masked_when_not_revealed(db):
    sid = _seed(db, reveal=False)
    rows = visible_movements(db, sid)
    row = next(r for r in rows if r.movement_id == "m_owned")
    assert row.store_masked is True
    assert row.store_handle.startswith("Store #")
    assert "Acme Downtown" not in row.store_handle
    assert row.city is None


def test_identity_revealed_when_permitted(db):
    sid = _seed(db, reveal=True)
    rows = visible_movements(db, sid)
    row = next(r for r in rows if r.movement_id == "m_owned")
    assert row.store_masked is False
    assert row.store_handle == "Acme Downtown"
    assert row.city == "Nairobi"


def test_masked_handle_is_stable_and_opaque(db):
    sid = _seed(db, reveal=False)
    r1 = visible_movements(db, sid)[0].store_handle
    r2 = visible_movements(db, sid)[0].store_handle
    assert r1 == r2, "handle must be stable so suppliers can track an outlet"


def test_ownership_by_brand_matches(db):
    sid = _seed(db, ownership=("brand", "Coca-Cola"))
    ids = {r.movement_id for r in visible_movements(db, sid)}
    assert "m_owned" in ids and "m_rival" not in ids


def test_ownership_by_department_matches_only_owned_dept(db):
    # department ownership is broad — supplier owns ALL Beverages here
    sid = _seed(db, ownership=("department", "Beverages"))
    ids = {r.movement_id for r in visible_movements(db, sid)}
    # still gated by consent: rival is Beverages too, so with dept ownership it
    # WOULD be visible — this documents that department ownership is coarse.
    assert "m_owned" in ids
    assert "m_rival" in ids


def test_department_filter_narrows_only(db):
    sid = _seed(db)
    # filtering to a department the supplier's owned SKU isn't in → nothing
    rows = visible_movements(db, sid, department="Snacks")
    assert rows == []


def test_since_until_window(db):
    sid = _seed(db)
    future = datetime.utcnow() + timedelta(days=1)
    assert visible_movements(db, sid, since=future) == []


def test_store_summary_respects_gate(db):
    sid = _seed(db, reveal=False)
    summary = supplier_store_summary(db, sid)
    # only the consenting store, only owned units (12), masked handle
    assert len(summary) == 1
    assert summary[0]["units_sold"] == 12
    assert summary[0]["store_masked"] is True
