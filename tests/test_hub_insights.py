"""
The Insight Push rail (portal P1) — ingestion, the double default-deny gate,
and the supplier-safe card shaper.

The security contract under test: a supplier sees an insight ONLY when the store
both (a) granted consent and (b) explicitly flipped that kind visible. Pushing a
card reveals nothing by itself.
"""

import importlib
import os
import sys

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import insight_emitter as IE
from oasis_hub.models import (
    Base, HubTenant, HubStore, HubSupplier, HubSupplierBrand, HubStoreConsent,
    HubSupplierInsight, HubInsightExposure,
)
from oasis_hub.visibility import visible_insights

ADMIN_KEY = "test-admin-key"


# ── the gate (unit, no HTTP) ─────────────────────────────────────────────
@pytest.fixture
def db():
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    s = sessionmaker(bind=engine, future=True, expire_on_commit=False)()
    s.add_all([
        HubTenant(id="t1", tenant_id="acme", name="Acme"),
        HubStore(id="sA", tenant_pk="t1", store_code="A", store_name="Acme Downtown",
                 city="Nairobi"),
        HubSupplier(id="supX", supplier_code="COKE", name="Coca-Cola"),
        HubSupplierBrand(id="r1", supplier_id="supX", match_type="supplier_cd",
                         match_value="SUP_COKE"),
        HubSupplierInsight(id="i1", store_id="sA", supplier_id="supX",
                           kind="reliability",
                           payload_json='{"reliability_class": "RELIABLE"}'),
    ])
    s.commit()
    yield s
    s.close()


def _grant(db, reveal=False):
    db.add(HubStoreConsent(id="c1", store_id="sA", supplier_id="supX",
                           status="granted", reveal_identity=reveal))
    db.commit()


def _expose(db, kind="reliability", visible=True):
    db.add(HubInsightExposure(id="e1", store_id="sA", supplier_id="supX",
                              kind=kind, visible=visible))
    db.commit()


def test_no_consent_no_exposure_sees_nothing(db):
    assert visible_insights(db, "supX") == []


def test_consent_alone_is_not_enough(db):
    _grant(db)
    assert visible_insights(db, "supX") == [], "exposure must also be flipped on"


def test_exposure_alone_is_not_enough(db):
    _expose(db)
    assert visible_insights(db, "supX") == [], "consent must also be granted"


def test_both_gates_open_reveals_card(db):
    _grant(db)
    _expose(db)
    rows = visible_insights(db, "supX")
    assert len(rows) == 1
    assert rows[0]["kind"] == "reliability"
    assert rows[0]["payload"]["reliability_class"] == "RELIABLE"


def test_exposure_false_hides_card(db):
    _grant(db)
    _expose(db, visible=False)
    assert visible_insights(db, "supX") == []


def test_exposure_is_per_kind(db):
    _grant(db)
    _expose(db, kind="sei", visible=True)      # a DIFFERENT kind is exposed
    assert visible_insights(db, "supX") == [], "reliability must stay hidden"


def test_revoked_consent_hides_even_when_exposed(db):
    db.add(HubStoreConsent(id="c1", store_id="sA", supplier_id="supX",
                           status="revoked", reveal_identity=True))
    db.commit()
    _expose(db)
    assert visible_insights(db, "supX") == []


def test_store_identity_masked_unless_revealed(db):
    _grant(db, reveal=False)
    _expose(db)
    row = visible_insights(db, "supX")[0]
    assert row["store_masked"] is True
    assert row["store_handle"].startswith("Store #")
    assert "Acme Downtown" not in row["store_handle"]

    row2_db = db
    row2_db.query(HubStoreConsent).filter_by(id="c1").update({"reveal_identity": True})
    row2_db.commit()
    row2 = visible_insights(db, "supX")[0]
    assert row2["store_masked"] is False and row2["store_handle"] == "Acme Downtown"


# ── the supplier-safe card shaper ────────────────────────────────────────
def test_reliability_card_drops_spend_and_keeps_own_standing():
    card = IE.reliability_card("COKE", {
        "classification": "WATCH", "avg_lead_time": 4.2,
        "total_spend": 9_000_000,          # store-private → must not survive
    })
    assert card["kind"] == "reliability"
    assert card["payload"]["reliability_class"] == "WATCH"
    assert card["payload"]["lead_time_days"] == 4.2
    assert "total_spend" not in card["payload"]


def test_sei_card_keeps_only_aggregate_scores():
    card = IE.sei_card("COKE", {"sei": 388000, "sku_count": 36,
                                "classification": "Elite Stabilizer",
                                "trapped_capital_kes": 500000})
    assert card["payload"]["sei"] == 388000
    # trapped capital is the store's own liquidity position — not shipped
    assert "trapped_capital_kes" not in card["payload"]


def test_guard_rejects_grn_cost_and_credit_fields():
    for bad in ({"grn_lines": [1]}, {"unit_cost": 12.5}, {"credit_days": 30},
                {"cogs_total": 1000}, {"margin_pct": 22}):
        with pytest.raises(ValueError):
            IE._card("COKE", IE.KIND_VELOCITY, bad)


def test_velocity_and_halo_cards_shape_expected_fields():
    v = IE.velocity_card("COKE", [{"sku_code": "COKE_500", "units": 300,
                                   "ads": 10.7, "trend_pct": -4.0,
                                   "unit_cost": 30}])
    assert v["payload"]["items"][0]["sku_code"] == "COKE_500"
    assert "unit_cost" not in v["payload"]["items"][0]

    h = IE.halo_card("COKE", [{"anchor_sku": "COKE_500",
                               "attachment_sku": "CRISPS", "confidence": .42,
                               "lift": 1.9}])
    assert h["payload"]["pairs"][0]["lift"] == 1.9


# ── end-to-end over HTTP ─────────────────────────────────────────────────
@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "hub_insights.db"
    monkeypatch.setenv("OASIS_HUB_DB_URL", f"sqlite:///{db_file.as_posix()}")
    monkeypatch.setenv("OASIS_HUB_ADMIN_KEY", ADMIN_KEY)
    monkeypatch.setenv("OASIS_HUB_TOKEN_SECRET", "test-secret")
    monkeypatch.setenv("OASIS_LICENSE_SALT", "test-salt")
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
        yield c


def _admin(c, path, **json):
    return c.post(path, json=json, headers={"X-Hub-Admin-Key": ADMIN_KEY})


def test_push_then_flex_reveals_over_http(client):
    _admin(client, "/admin/tenants", tenant_id="acme", name="Acme").raise_for_status()
    store = _admin(client, "/admin/stores", tenant_id="acme", store_code="A01",
                   store_name="Acme Downtown", city="Nairobi").json()
    tok = client.post(f"/admin/stores/{store['id']}/ingest-token",
                      headers={"X-Hub-Admin-Key": ADMIN_KEY}).json()["token"]
    _admin(client, "/admin/suppliers", supplier_code="COKE", name="Coca-Cola",
           password="s3cret").raise_for_status()
    _admin(client, "/admin/consent", store_id=store["id"], supplier_code="COKE",
           status="granted", reveal_identity=True).raise_for_status()

    card = IE.reliability_card("COKE", {"classification": "RELIABLE",
                                        "avg_lead_time": 3.0},
                               source_ref="rel-2026-07")
    r = client.post("/ingest/insights", json={"insights": [card]},
                    headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 200 and r.json()["accepted"] == 1
    # idempotent
    assert client.post("/ingest/insights", json={"insights": [card]},
                       headers={"Authorization": f"Bearer {tok}"}
                       ).json()["duplicates"] == 1

    session = client.post("/portal/login",
                          json={"supplier_code": "COKE", "password": "s3cret"}
                          ).json()["token"]
    h = {"Authorization": f"Bearer {session}"}

    # pushed but NOT exposed → supplier sees nothing
    assert client.get("/portal/insights", headers=h).json() == []

    # the retailer flips it on → now visible
    _admin(client, "/admin/insight-exposure", store_id=store["id"],
           supplier_code="COKE", kind="reliability", visible=True).raise_for_status()
    rows = client.get("/portal/insights", headers=h).json()
    assert len(rows) == 1 and rows[0]["payload"]["reliability_class"] == "RELIABLE"

    # and back off again
    _admin(client, "/admin/insight-exposure", store_id=store["id"],
           supplier_code="COKE", kind="reliability", visible=False).raise_for_status()
    assert client.get("/portal/insights", headers=h).json() == []


def test_ingest_rejects_unknown_kind_and_supplier(client):
    _admin(client, "/admin/tenants", tenant_id="acme", name="Acme").raise_for_status()
    store = _admin(client, "/admin/stores", tenant_id="acme", store_code="A01",
                   store_name="S", city="N").json()
    tok = client.post(f"/admin/stores/{store['id']}/ingest-token",
                      headers={"X-Hub-Admin-Key": ADMIN_KEY}).json()["token"]
    h = {"Authorization": f"Bearer {tok}"}
    _admin(client, "/admin/suppliers", supplier_code="COKE", name="C",
           password="x").raise_for_status()

    bad_kind = {"supplier_code": "COKE", "kind": "nonsense", "payload": {}}
    assert client.post("/ingest/insights", json={"insights": [bad_kind]},
                       headers=h).status_code == 422
    unknown_sup = {"supplier_code": "NOPE", "kind": "velocity", "payload": {}}
    assert client.post("/ingest/insights", json={"insights": [unknown_sup]},
                       headers=h).status_code == 404


def test_insights_ingest_requires_store_token(client):
    assert client.post("/ingest/insights", json={"insights": []}).status_code == 401
