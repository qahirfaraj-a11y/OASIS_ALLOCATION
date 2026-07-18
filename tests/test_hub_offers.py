"""
Supplier offers (portal P3) — the reverse rail: intelligence goes out, a funded
offer comes back.

Security contract: a supplier may only address a store that consented to them,
sees only their OWN offers, and addresses stores by the handle they were shown
(never an internal id) — so a masked store stays masked even while transacting.
"""

import importlib
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

ADMIN_KEY = "test-admin-key"


@pytest.fixture()
def client(tmp_path, monkeypatch):
    db_file = tmp_path / "hub_offers.db"
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


def _setup(c, reveal=True):
    _admin(c, "/admin/tenants", tenant_id="acme", name="Acme").raise_for_status()
    store = _admin(c, "/admin/stores", tenant_id="acme", store_code="A01",
                   store_name="Acme Westgate", city="Nairobi").json()
    _admin(c, "/admin/suppliers", supplier_code="COKE", name="Coca-Cola",
           password="s3cret").raise_for_status()
    _admin(c, "/admin/consent", store_id=store["id"], supplier_code="COKE",
           status="granted", reveal_identity=reveal).raise_for_status()
    session = c.post("/portal/login",
                     json={"supplier_code": "COKE", "password": "s3cret"}
                     ).json()["token"]
    return store, {"Authorization": f"Bearer {session}"}


def test_offer_roundtrip_pending_then_accepted(client):
    store, h = _setup(client)
    r = client.post("/portal/offers", headers=h, json={
        "store_handle": "Acme Westgate", "offer_type": "rebate",
        "terms": {"rebate_pct": 5, "volume_units": 10000},
        "message": "5% on 10k units this quarter"})
    assert r.status_code == 200
    offer = r.json()
    assert offer["status"] == "pending" and offer["terms"]["rebate_pct"] == 5

    # retailer sees it in the queue with the supplier named
    q = client.get("/admin/offers", headers={"X-Hub-Admin-Key": ADMIN_KEY}).json()
    assert len(q) == 1 and q[0]["supplier_code"] == "COKE"

    # retailer accepts
    resp = client.post(f"/admin/offers/{offer['offer_id']}/respond",
                       headers={"X-Hub-Admin-Key": ADMIN_KEY},
                       json={"status": "accepted", "retailer_note": "Agreed."})
    assert resp.status_code == 200 and resp.json()["status"] == "accepted"

    # supplier sees the outcome
    mine = client.get("/portal/offers", headers=h).json()
    assert mine[0]["status"] == "accepted" and mine[0]["retailer_note"] == "Agreed."


def test_masked_store_stays_masked_while_transacting(client):
    from oasis_hub.visibility import _mask_handle
    store, h = _setup(client, reveal=False)
    handle = _mask_handle(store["id"])       # the handle the supplier is shown
    assert handle.startswith("Store #")
    r = client.post("/portal/offers", headers=h, json={
        "store_handle": handle, "offer_type": "slotting", "terms": {"fee_amount": 50000}})
    assert r.status_code == 200
    body = r.json()
    assert body["store_masked"] is True
    assert "Acme Westgate" not in body["store_handle"]


def test_handle_may_be_omitted_with_a_single_consenting_store(client):
    """A store can consent and share insights before any movement data exists —
    the supplier must still be able to reach it, so an omitted handle resolves
    when there is exactly one consenting store."""
    store, h = _setup(client)
    r = client.post("/portal/offers", headers=h, json={
        "offer_type": "rebate", "terms": {"rebate_pct": 4}})
    assert r.status_code == 200
    assert r.json()["store_handle"] == "Acme Westgate"


def test_cannot_offer_to_a_non_consenting_store(client):
    store, h = _setup(client)
    # a second store that never consented to COKE
    other = _admin(client, "/admin/stores", tenant_id="acme", store_code="B02",
                   store_name="Acme Nyali", city="Mombasa").json()
    assert other["id"]
    r = client.post("/portal/offers", headers=h, json={
        "store_handle": "Acme Nyali", "offer_type": "rebate", "terms": {"rebate_pct": 3}})
    assert r.status_code == 404


def test_supplier_sees_only_their_own_offers(client):
    store, h = _setup(client)
    client.post("/portal/offers", headers=h, json={
        "store_handle": "Acme Westgate", "offer_type": "rebate",
        "terms": {"rebate_pct": 5}}).raise_for_status()

    # a rival supplier with consent to the same store
    _admin(client, "/admin/suppliers", supplier_code="PEPSI", name="Pepsi",
           password="p@ss").raise_for_status()
    _admin(client, "/admin/consent", store_id=store["id"], supplier_code="PEPSI",
           status="granted", reveal_identity=True).raise_for_status()
    rival = client.post("/portal/login",
                        json={"supplier_code": "PEPSI", "password": "p@ss"}
                        ).json()["token"]
    rows = client.get("/portal/offers",
                      headers={"Authorization": f"Bearer {rival}"}).json()
    assert rows == [], "a supplier must never see a rival's offer"


def test_rejects_unknown_offer_type(client):
    store, h = _setup(client)
    r = client.post("/portal/offers", headers=h, json={
        "store_handle": "Acme Westgate", "offer_type": "bribe", "terms": {}})
    assert r.status_code == 422


def test_cannot_respond_twice(client):
    store, h = _setup(client)
    offer = client.post("/portal/offers", headers=h, json={
        "store_handle": "Acme Westgate", "offer_type": "rebate",
        "terms": {"rebate_pct": 5}}).json()
    ah = {"X-Hub-Admin-Key": ADMIN_KEY}
    client.post(f"/admin/offers/{offer['offer_id']}/respond", headers=ah,
                json={"status": "declined"}).raise_for_status()
    again = client.post(f"/admin/offers/{offer['offer_id']}/respond", headers=ah,
                        json={"status": "accepted"})
    assert again.status_code == 409


def test_offer_endpoints_require_auth(client):
    assert client.post("/portal/offers", json={"offer_type": "rebate",
                                               "terms": {}}).status_code == 401
    assert client.get("/portal/offers").status_code == 401
    assert client.get("/admin/offers").status_code == 401
