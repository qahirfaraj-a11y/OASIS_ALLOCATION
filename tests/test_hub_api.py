"""
End-to-end hub API contract via FastAPI TestClient.

Exercises the whole vertical slice against a temp SQLite hub DB:
  admin provisions → store pushes telemetry → supplier logs in and reads,
  seeing ONLY their own products in the ONE store that consented, identity
  masked. Also asserts the three auth walls actually reject.
"""

import os
import sys
import importlib
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

ADMIN_KEY = "test-admin-key"
SALT = "test-license-salt"


@pytest.fixture()
def client(tmp_path, monkeypatch):
    # Point the hub at an isolated DB + set required secrets BEFORE import.
    db_file = tmp_path / "hub_test.db"
    monkeypatch.setenv("OASIS_HUB_DB_URL", f"sqlite:///{db_file.as_posix()}")
    monkeypatch.setenv("OASIS_HUB_ADMIN_KEY", ADMIN_KEY)
    monkeypatch.setenv("OASIS_HUB_TOKEN_SECRET", "test-token-secret")
    monkeypatch.setenv("OASIS_LICENSE_SALT", SALT)

    # Reload modules so module-level env captures + engine globals reset.
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


def _provision(c):
    """Full provisioning: tenant, store, ingest token, supplier, ownership, consent."""
    _admin(c, "/admin/tenants", tenant_id="acme", name="Acme Retail").raise_for_status()
    store = _admin(c, "/admin/stores", tenant_id="acme", store_code="A01",
                   store_name="Acme Downtown", city="Nairobi").json()
    tok = c.post(f"/admin/stores/{store['id']}/ingest-token",
                 headers={"X-Hub-Admin-Key": ADMIN_KEY}).json()["token"]
    _admin(c, "/admin/suppliers", supplier_code="COKE", name="Coca-Cola",
           password="s3cret").raise_for_status()
    _admin(c, "/admin/suppliers/ownership", supplier_code="COKE",
           match_type="supplier_cd", match_value="SUP_COKE").raise_for_status()
    _admin(c, "/admin/consent", store_id=store["id"], supplier_code="COKE",
           status="granted", reveal_identity=False).raise_for_status()
    return store, tok


def _movement(sku, sup_cd, ref, qty=10):
    return {
        "sku_code": sku, "movement_type": "sale", "qty": qty,
        "occurred_at": datetime.now(timezone.utc).isoformat(),
        "supplier_cd": sup_cd, "brand": "X", "department": "Beverages",
        "unit_price": 50, "source_ref": ref,
    }


def test_full_flow_supplier_sees_only_owned(client):
    store, tok = _provision(client)
    # store pushes one owned + one rival movement
    r = client.post("/ingest/movements",
                    json={"movements": [_movement("COKE_500", "SUP_COKE", "r1"),
                                        _movement("PEPSI_500", "SUP_PEPSI", "r2")]},
                    headers={"Authorization": f"Bearer {tok}"})
    assert r.status_code == 200 and r.json()["accepted"] == 2

    # supplier logs in
    login = client.post("/portal/login",
                        json={"supplier_code": "COKE", "password": "s3cret"})
    assert login.status_code == 200
    session = login.json()["token"]

    mv = client.get("/portal/movements",
                    headers={"Authorization": f"Bearer {session}"})
    assert mv.status_code == 200
    rows = mv.json()
    skus = {r["sku_code"] for r in rows}
    assert skus == {"COKE_500"}, "supplier saw a non-owned SKU"
    assert rows[0]["store_masked"] is True
    assert rows[0]["store_handle"].startswith("Store #")


def test_ingest_is_idempotent(client):
    store, tok = _provision(client)
    body = {"movements": [_movement("COKE_500", "SUP_COKE", "dup")]}
    h = {"Authorization": f"Bearer {tok}"}
    assert client.post("/ingest/movements", json=body, headers=h).json()["accepted"] == 1
    second = client.post("/ingest/movements", json=body, headers=h).json()
    assert second["accepted"] == 0 and second["duplicates"] == 1


def test_license_issue_round_trips_and_verifies(client):
    _admin(client, "/admin/tenants", tenant_id="acme", name="Acme").raise_for_status()
    r = _admin(client, "/admin/licenses", tenant_id="acme",
               expiry_date="2030-12-31", bundle="pro")
    assert r.status_code == 200
    key = r.json()["key"]
    assert key["tenant_id"] == "acme"
    assert set(r.json()["modules"]) == {"core", "ordering", "revenue"}
    # the issued key verifies under the same salt the on-prem client would use
    from oasis.logic.license_manager import OfflineLicenseManager
    mgr = OfflineLicenseManager()
    for mod, sig in key["authorized_modules"].items():
        assert sig == mgr._fingerprint("acme", mod, "2030-12-31")


# ── auth walls ───────────────────────────────────────────────────────────
def test_admin_requires_key(client):
    assert client.post("/admin/tenants", json={"tenant_id": "x", "name": "x"}).status_code == 401


def test_ingest_rejects_bad_token(client):
    r = client.post("/ingest/movements",
                    json={"movements": [_movement("C", "S", "r")]},
                    headers={"Authorization": "Bearer not-a-real-token"})
    assert r.status_code == 401


def test_portal_rejects_no_session(client):
    assert client.get("/portal/movements").status_code == 401


def test_portal_rejects_bad_password(client):
    _provision(client)
    r = client.post("/portal/login",
                    json={"supplier_code": "COKE", "password": "wrong"})
    assert r.status_code == 401
