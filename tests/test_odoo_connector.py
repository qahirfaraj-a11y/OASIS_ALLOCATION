"""
Odoo connector: mapping correctness, the hub-schema contract, push batching/
retry, addon manifest sanity, and a TRUE end-to-end (Odoo record → mapping →
push client → hub → supplier read) proving the connector speaks the hub's
language over the real ingestion path.
"""

import ast
import importlib
import os
import sys
from datetime import datetime, timezone

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from connectors.odoo.oasis_connector import mapping, push_client
from oasis_hub.schemas import MovementIn

ADDON = os.path.join(os.path.dirname(__file__), '..', 'connectors', 'odoo',
                     'oasis_connector')

ADMIN_KEY = "test-admin-key"
SALT = "test-license-salt"


# ── sample Odoo records ──────────────────────────────────────────────────
def _coke_product():
    return {
        "id": 1, "default_code": "COKE_500", "name": "Coke 500ml",
        "categ_id": [3, "Beverages"], "list_price": 55.0,
        "supplier_ref": "SUP_COKE", "product_brand_id": [9, "Coca-Cola"],
    }


def _pepsi_product():
    return {
        "id": 2, "default_code": "PEPSI_500", "name": "Pepsi 500ml",
        "categ_id": [3, "Beverages"], "list_price": 52.0,
        "supplier_ref": "SUP_PEPSI", "product_brand_id": [11, "Pepsi"],
    }


# ── mapping ──────────────────────────────────────────────────────────────
def test_map_pos_order_line_is_a_sale():
    line = {"id": 42, "qty": 6, "price_unit": 50.0}
    m = mapping.map_pos_order_line(line, _coke_product(),
                                   order_date="2026-07-01 10:00:00")
    assert m["movement_type"] == "sale"
    assert m["sku_code"] == "COKE_500"
    assert m["department"] == "Beverages"
    assert m["supplier_cd"] == "SUP_COKE"
    assert m["brand"] == "Coca-Cola"
    assert m["qty"] == 6 and m["unit_price"] == 50.0
    assert m["source_ref"] == "odoo:pos.order.line:42"
    assert m["occurred_at"] == "2026-07-01T10:00:00"


def test_map_stock_move_vendor_is_receipt():
    move = {"id": 7, "product_qty": 100, "date": "2026-07-02 08:00:00",
            "location_usage": "supplier", "location_dest_usage": "internal"}
    m = mapping.map_stock_move(move, _coke_product())
    assert m["movement_type"] == "receipt"
    assert m["source_ref"] == "odoo:stock.move:7"


def test_map_stock_move_to_customer_is_sale():
    move = {"id": 9, "product_qty": 3, "date": "2026-07-02 10:00:00",
            "location_usage": "internal", "location_dest_usage": "customer"}
    m = mapping.map_stock_move(move, _coke_product())
    assert m["movement_type"] == "sale"       # sell-through, POS or not


def test_map_stock_move_zero_price_falls_back_to_list_price():
    """price_unit=0 on a stock.move means 'not costed' — use list price."""
    move = {"id": 10, "product_qty": 3, "date": "2026-07-02 10:00:00",
            "location_usage": "internal", "location_dest_usage": "customer",
            "price_unit": 0.0}
    m = mapping.map_stock_move(move, _coke_product())
    assert m["unit_price"] == 55.0


def test_map_stock_move_internal_is_adjustment():
    move = {"id": 8, "product_qty": -3, "date": "2026-07-02 09:00:00",
            "location_usage": "internal", "location_dest_usage": "inventory"}
    m = mapping.map_stock_move(move, _coke_product())
    assert m["movement_type"] == "adjustment"


def test_map_stock_quant_is_on_hand_snapshot():
    q = {"id": 12, "quantity": 24, "in_date": "2026-07-03 00:00:00"}
    m = mapping.map_stock_quant(q, _coke_product())
    assert m["movement_type"] == "stock_on_hand"
    assert m["on_hand"] == 24 and m["qty"] == 24
    assert m["source_ref"] == "odoo:stock.quant:12:2026-07-03"


def test_m2o_pair_and_scalar_both_parse():
    prod = _coke_product()
    prod["categ_id"] = "Beverages"          # scalar form
    assert mapping.product_info(prod)["department"] == "Beverages"
    prod["categ_id"] = False                # missing
    assert mapping.product_info(prod)["department"] is None


def test_missing_timestamp_raises():
    with pytest.raises(ValueError):
        mapping.map_pos_order_line({"id": 1, "qty": 1}, _coke_product(),
                                   order_date=None)


# ── hub-schema contract ──────────────────────────────────────────────────
def test_mapped_movements_validate_against_hub_schema():
    """Every mapper's output must be a valid hub MovementIn — the contract."""
    samples = [
        mapping.map_pos_order_line({"id": 1, "qty": 2, "price_unit": 9},
                                   _coke_product(), order_date="2026-07-01 10:00:00"),
        mapping.map_stock_move({"id": 2, "product_qty": 5, "date": "2026-07-01 10:00:00",
                                "location_usage": "supplier",
                                "location_dest_usage": "internal"}, _coke_product()),
        mapping.map_stock_quant({"id": 3, "quantity": 7, "in_date": "2026-07-01 10:00:00"},
                                _coke_product()),
    ]
    for s in samples:
        MovementIn(**s)          # raises if the connector drifts from the schema


# ── push client ──────────────────────────────────────────────────────────
def test_push_client_batches_and_aggregates():
    seen = []

    def poster(url, headers, body):
        seen.append(len(body["movements"]))
        return 200, {"accepted": len(body["movements"]), "duplicates": 0,
                     "store_id": "s"}

    c = push_client.HubPushClient("http://h", "tok", poster=poster, batch_size=2)
    res = c.push([{"x": i} for i in range(5)])
    assert seen == [2, 2, 1]                     # chunked into 2+2+1
    assert res["accepted"] == 5 and res["batches"] == 3


def test_push_client_retries_transient_then_succeeds():
    calls = {"n": 0}

    def poster(url, headers, body):
        calls["n"] += 1
        if calls["n"] == 1:
            return 503, {"detail": "unavailable"}
        return 200, {"accepted": 1, "duplicates": 0}

    c = push_client.HubPushClient("http://h", "tok", poster=poster,
                                  max_retries=3, backoff=0)
    res = c.push([{"x": 1}])
    assert calls["n"] == 2 and res["accepted"] == 1


def test_push_client_raises_on_permanent_rejection():
    def poster(url, headers, body):
        return 401, {"detail": "bad token"}

    c = push_client.HubPushClient("http://h", "tok", poster=poster)
    with pytest.raises(push_client.HubPushError):
        c.push([{"x": 1}])


# ── addon manifest sanity ────────────────────────────────────────────────
def test_manifest_is_well_formed_and_data_files_exist():
    with open(os.path.join(ADDON, "__manifest__.py"), encoding="utf-8") as f:
        manifest = ast.literal_eval(f.read())
    assert manifest["name"] and manifest["version"]
    assert {"stock", "point_of_sale"} <= set(manifest["depends"])
    assert manifest["installable"] is True
    for rel in manifest["data"]:
        assert os.path.exists(os.path.join(ADDON, rel)), f"missing data file: {rel}"


# ── TRUE end-to-end: Odoo record → mapping → push → hub → supplier ───────
@pytest.fixture()
def hub(tmp_path, monkeypatch):
    db_file = tmp_path / "hub_odoo_e2e.db"
    monkeypatch.setenv("OASIS_HUB_DB_URL", f"sqlite:///{db_file.as_posix()}")
    monkeypatch.setenv("OASIS_HUB_ADMIN_KEY", ADMIN_KEY)
    monkeypatch.setenv("OASIS_HUB_TOKEN_SECRET", "test-token-secret")
    monkeypatch.setenv("OASIS_LICENSE_SALT", SALT)
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


def test_odoo_to_hub_to_supplier_end_to_end(hub):
    # provision: tenant, store, ingest token, Coke supplier owning SUP_COKE, consent
    _admin(hub, "/admin/tenants", tenant_id="acme", name="Acme").raise_for_status()
    store = _admin(hub, "/admin/stores", tenant_id="acme", store_code="A01",
                   store_name="Acme Downtown", city="Nairobi").json()
    tok = hub.post(f"/admin/stores/{store['id']}/ingest-token",
                   headers={"X-Hub-Admin-Key": ADMIN_KEY}).json()["token"]
    _admin(hub, "/admin/suppliers", supplier_code="COKE", name="Coca-Cola",
           password="s3cret").raise_for_status()
    _admin(hub, "/admin/suppliers/ownership", supplier_code="COKE",
           match_type="supplier_cd", match_value="SUP_COKE").raise_for_status()
    _admin(hub, "/admin/consent", store_id=store["id"], supplier_code="COKE",
           status="granted", reveal_identity=True).raise_for_status()

    # map two Odoo POS lines (owned Coke + rival Pepsi) through the REAL mapper
    movements = [
        mapping.map_pos_order_line({"id": 101, "qty": 6, "price_unit": 55},
                                   _coke_product(), order_date="2026-07-05 12:00:00"),
        mapping.map_pos_order_line({"id": 102, "qty": 4, "price_unit": 52},
                                   _pepsi_product(), order_date="2026-07-05 12:05:00"),
    ]

    # push via the REAL push client, transport pointed at the hub TestClient
    def poster(url, headers, body):
        r = hub.post(url, json=body, headers=headers)
        return r.status_code, (r.json() if r.content else {})

    client = push_client.HubPushClient("", tok, poster=poster)
    res = client.push(movements)
    assert res["accepted"] == 2

    # supplier logs in and sees ONLY their owned SKU, with identity revealed
    session = hub.post("/portal/login",
                       json={"supplier_code": "COKE", "password": "s3cret"}).json()["token"]
    rows = hub.get("/portal/movements",
                   headers={"Authorization": f"Bearer {session}"}).json()
    assert {r["sku_code"] for r in rows} == {"COKE_500"}
    assert rows[0]["store_handle"] == "Acme Downtown"
    assert rows[0]["qty"] == 6

    # re-push is idempotent (source_ref dedup)
    assert client.push(movements)["accepted"] == 0


# ── XML-RPC backfill (no-addon path) end-to-end ──────────────────────────
def _fake_execute_kw():
    """A stand-in for Odoo's execute_kw covering the calls the backfill makes."""
    products = {
        1: {"id": 1, "default_code": "COKE_500", "display_name": "Coke 500ml",
            "categ_id": [3, "Beverages"], "list_price": 55.0,
            "product_brand_id": [9, "Coca-Cola"], "seller_ids": [301]},
        2: {"id": 2, "default_code": "PEPSI_500", "display_name": "Pepsi 500ml",
            "categ_id": [3, "Beverages"], "list_price": 52.0,
            "product_brand_id": [11, "Pepsi"], "seller_ids": [302]},
    }
    suppliers = {301: {"id": 301, "partner_id": [50, "SUP_COKE"]},
                 302: {"id": 302, "partner_id": [51, "SUP_PEPSI"]}}

    def execute_kw(model, method, args, kw=None):
        if model == "pos.order.line" and method == "search_read":
            return [
                {"id": 201, "qty": 6, "price_unit": 55, "product_id": [1, "Coke"],
                 "order_id": [1, "O1"], "write_date": "2026-07-06 09:00:00"},
                {"id": 202, "qty": 4, "price_unit": 52, "product_id": [2, "Pepsi"],
                 "order_id": [2, "O2"], "write_date": "2026-07-06 09:05:00"},
            ]
        if model == "product.product" and method == "read":
            return [products[i] for i in args[0] if i in products]
        if model == "product.supplierinfo" and method == "read":
            return [suppliers[i] for i in args[0] if i in suppliers]
        return []

    return execute_kw


def test_xmlrpc_backfill_end_to_end(hub):
    from connectors.odoo import xmlrpc_sync

    _admin(hub, "/admin/tenants", tenant_id="acme", name="Acme").raise_for_status()
    store = _admin(hub, "/admin/stores", tenant_id="acme", store_code="A01",
                   store_name="Acme Downtown", city="Nairobi").json()
    tok = hub.post(f"/admin/stores/{store['id']}/ingest-token",
                   headers={"X-Hub-Admin-Key": ADMIN_KEY}).json()["token"]
    _admin(hub, "/admin/suppliers", supplier_code="COKE", name="Coca-Cola",
           password="s3cret").raise_for_status()
    _admin(hub, "/admin/suppliers/ownership", supplier_code="COKE",
           match_type="supplier_cd", match_value="SUP_COKE").raise_for_status()
    _admin(hub, "/admin/consent", store_id=store["id"], supplier_code="COKE",
           status="granted", reveal_identity=False).raise_for_status()

    def poster(url, headers, body):
        r = hub.post(url, json=body, headers=headers)
        return r.status_code, (r.json() if r.content else {})

    client = push_client.HubPushClient("", tok, poster=poster)
    res = xmlrpc_sync.backfill(_fake_execute_kw(), client,
                               since="2026-01-01", receipts=False)
    assert res["accepted"] == 2

    session = hub.post("/portal/login",
                       json={"supplier_code": "COKE", "password": "s3cret"}).json()["token"]
    rows = hub.get("/portal/movements",
                   headers={"Authorization": f"Bearer {session}"}).json()
    assert {r["sku_code"] for r in rows} == {"COKE_500"}
    assert rows[0]["store_masked"] is True          # consent without reveal
