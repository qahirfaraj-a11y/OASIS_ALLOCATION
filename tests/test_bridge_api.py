import os
import sys

# Pin the API key before the security module reads it at import time.
os.environ.setdefault("OASIS_API_KEY", "test-api-key")

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pytest
from fastapi.testclient import TestClient

from oasis.api import bridge
from oasis.api import security as api_security
from oasis.api.bridge import app

client = TestClient(app)

_AUTH = {"X-API-Key": api_security._API_KEY}


@pytest.fixture(autouse=True)
def _bridge_state():
    """Deterministic state so tests never depend on the last engine run."""
    bridge.pending_orders = [
        {
            "sku": "5001",
            "product_name": "Heinz Ketchup",
            "current_stock": 5,
            "recommended_qty": 40,
            "reasoning": "Low stock at depot",
            "priority": "high",
        }
    ]
    bridge.in_memory_alerts = []
    bridge.historical_stats = {
        "9002": {"avg_daily_sales": 100.0, "product_name": "Fresh Milk"}
    }
    yield
    bridge.pending_orders = []
    bridge.in_memory_alerts = []


def test_health():
    response = client.get("/")
    assert response.status_code == 200
    assert "Bridge Online" in response.json()["status"]


def test_unauthorized_rejected():
    response = client.get("/orders/review")
    assert response.status_code == 401


def test_pending_orders():
    response = client.get("/orders/review", headers=_AUTH)
    assert response.status_code == 200
    orders = response.json()
    assert len(orders) > 0
    assert orders[0]['sku'] == "5001"


def test_approve_order():
    response = client.post("/orders/approve/5001", headers=_AUTH)
    assert response.status_code == 200
    assert response.json()["status"] == "approved"

    response = client.get("/orders/review", headers=_AUTH)
    orders = response.json()
    assert not any(o['sku'] == "5001" for o in orders)


def test_approve_missing_order():
    response = client.post("/orders/approve/9999", headers=_AUTH)
    assert response.status_code == 404


def test_velocity_spike_alert():
    sales_batch = [
        {"sku": "9002", "qty": 50, "timestamp": "2026-02-17T10:00:00"},
        {"sku": "9002", "qty": 10, "timestamp": "2026-02-17T10:05:00"},
    ]

    response = client.post("/ingest/sales", json=sales_batch, headers=_AUTH)
    assert response.status_code == 200

    response = client.get("/alerts", headers=_AUTH)
    alerts = response.json()
    assert len(alerts) > 0
    assert alerts[-1]['type'] == "VELOCITY_SPIKE"
    assert alerts[-1]['product_name'] == "Fresh Milk"


def test_online_mix():
    response = client.get("/analysis/online-mix", headers=_AUTH)
    assert response.status_code == 200
    stats = response.json()
    assert "channel_mix_pct" in stats


if __name__ == "__main__":
    import logging
    logging.basicConfig(level=logging.INFO)
    test_health()
    test_unauthorized_rejected()
    test_pending_orders()
    test_approve_order()
    test_approve_missing_order()
    test_velocity_spike_alert()
    test_online_mix()
    print("--- ALL TESTS PASSED SUCCESSFULLY ---")
