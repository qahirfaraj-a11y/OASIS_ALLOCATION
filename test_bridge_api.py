import os
import logging

# Pin the API key before the security module reads it at import time.
os.environ.setdefault("OASIS_API_KEY", "test-api-key")

from fastapi.testclient import TestClient
from oasis.api.bridge import app

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("API_TEST")

client = TestClient(app, headers={"X-API-Key": os.environ["OASIS_API_KEY"]})

def test_health():
    response = client.get("/")
    assert response.status_code == 200
    assert "Bridge Online" in response.json()["status"]
    logger.info("Health Check Passed")

def test_pending_orders():
    response = client.get("/orders/review")
    assert response.status_code == 200
    orders = response.json()
    assert len(orders) > 0
    assert orders[0]['sku'] == "5001"
    logger.info(f"Pending Orders Check Passed. Found {len(orders)} orders.")

def test_approve_order():
    # Approve Heinz Ketchup (5001)
    response = client.post("/orders/approve/5001")
    assert response.status_code == 200
    assert response.json()["status"] == "approved"
    
    # Verify it's gone
    response = client.get("/orders/review")
    orders = response.json()
    assert not any(o['sku'] == "5001" for o in orders)
    logger.info("Order Approval Logic Passed")

def test_velocity_spike_alert():
    # Inject a massive sales batch for "Fresh Milk" (9002) which has avg 100/day (~8/hr)
    # sending 50 units in one go (approx 600% deviation)
    sales_batch = [
        {"sku": "9002", "qty": 50, "timestamp": "2026-02-17T10:00:00"},
        {"sku": "9002", "qty": 10, "timestamp": "2026-02-17T10:05:00"}
    ]
    
    # 1. Ingest
    response = client.post("/ingest/sales", json=sales_batch)
    assert response.status_code == 200
    
    # 2. Check Alerts
    response = client.get("/alerts")
    alerts = response.json()
    assert len(alerts) > 0
    assert alerts[-1]['type'] == "VELOCITY_SPIKE"
    assert alerts[-1]['product_name'] == "Fresh Milk"
    logger.info("Velocity Spike Alert Logic Passed")

def test_online_mix():
    response = client.get("/analysis/online-mix")
    assert response.status_code == 200
    stats = response.json()
    assert "channel_mix_pct" in stats
    logger.info(f"Online Mix Analysis Passed. Mix: {stats['channel_mix_pct']}%")

if __name__ == "__main__":
    logger.info("--- Starting API Verification ---")
    try:
        test_health()
        test_pending_orders()
        test_approve_order()
        test_velocity_spike_alert()
        test_online_mix()
        logger.info("--- ALL TESTS PASSED SUCCESSFULLY ---")
    except Exception as e:
        import traceback
        traceback.print_exc()
        logger.error(f"TEST FAILED: {repr(e)}")
