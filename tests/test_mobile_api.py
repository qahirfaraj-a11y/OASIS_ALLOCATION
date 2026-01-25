from fastapi.testclient import TestClient
import os
import sys

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from app.api.server import app, DATA_DIR

client = TestClient(app)

def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"status": "OASIS Mobile backend is running"}

def test_static_files():
    # Test index.html serving
    response = client.get("/app/index.html")
    assert response.status_code == 200
    assert "<title>OASIS Mobile</title>" in response.text

    # Test CSS serving
    response = client.get("/app/style.css")
    assert response.status_code == 200
    assert ":root" in response.text

def test_status_endpoint():
    response = client.get("/status")
    assert response.status_code == 200
    data = response.json()
    assert "state" in data
    assert "progress" in data

def test_results_endpoint():
    response = client.get("/results")
    assert response.status_code == 200
    assert "results" in response.json()

if __name__ == "__main__":
    try:
        test_read_root()
        print("Root Endpoint: PASS")
        test_static_files()
        print("Static Files: PASS")
        test_status_endpoint()
        print("Status Endpoint: PASS")
        test_results_endpoint()
        print("Results Endpoint: PASS")
        print("ALL TESTS PASSED")
    except Exception as e:
        print(f"TEST FAILED: {e}")
        exit(1)
