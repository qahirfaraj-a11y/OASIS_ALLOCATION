from fastapi.testclient import TestClient
import os
import sys

# Add project root to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from oasis.api import security as api_security
from oasis.api.server import app, DATA_DIR  # noqa: F401 (DATA_DIR re-exported)

client = TestClient(app)
# Authed endpoints require X-API-Key. Send the key the security module actually
# resolved (env var or its ephemeral generated one) so this passes regardless of
# import order / whether OASIS_API_KEY is configured.
AUTH = {"X-API-Key": api_security._API_KEY}


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
    assert client.get("/status").status_code == 401   # fail-closed without key
    response = client.get("/status", headers=AUTH)
    assert response.status_code == 200
    data = response.json()
    assert "state" in data
    assert "progress" in data


def test_results_endpoint():
    assert client.get("/results").status_code == 401  # fail-closed without key
    response = client.get("/results", headers=AUTH)
    assert response.status_code == 200
    assert "results" in response.json()
