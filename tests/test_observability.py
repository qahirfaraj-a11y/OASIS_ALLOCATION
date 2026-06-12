"""Tests for the shared /health and /metrics endpoints."""

import os
import sys
import tempfile

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

pytest.importorskip("fastapi")
from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from oasis.api.observability import attach_observability  # noqa: E402


@pytest.fixture
def client(monkeypatch):
    # Point the health probe at a throwaway SQLite DB so it reports "up".
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    monkeypatch.setenv("OASIS_DB_URL", f"sqlite:///{path}")
    monkeypatch.delenv("OASIS_DB_PATH", raising=False)

    app = FastAPI()
    attach_observability(app, service_name="test-service")

    @app.get("/echo")
    async def echo():
        return {"ok": True}

    with TestClient(app) as c:
        yield c
    os.unlink(path)


def test_health_returns_ok(client):
    resp = client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["database"] == "up"
    assert body["service"] == "test-service"
    assert body["uptime_seconds"] >= 0


def test_health_unauthenticated(client):
    # No X-API-Key or bearer token — probe must still work.
    resp = client.get("/health", headers={})
    assert resp.status_code == 200


def test_health_degraded_when_db_unreachable(client, monkeypatch):
    monkeypatch.setenv(
        "OASIS_DB_URL", "sqlite:///Z:/nonexistent/dir/no.db"
    )
    resp = client.get("/health")
    assert resp.status_code == 503
    assert resp.json()["database"] == "down"


def test_metrics_prometheus_format(client):
    client.get("/echo")
    client.get("/echo")
    resp = client.get("/metrics")
    assert resp.status_code == 200
    assert resp.headers["content-type"].startswith("text/plain")
    text = resp.text
    assert 'oasis_uptime_seconds{service="test-service"}' in text
    assert 'oasis_db_up{service="test-service"} 1' in text
    assert (
        'oasis_requests_total{service="test-service",method="GET",'
        'path="/echo",status="200"} 2' in text
    )
    assert "oasis_request_latency_seconds_sum" in text


def test_metrics_excludes_probe_endpoints(client):
    client.get("/health")
    resp = client.get("/metrics")
    assert 'path="/health"' not in resp.text
    assert 'path="/metrics"' not in resp.text
