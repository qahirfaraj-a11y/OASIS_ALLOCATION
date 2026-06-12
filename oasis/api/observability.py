"""
OASIS API Observability
=======================
Shared /health and /metrics endpoints for all OASIS FastAPI apps.

- ``/health``  — unauthenticated liveness + DB readiness probe, suitable for
  Docker HEALTHCHECK and load-balancer checks. Returns 200 when the process
  is up and the database answers, 503 when the DB is unreachable.
- ``/metrics`` — Prometheus text-format metrics (no prometheus_client
  dependency): uptime, request counts/latency per endpoint, DB status.

Usage in an app module::

    from .observability import attach_observability
    attach_observability(app, service_name="oasis-mobile-api")
"""

import json
import logging
import threading
import time
from collections import defaultdict

from fastapi import FastAPI, Response

logger = logging.getLogger("OASIS-API-Observability")

_START_TIME = time.time()


class _RequestStats:
    """Thread-safe per-endpoint request counters and latency totals."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.counts: dict = defaultdict(int)
        self.latency_sum: dict = defaultdict(float)

    def record(self, method: str, path: str, status: int, seconds: float) -> None:
        key = (method, path, status)
        with self._lock:
            self.counts[key] += 1
            self.latency_sum[(method, path)] += seconds

    def snapshot(self):
        with self._lock:
            return dict(self.counts), dict(self.latency_sum)


def _check_db() -> bool:
    try:
        from ..logic import db as oasis_db
        conn = oasis_db.get_raw_connection()
        conn.execute("SELECT 1")
        conn.close()
        return True
    except Exception as e:
        logger.error("Health check DB probe failed: %s", e)
        return False


def attach_observability(app: FastAPI, service_name: str) -> None:
    """Add /health, /metrics, and a request-stats middleware to *app*."""
    stats = _RequestStats()

    @app.middleware("http")
    async def _track_requests(request, call_next):
        start = time.perf_counter()
        response = await call_next(request)
        path = request.url.path
        # Don't let the probes themselves dominate the metrics.
        if path not in ("/health", "/metrics"):
            stats.record(
                request.method, path, response.status_code,
                time.perf_counter() - start,
            )
        return response

    @app.get("/health", tags=["observability"])
    async def health():
        db_ok = _check_db()
        body = {
            "service": service_name,
            "status": "ok" if db_ok else "degraded",
            "database": "up" if db_ok else "down",
            "uptime_seconds": round(time.time() - _START_TIME, 1),
        }
        return Response(
            content=json.dumps(body),
            media_type="application/json",
            status_code=200 if db_ok else 503,
        )

    @app.get("/metrics", tags=["observability"])
    async def metrics():
        counts, latencies = stats.snapshot()
        db_ok = _check_db()
        lines = [
            "# HELP oasis_uptime_seconds Process uptime in seconds.",
            "# TYPE oasis_uptime_seconds gauge",
            f'oasis_uptime_seconds{{service="{service_name}"}} '
            f"{time.time() - _START_TIME:.1f}",
            "# HELP oasis_db_up Database reachability (1 = up, 0 = down).",
            "# TYPE oasis_db_up gauge",
            f'oasis_db_up{{service="{service_name}"}} {1 if db_ok else 0}',
            "# HELP oasis_requests_total HTTP requests by method/path/status.",
            "# TYPE oasis_requests_total counter",
        ]
        for (method, path, status), count in sorted(counts.items()):
            lines.append(
                f'oasis_requests_total{{service="{service_name}",'
                f'method="{method}",path="{path}",status="{status}"}} {count}'
            )
        lines += [
            "# HELP oasis_request_latency_seconds_sum Cumulative request "
            "latency by method/path.",
            "# TYPE oasis_request_latency_seconds_sum counter",
        ]
        for (method, path), total in sorted(latencies.items()):
            lines.append(
                f'oasis_request_latency_seconds_sum{{service="{service_name}",'
                f'method="{method}",path="{path}"}} {total:.6f}'
            )
        return Response("\n".join(lines) + "\n", media_type="text/plain; version=0.0.4")
