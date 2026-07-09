"""
O.A.S.I.S. Heartbeat / Telemetry Service
==========================================
Sends lightweight operational status to a central webhook.
Contains ZERO sensitive data — only pipeline health metrics.

Supports: Slack, Discord, Teams, and generic HTTP POST.
"""

import os
import json
import logging
import sqlite3
import threading
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger("OASIS.Heartbeat")


class HeartbeatService:
    """
    Periodic telemetry sender for OASIS client deployments.

    Usage::
        from oasis.logic.heartbeat import HeartbeatService
        hb = HeartbeatService(config)
        hb.start()   # Runs in background thread
        hb.stop()
    """

    def __init__(self, config: Dict[str, Any]):
        hb_cfg = config.get("heartbeat", {})
        self.enabled = hb_cfg.get("enabled", False)
        self.endpoint_url = hb_cfg.get("endpoint_url")
        self.webhook_url = hb_cfg.get("webhook_url")
        self.webhook_type = hb_cfg.get("webhook_type", "slack")
        self.interval_hours = hb_cfg.get("interval_hours", 6)
        self.client_id = config.get("client", {}).get("client_id", "UNKNOWN")
        self.client_name = config.get("client", {}).get("client_name", "Unknown")
        self.db_path = config.get("paths", {}).get("db_path", "oasis.db")
        self.data_dir = config.get("paths", {}).get("data_dir", "oasis/data")

        self._timer: Optional[threading.Timer] = None
        self._running = False

    def start(self):
        """Start the heartbeat loop in a background thread."""
        if not self.enabled:
            logger.info("Heartbeat disabled in config. Skipping.")
            return
        if self._running:
            return
        self._running = True
        self._schedule_next()
        logger.info(f"Heartbeat started: every {self.interval_hours}h → {self.webhook_type}")

    def stop(self):
        """Stop the heartbeat loop."""
        self._running = False
        if self._timer:
            self._timer.cancel()
            self._timer = None
        logger.info("Heartbeat stopped.")

    def _schedule_next(self):
        if not self._running:
            return
        interval_seconds = self.interval_hours * 3600
        self._timer = threading.Timer(interval_seconds, self._tick)
        self._timer.daemon = True
        self._timer.start()

    def _tick(self):
        """Execute one heartbeat cycle."""
        try:
            payload = self.collect_metrics()
            self.send(payload)
        except Exception as e:
            logger.error(f"Heartbeat tick failed: {e}")
        finally:
            self._schedule_next()

    def send_now(self) -> Dict[str, Any]:
        """Manually trigger a heartbeat (for testing or on-demand)."""
        payload = self.collect_metrics()
        self.send(payload)
        return payload

    # ── Metric Collection ─────────────────────────────────────────────

    def collect_metrics(self) -> Dict[str, Any]:
        """Gather operational metrics. NO sensitive data."""
        metrics = {
            "client_id": self.client_id,
            "client_name": self.client_name,
            "timestamp": datetime.now().isoformat(),
            "oasis_version": "2.1.0",
            "system": {},
            "pipeline": {},
            "engines": {},
        }

        # System metrics
        try:
            db_size = os.path.getsize(self.db_path) if os.path.exists(self.db_path) else 0
            metrics["system"]["db_size_mb"] = round(db_size / (1024 * 1024), 2)
        except Exception:
            metrics["system"]["db_size_mb"] = -1

        # Pipeline status (last run)
        try:
            log_dir = os.path.join(self.data_dir, "pipeline_logs")
            if os.path.isdir(log_dir):
                logs = sorted([f for f in os.listdir(log_dir) if f.startswith("pipeline_run_")])
                if logs:
                    with open(os.path.join(log_dir, logs[-1]), 'r') as f:
                        last_run = json.load(f)
                    metrics["pipeline"]["last_run_id"] = last_run.get("run_id")
                    metrics["pipeline"]["last_status"] = last_run.get("status")
                    metrics["pipeline"]["last_time"] = last_run.get("start_time")
                    steps = last_run.get("steps", [])
                    metrics["pipeline"]["steps_ok"] = sum(1 for s in steps if s.get("status") == "OK")
                    metrics["pipeline"]["steps_failed"] = sum(1 for s in steps if s.get("status") == "FAILED")
        except Exception as e:
            metrics["pipeline"]["error"] = str(e)

        # Engine cache freshness
        for cache_name in ["amit_enforcement.json", "dharam_demand_patch.json", "supplier_patterns_2025.json"]:
            cache_path = os.path.join(self.data_dir, cache_name)
            if os.path.exists(cache_path):
                age_hours = (datetime.now().timestamp() - os.path.getmtime(cache_path)) / 3600
                metrics["engines"][cache_name] = {"age_hours": round(age_hours, 1)}

        # Stockout count (lightweight query)
        try:
            if os.path.exists(self.db_path):
                conn = sqlite3.connect(self.db_path)
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM OASIS_SYSTEM_CONFIG WHERE CONFIG_KEY = 'last_stockout_count'"
                )
                row = cursor.fetchone()
                conn.close()
                if row:
                    metrics["system"]["last_stockout_count"] = row[0]
        except Exception:
            pass

        return metrics

    # ── Webhook Dispatch ──────────────────────────────────────────────

    def send(self, payload: Dict[str, Any]):
        """Send the heartbeat payload via the configured webhook."""
        import urllib.request
        import urllib.error

        target_url = self.webhook_url or self.endpoint_url
        if not target_url:
            logger.warning("No webhook URL configured. Heartbeat not sent.")
            return

        if self.webhook_type == "slack":
            body = self._format_slack(payload)
        elif self.webhook_type == "discord":
            body = self._format_discord(payload)
        elif self.webhook_type == "teams":
            body = self._format_teams(payload)
        else:
            body = json.dumps(payload)

        data = body.encode("utf-8")
        req = urllib.request.Request(
            target_url, data=data,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(req, timeout=15) as resp:
                logger.info(f"Heartbeat sent: {resp.status}")
        except urllib.error.URLError as e:
            logger.error(f"Heartbeat send failed: {e}")

    def _format_slack(self, p: Dict) -> str:
        pipeline = p.get("pipeline", {})
        status_emoji = "✅" if pipeline.get("last_status") == "COMPLETED" else "🔴"
        text = (f"{status_emoji} *OASIS Heartbeat — {p['client_name']}*\n"
                f"• Pipeline: {pipeline.get('last_status', 'N/A')} "
                f"(OK: {pipeline.get('steps_ok', '?')}, Failed: {pipeline.get('steps_failed', '?')})\n"
                f"• DB Size: {p.get('system', {}).get('db_size_mb', '?')} MB\n"
                f"• Time: {p['timestamp']}")
        return json.dumps({"text": text})

    def _format_discord(self, p: Dict) -> str:
        pipeline = p.get("pipeline", {})
        return json.dumps({"content": (
            f"**OASIS Heartbeat — {p['client_name']}**\n"
            f"Pipeline: {pipeline.get('last_status', 'N/A')} | "
            f"DB: {p.get('system', {}).get('db_size_mb', '?')} MB | {p['timestamp']}"
        )})

    def _format_teams(self, p: Dict) -> str:
        pipeline = p.get("pipeline", {})
        return json.dumps({"text": (
            f"OASIS Heartbeat — {p['client_name']}: "
            f"Pipeline {pipeline.get('last_status', 'N/A')}, "
            f"DB {p.get('system', {}).get('db_size_mb', '?')} MB"
        )})
