"""
The OASIS service — keep the suite alive without a human (audit H1/H2/H3).

``python entrypoint.py --mode serve`` runs this supervisor in the foreground;
``register_service.bat`` wires it to Windows Task Scheduler at logon so a
reboot brings OASIS back on its own. It:

  * starts the configured consoles (Home, Operations, Command, Market
    Intelligence, Intelligence — plus the Cloud Hub when ``OASIS_SERVE_HUB=1``),
  * watches process liveness AND port responsiveness, restarting anything that
    dies (two consecutive failed port probes = hung → restart),
  * sends a fire-and-forget alert to ``OASIS_ALERT_WEBHOOK`` (if set) on every
    down/restart event — silence is the failure mode we're eliminating,
  * runs the daily DB backup at ``OASIS_BACKUP_HOUR`` (default 21:00, honoring
    ``OASIS_BACKUP_KEEP``) so backups are a habit, not a mode.

Policy functions are pure and unit-tested; the loop only wires them to
subprocesses, sockets, and the clock.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from datetime import date, datetime
from typing import Callable, Dict, Optional

from .console_launcher import CONSOLE_DEFS, port_live, start_console

logger = logging.getLogger("OASIS.Supervisor")

DEFAULT_SERVICES = ("home", "ops", "command", "stgat", "intel")
CHECK_INTERVAL_S = 20
PORT_STRIKES_FOR_RESTART = 2          # consecutive failed probes = hung
STARTUP_GRACE_TICKS = 3               # ticks before a fresh start is probed

#: home isn't a licensed console, so it lives outside CONSOLE_DEFS
_EXTRA_PORTS = {"home": 8490}


def service_port(key: str) -> int:
    if key in _EXTRA_PORTS:
        return _EXTRA_PORTS[key]
    return CONSOLE_DEFS[key].port


def configured_services() -> tuple:
    keys = list(DEFAULT_SERVICES)
    if os.getenv("OASIS_SERVE_HUB", "") in ("1", "true", "True"):
        keys.append("hub")
    return tuple(keys)


# ── pure policy (unit-tested) ────────────────────────────────────────────
def should_restart(alive: bool, port_ok: bool, strikes: int,
                   grace_left: int) -> tuple:
    """(restart?, new_strikes). Dead process → restart now. A living process
    gets STARTUP_GRACE_TICKS before port probes count; then two consecutive
    failed probes mean it's hung."""
    if not alive:
        return True, 0
    if grace_left > 0:
        return False, 0
    if port_ok:
        return False, 0
    strikes += 1
    if strikes >= PORT_STRIKES_FOR_RESTART:
        return True, 0
    return False, strikes


def backup_due(now: datetime, last_run: Optional[date], hour: int) -> bool:
    """Once per calendar day, at/after the configured hour."""
    return now.hour >= hour and last_run != now.date()


def alert_payload(event: str, service: str, detail: str = "") -> dict:
    return {"source": "oasis-supervisor", "event": event, "service": service,
            "detail": detail, "at": datetime.now().isoformat(timespec="seconds")}


# ── side-effects (thin, injectable) ──────────────────────────────────────
def send_alert(payload: dict, url: Optional[str] = None) -> bool:
    url = url or os.getenv("OASIS_ALERT_WEBHOOK", "")
    if not url:
        return False
    try:
        req = urllib.request.Request(
            url, data=json.dumps(payload).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        urllib.request.urlopen(req, timeout=10).read()
        return True
    except Exception as e:                      # alerting must never crash us
        logger.warning("alert webhook failed: %s", e)
        return False


def _start_home():
    """Home runs via its own entrypoint mode (not in CONSOLE_DEFS)."""
    import subprocess
    import sys
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    py = os.path.join(root, ".oasis_venv", "Scripts", "python.exe")
    if not os.path.exists(py):
        py = sys.executable
    proc = subprocess.Popen([py, os.path.join(root, "entrypoint.py"),
                             "--mode", "home"], cwd=root)
    return proc


class Supervisor:
    """Wires the pure policy to real processes. Fakes injectable for tests."""

    def __init__(self, services=None, *,
                 starter: Optional[Callable] = None,
                 prober: Optional[Callable] = None,
                 alerter: Optional[Callable] = None,
                 backup_fn: Optional[Callable] = None):
        self.keys = tuple(services or configured_services())
        self._start = starter or self._default_start
        self._probe = prober or (lambda key: port_live(service_port(key)))
        self._alert = alerter or send_alert
        self._backup = backup_fn or self._default_backup
        self.procs: Dict[str, object] = {}
        self.strikes: Dict[str, int] = {}
        self.grace: Dict[str, int] = {}
        self.last_backup: Optional[date] = None
        self.backup_hour = int(os.getenv("OASIS_BACKUP_HOUR", "21"))

    # thin default effects -------------------------------------------------
    @staticmethod
    def _default_start(key: str):
        if key == "home":
            return _start_home()
        pid_or_proc = start_console(key, return_proc=True) \
            if "return_proc" in start_console.__code__.co_varnames else None
        if pid_or_proc is not None:
            return pid_or_proc
        # console_launcher.start_console returns a pid; wrap it
        raise RuntimeError("start_console returned pid; unsupported")

    def _default_backup(self):
        from .backup_util import backup_db
        from .onboarding import resolved_db_path
        keep = int(os.getenv("OASIS_BACKUP_KEEP", "10"))
        return backup_db(resolved_db_path(), keep=keep)

    # loop -----------------------------------------------------------------
    def ensure_started(self):
        for key in self.keys:
            if key not in self.procs or self.procs[key] is None:
                self.procs[key] = self._start(key)
                self.strikes[key] = 0
                self.grace[key] = STARTUP_GRACE_TICKS
                logger.info("started %s", key)

    def tick(self, now: Optional[datetime] = None) -> list:
        """One watchdog pass. Returns the events it acted on (for tests)."""
        now = now or datetime.now()
        events = []
        for key in self.keys:
            proc = self.procs.get(key)
            alive = proc is not None and (getattr(proc, "poll", lambda: 0)() is None)
            grace_left = self.grace.get(key, 0)
            port_ok = True if grace_left > 0 else self._probe(key)
            restart, self.strikes[key] = should_restart(
                alive, port_ok, self.strikes.get(key, 0), grace_left)
            if grace_left > 0:
                self.grace[key] = grace_left - 1
            if restart:
                reason = "process died" if not alive else "port unresponsive"
                events.append((key, reason))
                self._alert(alert_payload("restart", key, reason))
                try:
                    if proc is not None and getattr(proc, "poll", lambda: 0)() is None:
                        proc.terminate()
                except Exception:
                    pass
                self.procs[key] = self._start(key)
                self.grace[key] = STARTUP_GRACE_TICKS
                logger.warning("restarted %s (%s)", key, reason)
        if backup_due(now, self.last_backup, self.backup_hour):
            try:
                path = self._backup()
                self.last_backup = now.date()
                events.append(("backup", str(path)))
                logger.info("daily backup: %s", path)
            except Exception as e:
                self._alert(alert_payload("backup_failed", "backup", str(e)))
                logger.error("daily backup failed: %s", e)
                self.last_backup = now.date()      # retry tomorrow, not every tick
        return events

    def run_forever(self, interval: int = CHECK_INTERVAL_S):
        logger.info("OASIS supervisor: %s (hub=%s)", ", ".join(self.keys),
                    "on" if "hub" in self.keys else "off")
        self.ensure_started()
        self._alert(alert_payload("started", "supervisor",
                                  ",".join(self.keys)))
        while True:
            time.sleep(interval)
            self.tick()
