"""The OASIS service watchdog (audit H1/H2/H3) — pure policy + wired loop."""

import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.supervisor import (
    Supervisor, alert_payload, backup_due, should_restart,
)


# ── pure policy ──────────────────────────────────────────────────────────
def test_dead_process_restarts_immediately():
    assert should_restart(alive=False, port_ok=True, strikes=0, grace_left=0) == (True, 0)


def test_grace_period_suppresses_port_probes():
    assert should_restart(alive=True, port_ok=False, strikes=0, grace_left=2) == (False, 0)


def test_hung_needs_two_consecutive_strikes():
    r1, s1 = should_restart(alive=True, port_ok=False, strikes=0, grace_left=0)
    assert (r1, s1) == (False, 1)
    r2, s2 = should_restart(alive=True, port_ok=False, strikes=s1, grace_left=0)
    assert (r2, s2) == (True, 0)


def test_recovered_port_clears_strikes():
    assert should_restart(alive=True, port_ok=True, strikes=1, grace_left=0) == (False, 0)


def test_backup_due_once_per_day_after_hour():
    now = datetime(2026, 7, 20, 21, 5)
    assert backup_due(now, None, 21)
    assert backup_due(now, date(2026, 7, 19), 21)
    assert not backup_due(now, date(2026, 7, 20), 21)          # already ran today
    assert not backup_due(datetime(2026, 7, 20, 9, 0), None, 21)  # too early


def test_alert_payload_shape():
    p = alert_payload("restart", "ops", "process died")
    assert p["event"] == "restart" and p["service"] == "ops" and p["at"]


# ── wired loop with fakes ────────────────────────────────────────────────
class FakeProc:
    def __init__(self):
        self.dead = False
        self.terminated = False

    def poll(self):
        return 1 if self.dead else None

    def terminate(self):
        self.terminated = True
        self.dead = True


def _sup(port_ok=True):
    started, alerts = [], []
    procs = {}

    def starter(key):
        started.append(key)
        procs[key] = FakeProc()
        return procs[key]

    sup = Supervisor(services=("ops", "intel"), starter=starter,
                     prober=lambda k: port_ok, alerter=alerts.append,
                     backup_fn=lambda: "backups/x.db")
    return sup, started, alerts, procs


def test_ensure_started_launches_everything_once():
    sup, started, _, _ = _sup()
    sup.ensure_started()
    sup.ensure_started()
    assert started == ["ops", "intel"]


def test_dead_service_is_restarted_and_alerted():
    sup, started, alerts, procs = _sup()
    sup.ensure_started()
    procs["ops"].dead = True
    events = sup.tick(now=datetime(2026, 7, 20, 10, 0))
    assert ("ops", "process died") in events
    assert started.count("ops") == 2 and started.count("intel") == 1
    assert any(a["service"] == "ops" for a in alerts)


def test_hung_port_restarts_after_grace_plus_two_strikes():
    sup, started, alerts, procs = _sup(port_ok=False)
    sup.ensure_started()
    now = datetime(2026, 7, 20, 10, 0)
    for _ in range(3):                    # grace ticks — no restarts
        assert sup.tick(now=now) == []
    assert sup.tick(now=now) == []        # strike 1
    events = sup.tick(now=now)            # strike 2 → restart both
    assert ("ops", "port unresponsive") in events
    assert started.count("ops") == 2


def test_daily_backup_runs_once():
    sup, _, _, _ = _sup()
    sup.ensure_started()
    late = datetime(2026, 7, 20, 21, 30)
    e1 = sup.tick(now=late)
    assert ("backup", "backups/x.db") in e1
    assert all(k != "backup" for k, _ in sup.tick(now=late))   # not twice
