"""Tests for UI usage telemetry (oasis/ui/telemetry.py) against a temp DB."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.ui import telemetry


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = str(tmp_path / "audit.db")
    # Route the central DB factory at this temp DB so page_view_counts reads it.
    monkeypatch.setenv("OASIS_DB_URL", f"sqlite:///{path}")
    monkeypatch.delenv("OASIS_DB_PATH", raising=False)
    from oasis.logic.db_connector import ensure_oasis_tables
    ensure_oasis_tables(path)
    return path


def test_log_page_view_and_count(db):
    telemetry.log_page_view(db, "ops_admin", "ordering")
    telemetry.log_page_view(db, "ops_admin", "ordering")
    telemetry.log_page_view(db, "ops_admin", "allocation")
    counts = telemetry.page_view_counts(db, days=7)
    assert counts.get("ordering") == 2
    assert counts.get("allocation") == 1


def test_counts_empty_when_no_events(db):
    assert telemetry.page_view_counts(db, days=7) == {}


def test_log_ui_action_does_not_raise(db):
    # Best-effort: should never raise even with odd input.
    telemetry.log_ui_action(db, "u", "RUN_ALLOCATION", details={"budget": 1000})


def test_telemetry_never_raises_on_bad_db():
    # Logging to a clearly-bad path must be swallowed (telemetry is best-effort).
    telemetry.log_page_view("Z:/nonexistent/x.db", "u", "home")  # no exception
    # And counting against a bad DB returns {} rather than raising.
    assert telemetry.page_view_counts("Z:/nonexistent/x.db") == {}


def test_only_page_views_counted(db):
    telemetry.log_page_view(db, "u", "home")
    telemetry.log_ui_action(db, "u", "QUEUE_TRANSFERS")  # not a PAGE_VIEW
    counts = telemetry.page_view_counts(db, days=7)
    assert counts == {"home": 1}
