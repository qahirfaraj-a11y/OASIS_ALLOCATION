"""Tests that high-value UI actions are now audit-logged (GAP-3)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import db as oasis_db
from oasis.logic.db_connector import ensure_oasis_tables
from oasis.ui.telemetry import log_ui_action


def test_log_ui_action_records_audit_row(tmp_path):
    p = str(tmp_path / "store.db")
    ensure_oasis_tables(p)
    log_ui_action(p, "alice", "push_purchase_order", {"org": "ORG001", "lines": 3})
    conn = oasis_db.get_raw_connection(db_url=f"sqlite:///{p}")
    try:
        rows = conn.execute(
            "SELECT USERNAME, ACTION, ENTITY_TYPE FROM OASIS_AUDIT_LOG "
            "WHERE ACTION = 'push_purchase_order'").fetchall()
    finally:
        conn.close()
    assert len(rows) == 1
    assert rows[0][0] == "alice" and rows[0][2] == "UI"


def test_shell_log_action_is_guarded():
    # _log_action must never raise, even with an unusable db path
    from oasis.ui import shell
    shell._log_action({"db_path": "/no/such/dir/x.db", "username": "x"},
                      "queue_transfers", {"count": 5})  # no exception = pass
