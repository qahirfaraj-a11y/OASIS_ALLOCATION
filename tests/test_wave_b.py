"""Tests for Wave B changes:
- S5: Unified auth in Command Center (ops_dashboard) and Home app
- S6: Suite links console key specificity
- S7: Dynamic DB path resolution in Home and entrypoint
"""

import os
import pytest
from unittest.mock import patch, MagicMock

from oasis.ui.home import suite_links
import entrypoint


def test_s6_suite_links_skips_current_console():
    """Verify suite_links skips the console matching current_key (S6)."""
    st_mock = MagicMock()
    st_mock.sidebar = MagicMock()

    # When inside ops console with current_key="ops"
    suite_links(st_mock, current_key="ops")

    # Verify st.sidebar.markdown was called
    st_mock.sidebar.markdown.assert_called_once()
    md_text = st_mock.sidebar.markdown.call_args[0][0]
    # Operations Console ("◎ Operations") should be skipped
    assert "◎ Operations" not in md_text
    # Other consoles (Command Center, Intelligence, etc.) should be present
    assert "🔮 Command" in md_text


def test_s7_home_system_snapshot_uses_resolved_db_path(tmp_path):
    """Verify Home's system_snapshot handles resolved_db_path correctly (S7)."""
    from oasis.ui.home import system_snapshot

    dummy_db = tmp_path / "custom_oasis.db"
    dummy_db.touch()

    snapshot = system_snapshot(str(dummy_db))
    assert snapshot["db"] == "custom_oasis.db"
    assert snapshot["db_exists"] is True


def test_s7_entrypoint_active_db_uses_resolved_db_path(tmp_path):
    """Verify entrypoint._active_db uses resolved_db_path (S7)."""
    dummy_db = tmp_path / "custom_entrypoint.db"
    dummy_db.touch()

    with patch("oasis.logic.onboarding.resolved_db_path", return_value=str(dummy_db)):
        active_path = entrypoint._active_db(str(tmp_path))
        assert active_path == str(dummy_db)


def test_s5_home_app_auth_gate_permits_unonboarded():
    """Verify home_app authentication gate allows setup when not onboarded (S5)."""
    with patch("oasis.logic.onboarding.is_onboarded", return_value=False), \
         patch("oasis.ui.auth.require_login") as mock_login:
        import home_app
        home_app._gate()
        mock_login.assert_not_called()
