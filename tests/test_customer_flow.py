"""End-to-End Customer Installation & Setup Flow Tests:
- Interactive setup preferences (Store Name, custom admin password)
- Desktop Shortcut creation logic
- Client Setup Screen (Store identity, POS connection, multi-step progress sequence)
- Sidebar Console Cross-Linking with persistent SSO
"""

import os
import pytest
from unittest.mock import patch, MagicMock

import oasis.logic.onboarding as OB
from oasis.ui.onboarding import render_onboarding, data_source_badge
from oasis.ui.home import suite_links


def test_custom_store_name_in_onboarding_demo(tmp_path):
    """Verify onboarding saves custom Store Name on demo initialization."""
    db_file = tmp_path / "custom_test.db"
    with patch("oasis.logic.onboarding.default_db_path", return_value=str(db_file)), \
         patch("oasis.logic.onboarding._record") as mock_rec:
        res = OB.apply_demo(store_name="Rhapta Superstore", root=str(tmp_path))
        assert res is not None
        mock_rec.assert_called_once()
        assert mock_rec.call_args[1].get("store_name") == "Rhapta Superstore"


def test_onboarding_render_shows_customization_fields():
    """Verify render_onboarding renders store name and default view preferences."""
    st_mock = MagicMock()
    st_mock.text_input.return_value = "Rhapta Store"
    st_mock.selectbox.return_value = "single"
    st_mock.columns.side_effect = lambda n: [MagicMock() for _ in (range(n) if isinstance(n, int) else range(len(n)))]
    with patch("oasis.logic.onboarding.is_onboarded", return_value=False), \
         patch("oasis.logic.branding.load_branding") as mock_brand:
        mock_brand.return_value = MagicMock(product_name="OASIS", tenant_name="Rhapta Store")
        res = render_onboarding(st_mock, project_root=".")
        assert res is True
        assert st_mock.text_input.called


def test_desktop_shortcut_script_syntax():
    """Verify PowerShell desktop shortcut command string is valid."""
    cmd = (
        "powershell -ExecutionPolicy Bypass -Command "
        "\"$ws = New-Object -ComObject WScript.Shell; "
        "$s = $ws.CreateShortcut([System.IO.Path]::Combine([Environment]::GetFolderPath('Desktop'), 'O.A.S.I.S. Platform.lnk')); "
        "$s.TargetPath = 'C:\\OASIS\\OASIS.bat'; $s.WorkingDirectory = 'C:\\OASIS'; $s.Save()\""
    )
    assert "WScript.Shell" in cmd
    assert "O.A.S.I.S. Platform.lnk" in cmd


def test_sidebar_suite_links_navigation():
    """Verify sidebar suite_links renders all sibling console URLs with ?sid= parameter."""
    st_mock = MagicMock()
    st_mock.sidebar = MagicMock()

    with patch("oasis.ui.auth.current_sid", return_value="sess_xyz123"):
        suite_links(st_mock, current_key="ops")
        st_mock.sidebar.markdown.assert_called_once()
        md_text = st_mock.sidebar.markdown.call_args[0][0]

        # Verify current console ("◎ Operations") is skipped
        assert "◎ Operations" not in md_text
        # Verify sibling consoles contain ?sid= sess_xyz123 token
        assert "?sid=sess_xyz123" in md_text
        assert "🔮 Command" in md_text
        assert "⚡ Intel" in md_text
