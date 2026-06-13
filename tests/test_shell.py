"""Tests for the unified shell registry + role visibility (oasis/ui/shell.py).

Pure registry/role logic only — no Streamlit rendering."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.ui import shell


class TestRegistry:
    def test_registry_is_journey_ordered(self):
        keys = [p.key for p in shell.build_registry()]
        assert keys[0] == "home"
        # journey-ordered IA
        for expected in ("diagnose", "shadow", "ordering", "transfers",
                         "suppliers", "allocation", "analytics", "settings"):
            assert expected in keys

    def test_keys_unique(self):
        keys = [p.key for p in shell.build_registry()]
        assert len(keys) == len(set(keys))


class TestRoleVisibility:
    def _reg(self):
        return shell.build_registry()

    def test_admin_sees_everything(self):
        vis = shell.visible_pages(self._reg(), "ops_admin")
        assert {p.key for p in vis} == {p.key for p in self._reg()}

    def test_branch_manager_restricted(self):
        vis = {p.key for p in shell.visible_pages(self._reg(), "branch_manager")}
        # _ALL pages only
        assert "home" in vis and "ordering" in vis and "allocation" in vis
        # management/admin-only pages hidden
        assert "settings" not in vis      # admin only
        assert "diagnose" not in vis      # admin only
        assert "shadow" not in vis        # mgmt only
        assert "transfers" not in vis     # mgmt only

    def test_regional_manager_sees_mgmt_not_admin(self):
        vis = {p.key for p in shell.visible_pages(self._reg(), "regional_manager")}
        assert "shadow" in vis and "transfers" in vis and "analytics" in vis
        assert "settings" not in vis   # admin only
        assert "diagnose" not in vis   # admin only

    def test_unknown_role_sees_only_open_pages(self):
        vis = {p.key for p in shell.visible_pages(self._reg(), "stranger")}
        assert vis == {"home", "ordering", "allocation"}  # the _ALL pages

    def test_page_visible_to_helper(self):
        p_admin = shell.Page("x", "X", "•", lambda c: None, ("ops_admin",))
        assert p_admin.visible_to("ops_admin")
        assert not p_admin.visible_to("branch_manager")
        p_all = shell.Page("y", "Y", "•", lambda c: None, ())
        assert p_all.visible_to("anyone")
