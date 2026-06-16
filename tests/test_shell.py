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


class TestGroupBySupplier:
    def test_groups_and_drops_zero_qty(self):
        recs = [
            {"product_name": "A", "recommended_quantity": 5, "supplier_name": "ACME"},
            {"product_name": "B", "recommended_quantity": 0, "supplier_name": "ACME"},
            {"product_name": "C", "recommended_quantity": 3, "supplier_name": "BETA"},
        ]
        grouped = shell.group_recs_by_supplier(recs)
        assert set(grouped.keys()) == {"ACME", "BETA"}
        assert len(grouped["ACME"]) == 1  # zero-qty B dropped
        assert len(grouped["BETA"]) == 1

    def test_missing_supplier_is_unknown(self):
        grouped = shell.group_recs_by_supplier(
            [{"product_name": "X", "recommended_quantity": 2}])
        assert "UNKNOWN" in grouped

    def test_ordering_page_in_registry_is_native(self):
        # The ordering page must now be the native renderer, not a bridge.
        ordering = next(p for p in shell.build_registry() if p.key == "ordering")
        assert ordering.render is shell.render_ordering

    def test_transfers_page_in_registry_is_native(self):
        transfers = next(p for p in shell.build_registry() if p.key == "transfers")
        assert transfers.render is shell.render_transfers
        assert transfers.roles == shell._MGMT  # mgmt-only

    def test_suppliers_and_shadow_native(self):
        reg = {p.key: p for p in shell.build_registry()}
        assert reg["suppliers"].render is shell.render_suppliers
        assert reg["shadow"].render is shell.render_shadow


class TestClassifySupplier:
    def test_reliable_at_or_below_one(self):
        assert shell.classify_supplier(0.8) == "RELIABLE"
        assert shell.classify_supplier(1.0) == "RELIABLE"

    def test_watch_band(self):
        assert shell.classify_supplier(1.2) == "WATCH"
        assert shell.classify_supplier(1.49) == "WATCH"

    def test_hostile_at_or_above_one_point_five(self):
        assert shell.classify_supplier(1.5) == "HOSTILE"
        assert shell.classify_supplier(2.0) == "HOSTILE"

    def test_none_defaults_reliable(self):
        assert shell.classify_supplier(None) == "RELIABLE"
