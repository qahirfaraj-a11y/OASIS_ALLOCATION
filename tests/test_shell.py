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
        assert vis == {"home", "allocation"}  # the _ALL pages

    # ── Journey role model ────────────────────────────────────────────
    def _vis(self, role):
        return {p.key for p in shell.visible_pages(self._reg(), role)}

    def test_ilink_operator_sees_everything(self):
        assert self._vis("ilink_operator") == {p.key for p in self._reg()}

    def test_executive_oversight_only(self):
        v = self._vis("executive")
        assert {"home", "shadow", "analytics", "allocation"} <= v
        assert "settings" not in v and "diagnose" not in v   # operator-only
        assert "ordering" not in v and "transfers" not in v  # operational

    def test_finance_capital_and_analytics_only(self):
        v = self._vis("finance")
        assert v == {"home", "allocation", "analytics"}

    def test_approval_manager_operational(self):
        v = self._vis("approval_manager")
        assert {"home", "ordering", "transfers", "suppliers", "allocation"} <= v
        assert "settings" not in v and "diagnose" not in v
        assert "shadow" not in v  # oversight, not operational

    def test_journey_roles_exist_in_auth(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        for r in ("ilink_operator", "executive", "finance", "approval_manager"):
            assert r in ROLE_PERMISSIONS

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
        assert transfers.roles == shell._OPERATIONS

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


class TestHealthMetrics:
    def _stock(self):
        return {
            "ORG001": [
                {"avg_daily_sales": 0.0, "current_stocks": 50},   # dead (ADS<0.2, SOH>15)
                {"avg_daily_sales": 5.0, "current_stocks": 0},    # stockout (ADS>0, SOH<1)
                {"avg_daily_sales": 3.0, "current_stocks": 40},   # healthy
            ],
            "ORG002": [
                {"avg_daily_sales": 0.1, "current_stocks": 30},   # dead
            ],
        }

    def test_counts_and_pcts(self):
        m = shell.compute_health_metrics(self._stock())
        assert m["total_skus"] == 4
        assert m["dead_stock"] == 2
        assert m["stockouts"] == 1
        assert m["dead_stock_pct"] == 50.0
        assert m["stockout_pct"] == 25.0

    def test_empty_is_safe(self):
        m = shell.compute_health_metrics({})
        assert m["total_skus"] == 0
        assert m["dead_stock_pct"] == 0.0

    def test_all_pages_native(self):
        # Milestone: every journey page is a real renderer, none are bridges.
        reg = {p.key: p.render for p in shell.build_registry()}
        assert reg["analytics"] is shell.render_analytics
        assert reg["settings"] is shell.render_settings
        assert reg["diagnose"] is shell.render_diagnose
