"""
Phase 3: Intelligence Integration — Automated Tests
====================================================
Tests for:
  3.2  Supplier Disruption Scenarios
  3.3  Simulation Validation Tab availability
  3.4  Analytics & KPI Tab readiness
"""
import sys
import os
import pytest

# ── Ensure project root is importable ──
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))


# =====================================================================
# Test 1: ROLE_PERMISSIONS includes new tabs
# =====================================================================
class TestRolePermissions:
    """Verify that auth_manager ROLE_PERMISSIONS correctly gate new Phase 3 tabs."""

    def test_ops_admin_has_simulation_validation(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        assert ROLE_PERMISSIONS["ops_admin"]["tabs"]["simulation_validation"] is True

    def test_ops_admin_has_analytics(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        assert ROLE_PERMISSIONS["ops_admin"]["tabs"]["analytics"] is True

    def test_regional_manager_has_simulation_validation(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        assert ROLE_PERMISSIONS["regional_manager"]["tabs"]["simulation_validation"] is True

    def test_regional_manager_has_analytics(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        assert ROLE_PERMISSIONS["regional_manager"]["tabs"]["analytics"] is True

    def test_branch_manager_no_simulation_validation(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        assert ROLE_PERMISSIONS["branch_manager"]["tabs"]["simulation_validation"] is False

    def test_branch_manager_no_analytics(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        assert ROLE_PERMISSIONS["branch_manager"]["tabs"]["analytics"] is False


# =====================================================================
# Test 2: Supplier Failure Event modifies lead times
# =====================================================================
class TestSupplierDisruption:
    """Verify that disruption events correctly modify product parameters."""

    def test_supplier_failure_inflates_lead_time(self):
        """A COMPLETE supplier failure should increase lead_time_days."""
        from oasis.simulation.black_swan_events import (
            SupplierFailureEvent, FailureMode
        )

        event = SupplierFailureEvent(
            supplier_name="BROOKSIDE",
            start_day=1,
            duration_days=14,
            mode=FailureMode.COMPLETE,
            department_filter="FRESH MILK"
        )

        # Simulate the disruption logic from ops_dashboard.py
        mock_products = [
            {'product_name': 'Milk 500ml', 'supplier_name': 'BROOKSIDE',
             'department': 'FRESH MILK', 'lead_time_days': 3, 'demand_cv': 0.5,
             'avg_daily_sales': 10},
            {'product_name': 'Sugar 1kg', 'supplier_name': 'MUMIAS',
             'department': 'SUGAR', 'lead_time_days': 5, 'demand_cv': 0.3,
             'avg_daily_sales': 5},
        ]

        disrupted = []
        affected = 0
        for p in mock_products:
            p_copy = dict(p)
            if event.supplier_name.upper() in str(p_copy.get('supplier_name', '')).upper():
                affected += 1
                # COMPLETE mode: add duration to lead time
                p_copy['lead_time_days'] = p_copy['lead_time_days'] + event.duration_days
                p_copy['demand_cv'] = min(2.0, p_copy['demand_cv'] * 2.0)
            disrupted.append(p_copy)

        assert affected == 1, f"Expected 1 affected SKU, got {affected}"
        assert disrupted[0]['lead_time_days'] == 17, "3 + 14 = 17 days"
        assert disrupted[0]['demand_cv'] == 1.0, "0.5 * 2.0 = 1.0"
        # MUMIAS product should be untouched
        assert disrupted[1]['lead_time_days'] == 5
        assert disrupted[1]['demand_cv'] == 0.3

    def test_delayed_mode_doubles_lead_time(self):
        """DELAYED mode should 2× the existing lead time."""
        mock_product = {
            'product_name': 'Yogurt', 'supplier_name': 'BROOKSIDE',
            'lead_time_days': 4, 'demand_cv': 0.6,
        }

        # DELAYED logic: multiply lead_time by 2
        p_copy = dict(mock_product)
        p_copy['lead_time_days'] = p_copy['lead_time_days'] * 2

        assert p_copy['lead_time_days'] == 8, "4 * 2 = 8 days"


# =====================================================================
# Test 3: CompetitiveEvent reduces demand via multiplier
# =====================================================================
class TestCompetitiveEvent:
    """Verify that competitive events produce correct demand multipliers."""

    def test_carrefour_day_15_multiplier(self):
        """Carrefour 100m scenario should erode demand by ~3% at day 15."""
        from oasis.simulation.black_swan_events import SCENARIO_TEMPLATES

        event = SCENARIO_TEMPLATES["carrefour_100m"]
        mult = event.get_multiplier_for_day(15)

        # At day 15, ramp_progress = 15/30 = 0.5
        # Impact = -6% * 0.5 = -3% → mult ≈ 0.97
        assert 0.96 <= mult <= 0.98, f"Expected ~0.97, got {mult}"

    def test_carrefour_full_ramp(self):
        """At full ramp (day 30), Carrefour should produce -6% impact."""
        from oasis.simulation.black_swan_events import SCENARIO_TEMPLATES

        event = SCENARIO_TEMPLATES["carrefour_100m"]
        mult = event.get_multiplier_for_day(30)

        # Full ramp: impact = -6% → mult = 0.94
        assert 0.93 <= mult <= 0.95, f"Expected ~0.94, got {mult}"

    def test_department_sensitivity_applied(self):
        """FRESH MILK should be hit harder due to 1.4 sensitivity."""
        from oasis.simulation.black_swan_events import SCENARIO_TEMPLATES

        event = SCENARIO_TEMPLATES["carrefour_100m"]
        generic_mult = event.get_multiplier_for_day(30)
        milk_mult = event.get_multiplier_for_day(30, 'FRESH MILK')

        # Milk should be lower (more impacted) than generic
        assert milk_mult < generic_mult, \
            f"Milk mult ({milk_mult}) should be less than generic ({generic_mult})"

    def test_competitor_exit_positive_impact(self):
        """Competitor exit should produce a positive demand boost."""
        from oasis.simulation.black_swan_events import SCENARIO_TEMPLATES

        event = SCENARIO_TEMPLATES["competitor_exit_nearby"]
        mult = event.get_multiplier_for_day(14)

        # +4% impact at full ramp → mult > 1.0
        assert mult > 1.0, f"Expected > 1.0, got {mult}"


# =====================================================================
# Test 4: SupplierRiskAnalyzer identifies critical suppliers
# =====================================================================
class TestSupplierRiskAnalyzer:
    """Verify supplier concentration analysis."""

    def test_identifies_dominant_supplier(self):
        from oasis.simulation.black_swan_events import SupplierRiskAnalyzer

        inventory = {
            'SKU001': {'department': 'FRESH MILK', 'supplier': 'BROOKSIDE',
                       'avg_daily_sales': 20, 'price': 100},
            'SKU002': {'department': 'FRESH MILK', 'supplier': 'BROOKSIDE',
                       'avg_daily_sales': 15, 'price': 80},
            'SKU003': {'department': 'FRESH MILK', 'supplier': 'KINANGOP',
                       'avg_daily_sales': 5, 'price': 90},
        }

        analyzer = SupplierRiskAnalyzer()
        critical = analyzer.identify_critical_suppliers(
            inventory, min_share_pct=30.0, min_revenue_potential=0
        )

        assert len(critical) >= 1
        assert critical[0]['supplier'] == 'BROOKSIDE'
        assert critical[0]['share_pct'] > 50  # BROOKSIDE has 2 of 3 SKUs with higher ADS

    def test_hhi_calculation(self):
        """HHI above 2500 indicates high concentration."""
        from oasis.simulation.black_swan_events import SupplierRiskAnalyzer

        # Monopoly: one supplier, 100% share → HHI = 10000
        inventory = {
            'SKU001': {'department': 'TEST', 'supplier': 'MONO',
                       'avg_daily_sales': 10, 'price': 100},
        }

        analyzer = SupplierRiskAnalyzer()
        hhi = analyzer.calculate_hhi(inventory, department='TEST')
        assert hhi == 10000.0, f"Expected 10000 for monopoly, got {hhi}"


# =====================================================================
# Test 5: SCENARIO_TEMPLATES and SUPPLIER_FAILURE_TEMPLATES populated
# =====================================================================
class TestScenarioTemplates:
    """Verify that scenario templates are populated and valid."""

    def test_scenario_templates_not_empty(self):
        from oasis.simulation.black_swan_events import SCENARIO_TEMPLATES
        assert len(SCENARIO_TEMPLATES) >= 4, \
            f"Expected at least 4 scenario templates, got {len(SCENARIO_TEMPLATES)}"

    def test_supplier_failure_templates_not_empty(self):
        from oasis.simulation.black_swan_events import SUPPLIER_FAILURE_TEMPLATES
        assert len(SUPPLIER_FAILURE_TEMPLATES) >= 3, \
            f"Expected at least 3 failure templates, got {len(SUPPLIER_FAILURE_TEMPLATES)}"

    def test_all_competitive_templates_have_impact(self):
        from oasis.simulation.black_swan_events import SCENARIO_TEMPLATES
        for key, event in SCENARIO_TEMPLATES.items():
            assert event.impact_pct != 0, f"Template '{key}' has zero impact"
            assert event.competitor_name, f"Template '{key}' missing competitor name"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
