"""
Tests for the core ordering logic: RuleBasedLLM guards, BudgetManager,
and order_logic_guards.

Converted from scripts/archive/verify_sales_aware_slow_mover.py,
verify_allocation_fixes.py, and verify_zero_sales.py.

Run: python -m pytest tests/test_ordering_logic.py -v --timeout=120
"""

import asyncio
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.budget_manager import BudgetManager
from oasis.logic.order_logic_guards import apply_safety_guards
from oasis.llm.inference import RuleBasedLLM


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def rule_engine():
    return RuleBasedLLM()


@pytest.fixture(scope="module")
def budget_manager():
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'oasis', 'data')
    return BudgetManager(data_dir)


def _run(coro):
    """Run an async coroutine synchronously (test helper)."""
    return asyncio.get_event_loop().run_until_complete(coro)


def _make_product(**overrides):
    """Build a product dict with sensible defaults, overridable per-test."""
    base = {
        'product_name': 'Test Product',
        'blocked_open_for_order': 'open',
        'current_stocks': 5,
        'last_days_since_last_delivery': 14,
        'avg_daily_sales': 1.0,
        'days_since_last_sale': 3,
        'total_units_sold_last_90d': 90,
        'avg_daily_sales_last_30d': 1.0,
        'historical_avg_order_qty': 20,
        'selling_price': 100.0,
        'shelf_life_days': 365,
        'sales_trend': 'stable',
        'target_coverage_days': 14.0,
        'pack_size': 1,
    }
    base.update(overrides)
    return base


# ---------------------------------------------------------------------------
# BudgetManager
# ---------------------------------------------------------------------------

class TestBudgetManager:
    def test_scaling_ratios_loaded(self, budget_manager):
        assert len(budget_manager.scaling_ratios) > 0, "Should load department scaling ratios"

    def test_no_zero_weight_departments(self, budget_manager):
        zeros = [d for d, w in budget_manager.scaling_ratios.items() if w == 0.0]
        assert len(zeros) == 0, f"Zero-weight departments found: {zeros[:5]}"

    def test_wallet_initialization(self, budget_manager):
        wallets = budget_manager.initialize_wallets(200_000)
        assert len(wallets) > 0, "Should create department wallets"
        total = sum(w['allocated_budget'] for w in wallets.values())
        assert total > 0, "Total allocated budget should be positive"

    def test_budget_fully_allocated(self, budget_manager):
        budget = 200_000
        wallets = budget_manager.initialize_wallets(budget)
        total = sum(w['allocated_budget'] for w in wallets.values())
        assert total >= budget * 0.90, (
            f"At least 90% of budget should be allocated, got {total/budget*100:.1f}%"
        )

    def test_staple_detection(self, budget_manager):
        assert budget_manager.is_staple(
            'FRESH FRI 500ML COOKING OIL', category='COOKING OIL', velocity=5.0
        ), "Cooking oil should be a staple"


# ---------------------------------------------------------------------------
# RuleBasedLLM — slow mover classification
# ---------------------------------------------------------------------------

class TestSlowMoverClassification:
    def test_dead_stock_blocked(self, rule_engine):
        """True dead stock (no sales in 180d) should get qty=0."""
        products = [_make_product(
            product_name='Dead Stock Item',
            current_stocks=2,
            last_days_since_last_delivery=220,
            avg_daily_sales=0,
            days_since_last_sale=220,
            total_units_sold_last_90d=0,
            avg_daily_sales_last_30d=0.0,
            historical_avg_order_qty=0,
            sales_trend='declining',
        )]
        results = _run(rule_engine.analyze(products))
        assert results[0]['recommended_quantity'] == 0

    def test_low_but_steady_allowed(self, rule_engine):
        """Low-but-steady mover (210d delivery, 8 units/90d) should still order."""
        products = [_make_product(
            product_name='Niche Steady Item',
            current_stocks=2,
            last_days_since_last_delivery=210,
            avg_daily_sales=0.3,
            days_since_last_sale=5,
            total_units_sold_last_90d=8,
            avg_daily_sales_last_30d=0.27,
            historical_avg_order_qty=18,
            selling_price=150.0,
        )]
        results = _run(rule_engine.analyze(products))
        assert results[0]['recommended_quantity'] > 0, (
            "Item with steady sales should not be blocked despite old delivery"
        )

    def test_stale_fresh_no_sales_blocked(self, rule_engine):
        """Stale fresh item with zero sales should be blocked."""
        products = [_make_product(
            product_name='Discontinued Perishable',
            current_stocks=8,
            last_days_since_last_delivery=140,
            avg_daily_sales=0,
            days_since_last_sale=140,
            total_units_sold_last_90d=0,
            avg_daily_sales_last_30d=0.0,
            historical_avg_order_qty=0,
            selling_price=90.0,
            shelf_life_days=7,
            is_fresh=True,
            sales_trend='declining',
        )]
        results = _run(rule_engine.analyze(products))
        assert results[0]['recommended_quantity'] == 0

    def test_no_cliff_effect_at_200d(self, rule_engine):
        """Day 199 vs 201 should not produce a large qty cliff if both sell."""
        base = dict(
            blocked_open_for_order='open',
            current_stocks=5,
            avg_daily_sales=1.0,
            days_since_last_sale=7,
            total_units_sold_last_90d=90,
            avg_daily_sales_last_30d=1.0,
            historical_avg_order_qty=20,
            selling_price=100.0,
            shelf_life_days=365,
            sales_trend='stable',
            target_coverage_days=14.0,
            pack_size=1,
        )
        products = [
            _make_product(product_name='Day 199', last_days_since_last_delivery=199, **base),
            _make_product(product_name='Day 201', last_days_since_last_delivery=201, **base),
        ]
        results = _run(rule_engine.analyze(products))
        diff = abs(results[0]['recommended_quantity'] - results[1]['recommended_quantity'])
        assert diff < 10, (
            f"Cliff effect: 199d={results[0]['recommended_quantity']}, "
            f"201d={results[1]['recommended_quantity']}"
        )
        assert results[1]['recommended_quantity'] > 0, "Day 201 with sales should not be blocked"


# ---------------------------------------------------------------------------
# RuleBasedLLM — zero sales edge cases
# ---------------------------------------------------------------------------

class TestZeroSalesHandling:
    def test_zero_sales_dead_sku_blocked(self, rule_engine):
        """Item with zero sales across all metrics should be blocked."""
        products = [_make_product(
            product_name='Zero Sales Dead',
            current_stocks=10,
            last_days_since_last_delivery=300,
            avg_daily_sales=0,
            days_since_last_sale=300,
            total_units_sold_last_90d=0,
            avg_daily_sales_last_30d=0,
            historical_avg_order_qty=0,
            sales_trend='declining',
        )]
        results = _run(rule_engine.analyze(products))
        assert results[0]['recommended_quantity'] == 0

    def test_recently_restocked_allowed(self, rule_engine):
        """Recently restocked item with good historical sales should order."""
        products = [_make_product(
            product_name='Restocked Fast Mover',
            current_stocks=0,
            last_days_since_last_delivery=3,
            avg_daily_sales=5.0,
            days_since_last_sale=1,
            total_units_sold_last_90d=450,
            avg_daily_sales_last_30d=5.0,
            historical_avg_order_qty=50,
        )]
        results = _run(rule_engine.analyze(products))
        assert results[0]['recommended_quantity'] > 0


# ---------------------------------------------------------------------------
# apply_safety_guards — pack rounding & constraints
# ---------------------------------------------------------------------------

class TestSafetyGuards:
    def test_negative_qty_zeroed(self):
        recs = [{'product_name': 'X', 'recommended_quantity': -5}]
        products_map = {'X': _make_product(product_name='X')}
        result = apply_safety_guards(recs, products_map, 'replenishment')
        assert result[0]['recommended_quantity'] >= 0

    def test_dead_stock_capped_by_guards(self):
        """Guards should zero out qty for 200d+ items with no 90d sales."""
        recs = [{'product_name': 'D', 'recommended_quantity': 50, 'reasoning': ''}]
        products_map = {'D': _make_product(
            product_name='D',
            last_days_since_last_delivery=250,
            total_units_sold_last_90d=0,
            current_stocks=5,
        )}
        result = apply_safety_guards(recs, products_map, 'replenishment')
        assert result[0]['recommended_quantity'] == 0

    def test_slow_mover_21d_cap(self):
        """Guards should cap 200d+ items with sales at 21d coverage."""
        recs = [{'product_name': 'S', 'recommended_quantity': 100, 'reasoning': ''}]
        products_map = {'S': _make_product(
            product_name='S',
            last_days_since_last_delivery=210,
            avg_daily_sales=1.0,
            avg_daily_sales_last_30d=1.0,
            total_units_sold_last_90d=30,
            current_stocks=5,
        )}
        result = apply_safety_guards(recs, products_map, 'replenishment')
        assert result[0]['recommended_quantity'] <= 16  # 21*1.0 - 5 = 16


class TestEngineBlockedHandling:
    def test_blocked_item_gets_zero(self, rule_engine):
        """Engine should return qty=0 for blocked items."""
        products = [_make_product(
            product_name='Blocked Item',
            blocked_open_for_order='blocked',
            avg_daily_sales=5.0,
            target_coverage_days=14.0,
        )]
        results = _run(rule_engine.analyze(products))
        assert results[0]['recommended_quantity'] == 0
