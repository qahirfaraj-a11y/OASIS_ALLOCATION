"""Tests for the stockout ledger & labels engine (Risk Redesign P1)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.stockout_ledger import (
    DayFlow, simulate_ledger, stockout_summary, forward_stockout_labels,
    days_of_cover, fill_rate, is_underfill, spread_monthly_to_daily,
)


def _flows(d):
    return {k: (v if isinstance(v, DayFlow) else DayFlow(**v)) for k, v in d.items()}


class TestSimulateLedger:
    def test_running_balance_receipts_and_demand(self):
        # open 10; d1 sell 3 -> 7; d2 receive 5 sell 4 -> 8
        states = simulate_ledger(10, [1, 2], _flows({
            1: {"demand": 3},
            2: {"receipts": 5, "demand": 4},
        }))
        assert [round(s.soh_end, 2) for s in states] == [7.0, 8.0]
        assert all(not s.stockout for s in states)

    def test_lost_sales_when_demand_exceeds_stock(self):
        # open 2; demand 5 -> sell 2, lose 3, soh 0, stockout
        s = simulate_ledger(2, [1], _flows({1: {"demand": 5}}))[0]
        assert s.sold == 2.0 and s.lost == 3.0 and s.soh_end == 0.0
        assert s.stockout is True

    def test_receipts_land_before_demand(self):
        # open 0; receive 10 same period, demand 4 -> sold 4, no stockout
        s = simulate_ledger(0, [1], _flows({1: {"receipts": 10, "demand": 4}}))[0]
        assert s.sold == 4.0 and s.lost == 0.0 and s.soh_end == 6.0
        assert s.stockout is False

    def test_committed_outflow_ships_first_and_is_capped(self):
        # open 5; outflow 8 (transfer out) -> ships only 5; demand 2 -> all lost
        s = simulate_ledger(5, [1], _flows({1: {"outflow": 8, "demand": 2}}))[0]
        assert s.shipped_out == 5.0
        assert s.sold == 0.0 and s.lost == 2.0 and s.soh_end == 0.0
        assert s.stockout is True

    def test_soh_never_negative(self):
        s = simulate_ledger(0, [1], _flows({1: {"demand": 9}}))[0]
        assert s.soh_end == 0.0 and s.lost == 9.0

    def test_no_demand_is_not_a_stockout(self):
        s = simulate_ledger(0, [1], _flows({1: {}}))[0]
        assert s.stockout is False and s.lost == 0.0


class TestStockoutSummary:
    def test_metrics(self):
        states = simulate_ledger(3, [1, 2, 3], _flows({
            1: {"demand": 1},   # sold 1, soh 2
            2: {"demand": 5},   # sold 2, lost 3, soh 0  -> stockout
            3: {"receipts": 4, "demand": 1},  # sold 1, soh 3
        }))
        m = stockout_summary(states)
        assert m["periods"] == 3
        assert m["stockout_periods"] == 1
        assert m["stockout_rate"] == round(1 / 3, 4)
        assert m["lost_units"] == 3.0
        assert m["demand_units"] == 7.0
        assert m["fill_rate"] == round(4 / 7, 4)

    def test_empty(self):
        assert stockout_summary([])["fill_rate"] == 1.0


class TestForwardLabels:
    def test_horizon_1_labels_self(self):
        states = simulate_ledger(2, [1, 2, 3], _flows({
            1: {"demand": 1}, 2: {"demand": 9}, 3: {"demand": 0},
        }))
        labels = forward_stockout_labels(states, horizon=1)
        assert labels == {1: 0, 2: 1, 3: 0}

    def test_horizon_2_looks_ahead(self):
        states = simulate_ledger(2, [1, 2, 3], _flows({
            1: {"demand": 1}, 2: {"demand": 9}, 3: {"demand": 0},
        }))
        # period 1 sees the stockout at period 2 within a 2-window
        labels = forward_stockout_labels(states, horizon=2)
        assert labels[1] == 1 and labels[2] == 1 and labels[3] == 0


class TestDaysOfCover:
    def test_basic(self):
        assert days_of_cover(30, 10) == 3.0

    def test_no_demand_with_stock_is_infinite(self):
        assert days_of_cover(5, 0) == float("inf")

    def test_no_demand_no_stock_is_zero(self):
        assert days_of_cover(0, 0) == 0.0


class TestSupplySignals:
    def test_fill_rate(self):
        assert fill_rate(12, 12) == 1.0
        assert fill_rate(12, 6) == 0.5
        assert fill_rate(0, 0) == 1.0      # no order -> no failure
        assert fill_rate(10, 20) == 1.0    # capped at 1

    def test_underfill(self):
        assert is_underfill(12, 6) is True
        assert is_underfill(12, 12) is False
        assert is_underfill(12, 11, tol=0.1) is False  # within tolerance


class TestSpreadMonthly:
    def test_uniform_spread(self):
        out = spread_monthly_to_daily(30, 30)
        assert len(out) == 30 and out[0] == 1.0 and sum(out) == 30.0

    def test_zero_days(self):
        assert spread_monthly_to_daily(30, 0) == []
