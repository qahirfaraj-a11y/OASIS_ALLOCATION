"""Tests for the risk backtest harness pure functions (Risk Redesign P4a)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.backtest import (
    base_rate, average_precision, brier_score, calibration_table,
    estimate_opening, backtest_samples, summarize,
    calibrated_evaluation, supply_risk, supplier_failure_pairs,
)
from oasis.logic.stockout_ledger import DayFlow, simulate_ledger


class TestMetrics:
    def test_base_rate(self):
        assert base_rate([(0.1, 0), (0.9, 1), (0.5, 1)]) == round(2 / 3, 4)
        assert base_rate([]) == 0.0

    def test_average_precision_perfect_ranker(self):
        # all positives scored above all negatives -> AP 1.0
        pairs = [(0.9, 1), (0.8, 1), (0.3, 0), (0.1, 0)]
        assert average_precision(pairs) == 1.0

    def test_average_precision_no_positives(self):
        assert average_precision([(0.5, 0), (0.2, 0)]) == 0.0

    def test_average_precision_worst_ranker(self):
        # positives all ranked last -> low AP
        pairs = [(0.9, 0), (0.8, 0), (0.3, 1), (0.1, 1)]
        assert average_precision(pairs) < 0.6

    def test_brier(self):
        # perfect confident predictions -> 0
        assert brier_score([(1.0, 1), (0.0, 0)]) == 0.0
        # worst -> 1
        assert brier_score([(0.0, 1), (1.0, 0)]) == 1.0

    def test_calibration_table_buckets(self):
        pairs = [(0.05, 0), (0.05, 0), (0.95, 1), (0.95, 1)]
        rows = calibration_table(pairs, n_buckets=10)
        first = rows[0]
        last = rows[-1]
        assert first["n"] == 2 and first["observed"] == 0.0
        assert last["n"] == 2 and last["observed"] == 1.0


class TestEstimateOpening:
    def test_removes_net_flow(self):
        # anchor 10; over window received 8, sold 5, shipped 1 -> net +2
        # opening = 10 - 2 = 8
        assert estimate_opening(10, receipts=8, demand=5, outflow=1) == 8.0

    def test_clamps_at_zero(self):
        assert estimate_opening(2, receipts=50, demand=0, outflow=0) == 0.0


class TestBacktestSamples:
    def test_pairs_and_forward_label(self):
        # build a series that stocks out around the middle
        days = list(range(10))
        flows = {d: DayFlow(demand=2) for d in days}
        flows[0] = DayFlow(receipts=6, demand=2)  # opening tiny -> depletes
        states = simulate_ledger(0, days, flows)
        pairs = backtest_samples(states, mu_ltd=2.0, sigma_ltd=1.4,
                                 horizon=3, stride=1)
        # every pair is (risk in [0,1], label in {0,1})
        assert all(0.0 <= r <= 1.0 and y in (0, 1) for r, y in pairs)
        # at least one stockout label given demand outstrips supply
        assert any(y == 1 for _, y in pairs)

    def test_no_pairs_when_series_too_short(self):
        days = [0, 1]
        states = simulate_ledger(100, days, {d: DayFlow(demand=1) for d in days})
        assert backtest_samples(states, 5, 2, horizon=7, stride=1) == []


class TestSummarize:
    def test_shape(self):
        pairs = [(0.9, 1), (0.1, 0), (0.8, 1), (0.2, 0)]
        s = summarize(pairs)
        assert s["samples"] == 4
        assert 0.0 <= s["pr_auc"] <= 1.0
        assert "calibration" in s and len(s["calibration"]) == 10


class TestCalibratedEvaluation:
    def test_calibration_lowers_brier_on_underconfident(self):
        # under-confident raw scores: 0.3 actually positive, 0.02 actually negative
        train = [(0.3, 1)] * 18 + [(0.3, 0)] * 2 + [(0.02, 0)] * 18 + [(0.02, 1)] * 2
        test = list(train)
        ev = calibrated_evaluation(train, test)
        assert ev["brier_calibrated"] <= ev["brier_raw"]
        # ranking (PR-AUC) unchanged by monotonic calibration
        assert ev["pr_auc"] == average_precision(test)


class TestSupplyBacktest:
    def test_supply_risk_worst_of(self):
        assert abs(supply_risk({"fill": 0.8, "short_rate": 0.1}) - 0.2) < 1e-9
        assert abs(supply_risk({"fill": 0.95, "short_rate": 0.3}) - 0.3) < 1e-9

    def test_failure_pairs_only_common_suppliers(self):
        h1 = {"A": {"fill": 0.6, "short_rate": 0.0}, "B": {"fill": 1.0, "short_rate": 0.0}}
        h2 = {"A": 1, "C": 0}   # B missing from h2 -> dropped
        pairs = supplier_failure_pairs(h1, h2)
        assert len(pairs) == 1
        assert pairs[0][1] == 1 and abs(pairs[0][0] - 0.4) < 1e-9
