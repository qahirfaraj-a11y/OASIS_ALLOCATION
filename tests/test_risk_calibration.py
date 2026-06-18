"""Tests for isotonic probability calibration (Risk Redesign P4a.1)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.risk_calibration import (
    fit_isotonic, apply_calibration, calibrate_pairs,
)


class TestFitIsotonic:
    def test_empty(self):
        assert fit_isotonic([]) == ([], [])

    def test_values_non_decreasing(self):
        pairs = [(0.1, 0), (0.2, 1), (0.3, 0), (0.4, 1), (0.9, 1)]
        _, values = fit_isotonic(pairs)
        assert all(values[i] <= values[i + 1] + 1e-9 for i in range(len(values) - 1))

    def test_separable_recovers_extremes(self):
        # low scores all 0, high scores all 1
        pairs = [(0.1, 0), (0.2, 0), (0.8, 1), (0.9, 1)]
        model = fit_isotonic(pairs)
        assert apply_calibration(model, 0.1) == 0.0
        assert apply_calibration(model, 0.9) == 1.0

    def test_pools_violators_to_mean(self):
        # decreasing label vs score -> all pooled to the overall mean
        pairs = [(0.1, 1), (0.2, 0), (0.3, 0)]
        model = fit_isotonic(pairs)
        # one block, mean = 1/3
        assert abs(apply_calibration(model, 0.2) - (1 / 3)) < 1e-9

    def test_underconfident_map_lifts_midrange(self):
        # mimic P4a: score ~0.3 but observed freq ~0.9 -> calibrated value high
        pairs = ([(0.3, 1)] * 9 + [(0.3, 0)] * 1 +     # 0.3 -> ~0.9 observed
                 [(0.02, 0)] * 90 + [(0.02, 1)] * 1)    # low -> ~0.01
        model = fit_isotonic(pairs)
        assert apply_calibration(model, 0.3) > 0.8
        assert apply_calibration(model, 0.02) < 0.1


class TestApplyAndCalibratePairs:
    def test_clamps_when_no_model(self):
        assert apply_calibration(([], []), 0.5) == 0.5

    def test_calibrate_pairs_preserves_labels(self):
        pairs = [(0.1, 0), (0.9, 1)]
        model = fit_isotonic(pairs)
        out = calibrate_pairs(model, pairs)
        assert [y for _, y in out] == [0, 1]
        assert all(0.0 <= s <= 1.0 for s, _ in out)
