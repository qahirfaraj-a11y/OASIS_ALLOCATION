"""
Probability calibration (Risk-Scoring Redesign, P4a.1).

The P4a backtest showed the newsvendor risk **ranks** stockouts well (PR-AUC
0.93) but is **under-confident** in the mid range — when it says 0.3 the real
stockout rate is ~0.9. This module fits a monotonic calibration map (isotonic
regression via Pool-Adjacent-Violators) from raw risk → empirical stockout
frequency.

Because isotonic regression is monotonic, calibration **preserves the ranking**
(PR-AUC is unchanged); it fixes the *probability* so a service-level decision
keyed off it is honest. Pure and unit tested. Must be fit on a **held-out**
split (the caller's job) to avoid optimistic in-sample calibration.
"""

from __future__ import annotations

from typing import List, Sequence, Tuple

Pair = Tuple[float, int]
# Calibration model = (ascending x thresholds, non-decreasing calibrated values).
Model = Tuple[List[float], List[float]]


def fit_isotonic(pairs: Sequence[Pair]) -> Model:
    """Fit a non-decreasing risk→probability map with Pool-Adjacent-Violators (pure)."""
    pts = sorted(((float(s), float(y)) for s, y in pairs), key=lambda p: p[0])
    if not pts:
        return ([], [])
    # blocks: [sum_y, count, x_max]
    blocks: List[list] = []
    for x, y in pts:
        blocks.append([y, 1.0, x])
        while len(blocks) >= 2 and (blocks[-2][0] / blocks[-2][1]) > (blocks[-1][0] / blocks[-1][1]):
            b2 = blocks.pop()
            b1 = blocks.pop()
            blocks.append([b1[0] + b2[0], b1[1] + b2[1], b2[2]])
    thresholds = [b[2] for b in blocks]
    values = [b[0] / b[1] for b in blocks]
    return (thresholds, values)


def apply_calibration(model: Model, score: float) -> float:
    """Map a raw risk score through the fitted isotonic model (pure)."""
    thresholds, values = model
    if not thresholds:
        return max(0.0, min(1.0, float(score)))
    s = float(score)
    for t, v in zip(thresholds, values):
        if s <= t:
            return v
    return values[-1]


def calibrate_pairs(model: Model, pairs: Sequence[Pair]) -> List[Pair]:
    """Apply the model to the scores of a (score, label) list (pure)."""
    return [(apply_calibration(model, s), y) for s, y in pairs]
