"""Tests for the capital-recovery feeder (oasis/logic/capital_recovery.py, SH-B)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import capital_recovery as CR
from oasis.logic import journey_state as JS


def _p(ads, soh, cost=None, sell=None):
    d = {"avg_daily_sales": ads, "current_stocks": soh}
    if cost is not None:
        d["cost_price"] = cost
    if sell is not None:
        d["selling_price"] = sell
    return d


class TestTrappedCapital:
    def test_dead_stock_counted_at_cost(self):
        # dead: ADS<0.2 & SOH>15 → 100 units × 50 cost = 5000
        prods = [_p(0.0, 100, cost=50), _p(5.0, 80, cost=50)]  # 2nd is a fast mover
        assert CR.trapped_capital(prods) == 5000.0

    def test_soh_threshold(self):
        # SOH must exceed 15 to count
        assert CR.trapped_capital([_p(0.0, 15, cost=10)]) == 0.0
        assert CR.trapped_capital([_p(0.0, 16, cost=10)]) == 160.0

    def test_cost_fallback_to_selling(self):
        # no cost → 75% of selling price
        assert CR.trapped_capital([_p(0.0, 20, sell=100)]) == 20 * 75.0

    def test_network_sums_orgs(self):
        net = {
            "A": [_p(0.0, 20, cost=10)],   # 200
            "B": [_p(0.0, 30, cost=10)],   # 300
        }
        assert CR.trapped_capital_network(net) == 500.0

    def test_empty(self):
        assert CR.trapped_capital([]) == 0.0
        assert CR.trapped_capital_network({}) == 0.0


class TestComputeRecovery:
    def test_first_run_baselines_target_zero_recovered(self):
        target, recovered = CR.compute_recovery(10000, prior_target=0)
        assert target == 10000.0
        assert recovered == 0.0

    def test_recovery_rises_as_trapped_falls(self):
        # peak was 10000; now only 4000 trapped → 6000 recovered
        target, recovered = CR.compute_recovery(4000, prior_target=10000)
        assert target == 10000.0
        assert recovered == 6000.0

    def test_growing_trapped_raises_target_holds_recovered(self):
        # trapped grew past prior peak → new target, recovered 0
        target, recovered = CR.compute_recovery(12000, prior_target=10000)
        assert target == 12000.0
        assert recovered == 0.0

    def test_fully_cleared(self):
        target, recovered = CR.compute_recovery(0, prior_target=8000)
        assert target == 8000.0 and recovered == 8000.0


class TestUpdateJourneyRecovery:
    def test_persists_and_progresses(self, tmp_path):
        p = str(tmp_path / "j.json")
        # First reading: 10k trapped → baseline target, 0 recovered
        s1 = CR.update_journey_recovery(10000, path=p)
        assert s1["value_target"] == 10000.0
        assert s1["value_recovered"] == 0.0
        # Later: dead stock cleared to 3k → 7k recovered, target held
        s2 = CR.update_journey_recovery(3000, path=p)
        assert s2["value_target"] == 10000.0
        assert s2["value_recovered"] == 7000.0
        # Persisted
        assert JS.load_state(p)["value_recovered"] == 7000.0
