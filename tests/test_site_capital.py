"""Tests for site capital — location to proposed opening budget.

The three defects this module exists to close are each pinned by a test:
circular sizing, share-without-a-denominator, and no capital at all.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import site_capital as SC
from oasis.logic import site_scoring as SS


def _estate():
    """Four placed stores with real revenue and stock — the minimum the gate
    will look at."""
    return [
        {"org_cd": "A", "name": "A", "lat": -1.2680, "lon": 36.7930, "size_sqft": 14000},
        {"org_cd": "B", "name": "B", "lat": -1.2790, "lon": 36.7690, "size_sqft": 9000},
        {"org_cd": "C", "name": "C", "lat": -1.3190, "lon": 36.7100, "size_sqft": 11000},
        {"org_cd": "D", "name": "D", "lat": -1.2570, "lon": 36.8030, "size_sqft": 20000},
    ]


def _rivals():
    return [{"lat": -1.2700, "lon": 36.8100, "size_sqft": 30000, "chain": "BigBox"},
            {"lat": -1.2900, "lon": 36.7800, "size_sqft": 15000, "chain": "Rival"}]


class TestEstateObservations:
    def test_a_store_is_scored_leave_one_out(self):
        """A store must not be scored against itself: it would sit at the
        distance floor and take a near-total share of its own catchment."""
        obs = SC.estate_observations(
            _estate(), _rivals(),
            revenue_by_org={"A": 1_000_000, "B": 600_000,
                            "C": 1_100_000, "D": 900_000})
        assert len(obs) == 4
        assert all(o["capture"] < 0.9 for o in obs), \
            "a self-scored store would approach a full share"

    def test_a_store_with_no_sales_is_not_an_observation(self):
        obs = SC.estate_observations(_estate(), _rivals(),
                                     revenue_by_org={"A": 1_000_000})
        assert [o["org_cd"] for o in obs] == ["A"]

    def test_an_unplaced_store_is_not_an_observation(self):
        stores = _estate()
        stores[0] = {"org_cd": "A", "name": "A", "size_sqft": 14000}
        obs = SC.estate_observations(
            stores, _rivals(),
            revenue_by_org={s["org_cd"]: 1_000_000 for s in stores})
        assert "A" not in [o["org_cd"] for o in obs]

    def test_implied_demand_is_revenue_over_capture(self):
        obs = SC.estate_observations(_estate(), _rivals(),
                                     revenue_by_org={"A": 1_000_000})
        o = obs[0]
        assert abs(o["implied_demand"] - o["revenue"] / o["capture"]) < 1e-6


class TestCalibrate:
    def test_empty_estate_is_unusable_and_says_why(self):
        cal = SC.calibrate([])
        assert cal["usable"] is False and cal["reason"]

    def test_cover_ratio_is_measured_not_assumed(self):
        obs = [{"org_cd": "A", "name": "A", "size_sqft": 1000, "capture": 0.1,
                "revenue": 1000.0, "stock_value": 300.0,
                "implied_demand": 10000.0, "revenue_per_sqft": 1.0},
               {"org_cd": "B", "name": "B", "size_sqft": 1000, "capture": 0.1,
                "revenue": 1000.0, "stock_value": 500.0,
                "implied_demand": 10000.0, "revenue_per_sqft": 1.0}]
        cal = SC.calibrate(obs)
        assert cal["cover_measured"] is True
        assert abs(cal["cover_ratio"] - 0.4) < 1e-9

    def test_cover_is_flagged_unmeasured_when_no_stock_values(self):
        obs = [{"org_cd": "A", "name": "A", "size_sqft": 1000, "capture": 0.1,
                "revenue": 1000.0, "stock_value": 0.0,
                "implied_demand": 10000.0, "revenue_per_sqft": 1.0}]
        cal = SC.calibrate(obs)
        assert cal["cover_measured"] is False and cal["cover_ratio"] == 0.0


def _obs(org, size, capture, revenue):
    return {"org_cd": org, "name": org, "size_sqft": float(size),
            "capture": capture, "revenue": float(revenue),
            "stock_value": revenue * 0.3,
            "implied_demand": revenue / capture,
            "revenue_per_sqft": revenue / size}


class TestTheGate:
    def test_too_few_stores_is_not_validated(self):
        obs = [_obs("A", 1000, 0.1, 1000), _obs("B", 1000, 0.2, 2000)]
        val = SC.loo_validate(obs)
        assert val["validated"] is False
        assert str(SC.MIN_CALIBRATION_STORES) in val["reason"]

    def test_geography_wins_when_revenue_really_tracks_capture(self):
        """Constant catchment, revenue = capture x 100,000, sizes uncorrelated
        with revenue. The capture model should win outright."""
        obs = [_obs("A", 5000, 0.10, 10_000), _obs("B", 30000, 0.20, 20_000),
               _obs("C", 8000, 0.30, 30_000), _obs("D", 25000, 0.40, 40_000),
               _obs("E", 12000, 0.50, 50_000)]
        val = SC.loo_validate(obs)
        assert val["validated"] is True
        assert val["mape_capture"] < val["mape_sqft_only"]
        assert val["mape_capture"] < val["mape_estate_median"]

    def test_geography_loses_when_revenue_only_tracks_floor_area(self):
        """Revenue = 10 x sqft, capture uncorrelated. Floor area must win, and
        the module must refuse to let the site set a budget."""
        obs = [_obs("A", 5000, 0.40, 50_000), _obs("B", 10000, 0.12, 100_000),
               _obs("C", 20000, 0.31, 200_000), _obs("D", 40000, 0.09, 400_000),
               _obs("E", 30000, 0.22, 300_000)]
        val = SC.loo_validate(obs)
        assert val["validated"] is False
        assert val["beaten_by"] == "floor area alone"

    def test_a_tie_is_a_loss(self):
        """The simpler predictor wins by default — it needs no map."""
        obs = [_obs(c, 1000, 0.1, 1000) for c in "ABCDE"]
        val = SC.loo_validate(obs)
        assert val["validated"] is False


class TestProposeCapital:
    def _validated(self):
        obs = [_obs("A", 5000, 0.10, 10_000), _obs("B", 30000, 0.20, 20_000),
               _obs("C", 8000, 0.30, 30_000), _obs("D", 25000, 0.40, 40_000),
               _obs("E", 12000, 0.50, 50_000)]
        return obs, SC.calibrate(obs), SC.loo_validate(obs)

    def test_an_isolated_site_gets_no_capital_however_high_it_scores(self):
        """DEFECT 2. A full share of an empty catchment is still a full share.
        The old recommend_format called that a flagship."""
        _, cal, val = self._validated()
        p = SC.propose_capital(100.0, 60_000, cal, val, isolated=True)
        assert p["basis"] == "insufficient-data"
        assert p["opening_capital"] is None and p["expected_revenue"] is None

    def test_capital_is_revenue_times_the_measured_cover_ratio(self):
        _, cal, val = self._validated()
        p = SC.propose_capital(25.0, 10_000, cal, val)
        assert p["basis"] == "estate-calibrated"
        assert abs(p["opening_capital"]
                   - p["expected_revenue"] * cal["cover_ratio"]) < 1.0

    def test_an_unvalidated_estate_falls_back_and_says_so(self):
        obs = [_obs("A", 5000, 0.40, 50_000), _obs("B", 10000, 0.12, 100_000),
               _obs("C", 20000, 0.31, 200_000), _obs("D", 40000, 0.09, 400_000)]
        cal, val = SC.calibrate(obs), SC.loo_validate(obs)
        p = SC.propose_capital(30.0, 10_000, cal, val)
        assert p["basis"] == "estate-productivity"
        assert p["expected_revenue"] == round(10_000 * cal["median_revenue_per_sqft"], 0)

    def test_the_fallback_ignores_the_site_and_the_note_admits_it(self):
        """Honest, not clever: when geography loses, the location genuinely
        does not enter the number, and two different sites get the same one."""
        obs = [_obs("A", 5000, 0.40, 50_000), _obs("B", 10000, 0.12, 100_000),
               _obs("C", 20000, 0.31, 200_000), _obs("D", 40000, 0.09, 400_000)]
        cal, val = SC.calibrate(obs), SC.loo_validate(obs)
        a = SC.propose_capital(5.0, 10_000, cal, val)
        b = SC.propose_capital(45.0, 10_000, cal, val)
        assert a["expected_revenue"] == b["expected_revenue"]
        assert "did not" in a["note"] or "adds nothing" in a["note"]

    def test_no_stock_values_means_revenue_only_never_an_invented_capital(self):
        obs = [{"org_cd": c, "name": c, "size_sqft": 10000.0, "capture": 0.2,
                "revenue": 100_000.0, "stock_value": 0.0,
                "implied_demand": 500_000.0, "revenue_per_sqft": 10.0}
               for c in "ABCDE"]
        cal, val = SC.calibrate(obs), SC.loo_validate(obs)
        p = SC.propose_capital(20.0, 10_000, cal, val)
        assert p["opening_capital"] is None
        assert p["expected_revenue"] is not None
        assert "no capital figure" in p["note"]

    def test_no_estate_means_no_number(self):
        p = SC.propose_capital(30.0, 10_000, SC.calibrate([]), {})
        assert p["basis"] == "insufficient-data" and p["opening_capital"] is None


class TestSizeIsNotCircular:
    """DEFECT 1. The old recommend_format restated its input: on one fixed
    location it returned 'Unsuitable' at 3,000 sqft and 'Hyper / Flagship' at
    80,000 sqft, because capture is computed FROM the size."""

    def _fixture(self):
        obs = [_obs("A", 5000, 0.10, 10_000), _obs("B", 30000, 0.20, 20_000),
               _obs("C", 8000, 0.30, 30_000), _obs("D", 25000, 0.40, 40_000),
               _obs("E", 12000, 0.50, 50_000)]
        return SC.calibrate(obs), SC.loo_validate(obs)

    def test_the_old_scorer_echoes_its_own_input(self):
        """Pins the defect, so a regression is caught rather than argued."""
        own = [{"lat": -1.2650, "lon": 36.8020, "size_sqft": 12000}]
        rivals = [{"lat": -1.2700, "lon": 36.8100, "size_sqft": 30000}]
        small = SS.score_site(-1.2750, 36.8050, own, rivals, size_sqft=3_000)
        large = SS.score_site(-1.2750, 36.8050, own, rivals, size_sqft=80_000)
        assert large["adjusted_capture_pct"] > small["adjusted_capture_pct"] * 5

    def test_revenue_per_sqft_falls_as_the_store_grows(self):
        """Huff share saturates, so productivity must decline — this is what
        gives the recommendation a real crossing point."""
        cal, val = self._fixture()
        own = [{"lat": -1.2650, "lon": 36.8020, "size_sqft": 12000}]
        rivals = [{"lat": -1.2700, "lon": 36.8100, "size_sqft": 30000}]
        rec = SC.recommend_size(
            lambda sz: SS.score_site(-1.2750, 36.8050, own, rivals,
                                     size_sqft=sz)["adjusted_capture_pct"],
            cal, val)
        rps = [r["revenue_per_sqft"] for r in rec["rungs"]]
        assert rps == sorted(rps, reverse=True), "productivity must decline"

    def test_it_recommends_nothing_when_nothing_clears_the_anchor(self):
        cal, val = self._fixture()
        rec = SC.recommend_size(lambda sz: 0.001, cal, val)
        assert rec["recommended_sqft"] is None
        assert "clears" in rec["note"]

    def test_the_anchor_is_external_to_the_candidate(self):
        """The comparison is against the estate's own productivity, not
        against the size that was typed in."""
        cal, val = self._fixture()
        rec = SC.recommend_size(lambda sz: 50.0, cal, val)
        assert rec["productivity_anchor"] == round(
            cal["median_revenue_per_sqft"], 2)

    def test_an_uncalibrated_estate_gets_a_ranking_not_a_size_decision(self):
        obs = [_obs("A", 5000, 0.40, 50_000), _obs("B", 10000, 0.12, 100_000),
               _obs("C", 20000, 0.31, 200_000), _obs("D", 40000, 0.09, 400_000)]
        cal, val = SC.calibrate(obs), SC.loo_validate(obs)
        rec = SC.recommend_size(lambda sz: 50.0, cal, val)
        assert rec["calibrated"] is False
        assert "ranking" in rec["note"]


class TestReport:
    def test_report_is_ascii_for_a_windows_console(self):
        obs = [_obs(c, 10000, 0.2, 100_000) for c in "ABCDE"]
        cal, val = SC.calibrate(obs), SC.loo_validate(obs)
        text = SC.format_report(cal, val, SC.propose_capital(20.0, 10_000, cal, val))
        text.encode("ascii")

    def test_an_unusable_estate_reports_the_reason(self):
        text = SC.format_report(SC.calibrate([]), {})
        assert "UNUSABLE" in text
