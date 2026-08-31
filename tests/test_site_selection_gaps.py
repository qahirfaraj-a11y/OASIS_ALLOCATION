"""The four gaps the site-selection audit measured, held closed.

Each class below corresponds to one finding, and each test states the
measurement that motivated it. The audit is at
oasis_vault-adjacent artifact "Where the Model Holds"; the numbers quoted in
the docstrings were taken against the live population grid and competitor field.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.desktop import data as D
from oasis.logic import site_capital as SC
from oasis.logic import site_scoring as S


def _estate():
    return [{"org_cd": c, "name": "Store " + c, "lat": la, "lon": lo,
             "size_sqft": sq} for c, la, lo, sq in
            [("A", -1.268, 36.793, 14000), ("B", -1.305, 36.830, 9000),
             ("C", -1.220, 36.880, 22000), ("D", -1.250, 36.720, 17000),
             ("E", -1.310, 36.900, 11000), ("F", -1.275, 36.850, 13000)]]


def _rivals():
    return [{"Latitude": -1.29, "Longitude": 36.81, "Chain": "Alpha",
             "size_sqft": 20000.0},
            {"Latitude": -1.24, "Longitude": 36.86, "Chain": "Beta",
             "size_sqft": 12000.0}]


# ── gap 1 ───────────────────────────────────────────────────────────────────
class TestDroppedStoresAreAccountedFor:
    """A count without the stores it left out is what misleads.

    Measured: widening the catchment to 20 km silently dropped two of eight
    stores below the capture floor, taking n from 8 to 6, with nothing in the
    return value, the report or the console saying so.
    """

    def test_a_store_with_no_sales_is_named(self):
        obs = SC.estate_observations(_estate(), _rivals(),
                                     {"A": 100e6, "B": 90e6})
        skipped = {s["org_cd"]: s for s in obs.skipped}
        assert set(skipped) == {"C", "D", "E", "F"}
        assert all(s["reason"] == "no-sales" for s in skipped.values())

    def test_an_unplaced_store_is_named(self):
        estate = _estate()
        estate[0] = dict(estate[0], lat=None, lon=None)
        obs = SC.estate_observations(estate, _rivals(),
                                     {s["org_cd"]: 100e6 for s in estate})
        assert [s["org_cd"] for s in obs.skipped] == ["A"]
        assert obs.skipped[0]["reason"] == "not-placed"

    def test_a_store_below_the_capture_floor_is_named_with_its_capture(self):
        """The most misleading of the three: it looks like a working store."""
        estate = _estate()
        # A store far from everything else takes almost none of a catchment
        # dominated by an enormous rival sitting on top of it.
        estate.append({"org_cd": "Z", "name": "Store Z", "lat": -1.30,
                       "lon": 36.81, "size_sqft": 200.0})
        rivals = _rivals() + [{"Latitude": -1.3001, "Longitude": 36.8101,
                               "Chain": "Mega", "size_sqft": 900000.0}]
        obs = SC.estate_observations(
            estate, rivals, {s["org_cd"]: 100e6 for s in estate})
        z = [s for s in obs.skipped if s["org_cd"] == "Z"]
        assert z, "the swamped store should have been reported, not vanished"
        assert z[0]["reason"] == "capture-too-low"
        assert "%" in z[0]["detail"]

    def test_calibrate_carries_the_accounting_to_its_caller(self):
        """The list is where the skip happens; calibrate's dict is what the
        report and the console actually read."""
        cal = SC.calibrate(SC.estate_observations(
            _estate(), _rivals(), {"A": 100e6, "B": 90e6, "C": 80e6,
                                   "D": 70e6}))
        assert cal["considered"] == 6
        assert cal["n"] == 4
        assert cal["skipped_count"] == 2
        assert {s["org_cd"] for s in cal["stores_skipped"]} == {"E", "F"}

    def test_the_report_prints_what_it_excluded(self):
        cal = SC.calibrate(SC.estate_observations(
            _estate(), _rivals(), {"A": 100e6, "B": 90e6, "C": 80e6,
                                   "D": 70e6}))
        text = SC.format_report(cal, {})
        assert "of 6 on file" in text
        assert "EXCLUDED" in text
        assert text.isascii(), "format_report prints to a Windows console"

    def test_an_all_skipped_estate_says_why_rather_than_going_quiet(self):
        cal = SC.calibrate(SC.estate_observations(_estate(), _rivals(), {}))
        assert cal["usable"] is False
        assert cal["skipped_count"] == 6
        assert "stores_skipped" in cal


# ── gap 2 ───────────────────────────────────────────────────────────────────
class TestReadinessKnowsWhatItTakesToGetABudget:
    """A retailer with two shops used to complete every setup step, be told
    they were ready, score a site, and only then meet the footnote."""

    def test_the_mirrored_minimum_matches_the_real_one(self):
        """data.py mirrors the constant to stay import-light. If the two drift,
        readiness promises a budget the calibrator will refuse."""
        assert D._MIN_CALIBRATION_STORES == SC.MIN_CALIBRATION_STORES

    def test_status_reports_the_shortfall(self, monkeypatch):
        monkeypatch.setattr(D, "store_map", lambda root=None: {
            "located": [{"org_cd": "A"}, {"org_cd": "B"}],
            "missing": [], "orphaned": [], "saved_total": 2})
        monkeypatch.setattr(D, "competitor_set", lambda root=None: {
            "rows": [{"Chain": "x"}], "chains": ["x"]})
        monkeypatch.setattr(D, "population_set", lambda root=None: {
            "rows": 10, "people": 1000.0})
        monkeypatch.setattr(D, "affluence_set", lambda root=None: {"rows": 0})
        r = D.region_data_status()
        assert r["ready"] is True, "two placed stores can still be RANKED"
        assert r["ready_for_capital"] is False
        assert r["capital_shortfall"] == SC.MIN_CALIBRATION_STORES - 2
        assert r["stores_for_capital"] == SC.MIN_CALIBRATION_STORES

    def test_a_full_estate_is_ready_for_both(self, monkeypatch):
        monkeypatch.setattr(D, "store_map", lambda root=None: {
            "located": [{"org_cd": c} for c in "ABCDE"],
            "missing": [], "orphaned": [], "saved_total": 5})
        monkeypatch.setattr(D, "competitor_set", lambda root=None: {
            "rows": [{"Chain": "x"}], "chains": ["x"]})
        monkeypatch.setattr(D, "population_set", lambda root=None: {
            "rows": 10, "people": 1000.0})
        monkeypatch.setattr(D, "affluence_set", lambda root=None: {"rows": 0})
        r = D.region_data_status()
        assert r["ready_for_capital"] is True
        assert r["capital_shortfall"] == 0


# ── gap 3 ───────────────────────────────────────────────────────────────────
def _obs(revenues, captures, sizes, people=None):
    """Hand-built observations, so the gate can be driven to an exact case."""
    out = []
    for i, (rev, cap, sz) in enumerate(zip(revenues, captures, sizes)):
        ppl = people[i] if people else None
        out.append({"org_cd": chr(65 + i), "name": chr(65 + i),
                    "size_sqft": float(sz), "capture": cap, "revenue": rev,
                    "stock_value": rev * 0.2,
                    "captured_population": ppl,
                    "catchment_population": (ppl * 10) if ppl else None,
                    "affluence_index": None,
                    "spend_per_person": (rev / ppl) if ppl else None,
                    "implied_demand": rev / cap,
                    "revenue_per_sqft": rev / sz})
    return SC.Observations(out)


class TestTheGateHasAMarginAndASignificanceTest:
    """It used to be a strict inequality: any margin at all counted as a win.

    Measured, that is not a gate. It compares MEDIANS of five or six errors,
    and on one estate a population model 'beat' floor area by 4% against 5%
    while losing on 3 of 5 individual stores.
    """

    def test_a_hairline_win_no_longer_validates(self):
        """Capture almost exactly tracks floor area, so both predictors are
        near-identical and neither should be allowed to set a budget."""
        sizes = [10000, 12000, 14000, 16000, 18000, 20000]
        rev = [s * 1000.0 for s in sizes]
        caps = [s / 200000.0 for s in sizes]
        v = SC.loo_validate(_obs(rev, caps, sizes))
        assert v["validated"] is False
        assert abs(v["margin"]) < SC.MIN_GATE_MARGIN

    #: An estate where the geography really is the truth: identical floor
    #: areas, so "big stores sell more" carries no information, and revenue
    #: exactly proportional to capture. The gate SHOULD pass here — a gate that
    #: never passes is not a discipline, it is a refusal.
    TRUE_CAPS_6 = [0.05, 0.09, 0.14, 0.19, 0.24, 0.30]
    TRUE_CAPS_5 = [0.05, 0.10, 0.15, 0.20, 0.25]

    def _truth(self, caps):
        return _obs([c * 1e9 for c in caps], caps, [10000] * len(caps))

    def test_a_real_signal_still_passes(self):
        v = SC.loo_validate(self._truth(self.TRUE_CAPS_6))
        assert v["validated"] is True
        assert v["basis"] == "capture"
        assert v["folds_won"] == v["folds_compared"] == 6
        assert v["provisional"] is False

    def test_a_validated_win_never_rests_on_a_minority_of_stores(self):
        """The sign test's whole purpose. A predictor worse on most of the
        estate must not set a budget however good its median looks — measured
        on a real six-store estate, one cleared the margin by 26% while losing
        on 3 of 5 stores."""
        for caps in (self.TRUE_CAPS_5, self.TRUE_CAPS_6):
            v = SC.loo_validate(self._truth(caps))
            assert v["validated"] is True, "guard against a vacuous assertion"
            assert v["folds_won"] * 2 > v["folds_compared"]

    def test_the_margin_and_fold_count_are_always_reported(self):
        for obs in (self._truth(self.TRUE_CAPS_6),
                    _obs([100e6] * 5, [0.1] * 5, [10000] * 5)):
            v = SC.loo_validate(obs)
            for k in ("margin", "margin_required", "folds_won",
                      "folds_compared", "sign_p"):
                assert k in v, k

    def test_five_stores_cannot_establish_a_win_and_say_so(self):
        """The same signal that is conclusive on six stores is only provisional
        on five: 4 of 4 folds is p=0.125, which a coin flip reaches often. The
        budget is still produced; the label travels with it."""
        v = SC.loo_validate(self._truth(self.TRUE_CAPS_5))
        assert v["validated"] is True
        assert v["provisional"] is True
        assert v["sign_p"] > SC.PROVISIONAL_P
        assert "provisional" in v["confidence_note"]

    def test_a_failed_gate_explains_which_way_it_failed(self):
        v = SC.loo_validate(_obs([100e6] * 5, [0.1] * 5, [10000] * 5))
        assert v["validated"] is False
        assert "productivity" in v["reason"]

    def test_the_report_shows_the_margin_and_the_confidence(self):
        obs = _obs([100e6, 200e6, 150e6, 90e6, 300e6],
                   [0.10, 0.22, 0.14, 0.08, 0.33],
                   [10000, 20000, 14000, 9000, 30000])
        text = SC.format_report(SC.calibrate(obs), SC.loo_validate(obs))
        assert "margin over baseline" in text
        assert text.isascii()

    def test_too_few_stores_is_still_refused_outright(self):
        v = SC.loo_validate(_obs([100e6] * 3, [0.1] * 3, [10000] * 3))
        assert v["validated"] is False
        assert str(SC.MIN_CALIBRATION_STORES) in v["reason"]


class TestTheSignTest:
    def test_a_clean_sweep_is_significant(self):
        s = SC._sign_test([0.1] * 6, [0.9] * 6)
        assert s["wins"] == 6 and s["p"] < 0.05

    def test_a_coin_flip_is_not(self):
        s = SC._sign_test([0.1, 0.1, 0.9, 0.9], [0.5] * 4)
        assert s["wins"] == 2 and s["p"] > 0.5

    def test_ties_carry_no_evidence_either_way(self):
        s = SC._sign_test([0.5] * 4, [0.5] * 4)
        assert s["folds"] == 0 and s["p"] == 1.0


# ── gap 4 ───────────────────────────────────────────────────────────────────
class TestSharedGeometryChangesNothingButTheCost:
    """The optimisation is only worth having if the numbers are untouched.

    The competitive denominator is ~100% of a score's cost and does not depend
    on the candidate's floor area at all; the distances behind it do not depend
    on beta either. Both are now computed once and shared.
    """

    SITES = [(-1.2841, 36.8155), (-1.3200, 36.7400), (-1.4500, 36.6000)]
    KEYS = ("capture_pct", "adjusted_capture_pct", "cannibalisation_pct",
            "captured_population", "catchment_population", "isolated",
            "own_stores_in_catchment", "competitors_within_2km",
            "nearest_own_km", "nearest_competitor_km", "verdict", "beta")

    @pytest.mark.parametrize("size", [2500.0, 12000.0, 60000.0])
    @pytest.mark.parametrize("beta", list(S.BETA_RANGE))
    def test_a_shared_field_scores_identically(self, size, beta):
        own, rivals = _estate(), _rivals()
        for lat, lon in self.SITES:
            plain = S.score_site(lat, lon, own, rivals, size_sqft=size,
                                 beta=beta)
            field = S.build_field(lat, lon, own, rivals, beta=beta)
            shared = S.score_site(lat, lon, own, rivals, size_sqft=size,
                                  field=field)
            for k in self.KEYS:
                assert plain[k] == shared[k], k

    def test_one_geometry_serves_every_beta(self):
        own, rivals = _estate(), _rivals()
        lat, lon = self.SITES[0]
        geo = S.build_geometry(lat, lon, own, rivals)
        for beta in S.BETA_RANGE:
            plain = S.score_site(lat, lon, own, rivals, beta=beta)
            f = S.build_field(lat, lon, own, rivals, beta=beta, geometry=geo)
            shared = S.score_site(lat, lon, own, rivals, field=f)
            for k in self.KEYS:
                assert plain[k] == shared[k], f"beta={beta} {k}"

    def test_a_fields_beta_wins_over_the_argument(self):
        """A field built at one exponent cannot answer for another, and
        silently accepting the argument would report a beta the numbers do
        not come from."""
        own, rivals = _estate(), _rivals()
        lat, lon = self.SITES[0]
        f = S.build_field(lat, lon, own, rivals, beta=3.0)
        r = S.score_site(lat, lon, own, rivals, beta=1.5, field=f)
        assert r["beta"] == 3.0
        assert r["capture_pct"] == S.score_site(lat, lon, own, rivals,
                                                beta=3.0)["capture_pct"]

    def test_the_ring_fallback_still_matches(self):
        """No population grid: the same equivalence must hold on the path a
        client without one takes."""
        own, rivals = _estate(), _rivals()
        for lat, lon in self.SITES:
            plain = S.score_site(lat, lon, own, rivals)
            shared = S.score_site(lat, lon, own, rivals,
                                  field=S.build_field(lat, lon, own, rivals))
            for k in self.KEYS:
                assert plain[k] == shared[k], k


class TestTheBandIsBuiltOnce:
    def test_rank_band_reports_the_band_it_already_computed(self):
        """rank_band scored every candidate at every exponent and kept only the
        ranking; the caller then scored the same points again through
        score_band to get the width. One sweep, one summary."""
        own, rivals = _estate(), _rivals()
        sites = [{"name": "a", "lat": -1.2841, "lon": 36.8155,
                  "size_sqft": 12000.0},
                 {"name": "b", "lat": -1.3200, "lon": 36.7400,
                  "size_sqft": 12000.0}]
        rows = S.rank_band(sites, own, rivals)
        for r in rows:
            wide = S.score_band(r["lat"], r["lon"], own, rivals,
                                size_sqft=r["size_sqft"])
            for k in ("adjusted_capture_pct_low", "adjusted_capture_pct_high",
                      "beta_sensitive", "beta_span_ratio"):
                assert r[k] == wide[k], k

    def test_rank_band_honours_the_callers_default_size(self):
        """It used to fall back to the module constant while rank_sites fell
        back to the caller's argument, so one candidate could be ranked at one
        floor area and banded at another."""
        own, rivals = _estate(), _rivals()
        sites = [{"name": "a", "lat": -1.2841, "lon": 36.8155}]
        big = S.rank_band(sites, own, rivals, size_sqft=60000.0)[0]
        small = S.rank_band(sites, own, rivals, size_sqft=2500.0)[0]
        assert big["size_sqft"] == 60000.0
        assert small["size_sqft"] == 2500.0
        assert big["adjusted_capture_pct"] > small["adjusted_capture_pct"]

    def test_rank_band_applies_travel_friction(self):
        """The central value was friction-scaled and the band edges were not,
        so at high friction the estimate could sit outside its own range."""
        own, rivals = _estate(), _rivals()
        base = S.rank_band([{"name": "a", "lat": -1.2841, "lon": 36.8155,
                             "size_sqft": 12000.0}], own, rivals)[0]
        slow = S.rank_band([{"name": "a", "lat": -1.2841, "lon": 36.8155,
                             "size_sqft": 12000.0, "travel_friction": 0.8}],
                           own, rivals)[0]
        assert slow["adjusted_capture_pct"] < base["adjusted_capture_pct"]
        assert (slow["adjusted_capture_pct_low"]
                <= slow["adjusted_capture_pct"]
                <= slow["adjusted_capture_pct_high"])
