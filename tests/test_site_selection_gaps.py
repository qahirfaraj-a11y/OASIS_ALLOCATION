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


def _grid():
    """A small synthetic population surface — no WorldPop file, no network.

    Deliberately UNEVEN: a uniform sheet would make captured population a pure
    function of the share, and the beta-dependence these tests exist to check
    would partly cancel out.
    """
    from oasis.logic.population import PopulationGrid
    cells = []
    for i in range(14):
        for j in range(14):
            lat = -1.36 + i * 0.012
            lon = 36.70 + j * 0.017
            people = 400.0 + 900.0 * ((i * 7 + j * 3) % 11)
            cells.append((lat, lon, people))
    return PopulationGrid(cells, attribution="test", source="test")


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


class TestTheCapitalBandIsBuiltAtOneExponent:
    """The band edges are captured population at beta 1.5 and 3.0. They used to
    be priced with a spend per person measured only at beta 2.0 — and spend per
    person IS revenue over captured population, so the exponent was counted
    twice. Measured on a validated seven-store estate, both edges came out 1.9%
    to 7.5% low.
    """

    def test_spend_per_person_actually_varies_with_the_exponent(self):
        """If it did not, the whole defect would be cosmetic."""
        own, rivals = _estate(), _rivals()
        rev = {s["org_cd"]: 100e6 + 10e6 * i
               for i, s in enumerate(own)}
        bb = SC.calibrate_by_beta(own, rivals, rev,
                                  {k: v * 0.2 for k, v in rev.items()},
                                  population=_grid())
        spends = {b: d["calibration"].get("median_spend_per_person")
                  for b, d in bb.items()}
        assert len(bb) == len(S.BETA_RANGE)
        assert len(set(round(s or 0, 2) for s in spends.values())) > 1, spends

    def test_every_exponent_gets_its_own_calibration_and_gate(self):
        own, rivals = _estate(), _rivals()
        rev = {s["org_cd"]: 100e6 + 10e6 * i for i, s in enumerate(own)}
        bb = SC.calibrate_by_beta(own, rivals, rev,
                                  {k: v * 0.2 for k, v in rev.items()},
                                  population=_grid())
        for b in S.BETA_RANGE:
            assert float(b) in bb
            assert "calibration" in bb[float(b)]
            assert "validation" in bb[float(b)]
            assert bb[float(b)]["calibration"]["considered"] == len(own)

    def test_observations_at_different_betas_differ(self):
        own, rivals = _estate(), _rivals()
        rev = {s["org_cd"]: 100e6 for s in own}
        lo = SC.estate_observations(own, rivals, rev, beta=1.5,
                                    population=_grid())
        hi = SC.estate_observations(own, rivals, rev, beta=3.0,
                                    population=_grid())
        assert [o["captured_population"] for o in lo] != \
               [o["captured_population"] for o in hi]

    def test_a_shared_geometry_cache_changes_no_number(self):
        """calibrate_by_beta pays for the distance matrix once. If that cache
        leaked between stores the leave-one-out would be wrong."""
        own, rivals = _estate(), _rivals()
        rev = {s["org_cd"]: 100e6 + 10e6 * i for i, s in enumerate(own)}
        cache = {}
        cached = SC.estate_observations(own, rivals, rev, beta=2.5,
                                        population=_grid(), geometries=cache)
        plain = SC.estate_observations(own, rivals, rev, beta=2.5,
                                       population=_grid())
        assert [o["capture"] for o in cached] == [o["capture"] for o in plain]
        # and reusing the SAME cache at another beta must still be right
        other = SC.estate_observations(own, rivals, rev, beta=1.5,
                                       population=_grid(), geometries=cache)
        fresh = SC.estate_observations(own, rivals, rev, beta=1.5,
                                       population=_grid())
        assert [o["capture"] for o in other] == [o["capture"] for o in fresh]

    def test_rank_band_exposes_the_per_exponent_runs(self):
        """Without them the caller cannot price each edge on its own estate."""
        rows = S.rank_band([{"name": "a", "lat": -1.2841, "lon": 36.8155,
                             "size_sqft": 12000.0}], _estate(), _rivals())
        runs = rows[0]["_beta_runs"]
        assert sorted(runs) == sorted(float(b) for b in S.BETA_RANGE)
        for b, r in runs.items():
            assert r["beta"] == b

    def test_basis_stability_across_the_band_is_reported(self):
        stable = {2.0: {"validation": {"basis": "population"}},
                  1.5: {"validation": {"basis": "population"}}}
        r = SC.basis_holds_across(stable)
        assert r["basis"] == "population"
        assert r["basis_stable_across_beta"] is True
        assert r["note"] is None

        wobbly = {2.0: {"validation": {"basis": "population"}},
                  1.5: {"validation": {"basis": None}}}
        r = SC.basis_holds_across(wobbly)
        assert r["basis"] == "population"
        assert r["basis_stable_across_beta"] is False
        assert r["earns_budget_at_every_beta"] is False
        assert "whether the location earns a budget at all" in r["note"]

    def test_a_basis_that_changes_route_is_not_a_basis_that_fails(self):
        """Conflating the two overstates the milder one. Population winning at
        one exponent and affluence at another still means the geography won
        every time — measured on a real seven-store estate, which is where this
        distinction came from."""
        r = SC.basis_holds_across({
            1.5: {"validation": {"basis": "affluence"}},
            2.0: {"validation": {"basis": "affluence"}},
            2.5: {"validation": {"basis": "population"}},
            3.0: {"validation": {"basis": "population"}}})
        assert r["basis_stable_across_beta"] is False
        assert r["earns_budget_at_every_beta"] is True
        assert "earns a budget at every" in r["note"]
        assert "whether the location earns a budget at all" not in r["note"]


class TestTheIndicativeSizeIsReachable:
    """recommend_size has a branch written for an estate whose geography did
    not clear the gate — "indicative only, treat this as a ranking". No caller
    could reach it: score_sites skipped the whole call whenever the gate failed,
    which is the COMMON case and the case on the live estate. So a retailer
    whose geography had not earned a budget saw no size guidance at all, rather
    than guidance with a caveat on it.
    """

    #: A calibration good enough to size against, standing in for a real estate.
    CAL = {"usable": True, "n": 6, "median_revenue_per_sqft": 900.0,
           "median_demand": 4.0e9, "median_spend_per_person": 0.0}

    def _fn(self, share_at):
        return lambda sz: {"adjusted_capture_pct": share_at(sz),
                           "captured_population": None}

    def test_an_unvalidated_estate_still_gets_rungs_and_a_caveat(self):
        r = SC.recommend_size(self._fn(lambda sz: 0.5 + sz / 4000.0),
                              self.CAL, {"validated": False,
                                         "reason": "the location beat the "
                                         "simpler predictor by only 3% - too "
                                         "close to call - and capital is "
                                         "proposed from productivity instead"})
        assert len(r["rungs"]) == len(SC.SIZE_LADDER)
        assert r["calibrated"] is False
        assert r["note"].startswith("Indicative only")

    def test_the_caveat_names_which_way_the_gate_failed(self):
        """"Beat the median but lost on most of your stores" is a different
        warning from "did not beat floor area at all", and the operator can act
        on the difference."""
        outlier = SC.recommend_size(
            self._fn(lambda sz: 1.0), self.CAL,
            {"validated": False,
             "reason": "the location beat the simpler predictor by 26% on the "
                       "median but lost on 3 of 5 of your stores, so the win "
                       "rests on one or two outliers - capital is proposed "
                       "from productivity instead"})
        assert "lost on 3 of 5 of your stores" in outlier["note"]
        assert "capital is proposed" not in outlier["note"], \
            "the trailing clause is about capital, not about size"

        nothing = SC.recommend_size(
            self._fn(lambda sz: 1.0), self.CAL,
            {"validated": False,
             "reason": "the location adds nothing over the simpler predictor "
                       "on this estate - capital is proposed from productivity "
                       "instead"})
        assert "adds nothing over the simpler predictor" in nothing["note"]

    def test_it_still_names_the_largest_format_that_clears(self):
        """The caveat is about confidence, not about refusing to answer. Huff
        saturates, so revenue per square foot falls as the store grows and
        there is a real crossing against the estate's own productivity."""
        # capture rises sub-linearly with size, so rev/sqft falls across rungs
        r = SC.recommend_size(self._fn(lambda sz: 3.0 * (sz / 10000.0) ** 0.5),
                              self.CAL, {"validated": False})
        clearing = [x["size_sqft"] for x in r["rungs"] if x["clears_estate"]]
        assert clearing, "no rung cleared - the test cannot see the branch"
        assert r["recommended_sqft"] == max(clearing)
        assert r["format"] == SC.SIZE_FORMATS[r["recommended_sqft"]]

    def test_revenue_per_sqft_falls_as_the_store_grows(self):
        """The saturation that makes this non-circular. If it rose, the answer
        would just be "build the biggest one" every time."""
        r = SC.recommend_size(self._fn(lambda sz: 3.0 * (sz / 10000.0) ** 0.5),
                              self.CAL, {"validated": False})
        rps = [x["revenue_per_sqft"] for x in r["rungs"]]
        assert rps == sorted(rps, reverse=True), rps

    def test_no_estate_still_refuses(self):
        r = SC.recommend_size(self._fn(lambda sz: 5.0),
                              {"usable": False}, {"validated": False})
        assert r["recommended_sqft"] is None
        assert r["rungs"] == []

    def test_score_sites_attaches_it_even_when_the_gate_fails(self,
                                                              monkeypatch):
        """The regression that mattered: the call was skipped entirely."""
        estate = _estate()
        # revenue proportional to floor area, so floor area wins and the
        # geography cannot clear the gate
        rev = {s["org_cd"]: s["size_sqft"] * 14000.0 for s in estate}
        monkeypatch.setattr(D, "store_map", lambda root=None: {
            "located": estate, "missing": [], "orphaned": [],
            "saved_total": len(estate)})
        monkeypatch.setattr(D, "estate_economics",
                            lambda days=90, root=None, refresh=False: {
                                "revenue": rev,
                                "stock_value": {k: v * 0.22
                                                for k, v in rev.items()},
                                "days": 90, "error": None})
        monkeypatch.setattr(D, "competitor_set", lambda root=None: {
            "rows": _rivals(), "attribution": "test", "error": None})
        monkeypatch.setattr(D, "population_set", lambda root=None: {
            "grid": _grid(), "rows": 196, "people": 100000.0})
        monkeypatch.setattr(D, "affluence_set", lambda root=None: {"grid": None})
        res = D.score_sites([{"name": "n1", "lat": -1.2841, "lon": 36.8155,
                              "size_sqft": 12000.0}])
        assert res["error"] is None, res["error"]
        assert res["validation"]["validated"] is False
        site = res["sites"][0]
        assert "size_recommendation" in site
        sz = site["size_recommendation"]
        assert sz["calibrated"] is False
        assert len(sz["rungs"]) == len(SC.SIZE_LADDER)
        assert sz["note"].startswith("Indicative only")
        # and the capital band is still withheld, which is a separate refusal
        assert site["capital"]["beta_varies"] is False
        assert site["capital"]["beta_low"] is None


class TestScoringWithoutRevenue:
    """Sales data exists only for the chain running OASIS. Every competitor —
    and every prospect being shown what the system would say before connecting
    anything — has none, and never will. The capital chain is defined on money,
    so it correctly refuses; what survives is the geography.
    """

    def test_the_revenue_path_yields_nothing_without_sales(self):
        """The premise. estate_observations skips any store with no revenue,
        so for a competitor it returns an empty set — correctly."""
        obs = SC.estate_observations(_estate(), _rivals(), {},
                                     population=_grid())
        assert list(obs) == []
        assert len(obs.skipped) == len(_estate())
        assert all(s["reason"] == "no-sales" for s in obs.skipped)

    def test_the_catchment_path_calibrates_on_the_same_estate(self):
        obs = SC.catchment_observations(_estate(), _rivals(),
                                        population=_grid())
        assert len(obs) == len(_estate())
        cal = SC.calibrate_catchment(obs)
        assert cal["usable"] is True
        assert cal["basis"] == "catchment"
        assert cal["median_people_per_sqft"] > 0
        assert "median_revenue_per_sqft" not in cal
        for o in obs:
            assert "revenue" not in o
            assert o["people_per_sqft"] == o["captured_population"] / o["size_sqft"]

    def test_an_unplaced_store_is_still_accounted_for(self):
        estate = _estate()
        estate[0] = dict(estate[0], lat=None, lon=None)
        obs = SC.catchment_observations(estate, _rivals(), population=_grid())
        assert [s["reason"] for s in obs.skipped] == ["not-placed"]

    def test_no_population_grid_means_no_catchment_to_measure(self):
        """Without a grid there is no headcount, so there is no anchor. Say so
        rather than fall back to something dimensionless."""
        obs = SC.catchment_observations(_estate(), _rivals(), population=None)
        assert list(obs) == []
        assert all(s["reason"] == "no-catchment" for s in obs.skipped)
        assert SC.calibrate_catchment(obs)["usable"] is False

    def test_the_size_recommendation_is_bounded_by_what_the_chain_operates(self):
        """Unbounded, the ratio test recommended a 60,000 sq ft flagship to a
        chain whose largest branch is 4,246 — a fourteen-fold extrapolation
        dressed as a finding."""
        cal = {"usable": True, "median_people_per_sqft": 1.0,
               "max_sqft": 4246.0, "sizes_are_uniform": True}
        rec = SC.recommend_size_by_catchment(
            lambda sz: {"captured_population": 200_000.0}, cal)
        assert rec["ceiling_sqft"] == round(SC.FORMAT_STRETCH * 4246.0)
        assert max(r["size_sqft"] for r in rec["rungs"]) <= rec["ceiling_sqft"]
        assert rec["recommended_sqft"] <= rec["ceiling_sqft"]
        assert "largest branch this chain operates" in rec["note"]

    def test_a_uniform_footprint_is_flagged_as_a_default(self):
        """All 14 branches on file at one area is a chain-level default, not a
        set of measurements, and the operator can fix it."""
        cal = SC.calibrate_catchment(
            SC.catchment_observations(_estate(), _rivals(),
                                      population=_grid()))
        assert cal["sizes_are_uniform"] is False, "the fixture varies sizes"
        uniform = SC.calibrate_catchment(SC.catchment_observations(
            [dict(s, size_sqft=4246.0) for s in _estate()], _rivals(),
            population=_grid()))
        assert uniform["sizes_are_uniform"] is True
        rec = SC.recommend_size_by_catchment(
            lambda sz: {"captured_population": 50_000.0}, uniform)
        assert "chain default rather than a measurement" in rec["note"]

    def test_a_chain_smaller_than_the_ladder_still_gets_an_answer(self):
        cal = {"usable": True, "median_people_per_sqft": 0.1,
               "max_sqft": 400.0}
        rec = SC.recommend_size_by_catchment(
            lambda sz: {"captured_population": 90_000.0}, cal)
        assert len(rec["rungs"]) == 1
        assert rec["rungs"][0]["size_sqft"] == min(SC.SIZE_LADDER)

    def test_it_never_produces_a_money_figure(self):
        """The whole point. Anyone reading a budget out of this has been
        misled, so there must be nothing budget-shaped to read."""
        cal = SC.calibrate_catchment(
            SC.catchment_observations(_estate(), _rivals(),
                                      population=_grid()))
        rec = SC.recommend_size_by_catchment(
            lambda sz: {"captured_population": 50_000.0}, cal)
        for blob in (cal, rec):
            for k in blob:
                assert "revenue" not in k and "capital" not in k, k
                assert "spend" not in k and "demand" not in k, k


class TestConcurrentSalesReadsAreSingleFlighted:
    """A web request that "hung on the POS" was four identical reads
    contending, not a deadlock. Measured against the live database:

        1 concurrent read     14.5 s
        2 concurrent reads    50.3 s   (3.5x)
        4 concurrent reads   221.4 s   (15.3x)

    The console polls jobs while pages load, so two surfaces asking at once is
    the normal case. The reads are identical and idempotent, so only one may
    run at a time and the rest take its answer.
    """

    def _slow_reader(self, monkeypatch, calls):
        import time as _t

        def fake(days, root, key):
            calls.append(1)
            _t.sleep(0.35)
            out = {"revenue": {"A": 1.0}, "stock_value": {"A": 0.2},
                   "days": days, "error": None}
            D._ESTATE_ECON_CACHE[key] = out
            D._ECON_GEN[key] = D._ECON_GEN.get(key, 0) + 1
            return out

        monkeypatch.setattr(D, "_read_estate_economics", fake)
        D._ESTATE_ECON_CACHE.clear()
        D._ECON_GEN.clear()
        D._ECON_LOCKS.clear()

    def test_eight_callers_cause_one_read(self, monkeypatch):
        import concurrent.futures as cf
        calls = []
        self._slow_reader(monkeypatch, calls)
        with cf.ThreadPoolExecutor(max_workers=8) as ex:
            out = [f.result() for f in
                   [ex.submit(D.estate_economics, 90, None) for _ in range(8)]]
        assert len(calls) == 1, f"{len(calls)} reads for 8 callers"
        assert all(o["revenue"] == {"A": 1.0} for o in out)

    def test_concurrent_refreshes_also_collapse(self, monkeypatch):
        """A refresh that arrives while one is in flight wants fresh data, and
        the answer landing a moment later is exactly that."""
        import concurrent.futures as cf
        calls = []
        self._slow_reader(monkeypatch, calls)
        with cf.ThreadPoolExecutor(max_workers=6) as ex:
            [f.result() for f in
             [ex.submit(D.estate_economics, 90, None, True) for _ in range(6)]]
        assert len(calls) == 1, f"{len(calls)} reads for 6 refreshes"

    def test_a_later_refresh_still_re_reads(self, monkeypatch):
        """Single-flight must not turn into a permanent cache: a refresh asked
        for after the previous one finished has to do the work."""
        calls = []
        self._slow_reader(monkeypatch, calls)
        D.estate_economics(90, None)
        D.estate_economics(90, None, True)
        assert len(calls) == 2

    def test_a_plain_call_after_a_read_uses_the_cache(self, monkeypatch):
        calls = []
        self._slow_reader(monkeypatch, calls)
        D.estate_economics(90, None)
        D.estate_economics(90, None)
        assert len(calls) == 1

    def test_different_windows_do_not_block_each_other(self, monkeypatch):
        """The lock is per cache key, not global — 30 days and 90 days are
        different questions."""
        calls = []
        self._slow_reader(monkeypatch, calls)
        D.estate_economics(30, None)
        D.estate_economics(90, None)
        assert len(calls) == 2


class TestTheSizeExponent:
    """Huff's A = S^alpha. The literature estimates alpha alongside beta — the
    standard estimator recovers both from observed market SHARES, which matters
    here because shares are the one calibration input obtainable for a
    competitor whose sales will never be visible.

    OASIS fixed it at 1.0 and never named it. Now a parameter, and measured:
    across 0.6 to 1.2 the top ten of a 200-site pool moved 0.2 to 0.4 places.
    Immaterial to the ordering, like beta, and for the same reason — it scales
    the whole field rather than reordering it.
    """

    def test_alpha_one_is_proportional_to_floor_area(self):
        assert S.attractiveness(20000, alpha=1.0) == 2 * S.attractiveness(
            10000, alpha=1.0)

    def test_below_one_saturates_and_above_one_amplifies(self):
        """The economic content: at alpha below 1 the second half of a big shop
        adds less draw than the first."""
        def ratio(a):
            return S.attractiveness(40000.0, alpha=a) / S.attractiveness(
                5000.0, alpha=a)
        assert ratio(0.6) < ratio(1.0) < ratio(1.2)
        assert ratio(1.0) == pytest.approx(8.0)

    def test_the_default_changes_nothing(self):
        """Shipped behaviour must be untouched: alpha 1.0 is the old
        arithmetic, not a re-derivation of it."""
        assert S.SIZE_EXPONENT == 1.0
        for sz in (0.0, 2500.0, 12345.0, 60000.0):
            assert S.attractiveness(sz) == S.attractiveness(sz, alpha=1.0)
        # and through a whole score, against every reported field
        own, rivals = _estate(), _rivals()
        a = S.score_site(-1.28, 36.81, own, rivals, population=_grid())
        b = S.score_site(-1.28, 36.81, own, rivals, population=_grid(),
                         alpha=1.0)
        for k in ("capture_pct", "captured_population", "cannibalisation_pct",
                  "verdict"):
            assert a[k] == b[k], k

    def test_alpha_reaches_the_score_and_moves_it(self):
        own, rivals = _estate(), _rivals()
        lo = S.score_site(-1.28, 36.81, own, rivals, size_sqft=40000.0,
                          alpha=0.6, population=_grid())
        hi = S.score_site(-1.28, 36.81, own, rivals, size_sqft=40000.0,
                          alpha=1.2, population=_grid())
        assert lo["alpha"] == 0.6 and hi["alpha"] == 1.2
        assert lo["capture_pct"] != hi["capture_pct"]

    def test_the_entrant_is_sized_on_the_field_it_competes_in(self):
        """Both sides of a Huff share must use one exponent. Sizing the entrant
        at alpha 1 against rivals raised to 0.6 compares two different
        quantities and the share means nothing."""
        own, rivals = _estate(), _rivals()
        for a in (0.6, 1.2):
            f = S.build_field(-1.28, 36.81, own, rivals, alpha=a,
                              population=_grid())
            assert f.alpha == a
            assert S.score_site(-1.28, 36.81, own, rivals,
                                field=f)["alpha"] == a

    def test_a_geometrys_alpha_wins_over_the_argument(self):
        """Alpha is baked into a geometry's stored attractiveness, so it cannot
        be re-specified later — the same rule as a field's beta, and the same
        trap if it were silent."""
        own, rivals = _estate(), _rivals()
        geo = S.build_geometry(-1.28, 36.81, own, rivals, alpha=0.6,
                               population=_grid())
        f = S.build_field(-1.28, 36.81, own, rivals, geometry=geo, alpha=1.2)
        assert f.alpha == 0.6
        assert S.score_site(-1.28, 36.81, own, rivals,
                            field=f)["alpha"] == 0.6

    def test_one_geometry_serves_every_beta_at_a_fixed_alpha(self):
        """The two parameters are independent: a beta sweep must not have to
        rebuild the distance matrix just because alpha is not 1."""
        own, rivals = _estate(), _rivals()
        geo = S.build_geometry(-1.28, 36.81, own, rivals, alpha=0.8,
                               population=_grid())
        for b in S.BETA_RANGE:
            direct = S.score_site(-1.28, 36.81, own, rivals, beta=b,
                                  alpha=0.8, population=_grid())
            shared = S.score_site(-1.28, 36.81, own, rivals, field=S.build_field(
                -1.28, 36.81, own, rivals, beta=b, geometry=geo))
            assert direct["capture_pct"] == shared["capture_pct"], b
            assert shared["alpha"] == 0.8

    def test_the_published_range_brackets_the_default(self):
        assert min(S.ALPHA_RANGE) < S.SIZE_EXPONENT < max(S.ALPHA_RANGE)


class TestTheDataBoundaryIsNotAnOpportunity:
    """A candidate near the edge of a fetched region has its catchment cut off
    by the DOWNLOAD, not by geography: its people are missing and so are its
    rivals, so it scores as gloriously uncontested.

    Measured on a real shortlist: four sites 3.5 km from the grid edge reported
    28-58% capture against 0-2 rivals within 10 km and went straight into a
    client's top twelve. Every one was an artefact of where the download
    stopped. This is the SUPPLY_KM truncation bug one layer out.
    """

    def test_a_point_in_the_middle_is_covered(self):
        g = _grid()
        mid_lat = (g.south + g.north) / 2
        mid_lon = (g.west + g.east) / 2
        assert g.covers(mid_lat, mid_lon, 1.0)

    def test_a_point_at_the_edge_is_not(self):
        g = _grid()
        assert not g.covers(g.south, g.west, 5.0)
        assert not g.covers(g.north, g.east, 5.0)

    def test_the_edge_distance_is_reported(self):
        g = _grid()
        assert g.edge_distance_km(g.south, g.west) == pytest.approx(0.0,
                                                                    abs=1e-6)
        mid_lat = (g.south + g.north) / 2
        mid_lon = (g.west + g.east) / 2
        assert g.edge_distance_km(mid_lat, mid_lon) > 5.0

    def test_an_empty_grid_covers_nothing(self):
        from oasis.logic.population import PopulationGrid
        empty = PopulationGrid()
        assert empty.covers(0.0, 0.0, 1.0) is False
        assert empty.edge_distance_km(0.0, 0.0) == 0.0


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
