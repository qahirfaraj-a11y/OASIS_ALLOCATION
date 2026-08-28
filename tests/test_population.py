"""Tests for catchment population — the denominator site scoring never had.

The point of population data is that it converts a SHARE into a HEADCOUNT. So
the tests that matter are: the maths still reduces to the old behaviour when
nobody has loaded a grid, and it separates two catchments the old score could
not tell apart.
"""

import io
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import population as P
from oasis.logic import site_capital as SC
from oasis.logic import site_scoring as SS

OWN = [{"lat": -1.2650, "lon": 36.8020, "size_sqft": 12000}]
RIVALS = [{"lat": -1.2700, "lon": 36.8100, "size_sqft": 30000,
           "chain": "BigBox"}]
LAT, LON = -1.2750, 36.8050


def _blanket(density: float, span: int = 8, step: float = 0.01):
    """A uniform grid of `density` people per cell around the test site."""
    return P.PopulationGrid([(LAT + i * step, LON + j * step, density)
                             for i in range(-span, span + 1)
                             for j in range(-span, span + 1)])


class TestParsing:
    def test_column_spellings_are_accepted(self):
        """Exports differ by tool; a client should not rename columns."""
        for lat_k, lon_k, pop_k in (("latitude", "longitude", "population"),
                                    ("lat", "lon", "pop"),
                                    ("Y", "X", "VALUE"),
                                    ("Lat", "Lng", "persons")):
            cells, _ = P.parse_rows([{lat_k: "-1.3", lon_k: "36.8",
                                      pop_k: "250"}])
            assert cells == [(-1.3, 36.8, 250.0)], f"{lat_k}/{lon_k}/{pop_k}"

    def test_unusable_rows_are_dropped_not_guessed(self):
        cells, _ = P.parse_rows([
            {"lat": "-1.3", "lon": "36.8", "pop": "250"},   # good
            {"lat": "-1.3", "lon": "36.8"},                  # no population
            {"lat": "", "lon": "36.8", "pop": "10"},         # no latitude
            {"lat": "-1.3", "lon": "36.8", "pop": "0"},      # empty cell
            {"lat": "-1.3", "lon": "36.8", "pop": "-5"},     # nonsense
            {"lat": "999", "lon": "36.8", "pop": "10"},      # off the planet
            {"lat": "x", "lon": "36.8", "pop": "10"},        # unparseable
        ])
        assert cells == [(-1.3, 36.8, 250.0)]

    def test_attribution_follows_the_source(self):
        assert "WorldPop" in P.attribution_for(["WorldPop 2020"])
        assert "GHSL" in P.attribution_for(["GHS-POP R2023"])
        assert "Kenya National Bureau" in P.attribution_for(["KNBS 2019"])

    def test_an_unknown_source_still_gets_an_attribution_line(self):
        """The obligation to attribute does not vanish when we cannot name it."""
        assert P.attribution_for(["something local"]) == P.GENERIC_ATTRIBUTION
        assert P.attribution_for([]) == P.GENERIC_ATTRIBUTION


class TestGrid:
    def test_near_respects_the_radius(self):
        g = P.PopulationGrid([(-1.2750, 36.8050, 100.0),     # at the point
                              (-1.2850, 36.8050, 100.0),     # ~1.1 km
                              (-1.9000, 37.4000, 100.0)])    # ~90 km
        assert len(g.near(LAT, LON, 5.0)) == 2
        assert len(g.near(LAT, LON, 0.5)) == 1
        assert len(g.near(LAT, LON, 200.0)) == 3

    def test_the_index_finds_what_a_linear_scan_would(self):
        """A bucketed lookup that quietly misses cells would understate
        every catchment it touches."""
        cells = [(LAT + i * 0.02, LON + j * 0.02, 10.0)
                 for i in range(-12, 13) for j in range(-12, 13)]
        g = P.PopulationGrid(cells)
        brute = [c for c in cells
                 if P.haversine_km(LAT, LON, c[0], c[1]) <= 10.0]
        assert len(g.near(LAT, LON, 10.0)) == len(brute)

    def test_population_within_totals_the_people(self):
        g = _blanket(100.0)
        assert g.population_within(LAT, LON, 10.0) > 0
        assert g.population_within(-40.0, 100.0, 10.0) == 0


class TestWeights:
    def test_no_grid_gives_every_point_the_same_vote(self):
        pts = SS._ring_points(LAT, LON)
        assert P.weights_for_points(None, LAT, LON, pts, 10.0) == [1.0] * len(pts)

    def test_the_partition_is_exact(self):
        """Every person is counted once: the weights must sum to the catchment
        population, no more and no less."""
        g = _blanket(100.0)
        pts = SS._ring_points(LAT, LON)
        w = P.weights_for_points(g, LAT, LON, pts, 10.0)
        assert abs(sum(w) - g.population_within(LAT, LON, 10.0)) < 1e-6

    def test_an_empty_region_weighs_nothing(self):
        g = _blanket(100.0)
        pts = SS._ring_points(-40.0, 100.0)
        assert sum(P.weights_for_points(g, -40.0, 100.0, pts, 10.0)) == 0.0


class TestScoringIsAStrictExtension:
    def test_equal_weights_reproduce_the_unweighted_score_exactly(self):
        """The invariant that makes population safe to add: a client who has
        loaded no grid sees no number change."""
        pts = SS._ring_points(LAT, LON)
        g = P.PopulationGrid([(p[0], p[1], 500.0) for p in pts])
        plain = SS.score_site(LAT, LON, OWN, RIVALS, size_sqft=20000)
        with_pop = SS.score_site(LAT, LON, OWN, RIVALS, size_sqft=20000,
                                 population=g)
        assert abs(plain["capture_pct"] - with_pop["capture_pct"]) < 1e-9

    def test_no_grid_reports_no_headcount(self):
        """A caller must never be able to mistake an unweighted score for
        people."""
        r = SS.score_site(LAT, LON, OWN, RIVALS)
        assert r["captured_population"] is None
        assert r["catchment_population"] is None
        assert r["has_population"] is False


class TestTheFailurePopulationFixes:
    def test_identical_share_different_people(self):
        """THE defect. Two catchments with the same competition score the same
        share; only a headcount can separate them."""
        dense = SS.score_site(LAT, LON, OWN, RIVALS, size_sqft=20000,
                              population=_blanket(4000.0))
        sparse = SS.score_site(LAT, LON, OWN, RIVALS, size_sqft=20000,
                               population=_blanket(5.0))
        assert abs(dense["capture_pct"] - sparse["capture_pct"]) < 1e-9
        assert dense["captured_population"] > sparse["captured_population"] * 100

    def test_empty_land_is_measured_empty_not_inferred_empty(self):
        """Before population this could only be guessed from an empty
        competitive field, which is why a desert and an underserved suburb
        scored alike."""
        r = SS.score_site(-1.9, 37.4, OWN, RIVALS, size_sqft=60000,
                          population=_blanket(4000.0))
        assert r["captured_population"] == 0
        assert r["isolated"] is True
        assert "nobody lives" in r["verdict"].lower()

    def test_ranking_prefers_people_over_share(self):
        """A large share of a deserted valley must not outrank a smaller share
        of a dense suburb."""
        cells = ([(-1.2750 + i * 0.01, 36.8050 + j * 0.01, 3000.0)
                  for i in range(-6, 7) for j in range(-6, 7)]
                 + [(-1.6000 + i * 0.01, 37.1000 + j * 0.01, 3.0)
                    for i in range(-6, 7) for j in range(-6, 7)])
        g = P.PopulationGrid(cells)
        ranked = SS.rank_sites(
            [{"name": "empty valley", "lat": -1.6000, "lon": 37.1000},
             {"name": "dense suburb", "lat": -1.2750, "lon": 36.8050}],
            OWN, RIVALS, population=g)
        assert ranked[0]["name"] == "dense suburb"
        # ...and share alone would have ordered them the other way.
        assert (ranked[1]["adjusted_capture_pct"]
                > ranked[0]["adjusted_capture_pct"])


def _obs(org, size, capture, revenue, people=None):
    return {"org_cd": org, "name": org, "size_sqft": float(size),
            "capture": capture, "revenue": float(revenue),
            "stock_value": revenue * 0.3,
            "captured_population": people,
            "catchment_population": (people * 3 if people else None),
            "spend_per_person": (revenue / people if people else None),
            "implied_demand": revenue / capture,
            "revenue_per_sqft": revenue / size}


class TestPopulationInTheGate:
    def test_population_wins_when_revenue_tracks_people(self):
        """Constant spend of 50 per person; capture and floor area carry no
        signal."""
        obs = [_obs("A", 5000, 0.40, 500_000, 10_000),
               _obs("B", 30000, 0.12, 1_000_000, 20_000),
               _obs("C", 8000, 0.31, 1_500_000, 30_000),
               _obs("D", 25000, 0.09, 2_000_000, 40_000),
               _obs("E", 12000, 0.22, 2_500_000, 50_000)]
        val = SC.loo_validate(obs)
        assert val["validated"] is True
        assert val["basis"] == "population"
        assert val["mape_population"] < val["mape_sqft_only"]
        assert "improved the forecast" in val["population_note"]

    def test_population_is_rejected_when_it_is_noise_and_says_so(self):
        """Buying a grid does not automatically make a forecast good. If spend
        per person is erratic the operator must be told, not sold."""
        obs = [_obs("A", 0.4, 0.40, 50_000, 900_000),
               _obs("B", 0.4, 0.12, 100_000, 3_000),
               _obs("C", 0.4, 0.31, 200_000, 640_000),
               _obs("D", 0.4, 0.09, 400_000, 1_200),
               _obs("E", 0.4, 0.22, 300_000, 810_000)]
        val = SC.loo_validate(obs)
        assert val["basis"] != "population"
        assert "did not improve" in (val.get("population_note") or "")

    def test_calibration_reports_spend_per_person_only_when_measured(self):
        with_pop = SC.calibrate([_obs("A", 5000, 0.4, 500_000, 10_000)])
        assert with_pop["has_population"] is True
        assert abs(with_pop["median_spend_per_person"] - 50.0) < 1e-9
        without = SC.calibrate([_obs("A", 5000, 0.4, 500_000)])
        assert without["has_population"] is False
        assert without["median_spend_per_person"] == 0.0


class TestCapitalOnPeople:
    def _fixture(self):
        obs = [_obs("A", 5000, 0.40, 500_000, 10_000),
               _obs("B", 30000, 0.12, 1_000_000, 20_000),
               _obs("C", 8000, 0.31, 1_500_000, 30_000),
               _obs("D", 25000, 0.09, 2_000_000, 40_000),
               _obs("E", 12000, 0.22, 2_500_000, 50_000)]
        return SC.calibrate(obs), SC.loo_validate(obs)

    def test_capital_is_people_times_measured_spend(self):
        cal, val = self._fixture()
        p = SC.propose_capital(25.0, 10_000, cal, val,
                               captured_population=12_000)
        assert p["basis"] == "population-calibrated"
        assert abs(p["expected_revenue"] - 12_000 * 50.0) < 1.0
        assert "people" in p["note"]

    def test_two_sites_with_equal_share_get_different_capital(self):
        """The whole point: capital now follows the headcount, not the share."""
        cal, val = self._fixture()
        big = SC.propose_capital(25.0, 10_000, cal, val,
                                 captured_population=40_000)
        small = SC.propose_capital(25.0, 10_000, cal, val,
                                   captured_population=2_000)
        assert big["expected_revenue"] > small["expected_revenue"] * 10

    def test_a_site_with_no_people_is_refused_a_budget(self):
        cal, val = self._fixture()
        p = SC.propose_capital(100.0, 60_000, cal, val, isolated=True,
                               captured_population=0.0)
        assert p["basis"] == "insufficient-data"
        assert p["opening_capital"] is None


class TestDensityIsNotAHeadcount:
    """WorldPop's Z column is persons per SQUARE KILOMETRE, and its "1 km"
    cells are 30 arc-seconds — 0.9277 km at the equator, narrowing with the
    cosine of latitude. Reading density as a count overstates Kenya by 17%
    (63.1M against a UN estimate of 53.8M); with the area correction it comes
    to 54.2M, within 0.9%. Everything downstream deals in people, so this
    conversion has to be right.
    """

    def test_a_cell_is_not_one_square_kilometre(self):
        step = 1.0 / 120.0                       # 30 arc-seconds
        equator = P.cell_area_km2(0.0, step)
        assert 0.85 < equator < 0.87, equator

    def test_cells_narrow_towards_the_poles(self):
        step = 1.0 / 120.0
        assert P.cell_area_km2(60.0, step) < P.cell_area_km2(0.0, step) * 0.55
        # Nairobi is close enough to the equator that the correction is small,
        # which is exactly why an unconverted grid looks plausible there.
        assert abs(P.cell_area_km2(-1.28, step)
                   - P.cell_area_km2(0.0, step)) < 0.001

    def test_density_converts_to_a_count(self):
        step = 1.0 / 120.0
        people = P.density_to_count(1000.0, 0.0, step)
        assert abs(people - 1000.0 * P.cell_area_km2(0.0, step)) < 1e-9
        assert people < 1000.0, "an uncorrected read would return 1000"

    def test_negative_density_cannot_become_negative_people(self):
        assert P.density_to_count(-50.0, 0.0, 1.0 / 120.0) == 0.0

    def test_the_step_is_read_off_the_data_not_assumed(self):
        rows = [{"X": "35.0000", "Y": "5.0", "Z": "1"},
                {"X": "35.0250", "Y": "5.0", "Z": "1"},
                {"X": "35.0500", "Y": "5.0", "Z": "1"}]
        assert abs(P.infer_step_deg(rows) - 0.025) < 1e-9

    def test_an_unreadable_grid_falls_back_to_thirty_arc_seconds(self):
        assert abs(P.infer_step_deg([]) - 1.0 / 120.0) < 1e-12


class TestLoading:
    def test_an_absent_grid_is_not_an_error_state(self, tmp_path):
        res = P.load_population(root=str(tmp_path))
        assert res["rows"] == 0
        assert not res["grid"]
        assert "No population data" in res["error"]

    def test_a_real_file_loads_with_its_attribution(self, tmp_path):
        d = tmp_path / "oasis" / "data"
        d.mkdir(parents=True)
        with io.open(d / P.CACHE_FILE, "w", encoding="utf-8", newline="") as f:
            f.write("latitude,longitude,population,source\n")
            f.write("-1.2750,36.8050,1200,WorldPop\n")
            f.write("-1.2850,36.8150,800,WorldPop\n")
        res = P.load_population(root=str(tmp_path))
        assert res["rows"] == 2 and res["people"] == 2000.0
        assert "WorldPop" in res["attribution"]
        assert res["error"] is None
        assert "2,000 people" in P.summarise(res)

    def test_a_file_of_unusable_rows_reports_that_clearly(self, tmp_path):
        d = tmp_path / "oasis" / "data"
        d.mkdir(parents=True)
        with io.open(d / P.CACHE_FILE, "w", encoding="utf-8", newline="") as f:
            f.write("a,b,c\n1,2,3\n")
        res = P.load_population(root=str(tmp_path))
        assert res["rows"] == 0
        assert "none carried a usable" in res["error"]
