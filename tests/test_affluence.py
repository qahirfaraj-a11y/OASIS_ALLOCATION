"""Tests for the catchment spend model.

This layer is deliberately GATED OFF after measurement: on five real Nairobi
catchments the OSM proxy fitted with the wrong sign and then lost its own
leave-one-out gate. The tests below pin both the mechanics and the guards that
keep an in-sample R2 from setting a budget.
"""

import io
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import affluence as A
from oasis.logic import site_capital as SC
from oasis.logic.population import PopulationGrid


class TestClassification:
    def test_discretionary_and_staple_are_separated(self):
        assert A.classify({"amenity": "bank"}) == "discretionary"
        assert A.classify({"shop": "mall"}) == "discretionary"
        assert A.classify({"tourism": "hotel"}) == "discretionary"
        assert A.classify({"shop": "convenience"}) == "staple"
        assert A.classify({"amenity": "pharmacy"}) == "staple"

    def test_irrelevant_tags_are_not_counted(self):
        assert A.classify({"amenity": "bench"}) is None
        assert A.classify({}) is None

    def test_a_supermarket_is_a_staple_not_a_luxury(self):
        """Groceries are bought everywhere at roughly population rate, so a
        supermarket carries no affluence signal — and counting our own trade
        as evidence of wealth would be circular."""
        assert A.classify({"shop": "supermarket"}) == "staple"


class TestTheIndexIsPerCapita:
    """The whole trick. Raw counts measure how urban a place is, not how rich;
    a dense low-income suburb has far more shops than an affluent sparse one.
    """

    def _grid(self, n_disc, lat=-1.28, lon=36.80):
        return A.AffluenceGrid([(lat + i * 0.001, lon, "discretionary")
                                for i in range(n_disc)])

    def _people(self, total, lat=-1.28, lon=36.80):
        # spread over a few cells so the catchment lookup finds them
        return PopulationGrid([(lat + i * 0.002, lon, total / 10.0)
                               for i in range(10)])

    def test_more_shops_but_more_people_scores_lower(self):
        dense = self._grid(50).index_at(-1.28, 36.80, self._people(500_000))
        sparse = self._grid(10).index_at(-1.28, 36.80, self._people(20_000))
        assert dense["discretionary"] > sparse["discretionary"]
        assert dense["index"] < sparse["index"], \
            "a raw count would rank the dense suburb higher"

    def test_a_thin_catchment_returns_none_not_zero(self):
        """Unknown must never be readable as poor."""
        r = self._grid(5).index_at(-1.28, 36.80, self._people(100))
        assert r["index"] is None

    def test_no_population_grid_means_no_index(self):
        r = self._grid(5).index_at(-1.28, 36.80, None)
        assert r["index"] is None
        assert r["discretionary"] == 5

    def test_composition_is_population_free(self):
        g = A.AffluenceGrid([(-1.28, 36.80, "discretionary"),
                             (-1.28, 36.80, "discretionary"),
                             (-1.28, 36.80, "staple")])
        r = g.index_at(-1.28, 36.80, None)
        assert abs(r["composition"] - 2 / 3) < 1e-9


class TestTheSpendModel:
    def _pairs(self, n, b=0.5, a=3.0):
        return [(float(i), math.exp(a + b * i)) for i in range(n)]

    def test_it_recovers_a_known_slope(self):
        m = A.fit_spend_model(self._pairs(8, b=0.5, a=3.0))
        assert m is not None
        assert abs(m["b"] - 0.5) < 1e-6 and abs(m["a"] - 3.0) < 1e-6
        assert m["r2"] > 0.999

    def test_too_few_stores_is_refused(self):
        """MEASURED: two parameters on four points reported R2 0.61 and then
        came LAST of five predictors under leave-one-out, at 46.8% median
        error against 24.9% for floor area alone."""
        assert A.MIN_STORES_FOR_SLOPE >= 6, \
            "raising this was a measured decision; see the module docstring"
        assert A.fit_spend_model(self._pairs(5)) is None
        assert A.fit_spend_model(self._pairs(A.MIN_STORES_FOR_SLOPE)) is not None

    def test_a_single_x_value_cannot_support_a_slope(self):
        """A fit through one x is a horizontal line pretending to be a model."""
        assert A.fit_spend_model([(1.0, 10.0)] * 9) is None

    def test_non_positive_spend_is_dropped_not_logged(self):
        pairs = self._pairs(8) + [(9.0, 0.0), (10.0, -5.0)]
        m = A.fit_spend_model(pairs)
        assert m is not None and m["n"] == 8

    def test_prediction_falls_back_without_a_model_or_an_index(self):
        m = A.fit_spend_model(self._pairs(8))
        assert A.predict_spend(None, 1.0, fallback=42.0) == 42.0
        assert A.predict_spend(m, None, fallback=42.0) == 42.0
        assert A.predict_spend(m, 0.0) > 0


def _obs(org, size, capture, revenue, people, aff):
    return {"org_cd": org, "name": org, "size_sqft": float(size),
            "capture": capture, "revenue": float(revenue),
            "stock_value": revenue * 0.3, "captured_population": people,
            "catchment_population": people * 3, "affluence_index": aff,
            "spend_per_person": revenue / people,
            "implied_demand": revenue / capture,
            "revenue_per_sqft": revenue / size}


class TestTheGate:
    def test_a_thin_estate_produces_no_affluence_predictor(self):
        """Five stores cannot support it, so it must not appear at all —
        not appear with a flattering number."""
        obs = [_obs(c, 10000, 0.2, 100_000 * (i + 1), 5_000 * (i + 1),
                    0.5 * (i + 1)) for i, c in enumerate("ABCDE")]
        val = SC.loo_validate(obs)
        assert val.get("mape_affluence") is None
        assert val.get("basis") != "affluence"
        assert SC.calibrate(obs)["spend_model"] is None

    def test_affluence_wins_when_it_genuinely_explains_spend(self):
        """Spend really is exp(2 + 0.8*index); population alone cannot see it."""
        obs = []
        for i in range(8):
            aff = 0.5 * i
            people = 5_000 + 900 * ((i * 5) % 7)
            spend = math.exp(2.0 + 0.8 * aff)
            obs.append(_obs(f"S{i}", 8000 + 1500 * ((i * 3) % 5),
                            0.2 + 0.01 * i, people * spend, people, aff))
        val = SC.loo_validate(obs)
        assert val["mape_affluence"] is not None
        assert val["basis"] == "affluence"
        assert val["mape_affluence"] < val["mape_sqft_only"]

    def test_capital_uses_the_modelled_spend_not_the_median(self):
        obs = []
        for i in range(8):
            aff = 0.5 * i
            people = 5_000 + 900 * ((i * 5) % 7)
            spend = math.exp(2.0 + 0.8 * aff)
            obs.append(_obs(f"S{i}", 8000 + 1500 * ((i * 3) % 5),
                            0.2 + 0.01 * i, people * spend, people, aff))
        cal, val = SC.calibrate(obs), SC.loo_validate(obs)
        rich = SC.propose_capital(20.0, 10_000, cal, val,
                                  captured_population=10_000,
                                  affluence_index=3.5)
        poor = SC.propose_capital(20.0, 10_000, cal, val,
                                  captured_population=10_000,
                                  affluence_index=0.0)
        assert rich["basis"] == "affluence-calibrated"
        assert rich["expected_revenue"] > poor["expected_revenue"] * 5
        assert "catchment" in rich["note"]


class TestLoading:
    def test_an_absent_extract_is_not_an_error_state(self, tmp_path):
        res = A.load_affluence(root=str(tmp_path))
        assert res["rows"] == 0 and not res["grid"]
        assert "No amenity data" in res["error"]

    def test_a_real_file_loads_with_attribution(self, tmp_path):
        d = tmp_path / "oasis" / "data"
        d.mkdir(parents=True)
        with io.open(d / A.CACHE_FILE, "w", encoding="utf-8", newline="") as f:
            f.write("latitude,longitude,kind\n")
            f.write("-1.2750,36.8050,discretionary\n")
            f.write("-1.2850,36.8150,staple\n")
        res = A.load_affluence(root=str(tmp_path))
        assert res["rows"] == 2
        assert res["discretionary"] == 1 and res["staple"] == 1
        assert "OpenStreetMap" in res["attribution"]
        assert "2 POIs" in A.summarise(res)

    def test_the_overpass_query_covers_both_groups(self):
        q = A._overpass_query((-1.6, 36.5, -1.0, 37.3))
        assert "bank" in q and "convenience" in q
        assert "out:json" in q and "out center" in q
