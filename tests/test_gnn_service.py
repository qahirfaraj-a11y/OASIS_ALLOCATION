"""Tests for the shared store-risk service (oasis/logic/gnn_service.py, SH-A S1).

Pure-math tests need no torch. The torch path is exercised only through its
graceful fallback (inventory-only) so the suite passes whether or not torch and
the checkpoint are present in CI.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import gnn_service as G


def _p(ads, soh):
    return {"avg_daily_sales": ads, "current_stocks": soh}


class TestStockoutRatios:
    def test_empty(self):
        assert G.stockout_ratios([]) == (0.0, 0.0)

    def test_depleted_with_ads_is_stockout(self):
        # ADS>0, on-hand<1 -> stockout; single active SKU -> ratio 1.0
        so, crit = G.stockout_ratios([_p(5.0, 0.0)])
        assert so == 1.0 and crit == 0.0

    def test_days_cover_thresholds(self):
        # 2 active SKUs: one <0.5 days cover (stockout), one <3 days (critical)
        prods = [_p(10.0, 2.0),   # 0.2 days -> stockout
                 _p(10.0, 20.0)]  # 2.0 days -> critical
        so, crit = G.stockout_ratios(prods)
        assert so == 0.5 and crit == 0.5

    def test_ads_zero_but_stocked_not_counted(self):
        # ADS=0, on-hand>=1 -> active but neither stockout nor critical
        so, crit = G.stockout_ratios([_p(0.0, 50.0)])
        assert so == 0.0 and crit == 0.0

    def test_ads_zero_depleted_is_stockout(self):
        so, crit = G.stockout_ratios([_p(0.0, 0.0)])
        # denom floored at 1 even though no active SKUs
        assert so == 1.0 and crit == 0.0


class TestInventoryRisk:
    def test_healthy_is_zero(self):
        assert G.inventory_risk([_p(1.0, 100.0)]) == 0.0

    def test_full_stockout_caps_at_one(self):
        # so_ratio=1 -> 1*1.5 capped at 1.0
        assert G.inventory_risk([_p(5.0, 0.0)]) == 1.0

    def test_weighting_matches_command_center(self):
        # 2 SKUs: 1 stockout + 1 critical -> so=.5 crit=.5
        # 0.5*1.5 + 0.5*0.5 = 1.0 -> capped
        prods = [_p(10.0, 2.0), _p(10.0, 20.0)]
        assert G.inventory_risk(prods) == 1.0

    def test_critical_only(self):
        # 1 critical of 2 active -> crit=.5 -> 0.5*0.5 = 0.25
        prods = [_p(10.0, 20.0), _p(1.0, 100.0)]
        assert G.inventory_risk(prods) == 0.25


class TestBlendRisk:
    def test_uniform_defers_to_inventory(self):
        # gnn_uniform -> alpha 0 -> returns inventory risk exactly
        assert G.blend_risk(0.99, 0.3, ratio=0.5, gnn_uniform=True) == 0.3

    def test_brake_reduces_gnn_weight_as_inv_rises(self):
        # inv=0 -> alpha = ratio; full GNN weight on top of zero inventory
        low = G.blend_risk(1.0, 0.0, ratio=0.5)   # alpha=0.5 -> 0.5
        assert abs(low - 0.5) < 1e-9
        # inv high -> brake shrinks alpha; inventory dominates
        high = G.blend_risk(1.0, 0.9, ratio=0.5)
        alpha = 0.5 * (1 - 0.81)  # 0.095
        assert abs(high - (1.0 * alpha + 0.9 * (1 - alpha))) < 1e-9

    def test_ratio_zero_is_pure_inventory(self):
        assert G.blend_risk(1.0, 0.4, ratio=0.0) == 0.4

    def test_clamps_inventory(self):
        # inv>1 clamped to 1 before blending
        assert G.blend_risk(0.0, 5.0, ratio=0.0) == 1.0


class TestStoreRiskFallback:
    def test_inventory_only_when_model_unavailable(self, monkeypatch):
        monkeypatch.setattr(G, "_load_model", lambda *a, **k: (None, None, "unavailable"))
        stock = {"ORG1": [_p(5.0, 0.0)], "ORG2": [_p(1.0, 100.0)]}
        out = G.store_risk(stock)
        assert out == {"ORG1": 1.0, "ORG2": 0.0}

    def test_inventory_only_when_untrained(self, monkeypatch):
        # a model object present but status untrained -> still inventory-only
        monkeypatch.setattr(G, "_load_model", lambda *a, **k: (object(), object(), "untrained"))
        out = G.store_risk({"ORG1": [_p(5.0, 0.0)]})
        assert out == {"ORG1": 1.0}

    def test_empty_input(self, monkeypatch):
        monkeypatch.setattr(G, "_load_model", lambda *a, **k: (None, None, "unavailable"))
        assert G.store_risk({}) == {}
        assert G.store_risk(None) == {}


class TestModelStatus:
    def test_status_is_one_of_known(self):
        # In CI the checkpoint/network JSON may be absent -> unavailable/untrained;
        # locally with both present -> trained. Any of the three is valid.
        assert G.model_status() in {"trained", "untrained", "unavailable"}


class TestS2BlendParity:
    """Golden (S2): service math == the Command Center's former inline formula.

    Reproduces get_all_store_risks' original inline block verbatim and asserts
    risk_from_ratios + blend_risk reproduce it exactly, so the S2 delegation
    provably does not move any Command Center risk score.
    """

    @staticmethod
    def _legacy(rscore, so_ratio, crit_ratio, gnn_blend, gnn_uniform):
        inv_risk = min(1.0, (so_ratio * 1.5) + (crit_ratio * 0.5))
        if gnn_uniform:
            alpha_dynamic = 0.0
            inventory_dynamic = 1.0
        else:
            alpha_dynamic = gnn_blend * (1.0 - (inv_risk ** 2))
            inventory_dynamic = 1.0 - alpha_dynamic
        return (rscore * alpha_dynamic) + (inv_risk * inventory_dynamic)

    def test_grid_parity(self):
        import itertools
        combos = itertools.product(
            [0.0, 0.13, 0.3, 0.7, 1.0],   # rscore
            [0.0, 0.25, 0.5, 0.75, 1.0],  # so_ratio
            [0.0, 0.33, 0.5, 1.0],        # crit_ratio
            [0.3, 0.5],                    # gnn_blend
            [False, True],                # gnn_uniform
        )
        for rscore, so, crit, blend, uni in combos:
            legacy = self._legacy(rscore, so, crit, blend, uni)
            inv = G.risk_from_ratios(so, crit)
            new = G.blend_risk(rscore, inv, blend, gnn_uniform=uni)
            assert abs(legacy - new) < 1e-12, (rscore, so, crit, blend, uni)


class TestStoreRiskContract:
    def test_keys_subset_of_input_with_trained_model(self):
        """With a trained model, result keys never exceed the input keys."""
        import pytest
        if G.model_status() != "trained":
            pytest.skip("no trained GNN checkpoint available in this environment")
        stock = {"ORG1": [_p(5.0, 0.0)], "ORG2": [_p(1.0, 100.0)]}
        out = G.store_risk(stock)
        assert set(out.keys()) == {"ORG1", "ORG2"}
        # ORG1 is fully stocked out -> brake forces inventory dominance -> 1.0
        assert out["ORG1"] == 1.0
