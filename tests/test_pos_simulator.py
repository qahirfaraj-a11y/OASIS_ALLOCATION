"""Tests for the affinity-aware POS simulator (pure basket construction)."""

import os
import random
import sys
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.pos_simulator import (
    assign_popularity, generate_basket, ring_up, _weighted_distinct,
)


class TestRingUpStockIntegrity:
    def _meta(self, *codes):
        return {c: (c, 10.0, 0.0) for c in codes}

    def test_never_oversells(self):
        # only 2 of X on hand; max_qty 5 → sells at most 2, stock hits 0 not negative
        lines, new_stock = ring_up(["X"], {"X": 2.0}, self._meta("X"),
                                   random.Random(0), max_qty=5)
        assert len(lines) == 1 and lines[0].qty <= 2.0
        assert new_stock["X"] >= 0.0

    def test_out_of_stock_item_gets_no_line(self):
        lines, new_stock = ring_up(["A", "B"], {"A": 0.0, "B": 3.0},
                                   self._meta("A", "B"), random.Random(0))
        sold = {line.itm_cd for line in lines}
        assert "A" not in sold and "B" in sold      # OOS item dropped entirely
        assert "A" not in new_stock

    def test_repeated_item_draws_down_same_pool(self):
        # same item twice in a basket can't exceed the single on-hand pool
        lines, new_stock = ring_up(["X", "X"], {"X": 3.0}, self._meta("X"),
                                   random.Random(1), max_qty=2)
        assert sum(line.qty for line in lines) <= 3.0
        assert new_stock["X"] >= 0.0


class TestPopularity:
    def test_zipfian_hero_dominates(self):
        pop = assign_popularity(["a", "b", "c", "d"], random.Random(1))
        assert len(pop) == 4
        assert max(pop.values()) == 1.0          # the hero (rank 0)
        assert min(pop.values()) == 0.25         # rank 3 -> 1/4


class TestWeightedDistinct:
    def test_distinct_and_capped(self):
        picks = _weighted_distinct({"x": 5.0, "y": 1.0, "z": 0.1}, 2, random.Random(0))
        assert len(picks) == 2 and len(set(picks)) == 2


class TestGenerateBasket:
    def test_pulls_complementary_department_not_others(self):
        dept_items = {"A": ["a1", "a2"], "B": ["b1", "b2"], "C": ["c1"]}
        pop = {k: 1.0 for k in ["a1", "a2", "b1", "b2", "c1"]}
        prior = {"A": {"B": 100.0}}              # A complements B only
        rng = random.Random(0)
        cnt = Counter()
        for _ in range(300):
            bk = generate_basket("A", dept_items, pop, prior, rng,
                                 max_attach=1, noise_p=0.0)
            cnt["anchorA"] += any(x.startswith("a") for x in bk)
            cnt["B"] += any(x.startswith("b") for x in bk)
            cnt["C"] += ("c1" in bk)
        assert cnt["anchorA"] == 300             # always an anchor from seed dept
        assert cnt["B"] > 250                    # complementary dept pulled in
        assert cnt["C"] == 0                     # never (not in prior, noise off)

    def test_no_prior_yields_anchor_only(self):
        dept_items = {"A": ["a1"], "B": ["b1"]}
        bk = generate_basket("A", dept_items, {"a1": 1.0, "b1": 1.0}, {},
                             random.Random(0), noise_p=0.0)
        assert bk == ["a1"]                       # no halo → just the anchor
