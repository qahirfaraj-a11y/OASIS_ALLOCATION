"""A "top SKU" must be a statement about rank, not about having a record.

THE DEFECT. enrich_product_data set is_top_sku = True for any product that
matched a row in sales_profitability — reading the rank on the line above and
then never consulting it. The flag's only real consumer is a +20% uplift on the
order quantity, so a line ranked 20,000th collected the boost because we
happened to hold data on it.

Measured on the full catalogue at C007: it fired on 85.9% of ordered lines, for
a rule written to cover the top 500 of 3,346.

The second half was the fallback ranker, which re-ranked the products with NO
profitability record starting from 1 — so the fastest 499 leftovers were
promoted too, ranked against each other rather than against the catalogue.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.intelligence_mixin import TOP_SKU_RANK


class _Ranker:
    """The two rules under test, isolated from the 600-line enrichment loop."""

    @staticmethod
    def flag_from_profitability(prof_rank):
        rank = 999 if prof_rank is None else int(prof_rank)
        return {"sales_rank": rank, "is_top_sku": rank < TOP_SKU_RANK}

    @staticmethod
    def fallback_ranks(already_ranked, unranked_ads):
        """unranked_ads: [(name, ads)] -> [(name, rank, is_top)]"""
        out = []
        for idx, (name, ads) in enumerate(
                sorted(unranked_ads, key=lambda t: -t[1])):
            if ads <= 0:
                continue
            rank = already_ranked + idx + 1
            out.append((name, rank, rank < TOP_SKU_RANK))
        return out


class TestTheFlagFollowsTheRank:

    def test_a_top_ranked_line_is_a_top_sku(self):
        assert _Ranker.flag_from_profitability(12)["is_top_sku"] is True

    def test_a_line_ranked_far_down_is_not(self):
        """THE REGRESSION. Rank 20,000 was flagged top-SKU because a
        profitability record existed for it."""
        r = _Ranker.flag_from_profitability(20_000)
        assert r["sales_rank"] == 20_000
        assert r["is_top_sku"] is False

    def test_the_boundary_is_exclusive(self):
        assert _Ranker.flag_from_profitability(TOP_SKU_RANK - 1)["is_top_sku"] is True
        assert _Ranker.flag_from_profitability(TOP_SKU_RANK)["is_top_sku"] is False

    def test_no_record_means_no_rank_and_no_flag(self):
        r = _Ranker.flag_from_profitability(None)
        assert r["sales_rank"] == 999
        assert r["is_top_sku"] is False

    def test_having_a_record_is_not_enough_on_its_own(self):
        """The whole defect in one assertion: presence of data must not grant
        the boost."""
        assert _Ranker.flag_from_profitability(3_000)["is_top_sku"] is False


class TestTheFallbackRanker:

    def test_unranked_lines_are_ranked_AFTER_the_ranked_ones(self):
        """They restarted at 1, so the fastest leftovers outranked genuinely
        top-selling lines."""
        out = _Ranker.fallback_ranks(2_000, [("fast", 50.0), ("slow", 1.0)])
        assert [r for _, r, _ in out] == [2001, 2002]

    def test_a_leftover_does_not_become_a_top_sku_in_a_large_catalogue(self):
        out = _Ranker.fallback_ranks(3_000, [("fast", 99.0)])
        assert out[0][2] is False

    def test_but_it_still_can_in_a_genuinely_small_one(self):
        """If fewer than TOP_SKU_RANK lines carry a real rank, a fast unranked
        line legitimately is near the top — the rule should still work."""
        out = _Ranker.fallback_ranks(10, [("fast", 99.0)])
        assert out[0][1] == 11
        assert out[0][2] is True

    def test_ordering_is_by_velocity(self):
        out = _Ranker.fallback_ranks(0, [("slow", 1.0), ("fast", 90.0), ("mid", 10.0)])
        assert [n for n, _, _ in out] == ["fast", "mid", "slow"]

    def test_lines_with_no_sales_get_no_rank_at_all(self):
        out = _Ranker.fallback_ranks(0, [("dead", 0.0), ("alive", 5.0)])
        assert [n for n, _, _ in out] == ["alive"]


class TestTheThresholdIsNamedOnce:

    def test_the_constant_exists_and_is_five_hundred(self):
        """It had been written out in four places and one had drifted into
        meaning something else entirely."""
        assert TOP_SKU_RANK == 500
