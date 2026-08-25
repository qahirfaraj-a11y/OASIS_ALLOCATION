"""The outcome measure that cannot be contaminated by the ordering habit.

Every earlier attempt to check a coverage horizon compared one estimate of
supplier cadence against another, and the observed interval is downstream of
our own past ordering — so agreement proved nothing. This measures what
actually happened after the fact: how much cover each delivery carried against
the gap it actually had to span.
"""

import os
import sys
from datetime import date, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.residual_cover import (
    MIN_ADS_FOR_COVER, collect_from_receipts, format_report, residual_days,
    score_intervals, summarise, verdict,
)


def days(n):
    return date(2025, 1, 1) + timedelta(days=n)


class TestResidualDays:

    def test_a_delivery_that_exactly_spans_the_interval_reads_zero(self):
        # 70 units at 7/day = 10 days of cover, and 10 days until the next one
        assert residual_days(70, 7.0, 10) == 0.0

    def test_over_delivery_is_positive(self):
        assert residual_days(140, 7.0, 10) == 10.0

    def test_under_delivery_is_negative(self):
        assert residual_days(35, 7.0, 10) == -5.0

    def test_a_line_that_barely_sells_returns_None_not_zero(self):
        """Zero would read as 'ran out exactly on time' and poison every
        average it touched — the lesson the 999 sentinel taught."""
        assert residual_days(10, 0.0, 10) is None
        assert residual_days(10, MIN_ADS_FOR_COVER / 2, 10) is None

    def test_a_nonsensical_interval_is_refused(self):
        assert residual_days(10, 5.0, 0) is None
        assert residual_days(10, 5.0, -3) is None


class TestScoreIntervals:

    def test_the_last_delivery_is_not_scored(self):
        """It has no successor, so nothing is known about how long it had to
        last. Scoring it would report every line as hugely over-delivered."""
        rows = score_intervals([(days(0), 70), (days(10), 70), (days(20), 70)], 7.0)
        assert len(rows) == 2

    def test_it_pairs_consecutive_deliveries_in_date_order(self):
        rows = score_intervals([(days(20), 70), (days(0), 70), (days(10), 70)], 7.0)
        assert [r["interval_days"] for r in rows] == [10, 10]

    def test_an_absurd_interval_is_dropped(self):
        """A year between deliveries is a delisting, not a rhythm."""
        rows = score_intervals([(days(0), 70), (days(300), 70)], 7.0)
        assert rows == []

    def test_the_ratio_says_how_many_intervals_the_delivery_covered(self):
        rows = score_intervals([(days(0), 140), (days(10), 10)], 7.0)
        assert rows[0]["ratio"] == 2.0

    def test_a_single_delivery_scores_nothing(self):
        assert score_intervals([(days(0), 70)], 7.0) == []


class TestSummarise:

    def _rows(self, residuals, ads=7.0, interval=10):
        out = []
        for r in residuals:
            qty = (r + interval) * ads
            out.extend(score_intervals([(days(0), qty), (days(interval), 1)], ads))
        return out

    def test_it_counts_over_covered_and_short_intervals(self):
        s = summarise(self._rows([30, 20, -5]))
        assert s["intervals"] == 3
        assert s["over_covered"] == 2
        assert s["ran_short"] == 1

    def test_idle_capital_prices_the_typical_leftover(self):
        # 20 days of residual at 7/day at KES 10 = 1,400
        s = summarise(self._rows([20]), unit_cost=10.0)
        assert round(s["idle_capital"]) == 1400

    def test_idle_capital_does_not_scale_with_how_often_a_line_is_delivered(self):
        """Summing across intervals priced the same shelf over and over, so
        the headline grew with delivery FREQUENCY rather than idle stock. On
        the real book that inflated it to KES 72M."""
        once = summarise(self._rows([20]), unit_cost=10.0)["idle_capital"]
        many = summarise(self._rows([20] * 20), unit_cost=10.0)["idle_capital"]
        assert round(once) == round(many)

    def test_nothing_to_summarise_is_not_a_crash(self):
        assert summarise([])["intervals"] == 0


class TestVerdict:
    """Judged on the RATIO of cover delivered to interval served.

    An absolute day threshold cannot serve both ends of the book: 13 days of
    leftover on a 15-day cycle is nearly double-covered, while the same 13 days
    on a 60-day cycle is unremarkable. The first version used absolute days and
    called the real book "about right" when every delivery was carrying 1.9x
    the gap it had to span.
    """

    def test_deep_horizon_is_named(self):
        assert "too deep" in verdict(30.0, 10.0)

    def test_a_horizon_more_than_double_is_called_out_harder(self):
        assert "far too deep" in verdict(25.0, 10.0)

    def test_short_horizon_is_named(self):
        assert "lean on stock" in verdict(-5.0, 14.0)

    def test_a_sensible_horizon_says_so(self):
        assert "about right" in verdict(2.0, 14.0)

    def test_the_same_residual_is_judged_against_its_own_interval(self):
        """THE REGRESSION. 13 days left on a 15-day cycle is nearly double
        cover; on a 60-day cycle it is fine."""
        assert "too deep" in verdict(13.0, 15.0)
        assert "about right" in verdict(13.0, 60.0)

    def test_nothing_measurable_says_that_rather_than_guessing(self):
        assert "not enough" in verdict(None, None)
        assert "not enough" in verdict(5.0, 0)


class TestCollect:

    def test_it_rolls_up_by_supplier(self):
        receipts = {
            "A": [(days(0), 140), (days(10), 140), (days(20), 1)],
            "B": [(days(0), 70), (days(10), 70), (days(20), 1)],
        }
        r = collect_from_receipts(receipts, {"A": 7.0, "B": 7.0},
                                  {"A": 10.0, "B": 10.0},
                                  {"A": "ACME", "B": "ACME"})
        assert r["lines_scored"] == 2
        assert r["by_supplier"]["ACME"]["lines"] == 2

    def test_a_line_with_no_sales_rate_is_skipped_and_counted(self):
        r = collect_from_receipts({"A": [(days(0), 10), (days(10), 10)]}, {})
        assert r["lines_scored"] == 0
        assert r["skipped_no_sales_rate"] == 1

    def test_a_line_with_one_delivery_is_skipped_and_counted(self):
        r = collect_from_receipts({"A": [(days(0), 10)]}, {"A": 7.0})
        assert r["lines_scored"] == 0
        assert r["skipped_too_few_deliveries"] == 1

    def test_over_delivery_shows_up_as_a_positive_median(self):
        receipts = {"A": [(days(0), 700), (days(10), 700), (days(20), 1)]}
        r = collect_from_receipts(receipts, {"A": 7.0})
        assert r["median_residual_days"] == 90.0


class TestReport:

    def test_it_is_console_safe(self):
        receipts = {"A": [(days(0), 140), (days(10), 140), (days(20), 1)]}
        r = collect_from_receipts(receipts, {"A": 7.0}, {"A": 10.0}, {"A": "ACME"})
        format_report(r).encode("cp1252")

    def test_an_empty_result_explains_itself(self):
        text = format_report(collect_from_receipts({}, {}))
        assert "Nothing to judge" in text

    def test_the_verdict_reaches_the_report(self):
        receipts = {"A": [(days(0), 700), (days(10), 700), (days(20), 1)]}
        text = format_report(collect_from_receipts(receipts, {"A": 7.0}))
        assert "too deep" in text
