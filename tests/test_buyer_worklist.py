"""The worklist must report what the engine actually decided, not re-decide.

The failure mode worth guarding is not a crash. It is a number that looks
authoritative and is wrong: a return-on-capital of infinity on a line we cannot
price, a gross profit built from the margin_pct field that arrives as 0.0 on
most of these rows, or a line that was never suppressed quietly appearing on a
buyer's list as though the engine had refused it.
"""
import math

import pytest

from oasis.logic import buyer_worklist as bw


def _rec(**kw):
    base = {
        "sku": "SKU1",
        "product_name": "Ironing Board",
        "supplier_name": "SX0001 - ACME",
        "auto_order_suppressed": True,
        "suppress_reason": "one pack exceeds 60 days of cover",
        "avg_daily_sales": 0.02,
        "current_stock": 0.0,
        "pack_size": 6,
        "unit_cost": 550.0,
        "selling_price": 750.0,
        "margin_pct": 0.0,
    }
    base.update(kw)
    return base


def test_only_suppressed_lines_appear():
    rows = bw.build([
        _rec(sku="KEPT"),
        _rec(sku="ORDERED", auto_order_suppressed=False),
        _rec(sku="ABSENT", auto_order_suppressed=None),
    ])
    assert [r["sku"] for r in rows] == ["KEPT"]


def test_gross_profit_is_derived_from_prices_not_margin_pct():
    # margin_pct is 0.0 on nearly every one of these rows. Trusting it would
    # price the whole worklist at zero and make it look like nothing is at
    # stake -- the exact opposite of the finding that motivated the list.
    row = bw.build([_rec()])[0]
    assert row["gp_per_unit"] == pytest.approx(200.0)
    assert row["gp_per_year"] == pytest.approx(0.02 * 365 * 200.0)


def test_margin_pct_is_the_fallback_when_a_price_is_missing():
    row = bw.build([_rec(selling_price=0, margin_pct=40.0)])[0]
    assert row["gp_per_unit"] == pytest.approx(550.0 * 0.40)


def test_one_pack_cover_and_capital():
    row = bw.build([_rec()])[0]
    assert row["one_pack_days"] == pytest.approx(6 / 0.02)
    assert row["capital_once"] == pytest.approx(6 * 550.0)
    assert row["return_on_capital"] == pytest.approx(
        row["gp_per_year"] / row["capital_once"])


def test_a_line_we_cannot_price_does_not_sort_above_one_we_can():
    # No cost -> no capital. Returning inf (or 0/0) would float an unpriceable
    # line to the top of a list whose whole purpose is ranking by what is at
    # stake.
    rows = bw.build([_rec(sku="NOCOST", unit_cost=0, cost_price=0)])
    assert rows[0]["return_on_capital"] is None


def test_no_sales_gives_no_annual_profit_and_infinite_cover():
    row = bw.build([_rec(avg_daily_sales=0)])[0]
    assert row["gp_per_year"] == 0.0
    assert math.isinf(row["one_pack_days"])
    assert row["trivial"] is True


def test_pack_size_floors_at_one():
    row = bw.build([_rec(pack_size=0)])[0]
    assert row["pack_size"] == 1.0


def test_sorted_worst_first_by_what_is_at_stake():
    rows = bw.build([
        _rec(sku="SMALL", avg_daily_sales=0.01),
        _rec(sku="BIG", avg_daily_sales=0.90),
        _rec(sku="MID", avg_daily_sales=0.10),
    ])
    assert [r["sku"] for r in rows] == ["BIG", "MID", "SMALL"]


def test_trivial_flag_tracks_the_threshold():
    just_under = (bw.TRIVIAL_GP_PER_YEAR - 1) / (365 * 200.0)
    just_over = (bw.TRIVIAL_GP_PER_YEAR + 1) / (365 * 200.0)
    rows = {r["sku"]: r for r in bw.build([
        _rec(sku="UNDER", avg_daily_sales=just_under),
        _rec(sku="OVER", avg_daily_sales=just_over),
    ])}
    assert rows["UNDER"]["trivial"] is True
    assert rows["OVER"]["trivial"] is False


def test_summarise_counts_empties_and_totals():
    rows = bw.build([
        _rec(sku="A", current_stock=0.0),
        _rec(sku="B", current_stock=3.0),
        _rec(sku="C", current_stock=0.0, avg_daily_sales=0.0001),
    ])
    s = bw.summarise(rows)
    assert s["lines"] == 3
    assert s["out_of_stock"] == 2
    assert s["worth_reviewing"] == 2          # C earns ~7 KES/yr
    assert s["gp_per_year"] == pytest.approx(sum(r["gp_per_year"] for r in rows))
    assert s["capital_once"] == pytest.approx(3 * 6 * 550.0)


def test_summarise_ignores_infinite_cover_in_the_median():
    # A no-sales line has infinite cover. Letting it into the median makes the
    # headline "median cover in one pack" either inf or nan.
    rows = bw.build([
        _rec(sku="A", avg_daily_sales=0.02),
        _rec(sku="B", avg_daily_sales=0.0),
    ])
    med = bw.summarise(rows)["median_one_pack_days"]
    assert med == pytest.approx(300.0)


def test_empty_inputs_are_safe():
    assert bw.build([]) == []
    assert bw.build(None) == []
    s = bw.summarise([])
    assert s["lines"] == 0
    assert s["median_one_pack_days"] is None


class TestTheTwoRefusalCausesStaySeparate:
    """Two causes now arrive on one channel, and they are not the same advice.

    "One pack is months of cover" is a pack-size problem: the line may be
    worth stocking, just not on a replenishment rule. "Your service target is
    met by holding nothing" is a velocity problem: at this rate the target
    itself says no stock. Collapsing them would hand a buyer one undifferentiated
    pile and lose the remedy.
    """

    def test_a_pack_cover_refusal_is_labelled_as_one(self):
        rows = bw.build([_rec(suppress_reason=(
            "one pack exceeds 60 days of cover -- special order or transfer, "
            "not replenishment"))])
        assert rows[0]["cause"] == bw.CAUSE_PACK_COVER

    def test_a_velocity_refusal_is_labelled_as_one(self):
        rows = bw.build([_rec(suppress_reason=(
            "90% service needs no stock at this velocity: 0.005/day over 14 "
            "days is 0.07 expected units -- stock it for presence, or delist"))])
        assert rows[0]["cause"] == bw.CAUSE_TOO_SLOW

    def test_an_unrecognised_reason_still_gets_a_label(self):
        rows = bw.build([_rec(suppress_reason="something new appeared")])
        assert rows[0]["cause"] == bw.CAUSE_OTHER

    def test_the_summary_splits_by_cause(self):
        rows = bw.build([
            _rec(sku="A", suppress_reason="one pack exceeds 60 days of cover"),
            _rec(sku="B", suppress_reason="one pack exceeds 60 days of cover"),
            _rec(sku="C", suppress_reason="90% service needs no stock at this "
                                          "velocity: 0.005/day"),
        ])
        by = bw.summarise(rows)["by_cause"]
        assert by[bw.CAUSE_PACK_COVER] == 2
        assert by[bw.CAUSE_TOO_SLOW] == 1
        assert sum(by.values()) == len(rows)

    def test_the_two_labels_are_distinguishable(self):
        assert bw.CAUSE_PACK_COVER != bw.CAUSE_TOO_SLOW
