"""Department capital weights must not starve the departments they cannot price.

THE DEFECT. department_scaling_ratios.csv carried Total_Value = 0 for 171 of
233 departments — a gap in the extract, not a decision. A department with no
value gets no weight, drops into the orphan reserve, and splits 5% of the
budget 171 ways. On a KES 27.5M store that left SWEETS holding a wallet of 103
shillings, and the starved categories were systematically the high-unit-cost
ones that need the most capital to fill a shelf.
"""

import csv
import io
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.dept_weights import (
    MIN_WEIGHT, aggregate_departments, build_rows, capital_weights, regenerate,
    staple_share,
)


def row(dept, price, ads, rev=None, product="X"):
    r = {"Department": dept, "Unit_Price": price, "Avg_Daily_Sales": ads,
         "Product": product}
    if rev is not None:
        r["Total_Revenue"] = rev
    return r


class TestAggregation:

    def test_it_totals_value_per_department(self):
        agg = aggregate_departments([
            row("BREAD", 20, 2, 100), row("BREAD", 30, 1, 200),
            row("MILK", 50, 10, 500)])
        assert agg["BREAD"]["value"] == 300
        assert agg["MILK"]["value"] == 500

    def test_it_counts_skus(self):
        agg = aggregate_departments([row("BREAD", 20, 2, 1)] * 3)
        assert agg["BREAD"]["sku_count"] == 3

    def test_department_names_are_normalised(self):
        agg = aggregate_departments([row(" bread ", 20, 2, 5),
                                     row("BREAD", 20, 2, 5)])
        assert list(agg) == ["BREAD"]
        assert agg["BREAD"]["sku_count"] == 2

    def test_missing_revenue_falls_back_to_price_times_rate(self):
        """A scorecard without Total_Revenue still yields a usable weight."""
        agg = aggregate_departments([row("BREAD", 20, 3)])
        assert agg["BREAD"]["value"] == 60

    def test_a_blank_department_is_dropped(self):
        agg = aggregate_departments([row("", 20, 2, 5), row("BREAD", 20, 2, 5)])
        assert list(agg) == ["BREAD"]


class TestWeights:

    def test_weights_are_shares_of_total_value(self):
        w = capital_weights({"A": {"value": 75.0}, "B": {"value": 25.0}})
        assert w["A"] == pytest.approx(0.75)
        assert w["B"] == pytest.approx(0.25)

    def test_they_sum_to_one(self):
        w = capital_weights({"A": {"value": 3.0}, "B": {"value": 7.0},
                             "C": {"value": 0.0}})
        assert sum(w.values()) == pytest.approx(1.0)

    def test_a_valueless_department_gets_a_floor_not_a_zero(self):
        """THE REGRESSION. Zero weight is what dropped 171 departments into an
        orphan reserve and left one of them with a 103-shilling wallet."""
        w = capital_weights({"BIG": {"value": 1000.0}, "UNPRICED": {"value": 0.0}})
        assert w["UNPRICED"] > 0

    def test_the_floor_cannot_take_capital_from_a_measured_department(self):
        """A floor applied without renormalising would let unpriced
        departments dilute the priced ones."""
        w = capital_weights({"BIG": {"value": 1000.0}, "UNPRICED": {"value": 0.0}})
        assert w["BIG"] > 0.99
        assert w["UNPRICED"] < MIN_WEIGHT * 2

    def test_all_valueless_falls_back_to_an_even_split(self):
        w = capital_weights({"A": {"value": 0.0}, "B": {"value": 0.0}})
        assert w["A"] == pytest.approx(0.5)
        assert w["B"] == pytest.approx(0.5)

    def test_no_departments_does_not_divide_by_zero(self):
        assert capital_weights({}) == {}


class TestRows:

    def test_it_emits_the_columns_the_file_has_always_had(self):
        rows = build_rows(aggregate_departments([row("BREAD", 20, 2, 100)]))
        assert set(rows[0]) == {"Department", "SKU_Count", "Avg_Price",
                                "Avg_Daily_Sales", "Total_Value",
                                "Capital_Weight", "SKU_per_Million"}

    def test_average_price_is_an_average_not_a_sum(self):
        rows = build_rows(aggregate_departments([
            row("BREAD", 10, 1, 5), row("BREAD", 30, 1, 5)]))
        assert rows[0]["Avg_Price"] == pytest.approx(20.0)

    def test_departments_come_out_sorted(self):
        rows = build_rows(aggregate_departments([
            row("MILK", 1, 1, 1), row("BREAD", 1, 1, 1)]))
        assert [r["Department"] for r in rows] == ["BREAD", "MILK"]


class TestRegenerate:

    def _scorecard(self, tmp_path, rows):
        p = tmp_path / "scorecard.csv"
        with io.open(str(p), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Product", "Department",
                                              "Unit_Price", "Avg_Daily_Sales",
                                              "Total_Revenue"])
            w.writeheader()
            for i, (d, pr, a, rev) in enumerate(rows):
                w.writerow({"Product": f"P{i}", "Department": d,
                            "Unit_Price": pr, "Avg_Daily_Sales": a,
                            "Total_Revenue": rev})
        return str(p)

    def _existing(self, tmp_path, priced, unpriced):
        p = tmp_path / "department_scaling_ratios.csv"
        with io.open(str(p), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Department", "SKU_Count",
                                              "Avg_Price", "Avg_Daily_Sales",
                                              "Total_Value", "Capital_Weight",
                                              "SKU_per_Million"])
            w.writeheader()
            for d in priced:
                w.writerow({"Department": d, "SKU_Count": 1, "Avg_Price": 10,
                            "Avg_Daily_Sales": 1, "Total_Value": 100,
                            "Capital_Weight": 0.5, "SKU_per_Million": 1})
            for d in unpriced:
                w.writerow({"Department": d, "SKU_Count": 1, "Avg_Price": 0,
                            "Avg_Daily_Sales": 1, "Total_Value": 0,
                            "Capital_Weight": 0.0001, "SKU_per_Million": 1})
        return str(p)

    def test_it_prices_departments_the_old_file_could_not(self, tmp_path):
        # Non-essential departments throughout, so the staple-share guard is
        # neutral and this test measures only the pricing behaviour.
        self._existing(tmp_path, ["GIFTWARE"], ["HEATERS", "SWEETS"])
        sc = self._scorecard(tmp_path, [("GIFTWARE", 20, 2, 100),
                                        ("HEATERS", 9000, 0.01, 500),
                                        ("SWEETS", 5, 3, 90)])
        r = regenerate(sc, str(tmp_path))
        assert r["priced_before"] == 1
        assert r["priced_after"] == 3
        assert r["written"] is True

    def test_it_keeps_the_previous_file(self, tmp_path):
        self._existing(tmp_path, ["GIFTWARE"], [])
        sc = self._scorecard(tmp_path, [("GIFTWARE", 20, 2, 100)])
        r = regenerate(sc, str(tmp_path))
        assert r["backup"]
        assert os.path.exists(os.path.join(str(tmp_path), r["backup"]))

    def test_it_refuses_to_replace_richer_data_with_thinner(self, tmp_path):
        """A fresher scorecard that prices FEWER departments is a worse input,
        however recent it is — the same rule the rhythm derivation keeps."""
        self._existing(tmp_path, ["GIFTWARE", "TOYS", "STATIONARIES"], [])
        sc = self._scorecard(tmp_path, [("GIFTWARE", 20, 2, 100)])
        r = regenerate(sc, str(tmp_path))
        assert r["written"] is False
        assert "refusing to replace richer data" in r["refused"]

    def test_force_overrides_the_refusal(self, tmp_path):
        self._existing(tmp_path, ["GIFTWARE", "TOYS", "STATIONARIES"], [])
        sc = self._scorecard(tmp_path, [("GIFTWARE", 20, 2, 100)])
        assert regenerate(sc, str(tmp_path), force=True)["written"] is True

    def test_dry_run_writes_nothing(self, tmp_path):
        self._existing(tmp_path, ["GIFTWARE"], ["HEATERS"])
        sc = self._scorecard(tmp_path, [("GIFTWARE", 20, 2, 100),
                                        ("HEATERS", 9000, 0.01, 500)])
        r = regenerate(sc, str(tmp_path), write=False)
        assert r["written"] is False
        assert r["priced_after"] == 2


class TestTheHierarchyGuard:
    """The engine allocates Width first, then Day-1 Depth on staples, and the
    documented wallet split is "Staples 60%, General 40%".

    Rebuilding weights from raw turnover share across a full catalogue halves
    that -- measured on the real book, essential departments went 60.7% ->
    31.3% and Fast Five anchors 35.0% -> 14.6%. Arithmetically clean,
    strategically backwards. It shipped, and it was reverted.
    """

    def test_staple_share_is_measurable(self):
        from oasis.logic.department_constants import ESSENTIAL_DEPARTMENTS
        first = " ".join(str(next(iter(ESSENTIAL_DEPARTMENTS))).upper().split())
        assert staple_share({first: 0.6, "SOMETHING ELSE": 0.4}) == pytest.approx(0.6)

    def test_an_unknown_department_contributes_nothing(self):
        assert staple_share({"NOT A REAL DEPARTMENT": 1.0}) == 0.0

    def test_a_rebuild_that_guts_the_staples_is_refused(self, tmp_path):
        from oasis.logic.department_constants import ESSENTIAL_DEPARTMENTS
        staple = " ".join(str(next(iter(ESSENTIAL_DEPARTMENTS))).upper().split())
        # existing file: the staple holds essentially everything
        p = tmp_path / "department_scaling_ratios.csv"
        with io.open(str(p), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Department", "SKU_Count", "Avg_Price",
                                              "Avg_Daily_Sales", "Total_Value",
                                              "Capital_Weight", "SKU_per_Million"])
            w.writeheader()
            w.writerow({"Department": staple, "SKU_Count": 1, "Avg_Price": 10,
                        "Avg_Daily_Sales": 1, "Total_Value": 100,
                        "Capital_Weight": 0.95, "SKU_per_Million": 1})
            w.writerow({"Department": "TOYS", "SKU_Count": 1, "Avg_Price": 0,
                        "Avg_Daily_Sales": 1, "Total_Value": 0,
                        "Capital_Weight": 0.05, "SKU_per_Million": 1})
        # scorecard: discretionary turnover swamps the staple
        sc = tmp_path / "sc.csv"
        with io.open(str(sc), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Product", "Department", "Unit_Price",
                                              "Avg_Daily_Sales", "Total_Revenue"])
            w.writeheader()
            w.writerow({"Product": "a", "Department": staple, "Unit_Price": 10,
                        "Avg_Daily_Sales": 1, "Total_Revenue": 100})
            w.writerow({"Product": "b", "Department": "TOYS", "Unit_Price": 10,
                        "Avg_Daily_Sales": 1, "Total_Revenue": 9000})
        r = regenerate(str(sc), str(tmp_path))
        assert r["written"] is False
        assert "staple share" in r["refused"]
        assert "Staples 60%" in r["refused"]

    def test_force_still_allows_it_deliberately(self, tmp_path):
        from oasis.logic.department_constants import ESSENTIAL_DEPARTMENTS
        staple = " ".join(str(next(iter(ESSENTIAL_DEPARTMENTS))).upper().split())
        p = tmp_path / "department_scaling_ratios.csv"
        with io.open(str(p), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Department", "SKU_Count", "Avg_Price",
                                              "Avg_Daily_Sales", "Total_Value",
                                              "Capital_Weight", "SKU_per_Million"])
            w.writeheader()
            w.writerow({"Department": staple, "SKU_Count": 1, "Avg_Price": 10,
                        "Avg_Daily_Sales": 1, "Total_Value": 100,
                        "Capital_Weight": 0.95, "SKU_per_Million": 1})
        sc = tmp_path / "sc.csv"
        with io.open(str(sc), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Product", "Department", "Unit_Price",
                                              "Avg_Daily_Sales", "Total_Revenue"])
            w.writeheader()
            w.writerow({"Product": "a", "Department": staple, "Unit_Price": 10,
                        "Avg_Daily_Sales": 1, "Total_Revenue": 100})
            w.writerow({"Product": "b", "Department": "TOYS", "Unit_Price": 10,
                        "Avg_Daily_Sales": 1, "Total_Revenue": 9000})
        assert regenerate(str(sc), str(tmp_path), force=True)["written"] is True

    def test_the_shipped_file_still_matches_the_documented_split(self):
        """The live guard: 60.7% in essential departments is the design."""
        path = os.path.join(os.path.dirname(__file__), "..", "oasis", "data",
                            "department_scaling_ratios.csv")
        if not os.path.exists(path):
            pytest.skip("no ratios file in this checkout")
        with io.open(path, encoding="utf-8", errors="replace", newline="") as f:
            w = {r["Department"].strip().upper(): float(r["Capital_Weight"] or 0)
                 for r in csv.DictReader(f)}
        assert 0.50 <= staple_share(w) <= 0.70, (
            "staple share is %.1f%%; the documented split is 'Staples 60%%'"
            % (100 * staple_share(w)))
