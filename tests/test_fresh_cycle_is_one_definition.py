"""The daily fresh cycle: one definition, read by every ordering path.

Bread is baked and delivered the same day, carries a 6-day best-before on most
lines and is pulled a day early -- 5 selling days on the shelf. The store keeps
bread fresh, so that fifth day should only ever bind on slow sellers. The order goes in after
close and is on the shelf before the next opening, so the lead adds no selling
days: the horizon is R, not R + L. The engine was planning R + L with L floored
at 1 and holding ~3 days of cover on lines the bakeries themselves drop at
1.2-1.5 days.

Everything here comes from `fresh_cycle` in the central engine config, so the
offline POS path, the Odoo review queue, AMIT's scoring and the classic path
cannot drift apart on it.
"""
import sys

import pytest

from oasis.logic import order_up_to as ou

sys.path.insert(0, __file__.rsplit("tests", 1)[0])
from devkit.probe_lead_time import correct_sunday_postings        # noqa: E402

D = 64.6          # a fast loaf: units a day
FEST = "DPL FESTIVE  LIMITED"


def line(dept="BREAD", lead=1.0, supplier=FEST, stock=0.0, sku="FESTIVE 400G MILKY WHITE SLICED"):
    return {"avg_daily_sales": D, "supplier_name": supplier, "current_stock": stock,
            "on_order_qty": 0.0, "lead_time_days": lead, "department": dept, "sku": sku}


class TestTheConfigIsTheSource:
    def test_bread_is_an_overnight_department_with_a_five_day_life(self):
        fc = ou.fresh_cycle()
        assert "BREAD" in fc["overnight"]
        assert ou.sellable_life_for("BREAD") == 5.0

    def test_a_per_sku_code_date_beats_the_department(self, monkeypatch):
        monkeypatch.setattr(ou, "_FRESH_CYCLE", {"overnight": frozenset({"BREAD"}), "overnight_max_lead": 1.0,
                                                 "life_dept": {"BREAD": 4.0}, "life_sku": {"SHORT LOAF": 2.0}})
        assert ou.sellable_life_for("BREAD", "SHORT LOAF") == 2.0
        assert ou.sellable_life_for("BREAD", "OTHER LOAF") == 4.0

    def test_the_label_beats_the_return_measured_shelf_life(self):
        # the returns book measures the SWAP age for bread, not the expiry
        assert ou.shelf_life_for("BREAD", sku="FESTIVE 400G MILKY WHITE SLICED") == 5.0


class TestSellingDayHorizon:
    def test_bread_from_an_overnight_supplier_adds_no_lead(self):
        lead, basis = ou.effective_lead_days(line())
        assert lead == 0.0 and "selling days" in basis

    def test_a_slow_supplier_in_the_same_department_keeps_its_calendar_lead(self):
        lead, basis = ou.effective_lead_days(line(supplier="KENAFRIC INDUSTRIES LTD", lead=9))
        assert lead >= 1.0 and basis == "calendar days"

    def test_other_departments_are_untouched(self):
        for dept in ("FRESH MILK", "YOGHURT", "GROCERY"):
            lead, basis = ou.effective_lead_days(line(dept=dept))
            assert lead == 1.0 and basis == "calendar days"

    def test_bread_is_ordered_to_about_a_day_and_a_half_not_three(self):
        t = ou.recommend(line())
        assert t["L"] == 0.0 and t["P"] == t["R"] == 1.0
        cover = t["S"] / D
        assert 1.2 <= cover <= 2.0, cover

    def test_fresh_milk_is_unchanged(self):
        t = ou.recommend(line(dept="FRESH MILK", sku="KCC 500ML FRESH W/MILK (POUCH)-84"))
        assert t["L"] == 1.0 and t["P"] == 2.0
        assert t["S"] / D == pytest.approx(2.0, abs=0.01)      # the fresh cover ceiling

    def test_cover_never_exceeds_the_sellable_life(self):
        t = ou.recommend(line(stock=0.0))
        assert t["S"] <= D * ou.sellable_life_for("BREAD") + 1e-9


class TestTheBridgeUsesTheSameHorizon:
    def test_the_shipped_path_orders_bread_on_the_selling_day_horizon(self, tmp_path):
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        util = SimulationOrderUtil(str(tmp_path))
        p = {"sku": "FESTIVE 400G MILKY WHITE SLICED", "itm_cd": "L1",
             "product_name": "FESTIVE 400G MILKY WHITE SLICED", "supplier_name": FEST,
             "avg_daily_sales": D, "sales_velocity": D, "current_stock": 0.0, "current_stocks": 0.0,
             "unit_cost": 58.0, "cost_price": 58.0, "selling_price": 65.0, "department": "BREAD",
             "pack_size": 1, "lead_time_days": 1.0, "is_fresh": True, "shelf_life_days": 4.0,
             "reorder_point": D * 2, "on_order_qty": 0.0}
        r = util.calculate_order_quantity([p], use_real_date=True)[0]
        q = float(r.get("recommended_quantity") or 0)
        assert 1.2 <= q / D <= 2.0, (q, r.get("reasoning"))
        assert "L=0d" in str(r.get("reasoning"))


class TestSundayPostingCorrection:
    def test_a_monday_receipt_on_an_overnight_supplier_is_a_sunday_delivery(self):
        raw = {"BAKERY": [(1.0, 1), (1.0, 2), (1.0, 3), (2.0, 0)]}       # Monday 2-day reading
        per, corrected = correct_sunday_postings(raw, no_sunday_posting=True)
        assert per["BAKERY"] == [1.0, 1.0, 1.0, 1.0]
        assert corrected["BAKERY"] == 1

    def test_a_weekly_supplier_keeps_its_two_day_monday_lead(self):
        raw = {"DRY": [(7.0, 1), (7.0, 2), (2.0, 0)]}
        per, corrected = correct_sunday_postings(raw, no_sunday_posting=True)
        assert sorted(per["DRY"]) == [2.0, 7.0, 7.0] and not corrected

    def test_nothing_is_corrected_when_the_book_does_post_on_sundays(self):
        raw = {"BAKERY": [(1.0, 1), (2.0, 0)]}
        per, corrected = correct_sunday_postings(raw, no_sunday_posting=False)
        assert sorted(per["BAKERY"]) == [1.0, 2.0] and not corrected

    def test_the_shipped_patterns_carry_the_correction(self):
        pats = ou.default_patterns()
        fest = pats["DPL FESTIVE LIMITED"]
        assert fest["sunday_posting_corrected"] > 0
        assert fest["lead_time_stdev"] < 0.3        # was 0.427 before the correction


class TestTheLongerLifeOnlyReachesSlowSellers:
    """Fresh bread stays fresh: the label's extra day moves slow lines only."""

    def _S(self, d, life, monkeypatch):
        monkeypatch.setattr(ou, "_FRESH_CYCLE", {"overnight": frozenset({"BREAD"}), "overnight_max_lead": 1.0,
                                                 "life_dept": {"BREAD": float(life)}, "life_sku": {}})
        p = dict(line(), avg_daily_sales=d)
        return ou.recommend(p)["S"]

    def test_a_fast_loaf_is_held_by_the_horizon_not_the_label(self, monkeypatch):
        assert self._S(D, 4, monkeypatch) == self._S(D, 5, monkeypatch)

    def test_a_slow_line_gets_the_extra_day(self, monkeypatch):
        slow = 0.3
        assert self._S(slow, 5, monkeypatch) >= self._S(slow, 4, monkeypatch)
        assert self._S(slow, 5, monkeypatch) <= slow * 5 + 1.0 + 1e-9
