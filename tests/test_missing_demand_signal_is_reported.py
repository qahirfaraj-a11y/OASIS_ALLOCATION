"""A line with no measured demand must say so, not look healthy.

recommend() returns early on d <= 0 with {"quantity": 0, "reason": "no
measured sales rate"} -- no S, no P, no terms. The bridge answered that with
"[order-up-to: position already covers P]". There is no P. And a line with no
rate gets a reorder point of 0, so ANY stock puts it above the trigger, where
it landed on "[Above ROP 0.0]" -- true, and useless.

Measured on the live store: 10,967 SKUs (27.6% of the catalogue) have no POS
sales history, and NONE of them is a join failure -- every SKU that has ever
sold does resolve a POS-derived rate, so the engine is right not to order
them. But 1,723 of them still hold stock: 17,966 units and KES 6,696,865 of
capital in lines that have never sold a unit at this store, described to the
buyer as adequately covered.

Not ordering them is correct. Saying nothing about them is not.
"""
import pytest

from oasis.logic import order_up_to as ou


class TestTheEngineRefusesWithAReason:
    def test_no_rate_yields_a_reason_and_no_terms(self):
        t = ou.recommend({"avg_daily_sales": 0, "supplier_name": "ACME",
                          "current_stock": 40, "department": "GENERAL"})
        assert t["quantity"] == 0.0
        assert "no measured sales rate" in t["reason"]
        # The stub deliberately carries no S: there is nothing to compute.
        assert "S" not in t

    @pytest.mark.parametrize("d", [0, 0.0, None, -1])
    def test_every_falsy_or_negative_rate_takes_that_path(self, d):
        t = ou.recommend({"avg_daily_sales": d, "supplier_name": "ACME",
                          "current_stock": 0, "department": "GENERAL"})
        assert t.get("reason") == "no measured sales rate"


class TestTheBridgeSaysWhichItIs:
    """Both sides of the trigger fork, since a stocked line never reaches the
    order-up-to branch at all."""

    def _util(self, tmp_path):
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        return SimulationOrderUtil(str(tmp_path))

    def _line(self, **kw):
        p = {
            "sku": "NEVER-SOLD-1", "itm_cd": "NEVER-SOLD-1",
            "product_name": "DISCONTINUED THING",
            "supplier_name": "SX0001 - ACME",
            "avg_daily_sales": 0.0,
            "ads_source": "none",
            "current_stock": 0.0, "current_stocks": 0.0,
            "unit_cost": 120.0, "selling_price": 200.0,
            "department": "GENERAL", "pack_size": 1,
            "lead_time_days": 3.0, "reorder_point": 0.0,
        }
        p.update(kw)
        return p

    def test_a_stocked_never_sold_line_is_flagged_and_named(self, tmp_path):
        util = self._util(tmp_path)
        recs = util.calculate_order_quantity(
            [self._line(current_stock=50.0, current_stocks=50.0)],
            use_real_date=True)
        r = recs[0]
        assert r.get("ads_missing") is True, "the flag must survive to the rec"
        assert float(r.get("recommended_quantity") or 0) == 0.0
        txt = str(r.get("reasoning") or "")
        assert "no demand signal" in txt
        assert "Above ROP" not in txt, (
            "a never-sold line reading 'Above ROP' is indistinguishable from "
            "a healthy one")

    def test_an_empty_never_sold_line_is_also_flagged(self, tmp_path):
        util = self._util(tmp_path)
        recs = util.calculate_order_quantity([self._line()], use_real_date=True)
        r = recs[0]
        assert r.get("ads_missing") is True
        txt = str(r.get("reasoning") or "")
        assert "position already covers P" not in txt, (
            "there is no protection interval on a line with no rate")

    def test_a_line_with_demand_is_not_flagged(self, tmp_path):
        util = self._util(tmp_path)
        recs = util.calculate_order_quantity(
            [self._line(avg_daily_sales=2.0, ads_source="pos_weighted",
                        current_stock=500.0, current_stocks=500.0,
                        reorder_point=20.0)],
            use_real_date=True)
        assert not recs[0].get("ads_missing")

    def test_the_source_is_carried_so_the_gap_is_attributable(self, tmp_path):
        util = self._util(tmp_path)
        recs = util.calculate_order_quantity(
            [self._line(current_stock=9.0, current_stocks=9.0)],
            use_real_date=True)
        assert recs[0].get("ads_source") == "none"
