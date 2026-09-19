"""Bread lines bypass the minimum-order gate; everything else is still gated.

The gate's KES 200 per-line floor and 10-unit / KES 5,000 supplier floor were
written for weekly dry-goods baskets. Bread is a few loaves a day from a bakery
that delivers daily, and a day-by-day replay of Apr-Sep 2026 against the
bakeries' real deliveries (devkit/bread_backtest.py) priced the gate at KES
209-227k a year of lost bread sales. So BREAD is exempt, by department.

CAKES joined once the supplier-pattern lookup was fixed: planned at the bakery's
real 1-day lead, cakes are ordered about 3 days deep instead of 11, and those
shallower lines fell under the per-line floor -- gate drops on the bread shelf
went 396 -> 1,548 and fill 98.1% -> 96.5%. Exempting CAKES brings drops back to
113 and fill to 98.1%. Mixed departments (BISCUITS) stay gated: exempting a
department exempts every supplier in it.

The list is this store's configuration (fresh_cycle.moq_exempt_departments in
its engine config); the shipped default exempts nothing.
"""
import pytest

from oasis.logic.simulation_bridge import SimulationOrderUtil, moq_exempt_departments


@pytest.fixture
def util(tmp_path):
    return SimulationOrderUtil(str(tmp_path))


def _line(sku, dept, qty, cost, supplier="BAKERY LTD", fresh=True):
    return {"sku": sku, "product_name": sku, "department": dept, "supplier_name": supplier,
            "recommended_quantity": qty, "cost_price": cost, "is_fresh": fresh, "pack_size": 1}


def test_bread_is_this_stores_exemption():
    assert "BREAD" in moq_exempt_departments()


def test_cakes_are_exempt_and_biscuits_are_not():
    assert "CAKES" in moq_exempt_departments()
    assert "BISCUITS" not in moq_exempt_departments()


def test_a_small_cake_line_is_ordered(util):
    out = util.apply_minimum_order_gate([_line("POUNDCAKE", "CAKES", 2, 60.0)])   # KES 120
    assert [r["sku"] for r in out["po_recs"]] == ["POUNDCAKE"]
    assert out["po_recs"][0]["moq_exempt"] is True


def test_a_small_bread_line_is_ordered(util):
    out = util.apply_minimum_order_gate([_line("LOAF", "BREAD", 2, 55.0)])   # KES 110, 2 units
    assert [r["sku"] for r in out["po_recs"]] == ["LOAF"]
    assert out["transfer_recs"] == []
    assert out["po_recs"][0]["moq_exempt"] is True


def test_the_same_line_outside_bread_is_still_gated(util):
    out = util.apply_minimum_order_gate([_line("YOGHURT", "YOGHURT", 2, 55.0)])
    assert out["po_recs"] == []
    assert [r["sku"] for r in out["transfer_recs"]] == ["YOGHURT"]


def test_department_match_is_normalised(util):
    out = util.apply_minimum_order_gate([_line("LOAF", "  bread ", 1, 30.0)])
    assert [r["sku"] for r in out["po_recs"]] == ["LOAF"]


def test_bread_counts_towards_its_suppliers_basket(util):
    # a gated line that clears its own item floor but not the supplier minimum
    # alone is carried by the bread on the same PO
    recs = [_line("LOAF", "BREAD", 60, 90.0), _line("COOKIES", "BISCUITS", 5, 60.0, fresh=False)]
    out = util.apply_minimum_order_gate(recs)
    assert {r["sku"] for r in out["po_recs"]} == {"LOAF", "COOKIES"}


def test_a_gated_basket_below_the_minimum_is_still_dropped(util):
    recs = [_line("COOKIES", "BISCUITS", 5, 60.0, supplier="BISCUITS ONLY LTD", fresh=False)]   # 5 units, KES 300
    out = util.apply_minimum_order_gate(recs)
    assert out["po_recs"] == []


def test_the_exemption_is_a_threshold_not_a_constant(tmp_path):
    u = SimulationOrderUtil(str(tmp_path), thresholds={"moq_exempt_departments": [],
                                                       "min_order_units": 10, "min_order_value_kes": 5000})
    out = u.apply_minimum_order_gate([_line("LOAF", "BREAD", 2, 55.0)])
    assert out["po_recs"] == []
