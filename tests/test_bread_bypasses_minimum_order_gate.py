"""Bread lines bypass the minimum-order gate; everything else is still gated.

The gate's KES 200 per-line floor and 10-unit / KES 5,000 supplier floor were
written for weekly dry-goods baskets. Bread is a few loaves a day from a bakery
that delivers daily, and a day-by-day replay of Apr-Sep 2026 against the
bakeries' real deliveries (devkit/bread_backtest.py) priced the gate at KES
209-227k a year of lost bread sales. So BREAD is exempt, by department.
"""
import pytest

from oasis.logic.simulation_bridge import MOQ_EXEMPT_DEPARTMENTS, SimulationOrderUtil


@pytest.fixture
def util(tmp_path):
    return SimulationOrderUtil(str(tmp_path))


def _line(sku, dept, qty, cost, supplier="BAKERY LTD", fresh=True):
    return {"sku": sku, "product_name": sku, "department": dept, "supplier_name": supplier,
            "recommended_quantity": qty, "cost_price": cost, "is_fresh": fresh, "pack_size": 1}


def test_bread_is_the_default_exemption():
    assert "BREAD" in MOQ_EXEMPT_DEPARTMENTS


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
    # a cake line that clears its own item floor but not the supplier minimum
    # alone is carried by the bread on the same PO
    recs = [_line("LOAF", "BREAD", 60, 90.0), _line("CAKE", "CAKES", 5, 60.0, fresh=True)]
    out = util.apply_minimum_order_gate(recs)
    assert {r["sku"] for r in out["po_recs"]} == {"LOAF", "CAKE"}


def test_a_non_bread_basket_below_the_minimum_is_still_dropped(util):
    recs = [_line("CAKE", "CAKES", 5, 60.0, supplier="CAKES ONLY LTD")]       # 5 units, KES 300
    out = util.apply_minimum_order_gate(recs)
    assert out["po_recs"] == []


def test_the_exemption_is_a_threshold_not_a_constant(tmp_path):
    u = SimulationOrderUtil(str(tmp_path), thresholds={"moq_exempt_departments": [],
                                                       "min_order_units": 10, "min_order_value_kes": 5000})
    out = u.apply_minimum_order_gate([_line("LOAF", "BREAD", 2, 55.0)])
    assert out["po_recs"] == []
