"""The slow tail of the bread shelf: presence, and the bakeries' lines past the gate.

Replayed over Apr-Sep 2026 on the bakeries' own deliveries, the engine lost to
the suppliers in one band only -- lines selling under one a day, 81.3% fill
against 88.4% -- for two reasons:

1. On next-morning delivery a line under ~0.1 a day needs no stock at 90%
   service, so it was suppressed; the empty shelf sold nothing, its rate
   decayed and it was never reordered. Every line launched mid-period hit it.
   In fresh_cycle.presence_departments a line that still sells is now held at
   one unit, and a line that cannot sell one within its life at exactly one.
2. The bakeries' lines filed outside BREAD/CAKES -- Supa cookies in BISCUITS --
   never cleared the KES 200 per-line floor. The gate now exempts the daily
   bakeries' lines by supplier (fresh_cycle.bakery_suppliers).

Together: the band reaches 88.1% at KES 41k of supplier expiry against 269k.
"""
import json
import os

import pytest

from oasis.logic import order_up_to as ou
from oasis.logic.simulation_bridge import SimulationOrderUtil

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def line(d, dept="BREAD", sku="SUPA 200G BREADCRUMBS REGULAR", supplier="MINI BAKERIES NBI LTD", stock=0.0):
    return {"avg_daily_sales": d, "supplier_name": supplier, "current_stock": stock, "on_order_qty": 0.0,
            "lead_time_days": 1.0, "department": dept, "sku": sku}


@pytest.fixture(autouse=True)
def fresh_config():
    ou.reset_fresh_cycle()
    yield
    ou.reset_fresh_cycle()


class TestPresence:
    def test_a_line_that_still_sells_is_never_planned_at_zero(self):
        t = ou.recommend(line(0.03))
        assert t["S"] == 1.0 and t["quantity"] == 1.0
        assert t["presence_hold"] and not t["auto_order_suppressed"]

    def test_a_slow_line_is_held_at_exactly_one(self):
        t = ou.recommend(line(0.15, sku="SLOW LOAF"))          # 0.15 x 5-day life < 1 unit
        assert t["S"] == 1.0

    def test_a_line_with_no_measured_sales_gets_nothing(self):
        t = ou.recommend(line(0.0))
        assert not t.get("quantity")

    def test_a_fast_loaf_is_untouched(self):
        t = ou.recommend(line(64.6, sku="FESTIVE 400G MILKY WHITE SLICED", supplier="DPL FESTIVE LIMITED"))
        assert not t["presence_hold"] and t["S"] > 60

    def test_it_stops_at_the_section(self):
        t = ou.recommend(line(0.03, dept="BISCUITS", sku="X"))
        assert t["S"] == 0.0 and t["auto_order_suppressed"]

    def test_it_is_config(self, monkeypatch):
        fc = dict(ou.fresh_cycle()); fc["presence"] = frozenset()
        monkeypatch.setattr(ou, "_FRESH_CYCLE", fc)
        assert ou.recommend(line(0.03))["auto_order_suppressed"]


def _rec(sku, dept, qty, cost, supplier):
    return {"sku": sku, "product_name": sku, "department": dept, "supplier_name": supplier,
            "recommended_quantity": qty, "cost_price": cost, "is_fresh": True, "pack_size": 1}


class TestTheBakeriesPassTheGate:
    @pytest.fixture
    def util(self, tmp_path):
        return SimulationOrderUtil(str(tmp_path))

    def test_a_bakery_cookie_in_biscuits_is_ordered(self, util):
        out = util.apply_minimum_order_gate([_rec("SUPA 200G ASSORTED COOKIES", "BISCUITS", 1, 69.0, "MINI BAKERIES NBI  LTD")])
        assert [r["sku"] for r in out["po_recs"]] == ["SUPA 200G ASSORTED COOKIES"]
        assert out["po_recs"][0]["moq_exempt"] is True

    def test_another_suppliers_biscuit_is_still_gated(self, util):
        out = util.apply_minimum_order_gate([_rec("SOME 200G BISCUIT", "BISCUITS", 1, 69.0, "BISCUIT CO LTD")])
        assert out["po_recs"] == []

    def test_the_supplier_list_is_a_threshold_too(self, tmp_path):
        u = SimulationOrderUtil(str(tmp_path), thresholds={"moq_exempt_departments": [], "moq_exempt_suppliers": [],
                                                           "min_order_units": 10, "min_order_value_kes": 5000,
                                                           "min_item_value_fresh_kes": 200.0, "min_item_value_dry_kes": 100.0})
        out = u.apply_minimum_order_gate([_rec("SUPA 200G ASSORTED COOKIES", "BISCUITS", 1, 69.0, "MINI BAKERIES NBI LTD")])
        assert out["po_recs"] == []


def test_both_config_tiers_carry_the_lists():
    for tier in ("oasis_engines_config.json", "oasis_engines_config.default.json"):
        fc = json.load(open(os.path.join(ROOT, "oasis", "data", tier), encoding="utf-8"))["fresh_cycle"]
        assert fc["presence_departments"] == ["BREAD", "CAKES"], tier
        assert len(fc["bakery_suppliers"]) == 4, tier
