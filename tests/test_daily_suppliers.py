"""The daily bakeries are derived from the receipt history, not listed.

A daily, overnight supplier is measurable wherever a receipt history exists:
the POS adapter, lata_shield and Odoo's supplier rhythm all write the same
fields (median_gap_days between receipts, estimated_delivery_days,
total_orders_2025), and the receipt file measures the lead. On the reference
store the rule finds 10 of 599 suppliers -- the four bakeries its config used
to list, two bread suppliers the list missed (Mibisco, Rabai) and four
dairies. The gate then keeps the ones that supply an exempt department.
"""
import pytest

from oasis.logic import order_up_to as ou


@pytest.fixture(autouse=True)
def fresh():
    ou.reset_fresh_cycle()
    yield
    ou.reset_fresh_cycle()


def sup(gap=1, lead=1, n=300, **kw):
    return dict({"median_gap_days": gap, "estimated_delivery_days": lead, "total_orders_2025": n}, **kw)


def derive(cadence, patterns=None):
    return ou.daily_suppliers(cadence=cadence, patterns=patterns or {})


class TestTheRule:
    def test_daily_and_overnight_qualifies(self):
        assert derive({"BAKERY LTD": sup()}) == {"BAKERY LTD"}

    def test_a_weekly_supplier_does_not(self):
        assert not derive({"WEEKLY LTD": sup(gap=7)})

    def test_a_slow_lead_does_not(self):
        assert not derive({"FAR LTD": sup(lead=3)})

    def test_two_receipts_a_day_apart_are_not_a_cadence(self):
        assert not derive({"FIREWORKS SR": sup(n=4)})

    def test_the_measured_lead_beats_the_stated_one(self):
        assert not derive({"BAKERY LTD": sup(lead=1)}, {"BAKERY LTD": {"lead_time_days": 2.0}})
        assert derive({"BAKERY LTD": sup(lead=7)}, {"BAKERY LTD": {"lead_time_days": 1.0}})

    def test_the_lata_gap_is_a_lead_not_a_cadence(self):
        # lata_median_gap_days is PO -> receipt; reading it as cadence called
        # 56 of the reference store's suppliers daily
        assert not derive({"WEEKLY LTD": sup(gap=7, lata_median_gap_days=1)})

    def test_names_are_matched_through_the_one_spelling(self):
        assert derive({"DPL FESTIVE  LIMITED": sup()}) == {"DPL FESTIVE LIMITED"}


class TestTheReferenceStore:
    def test_finds_its_bakeries_and_dairies_and_nothing_else(self):
        got = ou.daily_suppliers()
        bakeries = {"DPL FESTIVE LIMITED", "MINI BAKERIES NBI LTD", "KENAFRIC BAKERY LTD",
                    "BROADWAYS BAKERY LTD", "MIBISCO LTD", "RABAI FOODS LIMITED"}
        dairies = {"BROOKSIDE DAIRY LIMITED", "BIO FOOD PRODUCTS LTD", "GITHUNGURI DAIRY FARMERS",
                   "NEW KENYA COOP CREAMERIES LTD"}
        assert got == bakeries | dairies
