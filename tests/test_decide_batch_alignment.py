"""decide_batch must return decisions in the CALLER's order.

optimize_network consumes the result as

    for sf, decision in zip(all_shortfalls, decisions)

against the unsorted shortfall list. decide_batch used to sort the list for
donor-priority and return decisions in that sorted order, so whenever the two
orders disagreed -- i.e. whenever the shortfalls did not already arrive in
descending-ADS order -- every pair was mismatched. The transfer record then
took to_org/itm_cd from one shortfall and donor_org/qty from another's
decision, _adjust_order reduced the wrong store's purchase order, and
donor_additions compensated the wrong donor.

Priority must still be honoured: the highest-ADS shortfall has to be OFFERED
the donor excess first, or a slow line can drain a donor before a fast one is
considered. So the two orderings are genuinely different and both matter --
process by priority, return by position. These tests pin both halves.
"""
import pytest

from oasis.logic.fulfillment_decider import (
    FulfillmentDecider,
    NetworkAvailabilityMap,
    StoreSkuState,
)


def _shortfall(itm, org, ads, qty=5.0):
    return {
        "itm_cd": itm,
        "product_name": f"PRODUCT {itm}",
        "recipient_org": org,
        "shortfall_qty": qty,
        "avg_daily_sales": ads,
        "current_stock": 0.0,
        "lead_time_days": 3.0,
        "unit_cost": 100.0,
        "is_ordering_day": True,
        "original_rec": {"department": "GROCERY"},
    }


def _map_with_donor(items, donor_org="DONOR", stock=500.0, ads=1.0):
    nmap = NetworkAvailabilityMap()
    for itm in items:
        nmap.add(StoreSkuState(
            org_cd=donor_org, org_name="Donor Store", itm_cd=itm,
            product_name=f"PRODUCT {itm}", current_stock=stock,
            avg_daily_sales=ads, safety_stock=ads * 14,
            excess=stock - ads * 14, is_fresh=False, sell_price=150.0,
            department="GROCERY",
        ), "")
    return nmap


@pytest.fixture
def decider():
    return FulfillmentDecider()


def test_decisions_come_back_in_the_order_they_were_given(decider):
    """ASCENDING ads, so the priority order is the exact reverse."""
    items = ["A", "B", "C", "D"]
    shortfalls = [_shortfall(i, f"ORG{n}", ads)
                  for n, (i, ads) in enumerate(zip(items, [0.1, 1.0, 5.0, 50.0]))]
    decisions = decider.decide_batch(shortfalls, _map_with_donor(items),
                                     org_names={"DONOR": "Donor Store"})

    assert len(decisions) == len(shortfalls), (
        "a decision was dropped; zip() in optimize_network would silently "
        "re-misalign every pair after the gap")
    for sf, d in zip(shortfalls, decisions):
        assert d.itm_cd == sf["itm_cd"], "decisions are not caller-aligned"
        assert d.recipient_org == sf["recipient_org"]


def test_alignment_holds_for_an_already_sorted_list(decider):
    """The case the old code got right, kept so a fix cannot regress it."""
    items = ["A", "B", "C"]
    shortfalls = [_shortfall(i, f"ORG{n}", ads)
                  for n, (i, ads) in enumerate(zip(items, [50.0, 5.0, 0.1]))]
    decisions = decider.decide_batch(shortfalls, _map_with_donor(items),
                                     org_names={"DONOR": "Donor Store"})
    for sf, d in zip(shortfalls, decisions):
        assert d.itm_cd == sf["itm_cd"]


def test_a_transfer_never_names_the_recipient_as_its_own_donor(decider):
    """The symptom the misalignment produced in the wild.

    A decision carrying another shortfall's recipient could name the receiving
    store as the donor -- a transfer from a store to itself.
    """
    items = ["A", "B", "C", "D"]
    shortfalls = [_shortfall(i, f"ORG{n}", ads)
                  for n, (i, ads) in enumerate(zip(items, [0.1, 1.0, 5.0, 50.0]))]
    decisions = decider.decide_batch(shortfalls, _map_with_donor(items),
                                     org_names={"DONOR": "Donor Store"})
    for d in decisions:
        if d.donor_org:
            assert d.donor_org != d.recipient_org, (
                f"{d.itm_cd}: transfer from {d.recipient_org} to itself")


def test_the_fastest_line_is_offered_the_donor_first(decider):
    """Priority is the reason for sorting at all, so it must survive.

    One donor, only enough excess for one claimant, and the fast line last in
    the caller's list. It must still be the one that gets the units.
    """
    items = ["SLOW", "FAST"]
    nmap = NetworkAvailabilityMap()
    # Excess of ~16 units: enough for one 15-unit claim, not two.
    for itm in items:
        nmap.add(StoreSkuState(
            org_cd="DONOR", org_name="Donor", itm_cd=itm,
            product_name=f"PRODUCT {itm}", current_stock=30.0,
            avg_daily_sales=1.0, safety_stock=14.0, excess=16.0,
            is_fresh=False, sell_price=150.0, department="GROCERY",
        ), "")
    shortfalls = [_shortfall("SLOW", "ORG1", 0.05, qty=15.0),
                  _shortfall("FAST", "ORG2", 99.0, qty=15.0)]
    decisions = decider.decide_batch(shortfalls, nmap,
                                     org_names={"DONOR": "Donor"})
    by_item = {d.itm_cd: d for d in decisions}
    assert by_item["FAST"].transfer_qty >= by_item["SLOW"].transfer_qty, (
        "the slow line drained the donor before the fast one was considered")
