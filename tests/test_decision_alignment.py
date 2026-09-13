"""`decide_batch` must return decisions in the caller's order.

`ConsolidatedTransferService.optimize_network` does

    for sf, decision in zip(all_shortfalls, decisions)

against the list it passed in. `decide_batch` processes shortfalls in
descending-ADS order so the busiest branch gets first claim on donor excess —
correct — but it used to *return* them in that processing order too. Whenever
the shortfalls did not happen to arrive in descending-ADS order, every decision
was then attached to a different store's shortfall:

  * the transfer record took `to_org` and `itm_cd` from one shortfall and
    `donor_org`, `product_name` and `qty` from another's decision, so stock
    was routed to the wrong branch — visible as transfers whose `from_org`
    and `to_org` were the same store;
  * `_adjust_order` reduced the wrong store's purchase order;
  * `donor_additions` compensated the wrong donor.

The processing priority is still ADS-descending. Only the order of the
returned list changed. These tests hold both halves of that in place.
"""
import pytest

from oasis.logic.fulfillment_decider import (
    FulfillmentDecider, NetworkAvailabilityMap, StoreSkuState,
)


def _shortfall(org, itm, ads, qty=10.0):
    return {"itm_cd": itm, "product_name": f"ITEM {itm}", "recipient_org": org,
            "shortfall_qty": qty, "avg_daily_sales": ads, "current_stock": 0.0,
            "lead_time_days": 3.0, "unit_cost": 10.0, "original_rec": {}}


#: deliberately NOT in descending-ADS order, which is the only case that broke
SHORTFALLS = [
    _shortfall("ST1", "AAA", 1.0),
    _shortfall("ST2", "BBB", 99.0),
    _shortfall("ST3", "CCC", 50.0),
]


def test_decisions_align_with_the_shortfalls_passed_in():
    decisions = FulfillmentDecider().decide_batch(
        list(SHORTFALLS), NetworkAvailabilityMap(),
        org_names={o: o for o in ("ST1", "ST2", "ST3")})

    assert len(decisions) == len(SHORTFALLS)
    mismatched = [
        (sf["recipient_org"], sf["itm_cd"], d.recipient_org, d.itm_cd)
        for sf, d in zip(SHORTFALLS, decisions)
        if sf["recipient_org"] != d.recipient_org or sf["itm_cd"] != d.itm_cd
    ]
    assert not mismatched, (
        "decide_batch returned decisions in a different order from the "
        "shortfalls it was given. optimize_network zips the two together, so "
        "each of these would route stock to the wrong store:\n" +
        "\n".join(f"  shortfall {a}/{b} got the decision for {c}/{d}"
                  for a, b, c, d in mismatched))


def test_no_transfer_is_ever_from_a_store_to_itself():
    """The symptom the misalignment produced, guarded directly."""
    decisions = FulfillmentDecider().decide_batch(
        list(SHORTFALLS), NetworkAvailabilityMap(),
        org_names={o: o for o in ("ST1", "ST2", "ST3")})
    for sf, d in zip(SHORTFALLS, decisions):
        if d.decision in ("TRANSFER", "BOTH") and d.donor_org:
            assert d.donor_org != sf["recipient_org"], (
                f"{sf['recipient_org']} was selected as its own donor for "
                f"{sf['itm_cd']}")


def test_high_velocity_still_gets_first_claim_on_donor_excess():
    """The sort is a real behaviour, not incidental - it must survive the fix.

    One donor with enough excess for exactly one of two recipients. The busy
    branch must win it, regardless of the order the shortfalls arrive in.
    """
    nm = NetworkAvailabilityMap()
    nm.add(StoreSkuState(
        org_cd="DONOR", org_name="DONOR", itm_cd="AAA", product_name="ITEM AAA",
        current_stock=1000.0, avg_daily_sales=1.0, safety_stock=2.0,
        excess=100.0, sell_price=100.0, department="GENERAL"))

    # slow branch listed FIRST, busy branch second
    slow = _shortfall("SLOW", "AAA", ads=1.0, qty=100.0)
    busy = _shortfall("BUSY", "AAA", ads=500.0, qty=100.0)
    decisions = FulfillmentDecider().decide_batch(
        [slow, busy], nm, org_names={"DONOR": "DONOR", "SLOW": "SLOW", "BUSY": "BUSY"})

    by_org = {d.recipient_org: d for d in decisions}
    assert set(by_org) == {"SLOW", "BUSY"}, "decisions lost a recipient"
    assert by_org["BUSY"].transfer_qty >= by_org["SLOW"].transfer_qty, (
        "the slow branch took donor excess ahead of the busy one - the "
        "descending-ADS processing priority was lost when the return order "
        "was fixed")


def test_empty_batch_is_handled():
    assert FulfillmentDecider().decide_batch([], NetworkAvailabilityMap()) == []
