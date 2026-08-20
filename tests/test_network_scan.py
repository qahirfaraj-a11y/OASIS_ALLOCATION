"""Tests for ConsolidatedTransferService.scan_network_opportunities().

Validates the guarantees of the unified scan:
- PULL finds donors for deficit stores
- PUSH rebalances cold nodes to hot nodes with consistent donor protection
- Pending (REQUESTED/IN_TRANSIT) transfers suppress regeneration
- Fresh items are surfaced as manual_only
- Intra-scan booking prevents double-allocating one donor's excess
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService

ORG_NAMES = {"ORG001": "Nairobi Main", "ORG002": "Westlands", "ORG003": "Karen"}


def _product(itm, name, stock, ads, dept="GENERAL", price=100.0,
             reorder_point=0.0, is_fresh=False, supplier="ACME"):
    return {
        "item_code": itm,
        "product_name": name,
        "current_stocks": stock,
        "avg_daily_sales": ads,
        "department": dept,
        "selling_price": price,
        "reorder_point": reorder_point,
        "is_fresh": is_fresh,
        "supplier_name": supplier,
        "uom": "EA",
    }


def _service(stock_data, **kw):
    return ConsolidatedTransferService(
        org_names=ORG_NAMES, stock_data=stock_data,
        cold_node_days=kw.pop("cold_node_days", 60),
        hot_node_days=kw.pop("hot_node_days", 14),
        **kw,
    )


class TestPullScan:
    def _stock(self):
        return {
            # Donor: 600 units at 5/day = 120 days cover — heavy excess
            "ORG001": [_product("SKU1", "RICE 1KG", stock=600, ads=5.0)],
            # Recipient: 2 units at 4/day = 0.5 days cover — deficit
            "ORG002": [_product("SKU1", "RICE 1KG", stock=2, ads=4.0)],
            "ORG003": [],
        }

    def test_pull_opportunity_found(self):
        scan = _service(self._stock()).scan_network_opportunities()
        pulls = [o for o in scan.opportunities if o.type == "PULL"]
        assert len(pulls) == 1
        o = pulls[0]
        assert o.from_org == "ORG001"
        assert o.to_org == "ORG002"
        assert o.transfer_qty >= 1
        assert o.value_kes == o.transfer_qty * 100.0
        assert not o.manual_only

    def test_no_pull_when_recipient_covered(self):
        stock = self._stock()
        stock["ORG002"][0]["current_stocks"] = 100  # 25 days cover
        scan = _service(stock).scan_network_opportunities()
        assert [o for o in scan.opportunities if o.type == "PULL"] == []

    def test_moq_failure_acts_as_trigger(self):
        # Donor with excess but NOT an overstock donor: 40 days of cover sits
        # under the category's dead threshold (45d default), so PUSH stays out
        # of this and only the MOQ failure can trigger a transfer. This used to
        # read "50 days < 60" against the old flat cold_node_days; the gate is
        # now AMIT's per-category tier, which is lower.
        # Recipient at 8 days cover — neither a pull deficit (>7d) nor below ROP.
        stock = {
            "ORG001": [_product("SKU1", "RICE 1KG", stock=200, ads=5.0)],
            "ORG002": [_product("SKU1", "RICE 1KG", stock=32, ads=4.0)],
            "ORG003": [],
        }
        base = _service(stock).scan_network_opportunities()
        assert [o for o in base.opportunities if o.to_org == "ORG002"] == []

        # The ordering engine wanted 24 units but the PO failed MOQ — that
        # quantity becomes the shortfall floor for the transfer scan.
        scan = _service(stock).scan_network_opportunities(
            moq_failures={"ORG002": {"SKU1": 24.0}}
        )
        moq_pulls = [o for o in scan.opportunities if o.to_org == "ORG002"]
        assert len(moq_pulls) == 1
        assert moq_pulls[0].type == "PULL"
        assert moq_pulls[0].transfer_qty >= 1


class TestPendingAwareness:
    def _stock(self):
        return {
            "ORG001": [_product("SKU1", "RICE 1KG", stock=600, ads=5.0)],
            "ORG002": [_product("SKU1", "RICE 1KG", stock=2, ads=4.0)],
            "ORG003": [],
        }

    def test_pending_inbound_suppresses_regeneration(self):
        """Once every recommended movement for an item is queued, re-running
        the scan must not generate it again (the double-transfer bug)."""
        first = _service(self._stock()).scan_network_opportunities()
        to_org002 = [o for o in first.opportunities if o.to_org == "ORG002"]
        assert to_org002, "precondition: scan finds transfers"

        pending = [{
            "from_org": o.from_org, "to_org": o.to_org,
            "itm_cd": o.itm_cd, "qty": o.transfer_qty,
            "status": "REQUESTED",
        } for o in to_org002]

        second = _service(self._stock()).scan_network_opportunities(
            pending_transfers=pending
        )
        regenerated = [o for o in second.opportunities if o.to_org == "ORG002"]
        assert regenerated == [], "queued transfers were regenerated"

    def test_received_transfers_do_not_count(self):
        """Completed transfers are real stock already — no suppression."""
        scan = _service(self._stock()).scan_network_opportunities(
            pending_transfers=[{
                "from_org": "ORG001", "to_org": "ORG002",
                "itm_cd": "SKU1", "qty": 999, "status": "RECEIVED",
            }]
        )
        assert [o for o in scan.opportunities if o.to_org == "ORG002"]

    def test_outbound_commitment_reduces_donor_excess(self):
        """A donor with all its excess already committed cannot donate again."""
        scan = _service(self._stock()).scan_network_opportunities(
            pending_transfers=[{
                "from_org": "ORG001", "to_org": "ORG003",
                "itm_cd": "SKU1", "qty": 600, "status": "IN_TRANSIT",
            }]
        )
        assert [o for o in scan.opportunities if o.from_org == "ORG001"] == []

    def test_db_column_name_records_accepted(self):
        """Rows straight from INTEGRATION_TRANSFER_ORDERS (upper-case keys).
        60 inbound units bring the recipient above the 14-day hot threshold
        (2 + 60 = 62 units at 4/day = 15.5d) — nothing should regenerate."""
        scan = _service(self._stock()).scan_network_opportunities(
            pending_transfers=[{
                "FROM_ORG_CD": "ORG001", "TO_ORG_CD": "ORG002",
                "ITM_CD": "SKU1", "QUANTITY": 60, "STATUS": "REQUESTED",
            }]
        )
        regenerated = [o for o in scan.opportunities if o.to_org == "ORG002"]
        assert regenerated == []

    def test_partial_pending_reduces_recommendation(self):
        """A partial pending transfer shrinks (not just suppresses) the
        remaining recommended quantity for the same recipient."""
        without = _service(self._stock()).scan_network_opportunities()
        qty_without = sum(o.transfer_qty for o in without.opportunities
                          if o.to_org == "ORG002")
        with_pending = _service(self._stock()).scan_network_opportunities(
            pending_transfers=[{
                "from_org": "ORG001", "to_org": "ORG002",
                "itm_cd": "SKU1", "qty": 30, "status": "IN_TRANSIT",
            }]
        )
        qty_with = sum(o.transfer_qty for o in with_pending.opportunities
                       if o.to_org == "ORG002")
        assert qty_with < qty_without
        # Combined supply (pending + newly recommended) must not exceed the
        # 14-day hot-node target by more than rounding slack.
        target_14d = 14.0 * 4.0
        assert 2 + 30 + qty_with <= target_14d + 2.0


class TestCrossPathLedger:
    """One donor, both entry points, one book of what it has promised.

    Donor excess was drawn down by THREE mechanisms that could not see each
    other: the scan kept a local ``booked`` dict computed from stock_data,
    ``decide_batch`` mutated ``StoreSkuState`` on the availability map, and
    ``ProactiveRebalancer`` mutated ``current_stock`` privately. Run ordering
    and then a network scan and the scan re-offered every unit the other two
    had already promised.
    """

    ADS, STOCK = 5.0, 600.0
    EXCESS = STOCK - ADS * 14          # 530: excess above the 14-day floor

    def _stock(self):
        # recipient demand deliberately LARGER than anything the donor may
        # release, so every path wants more than it can have and the cap binds.
        # With a small recipient the total stays under the cap by luck and the
        # test passes even with the ledger disabled.
        return {
            "ORG001": [_product("SKU1", "RICE 1KG", stock=self.STOCK, ads=self.ADS)],
            "ORG002": [_product("SKU1", "RICE 1KG", stock=2, ads=40.0)],
            "ORG003": [],
        }

    def _order(self):
        return {"ORG002": [{
            "itm_cd": "SKU1", "product_name": "RICE 1KG",
            "recommended_quantity": 600, "avg_daily_sales": 40.0,
            "current_stocks": 2, "selling_price": 100.0, "cost_price": 70.0,
            "supplier_name": "ACME", "estimated_delivery_days": 3,
            "is_fresh": False, "department": "GENERAL",
        }]}

    def test_a_donor_never_promises_more_than_it_has_spare(self):
        svc = _service(self._stock())
        plan = svc.optimize_network(self._order())
        pledged = sum(t.qty for t in plan.transfers if t.from_org == "ORG001")
        assert pledged > 0, "precondition: the ordering path pledges something"

        scan = svc.scan_network_opportunities()
        scanned = sum(o.transfer_qty for o in scan.opportunities
                      if o.from_org == "ORG001")

        # Across BOTH entry points and all three claimants (decide_batch,
        # ProactiveRebalancer, the scan), one donor cannot promise more than it
        # actually has spare. With the ledger disabled this same fixture
        # promises ~1,568 units from a store holding 600 — the three claimants
        # each hand out the full excess, unaware of the others.
        assert pledged + scanned <= self.EXCESS, (
            f"donor over-promised: {pledged} + {scanned} > {self.EXCESS} spare")
        assert pledged + scanned <= self.STOCK, "promised more than it holds"

    def test_the_index_aliases_do_not_each_get_their_own_allowance(self):
        """One physical pile of stock, however many names it answers to.

        NetworkAvailabilityMap indexes every state under its code, its product
        name and its barcode. ProactiveRebalancer walks that index directly, so
        it visits the same stock once per alias. The ledger must therefore be
        keyed on the DONOR'S canonical code — keying it on the loop variable
        gives each alias a fresh allowance and duplicates the transfer.
        """
        svc = _service(self._stock())
        plan = svc.optimize_network(self._order())
        from_donor = [t for t in plan.transfers if t.from_org == "ORG001"]
        # the same donor/recipient/SKU must not appear twice at full size
        proactive = [t for t in from_donor if t.urgency == "LOW"]
        assert len(proactive) <= 1, (
            f"one pile of stock promised {len(proactive)} times over: "
            f"{[t.qty for t in proactive]}")

    def test_every_promise_is_recorded_in_the_ledger(self):
        """No path may take stock without writing it down."""
        svc = _service(self._stock())
        plan = svc.optimize_network(self._order())
        pledged = sum(t.qty for t in plan.transfers if t.from_org == "ORG001")
        # includes the ProactiveRebalancer's share, which used to mutate
        # local state privately and was the LARGEST claimant in a typical run
        assert svc.donor_ledger.total_booked == pytest.approx(pledged)

        scan = svc.scan_network_opportunities()
        scanned = sum(o.transfer_qty for o in scan.opportunities
                      if o.from_org == "ORG001")
        assert svc.donor_ledger.total_booked == pytest.approx(pledged + scanned)

    def test_the_scan_sees_what_ordering_already_took(self):
        """The scan's view of a donor must shrink after the ordering path runs."""
        fresh = _service(self._stock())
        alone = sum(o.transfer_qty
                    for o in fresh.scan_network_opportunities().opportunities
                    if o.from_org == "ORG001")
        assert alone > 0, "precondition: the scan finds something on its own"

        after = _service(self._stock())
        after.optimize_network(self._order())
        second = sum(o.transfer_qty
                     for o in after.scan_network_opportunities().opportunities
                     if o.from_org == "ORG001")
        assert second < alone, (
            "the scan offered as much after the ordering path had already "
            "claimed from the same donor — the two are not sharing a ledger")

    def test_a_fresh_service_starts_with_an_empty_book(self):
        svc = _service(self._stock())
        assert svc.donor_ledger.total_booked == 0
        assert len(svc.donor_ledger) == 0


class TestDonorLedger:
    def test_available_nets_off_bookings_and_applies_the_fraction(self):
        from oasis.logic.fulfillment_decider import DonorLedger
        led = DonorLedger()
        assert led.available("ORG001", "SKU1", 100.0, 0.5) == 50.0
        led.book("ORG001", "SKU1", 30.0)
        assert led.available("ORG001", "SKU1", 100.0, 0.5) == 20.0
        led.book("ORG001", "SKU1", 999.0)
        assert led.available("ORG001", "SKU1", 100.0, 0.5) == 0.0, "never negative"

    def test_bookings_are_per_donor_and_per_sku(self):
        from oasis.logic.fulfillment_decider import DonorLedger
        led = DonorLedger()
        led.book("ORG001", "SKU1", 10.0)
        assert led.booked("ORG001", "SKU1") == 10.0
        assert led.booked("ORG002", "SKU1") == 0.0
        assert led.booked("ORG001", "SKU2") == 0.0


class TestPushScan:
    def test_overstock_moves_as_a_PULL_once_the_horizon_is_real(self):
        """An overstocked SELLING line is a PULL, not a PUSH.

        This used to assert PUSH, on the old contract where PUSH moved anything
        with >60 days of cover to anything with <14. PUSH is now dead-stock
        clearance only (zero demand, silent 90+ days); a line that is merely
        overstocked is still turning over and is ordinary donor supply.

        The old premise — "10 days of cover is not a deficit" — was itself an
        artifact of the hardcoded 7-day trigger. Measured against a real relief
        horizon it plainly IS a deficit: a store holding 10 days while its
        supplier comes fortnightly runs out before relief lands. So the move
        still happens, as a PULL, and is sized to the actual gap rather than
        topped up to an arbitrary 14 days.
        """
        stock = {
            # 1000 units at 10/day = 100 days cover, but SELLING — not dead
            "ORG001": [_product("SKU2", "SOAP 500G", stock=1000, ads=10.0, price=50)],
            # 20 units at 2/day = 10 days cover
            "ORG002": [_product("SKU2", "SOAP 500G", stock=20, ads=2.0, price=50)],
            "ORG003": [],
        }
        svc = _service(stock)
        # a fortnightly supplier: relief lands in 15 + 3 = 18 days
        svc.supplier_rhythm = {
            (svc.stock_data["ORG002"][0].get("supplier_name") or "").strip().lower(): {
                "median_gap_days": 15, "estimated_delivery_days": 3,
                "lata_variance_multiplier": 1.0}}
        scan = svc.scan_network_opportunities()
        moves = [o for o in scan.opportunities if o.from_org == "ORG001"]
        assert moves, "overstock did not move at all"
        assert {o.type for o in moves} == {"PULL"}
        # sized to the gap: 18d x 2/day = 36 units needed, 20 held -> 16
        assert moves[0].to_org == "ORG002"
        assert moves[0].transfer_qty == 16

    def test_overstock_rebalances_even_without_a_measured_horizon(self):
        """A store on 100 days of cover beside one on 10 must move stock.

        Guards a regression that was briefly shipped: when the PUSH donor gate
        was narrowed to dead stock only, an overstocked but still-SELLING line
        stopped moving entirely, and with no LATA rhythm the PULL trigger falls
        back to a flat 7 days so nothing caught it either. The 100-day store
        simply kept its stock.

        Overstock is a PUSH donor in its own right. What differs from dead
        stock is the RELEASE RULE, not eligibility: a line still being traded
        releases only the protected fraction of its excess.
        """
        stock = {
            "ORG001": [_product("SKU2", "SOAP 500G", stock=1000, ads=10.0, price=50)],
            "ORG002": [_product("SKU2", "SOAP 500G", stock=20, ads=2.0, price=50)],
            "ORG003": [],
        }
        svc = _service(stock)
        scan = svc.scan_network_opportunities()
        moves = [o for o in scan.opportunities if o.from_org == "ORG001"]
        assert moves, "overstocked donor moved nothing"
        assert moves[0].type == "PUSH"
        assert moves[0].to_org == "ORG002"
        # sized to the receiver's relief target, not to its dead threshold:
        # 14d x 2/day = 28 units, less the 20 held -> 8
        assert moves[0].transfer_qty == 8
        # and drawn from the PROTECTED pool, never the donor's whole excess
        excess = 1000 - 10.0 * 14
        assert moves[0].transfer_qty <= excess * svc.RELEASE_FRACTION

    def test_push_respects_14day_donor_floor(self):
        """The old inline scan let PUSH strip donors to 2 days of cover.
        The unified scan must never offer more than stock − 14×ADS."""
        stock = {
            "ORG001": [_product("SKU2", "SOAP 500G", stock=700, ads=10.0)],  # 70d cover
            "ORG002": [_product("SKU2", "SOAP 500G", stock=20, ads=2.0)],
            "ORG003": [],
        }
        scan = _service(stock).scan_network_opportunities()
        donor_floor = 14.0 * 10.0  # safety stock units
        for o in scan.opportunities:
            if o.from_org == "ORG001":
                assert o.transfer_qty <= 700 - donor_floor

    def test_no_push_below_cold_threshold(self):
        stock = {
            "ORG001": [_product("SKU2", "SOAP 500G", stock=400, ads=10.0)],  # 40d < 60d
            "ORG002": [_product("SKU2", "SOAP 500G", stock=20, ads=2.0)],
            "ORG003": [],
        }
        scan = _service(stock).scan_network_opportunities()
        assert [o for o in scan.opportunities if o.type == "PUSH"] == []


class TestFreshAndBooking:
    def test_fresh_items_manual_only(self):
        stock = {
            "ORG001": [_product("SKU3", "FRESH MILK 500ML", stock=300, ads=10.0,
                                dept="FRESH MILK", is_fresh=True)],
            "ORG002": [_product("SKU3", "FRESH MILK 500ML", stock=1, ads=8.0,
                                dept="FRESH MILK", is_fresh=True)],
            "ORG003": [],
        }
        scan = _service(stock).scan_network_opportunities()
        for o in scan.opportunities:
            assert o.manual_only, "fresh item must be manual-only"

    def test_donor_excess_not_double_booked(self):
        """One donor's excess must not be promised to two recipients."""
        stock = {
            # Donor: 200 units at 2/day = 100d cover; excess = 200 − 28 = 172
            "ORG001": [_product("SKU4", "TEA 250G", stock=200, ads=2.0)],
            "ORG002": [_product("SKU4", "TEA 250G", stock=1, ads=4.0)],
            "ORG003": [_product("SKU4", "TEA 250G", stock=1, ads=4.0)],
        }
        scan = _service(stock).scan_network_opportunities()
        total_from_donor = sum(o.transfer_qty for o in scan.opportunities
                               if o.from_org == "ORG001")
        donor_excess = 200 - (14.0 * 2.0)
        assert total_from_donor <= donor_excess + 0.001

    def test_store_stats_populated(self):
        stock = {
            "ORG001": [_product("SKU1", "RICE 1KG", stock=600, ads=5.0)],
            "ORG002": [_product("SKU1", "RICE 1KG", stock=2, ads=4.0)],
            "ORG003": [],
        }
        scan = _service(stock).scan_network_opportunities()
        assert scan.store_stats["ORG001"]["overstock"] == 1
        assert scan.store_stats["ORG002"]["deficits"] == 1
        assert scan.store_stats["ORG003"]["total_skus"] == 0
