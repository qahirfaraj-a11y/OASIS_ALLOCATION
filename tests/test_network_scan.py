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
        # Donor with excess but NOT cold (50 days < 60) — no PUSH noise.
        # Recipient at 8 days cover — neither a pull deficit (>7d) nor below
        # ROP, so only the MOQ failure can trigger a transfer.
        stock = {
            "ORG001": [_product("SKU1", "RICE 1KG", stock=250, ads=5.0)],
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


class TestPushScan:
    def test_push_cold_to_hot(self):
        stock = {
            # Cold: 1000 units at 10/day = 100 days > 60 cold threshold
            "ORG001": [_product("SKU2", "SOAP 500G", stock=1000, ads=10.0, price=50)],
            # Hot: 10 units at 2/day = 5 days < 14 hot threshold...
            # but NOT a pull deficit (>= 7d requires < 7; 5d IS a pull too).
            "ORG002": [_product("SKU2", "SOAP 500G", stock=20, ads=2.0, price=50)],
            "ORG003": [],
        }
        scan = _service(stock).scan_network_opportunities()
        types = {o.type for o in scan.opportunities if o.from_org == "ORG001"}
        assert types, "no opportunities found"
        # 10 days cover at recipient → hot (<14) but not deficit (>7) → PUSH
        assert "PUSH" in types

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
