"""Unit tests for FulfillmentDecider.decide() — the gap-plug decision matrix.

Covers: fresh no-auto-transfer, no-gap → order only, pending-order
suppression, donor-less fallback (ORDER/BACKLOG), full transfer on
non-ordering days, and the BOTH gap-plug path.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.fulfillment_decider import (
    FulfillmentDecider, NetworkAvailabilityMap, StoreSkuState,
)


def _decider():
    return FulfillmentDecider(transfer_cost_kes=500.0, distance_map={}, warehouse_hubs=[])


def _map_with_donor(itm="SKU1", org="ORG001", stock=500.0, ads=5.0,
                    excess=430.0, dept="GENERAL"):
    nmap = NetworkAvailabilityMap()
    nmap.add(StoreSkuState(
        org_cd=org, org_name="Donor Store", itm_cd=itm, product_name="RICE 1KG",
        current_stock=stock, avg_daily_sales=ads, safety_stock=ads * 14.0,
        excess=excess, is_fresh=False, sell_price=100.0, department=dept,
        days_since_delivery=5, velocity_ratio=0.01,
    ))
    return nmap


def _decide(nmap=None, **kw):
    args = dict(
        itm_cd="SKU1", product_name="RICE 1KG", recipient_org="ORG002",
        shortfall_qty=20.0, network_map=nmap or NetworkAvailabilityMap(),
        is_ordering_day=True, lead_time_days=3.0, unit_cost=75.0,
        is_fresh=False, current_stock=2.0, avg_daily_sales=5.0,
    )
    args.update(kw)
    return _decider().decide(**args)


class TestFreshRule:
    def test_fresh_flag_forces_order(self):
        d = _decide(_map_with_donor(), is_fresh=True)
        assert d.decision == "ORDER"
        assert "FRESH" in d.reasoning.upper()
        assert d.transfer_qty == 0

    def test_fresh_department_forces_order(self):
        d = _decide(_map_with_donor(dept="DAIRY"), is_fresh=False, department="DAIRY")
        assert d.decision == "ORDER"
        assert "FRESH" in d.reasoning.upper()


class TestGapPlugRules:
    def test_no_gap_means_order_only(self):
        # 50 units at 5/day = 10 days of stock; delivery in 3 days → no gap.
        d = _decide(_map_with_donor(), current_stock=50.0)
        assert d.decision == "ORDER"
        assert "No stockout gap" in d.reasoning

    def test_gap_with_donor_transfers_and_keeps_order(self):
        # 2 units at 5/day = 0.4 days; lead 3d → 2.6-day gap → plug + order.
        d = _decide(_map_with_donor())
        assert d.decision == "BOTH"
        assert d.transfer_qty >= 1
        assert d.order_qty >= d.shortfall_qty  # full order kept
        assert d.donor_org == "ORG001"
        assert "GAP-PLUG" in d.reasoning

    def test_transfer_capped_by_donor_drain(self):
        # Donor has only 4 units excess — max drain 50% → under 1 unit → ORDER.
        d = _decide(_map_with_donor(stock=500.0, excess=1.5))
        assert d.decision == "ORDER"
        assert "excess" in d.reasoning.lower()


class TestPendingOrderRules:
    def test_imminent_delivery_suppresses_transfer(self):
        d = _decide(
            _map_with_donor(),
            pending_order_qty=15.0, pending_order_eta_days=1.0,
        )
        assert d.decision == "ORDER"
        assert "imminent" in d.reasoning.lower()

    def test_pending_covering_gap_means_order(self):
        # Gap qty = 2.6d × 5 = 13; pending 13 within 48h covers it fully.
        d = _decide(
            _map_with_donor(),
            pending_order_qty=13.0, pending_order_eta_days=2.0,
        )
        assert d.decision == "ORDER"
        assert "covers the gap" in d.reasoning.lower()

    def test_critical_stockout_bypasses_pending(self):
        # 0 stock → 0 hours to stockout → critical: transfer despite pending.
        d = _decide(
            _map_with_donor(),
            current_stock=0.0,
            pending_order_qty=15.0, pending_order_eta_days=1.0,
        )
        assert "CRITICAL" in d.reasoning
        assert d.decision in ("TRANSFER", "BOTH")


class TestNoDonorFallback:
    def test_order_when_no_donor_on_ordering_day(self):
        d = _decide(NetworkAvailabilityMap())
        assert d.decision == "ORDER"
        assert "No network donor" in d.reasoning

    def test_backlog_when_no_donor_off_schedule(self):
        d = _decide(NetworkAvailabilityMap(), is_ordering_day=False)
        assert d.decision == "BACKLOG"


class TestNonOrderingDay:
    def test_full_transfer_when_donor_covers_shortfall(self):
        # Stock 0, ads 5, lead 3 → gap qty 15 ≥ shortfall 10 → full transfer.
        d = _decide(
            _map_with_donor(),
            current_stock=0.0, shortfall_qty=10.0, is_ordering_day=False,
        )
        assert d.decision == "TRANSFER"
        assert d.transfer_qty >= 10
        assert d.order_qty == 0

    def test_partial_transfer_backlogs_remainder(self):
        # Donor excess 8 → max 4 transferable; shortfall 10 → BOTH.
        d = _decide(
            _map_with_donor(excess=8.0),
            current_stock=0.0, shortfall_qty=10.0, is_ordering_day=False,
        )
        assert d.decision == "BOTH"
        assert 1 <= d.transfer_qty <= 4
        assert d.order_qty > 0


class TestRiskOverride:
    def test_high_gnn_risk_forces_both(self):
        d = _decide(_map_with_donor(), risk_score=0.9)
        assert d.decision == "BOTH"
        assert "HIGH RISK" in d.reasoning
