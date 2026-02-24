"""
Consolidated Transfer Service
==============================
Orchestrator that connects individual per-store OASIS engines to the
network-level transfer optimization layer.

Architecture:
  1. Each store runs its own OrderEngine independently (UNTOUCHED)
  2. This service READS those per-store outputs
  3. Identifies cross-store transfer opportunities
  4. Produces:
     - Adjusted POs per store (original orders minus transfer fulfillments)
     - Transfer dispatch list
     - Donor replenishment additions

Does NOT modify any per-store OASIS engine logic.
"""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any, Tuple

from .transfer_state import TransferStateTracker, TransferRecord
from .fulfillment_decider import (
    FulfillmentDecider,
    FulfillmentDecision,
    NetworkAvailabilityMap,
    StoreSkuState,
)

logger = logging.getLogger("ConsolidatedTransferService")


# ---------------------------------------------------------------------------
# Output data structures
# ---------------------------------------------------------------------------

@dataclass
class NetworkPlan:
    """Result of network-level optimization."""

    # Per-store adjusted orders (original - transfer fulfillments)
    adjusted_orders: Dict[str, List[dict]] = field(default_factory=dict)

    # New transfers to execute
    transfers: List[TransferRecord] = field(default_factory=list)

    # Donor compensation: extra items to add to donor's PO
    donor_additions: Dict[str, List[dict]] = field(default_factory=dict)

    # Decisions log for UI display
    decisions: List[FulfillmentDecision] = field(default_factory=list)

    # Summary metrics
    total_items_transferred: int = 0
    total_units_transferred: float = 0.0
    total_orders_reduced: int = 0
    estimated_savings_kes: float = 0.0


# ---------------------------------------------------------------------------
# Main service
# ---------------------------------------------------------------------------

class ConsolidatedTransferService:
    """
    Network-level transfer optimization layer.
    
    Usage:
        service = ConsolidatedTransferService(org_names, stock_data)
        plan = service.optimize_network(all_store_orders)
        # plan.adjusted_orders  → adjusted POs per store
        # plan.transfers        → transfers to execute
        # plan.donor_additions  → extra items for donors
    """

    def __init__(self,
                 org_names: Dict[str, str],
                 stock_data: Dict[str, List[dict]],
                 transfer_cost_kes: float = 500.0,
                 min_shortfall_qty: float = 1.0):
        """
        Args:
            org_names: {org_cd: org_name} for all stores
            stock_data: {org_cd: [product dicts]} — current stock per store
            transfer_cost_kes: Fixed logistics cost per transfer
            min_shortfall_qty: Minimum shortfall to consider for transfer
        """
        self.org_names = org_names
        self.stock_data = stock_data
        self.transfer_cost_kes = transfer_cost_kes
        self.min_shortfall_qty = min_shortfall_qty

        self.tracker = TransferStateTracker()
        self.decider = FulfillmentDecider(transfer_cost_kes=transfer_cost_kes)

        # Build network availability map from stock data
        self.network_map = self._build_network_map()

    def _build_network_map(self) -> NetworkAvailabilityMap:
        """Build cross-store availability index from current stock data."""
        nmap = NetworkAvailabilityMap()

        for org_cd, products in self.stock_data.items():
            org_name = self.org_names.get(org_cd, org_cd)
            for p in products:
                ads = float(p.get('avg_daily_sales', 0) or 0)
                current = float(p.get('current_stocks', 0) or 0)
                safety = ads * 2.0  # 2 days cover as minimum safety stock
                excess = current - safety

                dept = str(p.get('department', p.get('product_category', 'GENERAL'))).upper()
                is_fresh = any(k in dept for k in [
                    'MILK', 'DAIRY', 'FRESH', 'MEAT', 'BREAD', 'BAKERY'
                ]) or p.get('is_fresh', False)

                nmap.add(StoreSkuState(
                    org_cd=org_cd,
                    org_name=org_name,
                    itm_cd=str(p.get('itm_cd', p.get('item_code', p.get('product_name', '')))),
                    product_name=str(p.get('product_name', 'Unknown')),
                    current_stock=current,
                    avg_daily_sales=ads,
                    safety_stock=safety,
                    excess=excess,
                    is_fresh=is_fresh,
                    sell_price=float(p.get('selling_price', p.get('sell_price', 0)) or 0),
                    department=dept,
                ))
        return nmap

    def optimize_network(self,
                         store_orders: Dict[str, List[dict]],
                         supplier_schedule: Dict[str, bool] = None,
                         pending_orders: Dict[str, Dict[str, dict]] = None,
                         ) -> NetworkPlan:
        """
        Main optimization entry point.
        
        Args:
            store_orders: {org_cd: [raw recommendations from that store's engine]}
                Each recommendation dict should have:
                  - product_name, itm_cd (or item_code)
                  - recommended_quantity (from per-store engine)
                  - avg_daily_sales, current_stocks, selling_price
                  - supplier_name, estimated_delivery_days
                  - is_fresh (bool)
            supplier_schedule: {supplier_name: is_ordering_day_today}
                If None, assumes all suppliers can be ordered from today.
            pending_orders: {org_cd: {itm_cd: {"qty": N, "eta_days": D}}}
                Pending supplier orders per store per item.
                If None, no pending-order awareness is applied.
        
        Returns:
            NetworkPlan with adjusted orders, transfers, and donor additions
        """
        if supplier_schedule is None:
            supplier_schedule = {}
        if pending_orders is None:
            pending_orders = {}

        plan = NetworkPlan()
        self.tracker.clear_all()

        # Step 1: Identify shortfalls across all stores
        all_shortfalls: List[Dict[str, Any]] = []

        for org_cd, recs in store_orders.items():
            org_pending = pending_orders.get(org_cd, {})

            for rec in recs:
                qty = float(rec.get('recommended_quantity', 0))
                if qty < self.min_shortfall_qty:
                    continue  # No shortfall, skip

                itm_cd = str(rec.get('itm_cd', rec.get('item_code', rec.get('product_name', ''))))
                supplier = str(rec.get('supplier_name', 'Unknown'))
                is_ordering_day = supplier_schedule.get(supplier, True)

                # Pending-order lookup for this SKU at this store
                po_info = org_pending.get(itm_cd, {})
                pending_qty = float(po_info.get('qty', 0))
                pending_eta = float(po_info.get('eta_days', 999.0))

                all_shortfalls.append({
                    'itm_cd': itm_cd,
                    'product_name': rec.get('product_name', 'Unknown'),
                    'recipient_org': org_cd,
                    'shortfall_qty': qty,
                    'is_ordering_day': is_ordering_day,
                    'lead_time_days': float(rec.get('estimated_delivery_days', 3) or 3),
                    'unit_cost': float(rec.get('cost_price', rec.get('selling_price', 0) or 0) * 0.75),
                    'is_fresh': rec.get('is_fresh', False),
                    'original_rec': rec,  # keep reference
                    # Pending-order awareness fields
                    'pending_order_qty': pending_qty,
                    'pending_order_eta_days': pending_eta,
                    'current_stock': float(rec.get('current_stocks', rec.get('current_stock', 0)) or 0),
                    'avg_daily_sales': float(rec.get('avg_daily_sales', 0) or 0),
                })

        logger.info(f"Network optimization: {len(all_shortfalls)} shortfalls across "
                    f"{len(store_orders)} stores")

        # Step 2: Run fulfillment decisions
        decisions = self.decider.decide_batch(
            all_shortfalls,
            self.network_map,
            org_names=self.org_names,
        )
        plan.decisions = decisions

        # Step 3: Build adjusted orders + transfer list + donor additions
        # Start with copies of original orders
        for org_cd, recs in store_orders.items():
            plan.adjusted_orders[org_cd] = [r.copy() for r in recs]

        for sf, decision in zip(all_shortfalls, decisions):
            org_cd = sf['recipient_org']
            itm_cd = sf['itm_cd']
            original_rec = sf['original_rec']

            if decision.decision in ("TRANSFER", "BOTH"):
                # Create transfer record
                transfer = TransferRecord(
                    from_org=decision.donor_org,
                    to_org=org_cd,
                    itm_cd=itm_cd,
                    product_name=decision.product_name,
                    qty=decision.transfer_qty,
                    department=original_rec.get('department', ''),
                    urgency="HIGH" if decision.decision == "BOTH" else "MEDIUM",
                    cost_kes=self.transfer_cost_kes,
                )
                self.tracker.register_transfer(transfer)
                plan.transfers.append(transfer)
                plan.total_items_transferred += 1
                plan.total_units_transferred += decision.transfer_qty

                # Adjust recipient's order qty
                if decision.decision == "TRANSFER":
                    # Full transfer → remove from PO entirely
                    self._adjust_order(plan.adjusted_orders[org_cd], original_rec, 0.0,
                                       f"[NETWORK: Fulfilled via transfer from {decision.donor_name}]")
                    plan.total_orders_reduced += 1
                    plan.estimated_savings_kes += decision.estimated_order_cost - decision.estimated_transfer_cost
                elif decision.decision == "BOTH":
                    # Partial transfer → keep full order for buffer
                    self._adjust_order_reasoning(
                        plan.adjusted_orders[org_cd], original_rec,
                        f"[NETWORK: +{decision.transfer_qty:.0f} via transfer from {decision.donor_name}, order kept for buffer]"
                    )

                # Add donor compensation
                if decision.donor_org:
                    if decision.donor_org not in plan.donor_additions:
                        plan.donor_additions[decision.donor_org] = []
                    plan.donor_additions[decision.donor_org].append({
                        'product_name': decision.product_name,
                        'itm_cd': itm_cd,
                        'recommended_quantity': decision.transfer_qty,
                        'reasoning': (
                            f"[DONOR REPLENISHMENT: Donated {decision.transfer_qty:.0f} "
                            f"to {self.org_names.get(org_cd, org_cd)}]"
                        ),
                        'is_donor_compensation': True,
                        'department': original_rec.get('department', ''),
                    })

        logger.info(
            f"Network plan: {plan.total_items_transferred} transfers, "
            f"{plan.total_orders_reduced} orders reduced, "
            f"est. savings KES {plan.estimated_savings_kes:,.0f}"
        )
        return plan

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _adjust_order(order_list: List[dict], original_rec: dict,
                      new_qty: float, reason_suffix: str):
        """Find and adjust a specific order in the list."""
        p_name = original_rec.get('product_name', '')
        for rec in order_list:
            if rec.get('product_name') == p_name:
                rec['original_quantity'] = rec.get('recommended_quantity', 0)
                rec['recommended_quantity'] = new_qty
                rec['reasoning'] = rec.get('reasoning', '') + f" {reason_suffix}"
                rec['network_adjusted'] = True
                return
        # If not found by name, try itm_cd
        itm = original_rec.get('itm_cd', original_rec.get('item_code', ''))
        for rec in order_list:
            if rec.get('itm_cd') == itm or rec.get('item_code') == itm:
                rec['original_quantity'] = rec.get('recommended_quantity', 0)
                rec['recommended_quantity'] = new_qty
                rec['reasoning'] = rec.get('reasoning', '') + f" {reason_suffix}"
                rec['network_adjusted'] = True
                return

    @staticmethod
    def _adjust_order_reasoning(order_list: List[dict], original_rec: dict,
                                reason_suffix: str):
        """Add reasoning to an order without changing quantity."""
        p_name = original_rec.get('product_name', '')
        for rec in order_list:
            if rec.get('product_name') == p_name:
                rec['reasoning'] = rec.get('reasoning', '') + f" {reason_suffix}"
                rec['network_adjusted'] = True
                return
