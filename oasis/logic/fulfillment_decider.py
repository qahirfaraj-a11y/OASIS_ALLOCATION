"""
Fulfillment Decider
===================
Decides whether a shortfall at a store should be fulfilled via:
  - TRANSFER from another branch (faster, saves capital)
  - ORDER from supplier (standard replenishment)
  - BOTH (transfer for immediate, order for buffer)

Part of the **consolidated transfer layer**. Does NOT modify any
per-store OASIS engine logic.
"""

import logging
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple, Any

logger = logging.getLogger("FulfillmentDecider")


# ---------------------------------------------------------------------------
# Decision output
# ---------------------------------------------------------------------------

@dataclass
class FulfillmentDecision:
    """Result of the fulfillment decision for a single SKU at a single store."""
    itm_cd: str
    product_name: str
    recipient_org: str
    shortfall_qty: float           # units needed
    decision: str                  # TRANSFER | ORDER | BOTH | BACKLOG
    transfer_qty: float = 0.0     # units to transfer (if decision includes TRANSFER)
    order_qty: float = 0.0        # units to keep in supplier PO (if ORDER or BOTH)
    donor_org: Optional[str] = None
    donor_name: Optional[str] = None
    donor_excess: float = 0.0     # how much excess the donor has
    reasoning: str = ""
    estimated_transfer_cost: float = 0.0
    estimated_order_cost: float = 0.0
    transfer_eta_hours: float = 4.0
    order_eta_days: float = 3.0


# ---------------------------------------------------------------------------
# Network availability helpers
# ---------------------------------------------------------------------------

@dataclass
class StoreSkuState:
    """Snapshot of a single SKU at a single store."""
    org_cd: str
    org_name: str
    itm_cd: str
    product_name: str
    current_stock: float
    avg_daily_sales: float
    safety_stock: float     # 2× ADS minimum
    excess: float           # current_stock - safety_stock
    is_fresh: bool = False
    sell_price: float = 0.0
    department: str = ""


class NetworkAvailabilityMap:
    """
    Cross-store availability index.
    Built once per optimization cycle from all stores' stock data.
    """

    def __init__(self):
        # itm_cd -> List[StoreSkuState]
        self._index: Dict[str, List[StoreSkuState]] = {}

    def add(self, state: StoreSkuState):
        if state.itm_cd not in self._index:
            self._index[state.itm_cd] = []
        self._index[state.itm_cd].append(state)

    def find_donors(self, itm_cd: str, exclude_org: str,
                    min_excess_ratio: float = 2.0) -> List[StoreSkuState]:
        """
        Find stores that have excess stock for this item.
        
        Args:
            itm_cd: Item to search for
            exclude_org: Don't consider this store as donor (it's the recipient)
            min_excess_ratio: Donor must have at least this many × safety stock
        
        Returns:
            List of potential donors sorted by excess (highest first)
        """
        candidates = self._index.get(itm_cd, [])
        donors = []
        for s in candidates:
            if s.org_cd == exclude_org:
                continue
            if s.excess > 0 and s.current_stock >= s.safety_stock * min_excess_ratio:
                donors.append(s)
        donors.sort(key=lambda x: -x.excess)
        return donors

    def get_total_network_excess(self, itm_cd: str, exclude_org: str) -> float:
        """Total excess units available across the network."""
        return sum(d.excess for d in self.find_donors(itm_cd, exclude_org))

    def get_store_state(self, org_cd: str, itm_cd: str) -> Optional[StoreSkuState]:
        """Get a specific store's state for an item."""
        for s in self._index.get(itm_cd, []):
            if s.org_cd == org_cd:
                return s
        return None


# ---------------------------------------------------------------------------
# Core decider
# ---------------------------------------------------------------------------

# Default logistics cost per transfer (KES)
# Can be overridden with distance-based calculation later
DEFAULT_TRANSFER_COST_KES = 500.0

# Maximum fraction of donor's excess we'll take
MAX_DONOR_DRAIN = 0.5

# Transfer is only worthwhile if it saves at least this much vs ordering
MIN_SAVINGS_RATIO = 0.3


class FulfillmentDecider:
    """
    Network-level optimization that decides transfer vs order.
    Does NOT modify per-store engine logic.
    """

    def __init__(self,
                 transfer_cost_kes: float = DEFAULT_TRANSFER_COST_KES,
                 max_donor_drain: float = MAX_DONOR_DRAIN,
                 fresh_transfer_max_hours: float = 6.0):
        self.transfer_cost_kes = transfer_cost_kes
        self.max_donor_drain = max_donor_drain
        self.fresh_transfer_max_hours = fresh_transfer_max_hours

    def decide(self,
               itm_cd: str,
               product_name: str,
               recipient_org: str,
               shortfall_qty: float,
               network_map: NetworkAvailabilityMap,
               is_ordering_day: bool = True,
               lead_time_days: float = 3.0,
               unit_cost: float = 0.0,
               is_fresh: bool = False,
               org_names: Dict[str, str] = None,
               pending_order_qty: float = 0.0,
               pending_order_eta_days: float = 999.0,
               current_stock: float = 0.0,
               avg_daily_sales: float = 0.0) -> FulfillmentDecision:
        """
        Make a fulfillment decision for a single shortfall.
        
        Args:
            itm_cd: Item code
            product_name: Human-readable name
            recipient_org: Store that needs stock
            shortfall_qty: Units needed
            network_map: Cross-store availability
            is_ordering_day: Can we place a supplier order today?
            lead_time_days: Supplier delivery time
            unit_cost: Cost per unit from supplier
            is_fresh: Is this a fresh/perishable item?
            org_names: {org_cd: org_name} for display
            pending_order_qty: Units already on a supplier PO for this SKU
            pending_order_eta_days: Expected days until the pending order arrives
            current_stock: Current stock level at the recipient store
            avg_daily_sales: ADS at the recipient store (for urgency calc)
        
        Returns:
            FulfillmentDecision
        """
        if org_names is None:
            org_names = {}

        # Find potential donors
        donors = network_map.find_donors(itm_cd, exclude_org=recipient_org)

        # Cost estimates
        order_cost = shortfall_qty * unit_cost if unit_cost > 0 else shortfall_qty * 100
        transfer_cost = self.transfer_cost_kes  # fixed per transfer

        # Base decision object
        decision = FulfillmentDecision(
            itm_cd=itm_cd,
            product_name=product_name,
            recipient_org=recipient_org,
            shortfall_qty=shortfall_qty,
            decision="ORDER",  # default
            order_qty=shortfall_qty,
            estimated_order_cost=order_cost,
            estimated_transfer_cost=transfer_cost,
            order_eta_days=lead_time_days,
        )

        # ── Implicit Rule: Pending-Order Awareness ──
        # Calculate urgency: hours until stockout at current rate
        hours_to_stockout = 999.0
        if avg_daily_sales > 0 and current_stock >= 0:
            hours_to_stockout = (current_stock / avg_daily_sales) * 24.0

        has_pending = pending_order_qty > 0 and pending_order_eta_days < 999.0
        is_critical_stockout = hours_to_stockout < 4.0  # will stock out before a transfer arrives

        if has_pending and not is_critical_stockout:
            # Rule 1: ORDER_ARRIVING — delivery imminent (≤24h) and covers ≥50%
            if (pending_order_eta_days <= 1.0
                    and pending_order_qty >= shortfall_qty * 0.5):
                decision.decision = "ORDER"
                decision.order_qty = shortfall_qty
                decision.reasoning = (
                    f"Supplier delivery imminent (ETA {pending_order_eta_days:.0f}d, "
                    f"{pending_order_qty:.0f} units incoming). "
                    f"Transfer suppressed — wait for order."
                )
                return decision

            # Rule 2: PARTIAL_COVER — delivery within 48h, reduce transfer qty
            if pending_order_eta_days <= 2.0 and pending_order_qty > 0:
                effective_shortfall = max(0, shortfall_qty - pending_order_qty)
                if effective_shortfall < 1.0:
                    decision.decision = "ORDER"
                    decision.order_qty = shortfall_qty
                    decision.reasoning = (
                        f"Pending order ({pending_order_qty:.0f} units, "
                        f"ETA {pending_order_eta_days:.1f}d) covers shortfall. "
                        f"No transfer needed."
                    )
                    return decision
                else:
                    # Reduce the shortfall that needs transfer coverage
                    shortfall_qty = effective_shortfall
                    decision.shortfall_qty = shortfall_qty
                    decision.reasoning = (
                        f"[Pending order reduces shortfall by {pending_order_qty:.0f}] "
                    )

        if is_critical_stockout and has_pending:
            # Rule 3: CRITICAL_OVERRIDE — stock already out or <4h, proceed with
            # transfer despite pending order
            decision.reasoning = (
                f"[CRITICAL: {hours_to_stockout:.1f}h to stockout, "
                f"bypassing pending order (ETA {pending_order_eta_days:.1f}d)] "
            )

        if not donors:
            # No donors available → ORDER (or BACKLOG if can't order today)
            if not is_ordering_day:
                decision.decision = "BACKLOG"
                decision.reasoning += (
                    f"No network donor available. "
                    f"Not supplier's ordering day. Backlogged for next order cycle."
                )
            else:
                decision.reasoning += (
                    f"No network donor available. Standard supplier order."
                )
            return decision

        # Best donor
        best = donors[0]
        max_transferable = min(
            best.excess * self.max_donor_drain,
            shortfall_qty
        )

        # Fresh items: only transfer if donor is nearby (< 6 hours)
        if is_fresh and best.is_fresh:
            # For fresh, transfers are actually preferred (faster than waiting for supplier)
            max_transferable = min(max_transferable, best.excess * 0.3)  # Be conservative

        decision.donor_org = best.org_cd
        decision.donor_name = org_names.get(best.org_cd, best.org_cd)
        decision.donor_excess = best.excess

        if max_transferable < 1.0:
            # Donor doesn't have enough excess
            decision.reasoning += (
                f"Donor {decision.donor_name} has only {best.excess:.0f} excess "
                f"(need {shortfall_qty:.0f}). Standard supplier order."
            )
            return decision

        # ── Decision Matrix ──

        if not is_ordering_day:
            # Can't order today → TRANSFER if possible
            decision.decision = "TRANSFER"
            decision.transfer_qty = round(max_transferable, 1)
            remaining = shortfall_qty - max_transferable
            if remaining > 0:
                decision.order_qty = remaining
                decision.decision = "BOTH"
                decision.reasoning += (
                    f"Not ordering day. Transfer {decision.transfer_qty:.0f} from "
                    f"{decision.donor_name}. Remaining {remaining:.0f} backlogged."
                )
            else:
                decision.order_qty = 0.0
                decision.reasoning += (
                    f"Not ordering day. Full transfer of {decision.transfer_qty:.0f} "
                    f"from {decision.donor_name} (excess: {best.excess:.0f})."
                )
            return decision

        # Both options available → compare economics
        # Transfer: immediate (4h) but fixed logistics cost
        # Order: cheaper per unit but takes lead_time_days

        daily_lost_sales = best.avg_daily_sales * best.sell_price if best.sell_price > 0 else 0
        lost_sales_during_lead = daily_lost_sales * lead_time_days

        if max_transferable >= shortfall_qty and transfer_cost < order_cost * 0.5:
            # Transfer is much cheaper
            decision.decision = "TRANSFER"
            decision.transfer_qty = round(shortfall_qty, 1)
            decision.order_qty = 0.0
            decision.reasoning += (
                f"Transfer from {decision.donor_name}: {decision.transfer_qty:.0f} units. "
                f"Saves KES {order_cost - transfer_cost:,.0f} vs supplier order."
            )
        elif lead_time_days > 2 and max_transferable >= shortfall_qty * 0.5:
            # Long lead time → transfer now, order for replenishment buffer
            xfer = round(min(max_transferable, shortfall_qty * 0.7), 1)
            decision.decision = "BOTH"
            decision.transfer_qty = xfer
            decision.order_qty = round(shortfall_qty, 1)  # full order for buffer
            decision.reasoning += (
                f"Lead time {lead_time_days:.0f}d. Transfer {xfer:.0f} from "
                f"{decision.donor_name} (immediate). Full order kept for buffer."
            )
        else:
            # Standard order is fine
            decision.reasoning += (
                f"Standard supplier order. "
                f"Donor {decision.donor_name} excess: {best.excess:.0f} "
                f"(transfer not cost-effective)."
            )

        return decision

    def decide_batch(self,
                     shortfalls: List[Dict[str, Any]],
                     network_map: NetworkAvailabilityMap,
                     org_names: Dict[str, str] = None) -> List[FulfillmentDecision]:
        """
        Run decisions for multiple shortfalls at once.
        
        Args:
            shortfalls: List of dicts with keys:
                itm_cd, product_name, recipient_org, shortfall_qty,
                is_ordering_day, lead_time_days, unit_cost, is_fresh,
                pending_order_qty, pending_order_eta_days,
                current_stock, avg_daily_sales
            network_map: Cross-store availability
            org_names: {org_cd: org_name}
        
        Returns:
            List of FulfillmentDecision
        """
        decisions = []
        for sf in shortfalls:
            d = self.decide(
                itm_cd=sf['itm_cd'],
                product_name=sf['product_name'],
                recipient_org=sf['recipient_org'],
                shortfall_qty=sf['shortfall_qty'],
                network_map=network_map,
                is_ordering_day=sf.get('is_ordering_day', True),
                lead_time_days=sf.get('lead_time_days', 3.0),
                unit_cost=sf.get('unit_cost', 0.0),
                is_fresh=sf.get('is_fresh', False),
                org_names=org_names,
                pending_order_qty=sf.get('pending_order_qty', 0.0),
                pending_order_eta_days=sf.get('pending_order_eta_days', 999.0),
                current_stock=sf.get('current_stock', 0.0),
                avg_daily_sales=sf.get('avg_daily_sales', 0.0),
            )
            decisions.append(d)
        return decisions

