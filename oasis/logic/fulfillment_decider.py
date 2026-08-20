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
import math
from dataclasses import dataclass
from typing import Dict, List, Optional, Any

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
    #: The DONOR's canonical item code. Not always the same string as `itm_cd`:
    #: NetworkAvailabilityMap indexes each state under several aliases (code,
    #: product name, barcode) so a caller may arrive holding any of them. The
    #: donor ledger must be keyed on ONE identity or the same physical stock is
    #: booked under two keys and promised twice.
    donor_itm_cd: Optional[str] = None
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
    days_since_delivery: int = 0
    velocity_ratio: float = 1.0
    uom: str = "EA"         # EA (each) or KG (weight-based)
    last_search_score: float = 0.0
    last_search_dist: float = 0.0


class NetworkAvailabilityMap:
    """
    Cross-store availability index.
    Built once per optimization cycle from all stores' stock data.
    """

    def __init__(self):
        # itm_cd -> List[StoreSkuState]
        self._index: Dict[str, List[StoreSkuState]] = {}

    def add(self, state: StoreSkuState, bcode: str = ''):
        keys = {state.itm_cd, state.product_name}
        # Also index by normalised lower-case name for case-insensitive fallback
        if state.product_name:
            keys.add(state.product_name.strip().lower())
        if bcode:
            keys.add(bcode)
        for k in keys:
            if k:
                if k not in self._index:
                    self._index[k] = []
                # avoid duplicates
                if not any(x.org_cd == state.org_cd for x in self._index[k]):
                    self._index[k].append(state)

    def find_donors(self, itm_cd: str, recipient_org: str,
                    min_excess_ratio: float = 2.0,
                    distance_calc: Optional[Any] = None,
                    use_dynamic_ratio: bool = True,
                    warehouse_hubs: Optional[List[str]] = None,
                    product_name: str = '',
                    ledger: Optional["DonorLedger"] = None) -> List[StoreSkuState]:
        """
        Find stores that have excess stock for this item, prioritized by proximity and volume.
        
        G8 Fix: Dynamic excess ratio — fast movers (ADS > 5) use 1.5×,
        slow movers (ADS ≤ 1) use 2.5×, otherwise default ratio.
        
        Warehouse-Hub Priority: Stores flagged as warehouse hubs
        receive a 3× score boost — they exist as distribution points.
        
        Dead-Stock Bonus: Donors where days_since_delivery > 45 and velocity_ratio < 0.05
        receive a 2× score bonus — clearing dead stock is desirable.
        
        Args:
            itm_cd: Item to search for
            recipient_org: Don't consider this store as donor
            min_excess_ratio: Base donor excess ratio (overridden by dynamic if enabled)
            distance_calc: Optional callback(org1, org2) -> km
            use_dynamic_ratio: If True, adjust ratio based on item velocity
            warehouse_hubs: List of org_cd values that are warehouse/distribution hubs
        
        Returns:
            List of potential donors sorted by score (Excess / (Distance + 1))
        """
        if warehouse_hubs is None:
            warehouse_hubs = []

        candidates = self._index.get(itm_cd, [])
        # Fallback 1: try product_name key if itm_cd found nothing
        if not candidates and product_name:
            candidates = self._index.get(product_name, [])
        # Fallback 2: try normalised lower-case product name
        if not candidates and product_name:
            candidates = self._index.get(product_name.strip().lower(), [])

        donors = []
        for s in candidates:
            if s.org_cd == recipient_org:
                continue
            
            # G8 Fix: Dynamic ratio based on item velocity
            effective_ratio = min_excess_ratio
            if use_dynamic_ratio:
                if s.avg_daily_sales > 5.0:
                    effective_ratio = 1.5  # Fast movers: lower bar
                elif s.avg_daily_sales <= 1.0:
                    effective_ratio = 2.5  # Slow movers: higher bar
                # else: use default (2.0)
            
            # Net off what is already promised. Without this a donor whose
            # excess was spent by an earlier decision — or by the other code
            # path entirely — keeps being offered at full strength, and every
            # recipient is told the same units are available.
            spoken_for = ledger.booked(s.org_cd, s.itm_cd) if ledger else 0.0
            net_excess = max(0.0, s.excess - spoken_for)
            net_stock = max(0.0, s.current_stock - spoken_for)

            if net_excess > 0 and net_stock >= s.safety_stock * effective_ratio:
                # ranking uses the NET figure too: a donor with 100 units of
                # excess and 99 already promised should not outrank one with 10
                # free, which is what scoring on the gross figure did
                s.net_excess = net_excess
                # Calculate distance-aware score
                dist = 50.0  # default large distance if no mapper
                if distance_calc is not None:
                    try:
                        d = distance_calc(s.org_cd, recipient_org)
                        if d is not None:
                            dist = float(d)
                    except Exception:
                        pass
                
                # Ranking Score: High excess and low distance is best
                score = float(net_excess) / (dist + 0.1)
                
                # Warehouse-Hub Priority: 3× boost for distribution hubs
                if s.org_cd in warehouse_hubs:
                    score *= 3.0
                
                # Dead-Stock Bonus: 2× boost for aged, slow-moving stock
                if s.days_since_delivery > 45 and s.velocity_ratio < 0.05:
                    score *= 2.0
                
                s.last_search_score = score
                s.last_search_dist = dist
                donors.append(s)
        
        # Sort by score descending, org_cd breaking ties.
        #
        # Without the tiebreak, equally-scored donors keep whatever order they
        # were indexed in — which is the order stock_data was iterated, i.e. the
        # store order. Fair-share allocation is order-independent by
        # construction, but it can only be as deterministic as the donor list it
        # is handed: with ties unresolved, reversing the store order still moved
        # 1.0% of volume purely by reshuffling equal candidates.
        donors.sort(key=lambda x: (-getattr(x, 'last_search_score', 0), x.org_cd))
        return donors

    def get_total_network_excess(self, itm_cd: str, recipient_org: str, distance_calc: Optional[Any] = None) -> float:
        """Total excess units available across the network."""
        return sum(d.excess for d in self.find_donors(itm_cd, recipient_org, distance_calc=distance_calc))

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
DEFAULT_TRANSFER_COST_KES = 200.0

# G9 Fix: Distance-based cost rate (KES per km)
DEFAULT_PER_KM_RATE = 50.0

#: Maximum fraction of a donor's excess that may leave.
#:
#: THE one definition. ConsolidatedTransferService imports this as its
#: RELEASE_FRACTION rather than declaring a second 0.5, because two constants
#: for one idea drift apart silently — which is exactly how PULL and PUSH ended
#: up protecting the same donor with 0.5 and 0.4 respectively.
#:
#: Still worth knowing: the two code paths keep SEPARATE ledgers. The scan path
#: books releases in one dict; decide() sizes each decision on its own. Within a
#: single run only one path executes, so nothing double-spends, but a session
#: that runs ordering and then a network scan can release this fraction twice
#: against the same excess. Unifying that needs shared state across entry
#: points, and is not done here.
DONOR_RELEASE_FRACTION = 0.5

#: Backwards-compatible alias — the name the decider's own signature uses.
MAX_DONOR_DRAIN = DONOR_RELEASE_FRACTION

#: A transfer must cost less than this share of what ordering would cost.
#:
#: Named because it was previously an unexplained `* 0.4` sitting inline while a
#: MIN_SAVINGS_RATIO = 0.3 was declared above and referenced NOWHERE. The 0.3
#: never ran; 0.4 is the behaviour that has always been in effect, so the
#: constant is set to the live value rather than the aspirational one.
MAX_TRANSFER_COST_RATIO = 0.4

# Fresh departments that should NOT be auto-transferred
FRESH_DEPARTMENTS = {'MILK', 'DAIRY', 'FRESH', 'MEAT', 'BREAD', 'BAKERY',
                     'SEAFOOD', 'FISH', 'POULTRY', 'PRODUCE', 'FRUITS', 'VEGETABLES'}

# Departments typically sold by weight (KG) — keep decimal precision
KG_DEPARTMENTS = {'MEAT', 'SEAFOOD', 'FISH', 'CHEESE', 'SPICES', 'DELI',
                  'FRUITS', 'VEGETABLES', 'PRODUCE', 'BUTCHERY'}


def _is_kg_item(department: str) -> bool:
    """Check if an item's department indicates it is sold by weight (KG)."""
    dept_upper = department.upper().strip()
    return any(kd in dept_upper for kd in KG_DEPARTMENTS)


def _round_transfer_qty(qty: float, department: str = "") -> float:
    """Round transfer quantity based on UOM.
    
    - KG items (meat, cheese, spices, seafood): round to 1 decimal place
    - EA items (everything else): ceil to whole units
    """
    if qty <= 0:
        return 0.0
    if _is_kg_item(department):
        return round(qty, 1)
    return float(math.ceil(qty))


def _is_fresh_department(department: str) -> bool:
    """Check if a department is a fresh/perishable category."""
    dept_upper = department.upper().strip()
    return any(fd in dept_upper for fd in FRESH_DEPARTMENTS)


class DonorLedger:
    """Units of a donor's excess already promised, whoever promised them.

    WHY THIS EXISTS
    ---------------
    Donor excess was drawn down by two different mechanisms that could not see
    each other:

      * ``scan_network_opportunities`` kept a local ``booked`` dict, computing
        available excess from ``stock_data``;
      * ``decide_batch`` MUTATED ``StoreSkuState.excess`` on the availability
        map instead.

    Both are correct alone. Together they double-spend: run the ordering path,
    then the network scan, and the scan recomputes excess from ``stock_data``
    and re-offers units the ordering path already promised. Two protection
    rules, each believing it was the only one — the same shape of bug as the
    0.5/0.4 release fractions, one level up.

    One ledger, consulted by both, is the fix. It is deliberately a plain
    quantity book rather than a transfer registry: it answers only "how much of
    this donor's excess is already spoken for".

    SCOPE: one service instance, spanning every pass and every entry point on
    it. Stock committed in the ERP arrives separately as ``pending_transfers``
    and must NOT also be booked here, or it would be counted twice.
    """

    __slots__ = ("_booked",)

    def __init__(self):
        self._booked: Dict[tuple, float] = {}

    def booked(self, org_cd: str, itm_cd: str) -> float:
        return self._booked.get((str(org_cd), str(itm_cd)), 0.0)

    def book(self, org_cd: str, itm_cd: str, qty: float) -> None:
        if qty and qty > 0:
            k = (str(org_cd), str(itm_cd))
            self._booked[k] = self._booked.get(k, 0.0) + float(qty)

    def available(self, org_cd: str, itm_cd: str, excess: float,
                  fraction: float = 1.0) -> float:
        """What is still releasable from ``excess`` after prior bookings."""
        return max(0.0, float(excess) * float(fraction)
                   - self.booked(org_cd, itm_cd))

    def clear(self) -> None:
        self._booked.clear()

    @property
    def total_booked(self) -> float:
        return sum(self._booked.values())

    def __len__(self) -> int:
        return len(self._booked)


class FulfillmentDecider:
    """
    Network-level optimization that decides transfer vs order.
    Does NOT modify per-store engine logic.
    """

    def __init__(self,
                 transfer_cost_kes: float = DEFAULT_TRANSFER_COST_KES,
                 per_km_rate: float = DEFAULT_PER_KM_RATE,
                 max_donor_drain: float = MAX_DONOR_DRAIN,
                 fresh_transfer_max_hours: float = 6.0,
                 distance_map: Optional[Dict[str, Dict[str, float]]] = None,
                 risk_threshold: float = 0.40,
                 warehouse_hubs: Optional[List[str]] = None,
                 ledger: Optional["DonorLedger"] = None):
        self.transfer_cost_kes = transfer_cost_kes
        self.per_km_rate = per_km_rate  # G9 Fix
        self.max_donor_drain = max_donor_drain
        self.fresh_transfer_max_hours = fresh_transfer_max_hours
        self.distance_map = distance_map or {}
        self.risk_threshold = risk_threshold
        # Warehouse hub org codes (e.g. ["016"])
        self.warehouse_hubs = warehouse_hubs or []
        # Shared with whoever else draws on the same donors. When absent this
        # decider keeps its own, and decide_batch falls back to mutating the
        # availability map as it always did — standalone callers are unchanged.
        self.ledger = ledger

    def _resolve_org_key(self, org: str):
        """Find an org in the distance map, tolerating code-format drift.

        store_coords.json is keyed '001', '002', '016' while the adapters emit
        'ORG001', 'ORG002' — a prefix mismatch that made EVERY lookup miss. The
        cost of that miss was invisible: this method returned its fallback for
        every pair, so the donor score

            excess / (distance + 0.1)

        divided every candidate by the same constant and collapsed to
        excess-only ranking. The 3x warehouse-hub boost never fired either,
        since hubs are identified from the same map. Two features silently
        inert, with no error anywhere.
        """
        if not self.distance_map:
            return None
        if org in self.distance_map:
            return org
        key = str(org or "")
        digits = key.lstrip("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz_-")
        for cand in (digits, digits.lstrip("0"), digits.zfill(3),
                     key.upper(), key.lower()):
            if cand and cand in self.distance_map:
                return cand
        return None

    def _calculate_distance_km(self, org1: str, org2: str) -> float:
        """Calculate approximate distance between stores using coordinates."""
        k1 = self._resolve_org_key(org1)
        k2 = self._resolve_org_key(org2)
        if k1 is None or k2 is None:
            return 10.0 # Default fallback distance
            
        c1 = self.distance_map[k1]
        c2 = self.distance_map[k2]
        
        # Simple Haversine approximation
        lat1, lon1 = c1['lat'], c1['lon']
        lat2, lon2 = c2['lat'], c2['lon']
        
        R = 6371.0 # Radius of Earth
        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = math.sin(dlat / 2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2)**2
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        return R * c

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
               avg_daily_sales: float = 0.0,
               risk_score: float = 0.0,
               department: str = "") -> FulfillmentDecision:
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
            risk_score: GNN risk score for the store (0.0 to 1.0)
        
        Returns:
            FulfillmentDecision
        """
        if org_names is None:
            org_names = {}

        # Find potential donors with distance ranking + warehouse hub priority
        donors = network_map.find_donors(
            itm_cd,
            recipient_org=recipient_org,
            distance_calc=self._calculate_distance_km,
            warehouse_hubs=self.warehouse_hubs,
            product_name=product_name,  # enables multi-key fallback
            ledger=self.ledger,         # excess already promised elsewhere
        )

        # Cost estimates
        order_cost = shortfall_qty * unit_cost if unit_cost > 0 else shortfall_qty * 100
        
        # G9 Fix: Distance-based transfer cost
        donor_distance = 10.0  # default
        if donors:
            donor_distance = getattr(donors[0], 'last_search_dist', 10.0)
        transfer_cost = self.transfer_cost_kes + (donor_distance * self.per_km_rate)

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

        # ── FRESH ITEM RULE: No auto-transfers for perishables ──
        # Fresh items (milk, dairy, meat, bread, bakery, seafood, etc.) must
        # NOT be auto-transferred. Each store orders fresh stock calibrated
        # to their own sell-through; transferring adds transit time and
        # shortens shelf life, increasing waste/returns.
        if is_fresh or _is_fresh_department(department):
            decision.decision = "ORDER"
            decision.order_qty = shortfall_qty
            decision.reasoning = (
                f"[FRESH ITEM — NO AUTO-TRANSFER] "
                f"{product_name} is perishable ({department}). "
                f"Standard supplier order only. Transfer available for manual dispatch by ground team."
            )
            return decision

        # ── GAP-PLUG PHILOSOPHY ──
        # Transfers are a temporary plug to avoid stockout while waiting for
        # the next supplier replenishment. They are NOT a replacement for orders.
        # gap_days = how many days of stockout between stock depletion
        #            and supplier delivery arrival
        days_of_stock = 0.0
        if avg_daily_sales > 0:
            days_of_stock = current_stock / avg_daily_sales
        
        # Effective lead time: use pending order ETA if one is coming sooner
        effective_lead_days = lead_time_days
        has_pending = pending_order_qty > 0 and pending_order_eta_days < 999.0
        if has_pending and pending_order_eta_days < effective_lead_days:
            effective_lead_days = pending_order_eta_days
        
        gap_days = effective_lead_days - days_of_stock
        # gap_qty = units needed to survive the gap between stock depletion
        # and replenishment arrival
        gap_qty = max(0.0, gap_days * avg_daily_sales) if gap_days > 0 else 0.0

        # ── Implicit Rule: GNN Risk-Aware override ──
        is_high_risk = risk_score > self.risk_threshold

        # ── Urgency check ──
        hours_to_stockout = days_of_stock * 24.0 if avg_daily_sales > 0 else 999.0
        is_critical_stockout = hours_to_stockout < 4.0

        # ── GAP-PLUG Rule 0: No gap → ORDER only ──
        # If current stock covers until replenishment arrives, no transfer needed
        if gap_days <= 0 and not is_high_risk and not is_critical_stockout:
            decision.decision = "ORDER"
            decision.order_qty = shortfall_qty
            decision.reasoning = (
                f"Stock covers {days_of_stock:.1f}d, replenishment in {effective_lead_days:.1f}d. "
                f"No stockout gap — standard supplier order only."
            )
            return decision

        # ── Pending-Order Awareness ──
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

            # Rule 2: PARTIAL_COVER — delivery within 48h, reduce gap qty
            if pending_order_eta_days <= 2.0 and pending_order_qty > 0:
                gap_qty = max(0, gap_qty - pending_order_qty)
                if gap_qty < 1.0:
                    decision.decision = "ORDER"
                    decision.order_qty = shortfall_qty
                    decision.reasoning = (
                        f"Pending order ({pending_order_qty:.0f} units, "
                        f"ETA {pending_order_eta_days:.1f}d) covers the gap. "
                        f"No transfer needed."
                    )
                    return decision
                else:
                    decision.reasoning = (
                        f"[Pending order reduces gap by {pending_order_qty:.0f}] "
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
                    "No network donor available. "
                    "Not supplier's ordering day. Backlogged for next order cycle."
                )
            else:
                decision.reasoning += (
                    "No network donor available. Standard supplier order."
                )
            return decision

        # Best donor
        best = donors[0]

        # GAP-PLUG: Transfer only what's needed to cover the gap, not full shortfall.
        # IMPORTANT FIX: When ADS=0 (gap_qty≈0) but shortfall_qty>0, the ordering engine
        # has detected a real need — use shortfall_qty directly as the transfer target.
        # Zeroing transfer_target here would silently block all transfers for ~53% of items.
        if gap_qty < 0.1 and gap_days > 0:
            # ADS=0 or very low velocity: engine-computed shortfall is our best signal
            transfer_target = shortfall_qty if shortfall_qty > 0 else 0.0
        elif gap_qty > 0:
            transfer_target = min(gap_qty, shortfall_qty)
        else:
            transfer_target = shortfall_qty

        # Size against what is genuinely left, not the gross excess. `net_excess`
        # is set by find_donors and already has prior bookings — from this pass
        # or from the network scan — taken out of it.
        best_excess = getattr(best, "net_excess", None)
        if best_excess is None:
            best_excess = best.excess
        max_transferable = min(
            best_excess * self.max_donor_drain,
            transfer_target
        )

        # NOTE: Fresh items are already blocked above and will never reach here.
        # This is a safety net in case the check is bypassed.
        if is_fresh or _is_fresh_department(department):
            max_transferable = 0.0

        decision.donor_org = best.org_cd
        decision.donor_name = org_names.get(best.org_cd, best.org_cd)
        decision.donor_itm_cd = best.itm_cd
        # report what is ACTUALLY spare, not the gross figure — an operator
        # reading "excess: 400" on a donor with 390 already promised is being
        # told something untrue
        decision.donor_excess = best_excess

        if max_transferable < 1.0:
            # Donor doesn't have enough excess
            decision.reasoning += (
                f"Donor {decision.donor_name} has only {best.excess:.0f} excess "
                f"(need {transfer_target:.0f} to plug gap). Standard supplier order."
            )
            return decision

        # ── Decision Matrix (with High-Risk override) ──

        if is_high_risk and max_transferable >= 1.0:
            # GNN Risk Trigger: High volatility → Transfer now AND order for safety
            decision.decision = "BOTH"
            decision.transfer_qty = _round_transfer_qty(max_transferable, department)
            decision.order_qty = _round_transfer_qty(shortfall_qty, department)
            decision.reasoning += (
                f"[GNN HIGH RISK: Score {risk_score:.2f}] "
                f"Aggressive replenishment: Transfer {decision.transfer_qty:.0f} from "
                f"{decision.donor_name} PLUS full supplier order kept."
            )
            return decision

        if not is_ordering_day:
            # Can't order today → TRANSFER if possible
            decision.decision = "TRANSFER"
            decision.transfer_qty = _round_transfer_qty(max_transferable, department)
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

        # ── GAP-PLUG: Transfer to bridge the stockout gap, order for full replenishment ──
        # Transfer: immediate (4h) to plug the gap
        # Order: full replenishment for the store's ongoing needs
        if gap_days > 0 and max_transferable >= 1.0:
            xfer = _round_transfer_qty(max_transferable, department)
            decision.decision = "BOTH"
            decision.transfer_qty = xfer
            decision.order_qty = _round_transfer_qty(shortfall_qty, department)  # full order for replenishment
            is_from_hub = best.org_cd in self.warehouse_hubs
            hub_tag = " [WAREHOUSE HUB]" if is_from_hub else ""
            decision.reasoning += (
                f"GAP-PLUG: {gap_days:.1f}d stockout gap before replenishment. "
                f"Transfer {xfer:.0f} units from {decision.donor_name}{hub_tag} "
                f"to bridge gap. Full supplier order ({shortfall_qty:.0f}) kept for replenishment."
            )
        elif max_transferable >= shortfall_qty:
            # Cost check: Is transfer significantly cheaper than order? Or is it a micro-order that would fail MOQ?
            is_small_order = (shortfall_qty < 2.0) or (order_cost < 200.0)
            if is_small_order or (transfer_cost < order_cost * MAX_TRANSFER_COST_RATIO):
                decision.decision = "TRANSFER"
                decision.transfer_qty = _round_transfer_qty(max_transferable, department)
                decision.order_qty = 0.0
                decision.reasoning += (
                    f"Selected TRANSFER: Cost {transfer_cost} vs Order {order_cost:.0f}. "
                    f"Moving {decision.transfer_qty:.0f} units from {decision.donor_name}."
                )
                return decision
            else:
                decision.reasoning += (
                    f"Standard supplier order. "
                    f"Donor {decision.donor_name} excess: {best.excess:.0f} "
                    f"(no stockout gap, transfer not required)."
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
                     org_names: Dict[str, str] = None,
                     risk_scores: Dict[str, float] = None) -> List[FulfillmentDecision]:
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
            risk_scores: {org_cd: score} from GNN
        
        Returns:
            List of FulfillmentDecision
        """
        if risk_scores is None:
            risk_scores = {}
            
        # PROPORTIONAL ROUTING: Sort shortfalls by average daily sales (descending) 
        # so high-velocity/critical branches receive priority on donor excess.
        sorted_shortfalls = sorted(shortfalls, key=lambda x: x.get('avg_daily_sales', 0.0), reverse=True)
            
        decisions = []
        for sf in sorted_shortfalls:
            org_cd = sf['recipient_org']
            d = self.decide(
                itm_cd=sf['itm_cd'],
                product_name=sf['product_name'],
                recipient_org=org_cd,
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
                risk_score=risk_scores.get(org_cd, 0.0),
                department=sf.get('original_rec', {}).get('department', ''),
            )
            
            # PREVENT DUPLICATION: record the pledge so the same excess is not
            # promised twice.
            #
            # Through the LEDGER when one is shared, so the network scan sees
            # these pledges too — it recomputes excess from stock_data and would
            # otherwise re-offer every unit promised here. Mutating the
            # availability map, as this did unconditionally, is invisible to it.
            #
            # Without a ledger the old mutation still applies, so a standalone
            # FulfillmentDecider behaves exactly as before.
            if d.decision in ("TRANSFER", "BOTH") and d.transfer_qty > 0 and d.donor_org:
                if self.ledger is not None:
                    self.ledger.book(d.donor_org, d.donor_itm_cd or d.itm_cd,
                                     d.transfer_qty)
                else:
                    donor_state = network_map.get_store_state(d.donor_org, d.itm_cd)
                    if donor_state:
                        donor_state.current_stock -= d.transfer_qty
                        donor_state.excess = max(0.0, donor_state.excess - d.transfer_qty)
                    
            decisions.append(d)
        return decisions


#: ``ProactiveRebalancer`` lived here. It moved cold stock to hot stores --
#: the same job as ConsolidatedTransferService's PUSH pass -- with its own
#: donor test (cover > 60d), its own protection (safety_stock x 2), its own
#: fill target (30 days) and its own private bookkeeping. Two implementations
#: of one idea, disagreeing on every number, reachable from different tabs.
#:
#: Consolidated into ConsolidatedTransferService._push_opportunities(), which
#: takes its thresholds from AMIT and its horizons from LATA instead of holding
#: constants of its own, and books through the shared DonorLedger.
