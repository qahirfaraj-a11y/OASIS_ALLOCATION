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
from typing import Dict, List, Optional, Any

from .transfer_state import TransferStateTracker, TransferRecord
from .fulfillment_decider import (
    DONOR_RELEASE_FRACTION,
    DonorLedger,
    FulfillmentDecider,
    FulfillmentDecision,
    NetworkAvailabilityMap,
    StoreSkuState,
    _round_transfer_qty,
    _releasable_transfer_qty,
    _is_fresh_department,
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


@dataclass
class TransferOpportunity:
    """One recommended stock movement found by scan_network_opportunities()."""
    type: str               # "PULL" (deficit-driven) or "PUSH" (cold→hot rebalance)
    itm_cd: str
    product_name: str
    from_org: str
    to_org: str
    transfer_qty: float
    donor_days_cover: float
    recipient_days_cover: float
    donor_excess: float
    value_kes: float
    department: str = ""
    supplier: str = ""
    uom: str = "EA"
    is_fresh: bool = False
    manual_only: bool = False   # fresh items: surface but never auto-queue


@dataclass
class NetworkScanResult:
    """Output of scan_network_opportunities()."""
    opportunities: List[TransferOpportunity] = field(default_factory=list)
    # {org_cd: {"total_skus", "overstock", "deficits", "push_from"}}
    store_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)
    pending_outbound_units: float = 0.0
    pending_inbound_units: float = 0.0


# ---------------------------------------------------------------------------
# Inputs the transfer engine should not invent for itself
# ---------------------------------------------------------------------------

def load_supplier_rhythm(data_dir: str) -> Dict[str, dict]:
    """supplier (lowercased) -> LATA's measured delivery rhythm.

    Written by ``lata_shield`` into supplier_patterns_2025.json from GRN
    history. Read-only here. Missing file is not fatal: the caller falls back to
    fixed horizons and says so in the log, which is better than pretending to
    know a cadence.
    """
    import json
    import os
    out: Dict[str, dict] = {}
    path = os.path.join(data_dir or "", "supplier_patterns_2025.json")
    if not os.path.exists(path):
        return out
    try:
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
    except Exception as e:
        logger.warning("supplier rhythm unreadable (%s)", str(e)[:120])
        return out
    for name, rec in (raw or {}).items():
        if isinstance(rec, dict):
            out[str(name).strip().lower()] = rec
    return out


def load_dead_stock_config(data_dir: str) -> Dict[str, Any]:
    """AMIT's dead-stock thresholds — category tiers, default days, capital floor."""
    try:
        from .engines_config import load_engines_config
        return (load_engines_config(data_dir).get("engines", {})
                .get("dead_stock", {}) or {})
    except Exception as e:
        logger.warning("dead-stock config unavailable (%s)", str(e)[:120])
        return {}


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
                 min_shortfall_qty: float = 1.0,
                 registry_path: Optional[str] = None,
                 distance_map: Optional[Dict[str, Dict[str, float]]] = None,
                 cold_node_days: int = 60,
                 hot_node_days: int = 14,
                 target_cover_days: float = 14.0,
                 next_delivery_days: Optional[Dict[str, float]] = None,
                 safety_days_by_org: Optional[Dict[str, float]] = None,
                 supplier_rhythm: Optional[Dict[str, dict]] = None,
                 dead_stock_config: Optional[Dict[str, Any]] = None,
                 data_dir: Optional[str] = None,
                 settings: Optional[Dict[str, Any]] = None,
                 settings_db: Optional[str] = None):
        """
        Args:
            org_names: {org_cd: org_name} for all stores
            stock_data: {org_cd: [product dicts]} — current stock per store
            transfer_cost_kes: Fixed logistics cost per transfer
            min_shortfall_qty: Minimum shortfall to consider for transfer
            registry_path: Path to persistent transfer registry file
            distance_map: Optional {org_cd: {lat: L, lon: L}} for distance-aware selection
        """
        # ── operator overrides ────────────────────────────────────────────
        # Applied over the DERIVED defaults, never instead of them: a key the
        # operator has not set stays derived. Every override is logged, because
        # a hand-set horizon must never be mistaken for a measured one.
        if settings is None and settings_db:
            from . import transfer_settings as _ts
            settings = _ts.load(settings_db)
        self.settings = settings or {}

        def _tuned(key, default):
            return self.settings.get(key, default)

        self.org_names = org_names
        self.stock_data = stock_data
        self.transfer_cost_kes = _tuned("max_transfer_cost_kes", transfer_cost_kes)
        # instance-level shadows of the class constants, so an override changes
        # THIS scan and not the process
        self.RELEASE_FRACTION = _tuned("release_fraction", type(self).RELEASE_FRACTION)
        self.DEAD_STOCK_DAYS = _tuned("dead_stock_days", type(self).DEAD_STOCK_DAYS)
        self.MAX_RELIEF_DAYS = float(_tuned("max_relief_days", type(self).MAX_RELIEF_DAYS))
        #: fallback windows, reached only when no source knows the supplier
        self.fallback_deficit_days = _tuned("fallback_deficit_days", 7)
        #: donor eligibility multiple. Shown in Settings since the first
        #: release and read by nothing until now.
        self.min_excess_ratio = _tuned("min_excess_ratio", 2.0)
        self.min_shortfall_qty = min_shortfall_qty
        #: Days of cover a store keeps for itself before any of its stock is
        #: donatable — per store, because a forecourt and a 22,500 sqft anchor
        #: do not deserve the same floor.
        #:
        #: The floor was a hardcoded `ADS x 14` at both the sites that compute
        #: excess, while every store record carried a `safety_days` that was
        #: read ZERO times. This makes that field live. Absent a value the
        #: default is still 14, so a caller that passes nothing sees exactly
        #: the previous behaviour.
        #:
        #: Worth knowing before trusting it: in the 14-store depot every store
        #: seeds to the SAME safety_days (10), because build_store_network_seed
        #: falls back to a literal 10 and the source network file carries no
        #: per-store value. Wiring the input does not by itself differentiate
        #: the protection — that needs the seed to derive safety_days from real
        #: store traits, the way assortment and demand already are.
        self.safety_days_by_org = dict(safety_days_by_org or {})
        self.default_safety_days = float(_tuned("default_safety_days", 14.0))
        self.registry_path = registry_path
        self.distance_map = distance_map or {}
        self.cold_node_days = cold_node_days
        self.hot_node_days = hot_node_days
        # ONE target, used by BOTH passes. PULL used to fill to
        # pull_deficit_days (7) while PUSH topped up to hot_node_days (14), so a
        # store served by both landed at the union of two targets that were
        # never meant to disagree — measured median fill 2.88x the stated need.
        # pull_deficit_days remains the TRIGGER (is this a deficit); this is the
        # TARGET (how much cover to restore).
        self.target_cover_days = float(_tuned("fallback_target_days", target_cover_days))
        # supplier -> days until their next order window, from the calendar.
        # Turns the target from an arbitrary constant into "cover until relief
        # actually arrives".
        self.next_delivery_days = next_delivery_days or {}
        # LATA's measured supplier rhythm, and AMIT's dead-stock thresholds.
        # Both are loaded from the same data the rest of the system uses, so the
        # transfer engine stops holding private opinions about lead time and
        # deadness that disagree with the engines built to decide them.
        if supplier_rhythm is None and data_dir:
            supplier_rhythm = load_supplier_rhythm(data_dir)
        self.supplier_rhythm = supplier_rhythm or {}
        if dead_stock_config is None and data_dir:
            dead_stock_config = load_dead_stock_config(data_dir)
        dsc = dead_stock_config or {}
        self._perishability = {str(k).upper(): float(v) for k, v in
                               (dsc.get("perishability_tiers") or {}).items()}
        self._dead_days_default = float(dsc.get("days_default", 45))
        # AMIT's own capital floor is deliberately NOT used as the viability
        # test. AMIT asks "is this much capital worth an operator's attention";
        # a transfer asks "does moving it recover more than the move costs".
        # The second is the right question here and its answer is already
        # known — transfer_cost_kes — so a fixed 500 would be a second, weaker
        # opinion about the same thing. Kept for reporting only.
        self._amit_capital_floor = float(dsc.get("capital_floor", 0.0))
        # Fallback horizon for a supplier LATA has never seen: the MEDIAN of the
        # ones it has. Derived from this network's own measured behaviour rather
        # than a constant, because an unknown supplier is far more likely to
        # resemble the rest of the book than to resemble the number 7. Only if
        # there is no rhythm data at all do the passed-in constants apply.
        self._median_relief: Optional[float] = None
        if self.supplier_rhythm:
            seen = []
            for name in self.supplier_rhythm:
                d = self._relief_days(name, 0.0)
                if d:
                    seen.append(d)
            if seen:
                seen.sort()
                self._median_relief = seen[len(seen) // 2]
            logger.info("Transfer service: LATA rhythm for %d suppliers, "
                        "median relief horizon %.1fd",
                        len(self.supplier_rhythm), self._median_relief or 0.0)
        else:
            logger.warning("Transfer service: NO supplier rhythm available — "
                           "relief horizons fall back to the fixed %.0fd target. "
                           "Run lata_shield to derive them from GRN history.",
                           self.target_cover_days)

        self.tracker = TransferStateTracker()
        if self.registry_path:
            self.tracker.load_from_file(self.registry_path)

        # Detect warehouse hubs from distance_map (entries with is_warehouse_hub=True)
        # Register hubs under BOTH the map key and the ORG form: the map is
        # keyed '016' while find_donors compares against 'ORG016', so the raw
        # key alone meant the 3x hub boost could never match.
        warehouse_hubs = []
        for org_cd, info in self.distance_map.items():
            if isinstance(info, dict) and info.get('is_warehouse_hub', False):
                warehouse_hubs.append(org_cd)
                warehouse_hubs.append("ORG" + str(org_cd).zfill(3))

        # ONE book of what each donor has already promised, shared by every
        # path on this service. scan_network_opportunities and the decide()
        # path both draw on the same donors; until this existed they tracked
        # that in two different places and neither could see the other, so
        # running ordering and then a network scan offered the same units
        # twice.
        self.donor_ledger = DonorLedger()

        self.decider = FulfillmentDecider(
            transfer_cost_kes=transfer_cost_kes,
            distance_map=self.distance_map,
            warehouse_hubs=warehouse_hubs,
            # explicit, so the ordering path protects donors by exactly the
            # same fraction this service does
            max_donor_drain=self.RELEASE_FRACTION,
            ledger=self.donor_ledger,
        )

        # Build network availability map from stock data
        self.network_map = self._build_network_map()

    def _safety_days(self, org_cd: str) -> float:
        """Days of cover this store keeps before its stock may be donated.

        Per store when the caller supplied one, otherwise the default. A zero
        or unparseable value falls back rather than dropping the floor to
        nothing — an unreadable field must never make a store fully drainable.
        """
        try:
            v = float(self.safety_days_by_org.get(org_cd) or 0)
        except (TypeError, ValueError):
            v = 0.0
        return v if v > 0 else self.default_safety_days

    def _build_network_map(self) -> NetworkAvailabilityMap:
        """Build cross-store availability index from current stock data."""
        nmap = NetworkAvailabilityMap()
        
        # Load barcode map
        import json
        import os
        try:
            _data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
            _map_path = os.path.join(_data_dir, 'product_barcode_map.json')
            if not os.path.exists(_map_path):
                _map_path = os.path.join(os.getcwd(), 'oasis', 'data', 'product_barcode_map.json')
                
            with open(_map_path, 'r', encoding='utf-8') as f:
                bcode_map = json.load(f)
        except Exception:
            bcode_map = {}

        for org_cd, products in self.stock_data.items():
            org_name = self.org_names.get(org_cd, org_cd)
            safety_days = self._safety_days(org_cd)
            for p in products:
                ads = float(p.get('avg_daily_sales', 0) or 0)
                current = float(p.get('current_stocks', 0) or 0)
                days_cover = (current / ads) if ads > 0 else 999.0
                dept = str(p.get('department', p.get('product_category', 'GENERAL'))).upper()
                is_fresh = any(k in dept for k in [
                    'MILK', 'DAIRY', 'FRESH', 'MEAT', 'BREAD', 'BAKERY'
                ]) or p.get('is_fresh', False)

                safety = ads * safety_days  # this store's own floor, not a literal
                overstock_threshold = 14.0 if is_fresh else 30.0
                excess = 0.0
                if ads > 0 and days_cover > overstock_threshold and (current - safety) > (ads * 7.0):
                    excess = current - safety
                elif ads == 0 and current > 0:
                    excess = current

                pname = str(p.get('product_name', 'Unknown'))
                bcode = bcode_map.get(pname, '')

                nmap.add(StoreSkuState(
                    org_cd=org_cd,
                    org_name=org_name,
                    itm_cd=str(p.get('itm_cd', p.get('item_code', p.get('product_name', '')))),
                    product_name=pname,
                    current_stock=current,
                    avg_daily_sales=ads,
                    safety_stock=safety,
                    excess=excess,
                    is_fresh=is_fresh,
                    sell_price=float(p.get('selling_price', p.get('sell_price', 0)) or 0),
                    department=dept,
                    days_since_delivery=int(p.get('last_days_since_last_delivery', 0) or 0),
                    velocity_ratio=float(ads / max(1.0, current)) if current > 0 else 0.0
                ), bcode)
        return nmap

    #: Share of a donor's excess that may leave in one scan, across BOTH
    #: passes. One number, not 0.5 for PULL and 0.4 for PUSH — and now not a
    #: second declaration either: it IS fulfillment_decider's constant, so the
    #: scan path and decide() cannot drift to different values.
    #: Dead stock is exempt (no demand, no service to protect) — see the PUSH pass.
    RELEASE_FRACTION = DONOR_RELEASE_FRACTION

    #: A line is DEAD when it has zero demand AND has been silent this long.
    #:
    #: The second half is not redundant. Zero ADS on its own also describes a
    #: line ranged last week that has not had its first sale yet, and shipping
    #: those away is the opposite of useful. Deadness is a sustained condition,
    #: so it needs the age as well as the silence.
    DEAD_STOCK_DAYS = 90

    #: Last-resort ceiling on a relief horizon, used ONLY where the category is
    #: unknown so no perishability threshold can be derived. Where the category
    #: IS known the cap comes from AMIT's tier for it — a bakery line is capped
    #: at 5 days because that is when it dies, not because of a number chosen
    #: here. See _relief_days.
    MAX_RELIEF_DAYS = 45.0

    def is_dead(self, ads: float, stock: float, days_since_delivery: float,
                department: str = "", unit_cost: float = 0.0) -> bool:
        """Dead stock, per AMIT's definition rather than a second opinion.

        ``amit_governance`` already classifies dead stock for the whole system
        and does it better than a flat days-of-cover rule: its thresholds are
        CATEGORY-AWARE (bakery dies at 5 days, cereals at 60). PUSH's own
        ``cold_node_days > 60`` was a third, blunter definition of the same
        thing.

        The viability floor here is the TRANSFER COST, not AMIT's capital
        floor. They answer different questions — AMIT asks whether the trapped
        capital deserves an operator's attention, a transfer asks whether
        moving it recovers more than the lorry costs — and only the second one
        decides whether to load a van.

        DEAD means zero demand AND silent for DEAD_STOCK_DAYS. Deliberately
        NARROWER than AMIT, which also classes a still-selling line sitting on
        more than its category threshold as dead. That second case is
        OVERSTOCK, not dead stock: it is turning over, just too slowly, and it
        is already the donor supply the PULL pass draws on via _excess_units.
        Feeding it to PUSH as well double-counts the same units in two passes —
        measured, that moved 281% of the network's actual dead stock.

        The category threshold still governs how much a RECIPIENT can absorb
        (see the PUSH pass), which is where per-category shelf life genuinely
        belongs.
        """
        stock = float(stock or 0)
        if stock <= 0 or ads > 0:
            return False
        if float(days_since_delivery or 0) < self.DEAD_STOCK_DAYS:
            return False
        # worth a lorry? Derived from what the move actually costs, not a fixed
        # floor -- recovering less trapped capital than the transfer costs is
        # not a recovery.
        if unit_cost:
            return stock * float(unit_cost) >= self.transfer_cost_kes
        return True

    def _dead_days_for(self, department: str) -> float:
        """AMIT's per-category dead-stock threshold, defaulting to its default."""
        return float(self._perishability.get(str(department or "").upper(),
                                             self._dead_days_default))

    def _relief_days(self, supplier: str, lead_days: float,
                     department: str = "") -> Optional[float]:
        """Days until replenishment actually LANDS, or None if unknown.

        This is the horizon every number in the PULL pass is measured against: a
        store is short when it cannot reach this date, and a transfer restores
        exactly enough to reach it — no more, because a transfer plugs a gap and
        is not a replenishment.

        Sourced from LATA rather than from a stated lead time. LATA's whole
        premise is that the supplier's promise is the least reliable number
        available, so it derives the real cadence from GRN history:

            relief = median gap between deliveries + lead x variance multiplier

        The multiplier lands on the LEAD, not on the whole horizon. The cadence
        is an observed fact; the uncertainty is in whether the delivery arrives
        when promised, which is exactly what LATA's multiplier measures.
        Inflating the cadence as well double-counts it: across these 599
        suppliers it puts the median horizon at 46 days and pins half of them
        against the cap, versus a defensible 23 days this way.

        The multiplier still does real work — Brookside's measured variance
        earns 2.749x on its lead. A store served by a supplier that misses is
        short EARLIER than one served by a supplier that does not, and only
        LATA knows the difference.

        Returns None when the supplier is unknown, so callers decide the
        fallback. Substituting a number here is how a missing schedule becomes a
        confident wrong answer.
        """
        # The horizon can never usefully exceed the point at which the goods
        # spoil or go stale: holding 60 days of cover of a line that dies in 5
        # is not coverage, it is future waste. AMIT already knows that number
        # per category, so the ceiling is derived from the goods rather than
        # picked. MAX_RELIEF_DAYS applies only where the category is unknown.
        ceiling = min(self._dead_days_for(department), self.MAX_RELIEF_DAYS) \
            if department else self.MAX_RELIEF_DAYS

        key = (supplier or "").strip().lower()
        r = self.supplier_rhythm.get(key)
        if r:
            gap = float(r.get("median_gap_days") or r.get("avg_gap_days") or 0)
            lead = float(r.get("estimated_delivery_days") or lead_days or 0)
            mult = float(r.get("lata_variance_multiplier") or 1.0)
            days = gap + lead * max(0.5, mult)
            if days > 0:
                return max(1.0, min(ceiling, days))
        # legacy path: a plain calendar lookup with no reliability information
        nxt = self.next_delivery_days.get(key)
        if nxt is not None:
            return max(1.0, min(ceiling, float(nxt) + float(lead_days or 0)))
        # unknown supplier: behave like the rest of this network's book, not
        # like a constant nobody derived. Guarded against recursion — this is
        # only consulted after the table has been summarised.
        if getattr(self, "_median_relief", None):
            return max(1.0, min(ceiling, self._median_relief))
        return None

    def _target_cover(self, supplier: str, lead_days: float,
                      department: str = "") -> float:
        """Days of cover a transfer should restore: exactly enough to reach relief.

        A TRANSFER PLUGS A GAP. It is not a replenishment, so the target is the
        relief horizon itself and nothing beyond it — covering past the next
        delivery moves stock the supplier is about to deliver anyway, and books
        a lorry to do it.

        This used to be ``min(14, relief)``, which capped the horizon at a fixed
        14 days. That is wrong in both directions: it over-fills a store whose
        supplier arrives on Tuesday, and under-fills one on a fortnightly
        cycle with an unreliable supplier — precisely the store most likely to
        stock out. The 14 survives only as the fallback for a supplier LATA has
        never seen.
        """
        relief = self._relief_days(supplier, lead_days, department)
        return relief if relief is not None else self.target_cover_days

    def _target_units(self, entry: dict) -> float:
        """Units of cover this store should hold for this SKU.

        Prefers the horizon already resolved onto the entry, because that one
        knows about an open purchase order and this recomputation would not.
        Without it the trigger and the target disagree: a store admitted for
        being short over three days would be filled for eighteen, and the
        transfer would carry stock the supplier is about to deliver.
        """
        ads = float(entry.get('avg_daily_sales') or 0)
        if ads <= 0:
            return 0.0
        horizon = entry.get('relief_days')
        if horizon is None:
            horizon = self._target_cover(entry.get('supplier', ''),
                                         entry.get('lead_days', 0.0),
                                         entry.get('department', ''))
        return ads * float(horizon)

    @staticmethod
    def _excess_units(ads: float, stock: float, is_fresh: bool,
                      safety_days: float = 14.0) -> float:
        """Donor excess above safety stock — single definition for PULL and PUSH.

        Mirrors _build_network_map(): the store's own safety floor, overstock
        gate at 14d (fresh) / 30d (dry), and a 7-day buffer above safety before
        any units count as excess. Zero-ADS items with stock are fully excess.

        ``safety_days`` defaults to 14 so existing callers — including devkit
        analysers that call this statically — keep their previous answer.
        """
        if ads > 0:
            days_cover = stock / ads
            safety = ads * safety_days
            overstock_threshold = 14.0 if is_fresh else 30.0
            if days_cover > overstock_threshold and (stock - safety) > (ads * 7.0):
                return stock - safety
            return 0.0
        return stock if stock > 0 else 0.0

    @staticmethod
    def _item_key(p: dict) -> str:
        return str(p.get('itm_cd', p.get('item_code',
                                         p.get('product_name', ''))) or '')

    def _coverage_entry(self, org_cd: str, p: dict,
                        outbound: Dict[tuple, float],
                        inbound: Dict[tuple, float]) -> dict:
        """One store's position on one SKU — the view BOTH passes work from.

        Extracted so the PUSH pass can be reached from ``optimize_network`` as
        well as from the scan without rebuilding this by hand. A second,
        hand-rolled view of the same numbers is precisely how ProactiveRebalancer
        came to hold its own thresholds and its own private ledger.
        """
        itm = self._item_key(p)
        ads = float(p.get('avg_daily_sales', 0) or 0)
        stock = float(p.get('current_stocks', p.get('current_stock', 0)) or 0)
        dept = str(p.get('department', p.get('product_category', 'GENERAL'))).upper()
        fresh = bool(p.get('is_fresh', False)) or _is_fresh_department(dept)

        # Donor view: outbound commitments reduce what we can give.
        donor_stock = max(0.0, stock - outbound.get((org_cd, itm), 0.0))
        donor_excess = self._excess_units(ads, donor_stock, fresh,
                                          self._safety_days(org_cd))
        # Recipient view: inbound commitments count as supply.
        eff_stock = donor_stock + inbound.get((org_cd, itm), 0.0)

        return {
            'itm_cd': itm,
            'product_name': str(p.get('product_name', '')),
            'org_cd': org_cd,
            'current_stock': eff_stock,
            'avg_daily_sales': ads,
            'days_cover': (eff_stock / ads) if ads > 0 else 999.0,
            'donor_excess': donor_excess,
            'sell_price': float(p.get('selling_price', p.get('sell_price', 0)) or 0),
            'cost_price': float(p.get('cost_price', 0) or 0),
            'department': dept,
            'supplier': str(p.get('supplier_name', '') or ''),
            'lead_days': float(p.get('estimated_delivery_days', 0) or 0),
            'uom': str(p.get('uom', 'EA')).upper(),
            'is_fresh': fresh,
            # Stock already on a supplier order. Counted as supply only where
            # it lands before relief is needed — a pallet due in three weeks
            # does not help a store that runs out on Thursday, and treating it
            # as though it did would suppress a transfer the store genuinely
            # needs. The trigger applies the ETA test, because only there is
            # the relief horizon known.
            'on_order_qty': float(p.get('on_order_qty', 0) or 0),
            'on_order_eta': float(p.get('on_order_eta_days', 999) or 999),
            # needed by the dead-stock test: silence alone does not make a line
            # dead, it has to have been silent for a while
            'days_since_delivery': int(
                p.get('last_days_since_last_delivery',
                      p.get('days_since_delivery', 0)) or 0),
        }

    def _coverage_index(self, outbound: Optional[Dict[tuple, float]] = None,
                        inbound: Optional[Dict[tuple, float]] = None
                        ) -> Dict[str, Dict[str, dict]]:
        """{itm_cd: {org_cd: entry}} across the whole network."""
        outbound = outbound or {}
        inbound = inbound or {}
        cov: Dict[str, Dict[str, dict]] = {}
        for org_cd, products in self.stock_data.items():
            for p in products:
                itm = self._item_key(p)
                if itm:
                    cov.setdefault(itm, {})[org_cd] = self._coverage_entry(
                        org_cd, p, outbound, inbound)
        return cov

    @staticmethod
    def _pending_flows(pending_transfers: List[dict]):
        """Aggregate REQUESTED/IN_TRANSIT transfers into committed stock flows.

        Returns ({(from_org, itm_cd): qty_out}, {(to_org, itm_cd): qty_in}).
        Accepts both lowercase service keys and INTEGRATION_TRANSFER_ORDERS
        column names (FROM_ORG_CD/TO_ORG_CD/ITM_CD/QUANTITY/STATUS).
        """
        outbound: Dict[tuple, float] = {}
        inbound: Dict[tuple, float] = {}
        for t in pending_transfers or []:
            status = str(t.get('status', t.get('STATUS', ''))).upper()
            if status not in ('REQUESTED', 'IN_TRANSIT'):
                continue
            from_org = str(t.get('from_org', t.get('FROM_ORG_CD', '')) or '')
            to_org = str(t.get('to_org', t.get('TO_ORG_CD', '')) or '')
            itm_cd = str(t.get('itm_cd', t.get('ITM_CD', '')) or '')
            qty = float(t.get('qty', t.get('QUANTITY', 0)) or 0)
            if not itm_cd or qty <= 0:
                continue
            if from_org:
                outbound[(from_org, itm_cd)] = outbound.get((from_org, itm_cd), 0.0) + qty
            if to_org:
                inbound[(to_org, itm_cd)] = inbound.get((to_org, itm_cd), 0.0) + qty
        return outbound, inbound

    def _donor_pool(self, item_coverage, booked, donor_org: str,
                    itm: str) -> float:
        """What this donor may still release for this SKU — ONE pool.

        Donor protection used to be applied twice with different numbers: PULL
        took min(0.5 x excess, ...) per transfer and PUSH took
        min(0.4 x excess, ...), both against the same excess. A donor could give
        0.5 of its excess to pulls and a further 0.4 to pushes — 90% of what the
        safety floor was meant to protect, from two rules that each believed
        they were the only one.

        Now a single pool, RELEASE_FRACTION of excess, drawn down through the
        shared ledger. A METHOD rather than a closure so the PUSH pass can be
        reached from optimize_network as well as from the scan.
        """
        cov = item_coverage.get(itm, {}).get(donor_org)
        base = cov['donor_excess'] if cov else 0.0
        return booked.available(donor_org, itm, base, self.RELEASE_FRACTION)

    def scan_network_opportunities(self,
                                   moq_failures: Optional[Dict[str, set]] = None,
                                   pending_transfers: Optional[List[dict]] = None,
                                   pull_deficit_days: Optional[float] = None,
                                   max_pull_per_store: int = 0,
                                   max_push: int = 0,
                                   ) -> NetworkScanResult:
        """
        Network-wide PULL + PUSH transfer opportunity scan.

        This is the single implementation of the scan that previously lived
        inline in the Transfer Intelligence dashboard tab. Key guarantees:

        - Pending awareness: REQUESTED/IN_TRANSIT transfers count as committed
          supply — donors lose that stock, recipients gain it — so re-running
          the scan does not regenerate transfers already queued.
        - Consistent donor protection: PULL and PUSH both use _excess_units()
          (14-day safety floor); PUSH can no longer strip a donor to 2 days.
        - Intra-scan booking: once a donor's excess is allocated to one
          recipient, it is not offered again to the next.
        - Single fresh definition (fulfillment_decider): fresh items are
          surfaced as manual_only, never auto-queued.

        Args:
            moq_failures: {org_cd: {itm_cd}} from the MOQ failure store —
                items too small to order become pull triggers here.
            pending_transfers: open transfer records (dicts or DB rows).
            pull_deficit_days: days-of-cover threshold that defines a deficit.
            max_pull_per_store: cap on deficit items evaluated per store,
                AFTER ranking by margin at risk. 0 means no cap — a truncated
                transfer list is worse than a short one, because it looks
                complete. Defaults to 0 for that reason.
            max_push: cap on PUSH opportunities (highest value first). 0 = none.
        """
        moq_failures = moq_failures or {}
        # None means "use the configured fallback"; an explicit value from the
        # caller still wins, so devkit tools and tests can sweep it.
        if pull_deficit_days is None:
            pull_deficit_days = self.fallback_deficit_days
        outbound, inbound = self._pending_flows(pending_transfers or [])
        result = NetworkScanResult(
            pending_outbound_units=sum(outbound.values()),
            pending_inbound_units=sum(inbound.values()),
        )

        def _key(p: dict) -> str:
            return str(p.get('itm_cd', p.get('item_code', p.get('product_name', ''))) or '')

        def _fresh(p: dict, dept: str) -> bool:
            return bool(p.get('is_fresh', False)) or _is_fresh_department(dept)

        # ── Pass 1: per-store deficit detection + per-item coverage index ──
        store_deficits: Dict[str, List[dict]] = {}
        item_coverage: Dict[str, Dict[str, dict]] = {}

        for org_cd, products in self.stock_data.items():
            deficits = []
            n_excess = 0
            for p in products:
                itm = _key(p)
                if not itm:
                    continue
                entry = self._coverage_entry(org_cd, p, outbound, inbound)
                ads = entry['avg_daily_sales']
                eff_stock = entry['current_stock']
                days_cover = entry['days_cover']
                if entry['donor_excess'] > 0:
                    n_excess += 1
                item_coverage.setdefault(itm, {})[org_cd] = entry

                rop = float(p.get('reorder_point', 0) or 0)
                org_moq = moq_failures.get(org_cd) or {}
                # A store is short when it cannot reach its next delivery --
                # not when it drops under a flat 7 days. A supplier arriving
                # tomorrow makes 3 days of cover perfectly safe; a fortnightly
                # one with a poor LATA record makes 10 days an emergency. The
                # flat figure is only the fallback for suppliers LATA cannot
                # speak for.
                relief = self._relief_days(entry['supplier'], entry['lead_days'],
                                           entry['department'])
                trigger_days = relief if relief is not None else pull_deficit_days
                # Only a DERIVED horizon is recorded on the entry. The trigger
                # and the target have different fallbacks on purpose — a store
                # is short below 7 days but is filled to 14 — so writing the
                # trigger's fallback here would quietly become the target and
                # under-fill every line whose supplier is unknown.
                if relief is not None:
                    entry['relief_days'] = relief

                # AN OPEN ORDER SHORTENS THE HORIZON. IT DOES NOT CANCEL THE
                # TRANSFER.
                #
                # A delivery landing in three days does not help a store that
                # is empty today: it still loses three days of sales. What the
                # order changes is HOW FAR the store has to be carried — only
                # to the day the pallet arrives, not across the supplier's
                # whole typical cycle. So the transfer shrinks to the real gap
                # instead of disappearing.
                #
                # This horizon is also better than LATA's: LATA gives the
                # supplier's usual cadence, while an open PO gives the date of
                # THIS delivery — placed order plus its lead time. A specific
                # fact beats a measured average.
                if entry['on_order_qty'] > 0:
                    eta = max(1.0, entry['on_order_eta'])
                    trigger_days = min(trigger_days, eta)
                    # shortens the TARGET too, so the transfer carries the
                    # store to the delivery and no further
                    entry['relief_days'] = min(
                        float(entry.get('relief_days', eta)), eta)
                    entry['horizon_source'] = 'open order'

                pull_trigger = (
                    (ads > 0 and (days_cover < trigger_days or eff_stock <= rop))
                    or (ads == 0 and eff_stock < 1.0)
                    or (itm in org_moq)
                )
                if pull_trigger:
                    # An item that failed the MOQ gate carries the order qty
                    # the store actually needs — use it as a shortfall floor
                    # (supports both {itm: qty} dicts and legacy {itm} sets).
                    if isinstance(org_moq, dict):
                        entry['moq_qty'] = float(org_moq.get(itm, 0) or 0)
                    else:
                        entry['moq_qty'] = 0.0
                    deficits.append(entry)

            # RANK BEFORE CAPPING.
            #
            # This list used to be consumed as deficits[:max_pull_per_store] in
            # whatever order the catalogue came back in. On the seeded network
            # that is 13,687 deficits competing for 700 slots chosen ARBITRARILY,
            # and it is why recall against the answer key was 10%: the engine was
            # not failing to match, it was never looking at 95% of the lines.
            #
            # Ranking by margin at risk makes the cap defensible — whatever is
            # dropped is now the least valuable rather than the last read.
            for e in deficits:
                a = e['avg_daily_sales']
                # rank on the gap to THIS line's own relief horizon, so the
                # ranking agrees with the trigger that admitted it
                short = max(a * float(e.get('relief_days') or pull_deficit_days)
                            - e['current_stock'],
                            e.get('moq_qty', 0.0), 0.0)
                sell, cost = e['sell_price'], e['cost_price']
                if sell > 0 and 0 < cost < sell:
                    margin = sell - cost
                    e['margin_estimated'] = False
                else:
                    # No usable cost: assume a retail margin rather than scoring
                    # the line at zero and burying it. Counted and reported, so
                    # a catalogue with no cost data is visible rather than
                    # silently reshuffling the whole ranking.
                    margin = sell * 0.25
                    e['margin_estimated'] = True
                e['shortfall_units'] = round(short, 2)
                e['risk_kes'] = round(short * margin, 2)
            deficits.sort(key=lambda e: -e['risk_kes'])

            store_deficits[org_cd] = deficits
            result.store_stats[org_cd] = {
                'total_skus': len(products),
                'overstock': n_excess,
                'deficits': len(deficits),
                'push_from': 0,
            }

        # ── Pass 2: PULL — fair-share, not first-come ──
        #
        # This used to walk store_deficits in dict order and let each store take
        # what it wanted before the next was considered. Measured consequence:
        # reversing the store order moved 37,544 units — 12.6% of volume — and
        # swung individual stores by up to 19%. Nothing about those stores
        # changed, only their position in a dict. Roughly a third of donors run
        # out (29.8% fully exhausted, 5.7% near), so who is served first is not
        # a neutral detail; it decides who is served at all.
        #
        # Now: every deficit across the network is collected first, then each
        # contended donor splits its releasable pool PROPORTIONALLY TO NEED.
        # Weight is risk_kes — margin at risk if the line is not served — so a
        # store about to lose more money gets more of a scarce donor, and two
        # stores with equal exposure are treated equally regardless of order.
        #
        # Proportional allocation is order-independent by construction, which is
        # the acceptance test: devkit/measure_order_sensitivity.py must report
        # zero divergence between orderings.
        # THE shared donor book, not a local one. Anything the decide() path
        # already promised is visible here, and anything promised here is
        # visible there. `recip_booked` stays local: it tracks what a RECIPIENT
        # has been sent within this scan, which is a per-scan question.
        booked = self.donor_ledger
        recip_booked: Dict[tuple, float] = {}
        dropped_by_cap = 0

        requests = []
        for rec_org, deficits in store_deficits.items():
            considered = deficits if max_pull_per_store <= 0 \
                else deficits[:max_pull_per_store]
            dropped_by_cap += max(0, len(deficits) - len(considered))
            for item in considered:
                itm = item['itm_cd']
                ads = item['avg_daily_sales']
                target_qty = self._target_units(item) if ads > 0 else 2.0
                shortfall = max(0.0, target_qty - item['current_stock'],
                                item.get('moq_qty', 0.0))
                if shortfall < 0.1:
                    continue
                requests.append({
                    'rec_org': rec_org, 'itm': itm, 'item': item,
                    'remaining': shortfall,
                    # margin at risk, computed in Pass 1. Falls back to the raw
                    # shortfall so a catalogue without costs still ranks by need
                    # rather than collapsing every weight to zero.
                    'weight': max(float(item.get('risk_kes') or 0.0),
                                  shortfall * 0.01),
                    'tried': set(),
                })

        # deterministic input order — the allocation must not depend on it, and
        # sorting makes that testable rather than merely hoped for
        requests.sort(key=lambda r: (r['itm'], r['rec_org']))

        def _pool(donor_org: str, itm: str) -> float:
            return self._donor_pool(item_coverage, booked, donor_org, itm)

        donor_cache: Dict[tuple, list] = {}

        def _donors_for(req):
            k = (req['itm'], req['rec_org'])
            if k not in donor_cache:
                donor_cache[k] = self.network_map.find_donors(
                    req['itm'], req['rec_org'],
                    product_name=req['item']['product_name'],
                    distance_calc=self.decider._calculate_distance_km,
                    warehouse_hubs=self.decider.warehouse_hubs,
                    # operator-tunable donor eligibility. Velocity still adjusts
                    # it (1.5x fast / 2.5x slow); this is the middle case.
                    min_excess_ratio=self.min_excess_ratio,
                    ledger=self.donor_ledger,
                )
            return donor_cache[k]

        # Rounds: a recipient whose preferred donor is exhausted falls back to
        # its next-best. Three passes is enough to drain realistic contention
        # without turning an O(n) scan into something quadratic.
        MAX_ROUNDS = 3
        pulls: Dict[tuple, float] = {}          # (donor, itm, rec) -> qty
        for _ in range(MAX_ROUNDS):
            groups: Dict[tuple, list] = {}
            for r in requests:
                if r['remaining'] < 0.1:
                    continue
                for d in _donors_for(r):
                    if d.org_cd in r['tried']:
                        continue
                    if _pool(d.org_cd, r['itm']) > 0:
                        groups.setdefault((d.org_cd, r['itm']), []).append(r)
                        break
            if not groups:
                break
            for (donor_org, itm) in sorted(groups):
                rs = groups[(donor_org, itm)]
                avail = _pool(donor_org, itm)
                if avail <= 0:
                    for r in rs:
                        r['tried'].add(donor_org)
                    continue
                total_w = sum(r['weight'] for r in rs) or 1.0
                # proportional split, each capped by its own remaining need
                for r in sorted(rs, key=lambda x: (-x['weight'], x['rec_org'])):
                    if avail <= 0:
                        break
                    share = min(r['remaining'], avail * (r['weight'] / total_w))
                    take = _round_transfer_qty(min(share, avail),
                                               r['item']['department'])
                    # The rounding ceils, so an `avail` of 0.44 would ship a
                    # whole unit and drive this donor past RELEASE_FRACTION.
                    # Bound it by what is genuinely releasable.
                    take = min(take, _releasable_transfer_qty(
                        avail, r['item']['department']))
                    if take >= 1:
                        pulls[(donor_org, itm, r['rec_org'])] = \
                            pulls.get((donor_org, itm, r['rec_org']), 0.0) + take
                        booked.book(donor_org, itm, take)
                        recip_booked[(r['rec_org'], itm)] = \
                            recip_booked.get((r['rec_org'], itm), 0.0) + take
                        r['remaining'] -= take
                        avail -= take
                    r['tried'].add(donor_org)
                # anything left over goes to whoever is still short, heaviest
                # first — otherwise a rounding remainder strands usable stock
                if avail >= 1:
                    for r in sorted(rs, key=lambda x: (-x['weight'], x['rec_org'])):
                        if avail < 1 or r['remaining'] < 1:
                            continue
                        take = _round_transfer_qty(min(r['remaining'], avail),
                                                   r['item']['department'])
                        take = min(take, _releasable_transfer_qty(
                            avail, r['item']['department']))
                        if take < 1:
                            continue
                        pulls[(donor_org, itm, r['rec_org'])] = \
                            pulls.get((donor_org, itm, r['rec_org']), 0.0) + take
                        booked.book(donor_org, itm, take)
                        recip_booked[(r['rec_org'], itm)] = \
                            recip_booked.get((r['rec_org'], itm), 0.0) + take
                        r['remaining'] -= take
                        avail -= take

        by_req = {(r['itm'], r['rec_org']): r for r in requests}
        for (donor_org, itm, rec_org), xfer in sorted(pulls.items()):
            req = by_req.get((itm, rec_org))
            if req is None or xfer < 1:
                continue
            item = req['item']
            donor_cov = item_coverage.get(itm, {}).get(donor_org, {})
            result.opportunities.append(TransferOpportunity(
                type="PULL",
                itm_cd=itm,
                product_name=item['product_name'],
                from_org=donor_org,
                to_org=rec_org,
                transfer_qty=xfer,
                donor_days_cover=round(donor_cov.get('days_cover', 999.0), 1),
                recipient_days_cover=round(item['days_cover'], 1),
                donor_excess=round(donor_cov.get('donor_excess', 0.0), 1),
                value_kes=round(xfer * item['sell_price'], 0),
                department=item['department'],
                supplier=item['supplier'],
                uom=item['uom'],
                is_fresh=item['is_fresh'],
                manual_only=item['is_fresh'],
            ))

        # ── Pass 3: PUSH, via the shared implementation ──
        push_opps = self._push_opportunities(item_coverage, booked, recip_booked)
        push_opps.sort(key=lambda o: -o.value_kes)
        kept_push = push_opps if max_push <= 0 else push_opps[:max_push]
        for o in kept_push:
            result.store_stats.setdefault(o.from_org, {}).setdefault('push_from', 0)
            result.store_stats[o.from_org]['push_from'] += 1
        result.opportunities.extend(kept_push)

        result.opportunities.sort(key=lambda o: -o.value_kes)
        est = sum(1 for ds in store_deficits.values()
                  for e in ds if e.get('margin_estimated'))
        total_def = sum(len(ds) for ds in store_deficits.values())
        if dropped_by_cap:
            logger.warning("Network scan: %d of %d deficit lines DROPPED by "
                           "max_pull_per_store — the list is truncated, not "
                           "complete", dropped_by_cap, total_def)
        if est:
            logger.info("Network scan: %d of %d deficit lines ranked on an "
                        "ESTIMATED margin (no usable cost price)", est, total_def)
        logger.info(
            "Network scan: %d opportunities (%d pull / %d push), "
            "pending committed: %.0f out / %.0f in",
            len(result.opportunities),
            sum(1 for o in result.opportunities if o.type == "PULL"),
            sum(1 for o in result.opportunities if o.type == "PUSH"),
            result.pending_outbound_units, result.pending_inbound_units,
        )
        return result

    def _push_opportunities(self, item_coverage, booked, recip_booked):
        """Idle capital -> nodes that will sell it. THE one implementation.

        Reachable from both entry points. ``optimize_network`` used to call a
        separate ProactiveRebalancer for the same job, with its own donor test
        (cover > 60d), its own protection (safety x 2), its own fill target
        (30 days) and its own private bookkeeping -- four numbers and a ledger
        that disagreed with this pass on every one of them.
        """
        # ── Pass 3: PUSH — cold nodes (dead capital) → hot nodes, fair-shared ──
        #
        # Same treatment as PULL: collect every hot recipient's need for a SKU
        # first, then split each cold donor's pool PROPORTIONALLY, rather than
        # letting the first donor walk the recipient list and give to whoever it
        # reaches first. Weight is need x unit margin — the same shape as the
        # risk_kes used by PULL, so both passes rank need the same way.
        #
        # Iteration is sorted throughout: item_coverage and its inner maps are
        # keyed in store-insertion order, and walking them raw is what left 1.0%
        # of volume moving when the store order was reversed.
        # PUSH answers a DIFFERENT question from PULL. Its job is not "who is
        # short" but "what capital is sitting still, and where would it sell".
        #
        # TWO kinds of donor, and the difference is the RELEASE RULE, not
        # eligibility:
        #
        #   DEAD       zero demand, silent 90+ days. Its safety stock is
        #              ADS x horizon = 0, so withholding any of it protects
        #              nothing. Releases in FULL.
        #   OVERSTOCK  still selling, but sitting on more cover than the
        #              category survives. Genuinely surplus, but the store is
        #              still trading on it, so it releases the protected
        #              fraction through the shared pool exactly as PULL does.
        #
        # An earlier revision made the donor gate dead-only, which quietly
        # deleted overstock rebalancing: a store on 100 days of cover next to
        # one on 10 moved nothing. That was never the intent — the thing that
        # needed fixing was the release rule (full release had been applied to
        # BOTH kinds, over-draining lines that were still trading), not the
        # eligibility.
        #
        # Recipient side is ACTIVE, not SHORT. A store selling the line steadily
        # is where idle stock turns back into cash, whether or not it happens to
        # have a gap. Requiring a gap is a PULL criterion, and applying it here
        # is why this pass emitted 29 lines against 240 dead ones — and 2 once
        # relief horizons made gaps smaller.
        #
        # Sizing is DERIVED: a recipient may absorb what it can sell before the
        # line would go dead at ITS OWN velocity, less what it already holds.
        # That is the largest move that does not simply relocate the problem.
        push_opps: List[TransferOpportunity] = []
        for itm in sorted(item_coverage):
            cov_map = item_coverage[itm]
            cold = []
            for c in cov_map.values():
                if c['donor_excess'] <= 0:
                    continue
                dead = self.is_dead(c['avg_daily_sales'], c['current_stock'],
                                    c.get('days_since_delivery', 0),
                                    c['department'], c.get('cost_price', 0.0))
                # overstock: still selling, but past the point where the
                # category itself says the stock has stopped working
                over = (c['avg_daily_sales'] > 0
                        and c['days_cover'] > self._dead_days_for(c['department']))
                if dead or over:
                    cold.append(dict(c, _dead=dead))
            cold.sort(key=lambda c: (-c['days_cover'], c['org_cd']))
            if not cold:
                continue

            # every ACTIVE store's absorption capacity, measured once
            demands = []
            for recip in sorted((c for c in cov_map.values()
                                 if c['avg_daily_sales'] > 0),
                                key=lambda c: (-c['avg_daily_sales'], c['org_cd'])):
                recip_in = recip_booked.get((recip['org_cd'], itm), 0.0)
                eff_stock = recip['current_stock'] + recip_in
                recip_ads = recip['avg_daily_sales']
                sell = float(recip.get('sell_price') or 0)
                cost = float(recip.get('cost_price') or 0)
                margin = (sell - cost) if 0 < cost < sell else sell * 0.25

                # TWO capacities, because the alternatives differ.
                #
                # From a DEAD donor the alternative is that the stock stays
                # dead, so it is worth filling the receiver as deep as it can
                # trade the line out — its category's own threshold.
                #
                # From an OVERSTOCK donor the alternative is that a store which
                # is still selling keeps it. Filling the receiver past what it
                # needs to reach its next delivery would just move the surplus,
                # so that capacity is the ordinary relief horizon — the same
                # target PULL uses.
                cap_dead = recip_ads * self._dead_days_for(recip['department'])
                cap_over = self._target_units(recip)
                if cap_dead <= eff_stock and cap_over <= eff_stock:
                    continue
                demands.append({
                    'recip': recip,
                    'need_dead': max(0.0, cap_dead - eff_stock),
                    'need_over': max(0.0, cap_over - eff_stock),
                    # rank by the cash a move releases per day of shelf life
                    # used: velocity x margin, not raw need
                    'weight': max(recip_ads * margin, 0.01),
                    'eff_days': eff_stock / recip_ads,
                })
            if not demands:
                continue

            for donor in cold:
                if donor['_dead']:
                    # No demand means no service to protect: the safety stock
                    # for this line is ADS x horizon = 0. Withholding half of it
                    # would contradict the objective outright -- measured, the
                    # release fraction alone capped dead-stock recovery at
                    # exactly 49.7%.
                    pool = booked.available(donor['org_cd'], itm, donor['donor_excess'])
                else:
                    # Still trading on it. Same protected pool PULL draws from,
                    # and the same shared ledger, so the two passes cannot
                    # release the same units twice.
                    pool = self._donor_pool(item_coverage, booked,
                                            donor['org_cd'], itm)
                if pool < 1:
                    continue
                # and the move must be worth making: recovering less trapped
                # capital than the lorry costs is not a saving. Derived from the
                # real transfer cost, not a fixed floor.
                unit_cost = float(donor.get('cost_price') or 0)
                if unit_cost > 0 and pool * unit_cost < self.transfer_cost_kes:
                    continue
                # which capacity applies depends on what this donor is
                need_key = 'need_dead' if donor['_dead'] else 'need_over'
                active = [d for d in demands
                          if d[need_key] >= 1
                          and d['recip']['org_cd'] != donor['org_cd']]
                if not active:
                    continue
                total_w = sum(d['weight'] for d in active) or 1.0

                def _give(d, units, pool_left, _k=need_key):
                    xfer = _round_transfer_qty(units, donor['department'])
                    # Same ceiling trap as PULL, one unit up: a pool of 1.4
                    # rounds to 2 and overdraws the donor by 0.6. The `pool < 1`
                    # gate above only stops the sub-unit case.
                    xfer = min(xfer, _releasable_transfer_qty(
                        pool_left, donor['department']))
                    if xfer < 1:
                        return 0.0
                    r = d['recip']
                    booked.book(donor['org_cd'], itm, xfer)
                    recip_booked[(r['org_cd'], itm)] = \
                        recip_booked.get((r['org_cd'], itm), 0.0) + xfer
                    # both capacities shrink: units received are units held,
                    # whichever door they came through
                    d['need_dead'] = max(0.0, d['need_dead'] - xfer)
                    d['need_over'] = max(0.0, d['need_over'] - xfer)
                    push_opps.append(TransferOpportunity(
                        type="PUSH",
                        itm_cd=itm,
                        product_name=donor['product_name'],
                        from_org=donor['org_cd'],
                        to_org=r['org_cd'],
                        transfer_qty=xfer,
                        donor_days_cover=round(donor['days_cover'], 1),
                        recipient_days_cover=round(d['eff_days'], 1),
                        donor_excess=round(donor['donor_excess'], 1),
                        value_kes=round(xfer * donor['sell_price'], 0),
                        department=donor['department'],
                        supplier=donor['supplier'],
                        uom=donor['uom'],
                        is_fresh=donor['is_fresh'],
                        manual_only=donor['is_fresh'],
                    ))
                    return xfer

                for d in sorted(active, key=lambda x: (-x['weight'],
                                                       x['recip']['org_cd'])):
                    if pool < 1:
                        break
                    pool -= _give(d, min(d[need_key],
                                         pool * d['weight'] / total_w), pool)
                # remainder to whoever can still take it, heaviest first —
                # otherwise rounding strands stock the donor was willing to give
                if pool >= 1:
                    for d in sorted(active, key=lambda x: (-x['weight'],
                                                           x['recip']['org_cd'])):
                        if pool < 1 or d[need_key] < 1:
                            continue
                        pool -= _give(d, min(d[need_key], pool), pool)

        return push_opps

    def optimize_network(self,
                         store_orders: Dict[str, List[dict]],
                         supplier_schedule: Dict[str, bool] = None,
                         pending_orders: Dict[str, Dict[str, dict]] = None,
                         risk_scores: Dict[str, float] = None,
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
            risk_scores: {org_cd: score} from GNN
        
        Returns:
            NetworkPlan with adjusted orders, transfers, and donor additions
        """
        if supplier_schedule is None:
            supplier_schedule = {}
        if pending_orders is None:
            pending_orders = {}
        if risk_scores is None:
            risk_scores = {}

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

                # Real cost price when the engine cached one; the 25%-margin
                # heuristic on selling price is only a last-resort fallback.
                _real_cost = rec.get('cost_price')
                if _real_cost:
                    unit_cost = float(_real_cost)
                else:
                    unit_cost = float(rec.get('selling_price', 0) or 0) * 0.75

                all_shortfalls.append({
                    'itm_cd': itm_cd,
                    'product_name': rec.get('product_name', 'Unknown'),
                    'recipient_org': org_cd,
                    'shortfall_qty': qty,
                    'is_ordering_day': is_ordering_day,
                    'lead_time_days': float(rec.get('estimated_delivery_days', 3) or 3),
                    'unit_cost': unit_cost,
                    'is_fresh': rec.get('is_fresh', False),
                    'original_rec': rec,  # keep reference
                    # Pending-order awareness fields
                    'pending_order_qty': pending_qty,
                    'pending_order_eta_days': pending_eta,
                    'current_stock': float(rec.get('current_stocks', rec.get('current_stock', 0)) or 0),
                    'avg_daily_sales': float(rec.get('avg_daily_sales', 0) or 0),
                })

        # Second pass: Inject suppressed demand (MOQ-failed or <3 days cover) that the PO engine skipped
        existing_shortfall_keys = {(sf['recipient_org'], sf['itm_cd']) for sf in all_shortfalls}
        
        for itm_cd, states in self.network_map._index.items():
            for state in states:
                key = (state.org_cd, state.itm_cd)
                if key in existing_shortfall_keys:
                    continue  # Already tracking this shortfall
                    
                days_cover = (state.current_stock / state.avg_daily_sales) if state.avg_daily_sales > 0 else 999.0
                
                # If cover is critically low (<3 days) and there's ADS, create a shortfall!
                # Skip fresh items — they should NOT be auto-transferred
                if days_cover < 3.0 and state.avg_daily_sales > 0 and not state.is_fresh and not _is_fresh_department(state.department):
                    gap_qty = max(1.0, (3.0 - days_cover) * state.avg_daily_sales)
                    
                    po_info = pending_orders.get(state.org_cd, {}).get(state.itm_cd, {})
                    pending_qty = float(po_info.get('qty', 0))
                    
                    if pending_qty < gap_qty:
                        all_shortfalls.append({
                            'itm_cd': state.itm_cd,
                            'product_name': state.product_name,
                            'recipient_org': state.org_cd,
                            'shortfall_qty': gap_qty - pending_qty,
                            'is_ordering_day': False, # Treat as non-ordering day to force transfer preference
                            'lead_time_days': 3.0,
                            'unit_cost': state.sell_price * 0.75,
                            'is_fresh': state.is_fresh,
                            'original_rec': {
                                'department': state.department,
                                'reasoning': f"[RESCUED SHORTFALL] <3 days cover ({days_cover:.1f}d), bypassed by PO engine."
                            },
                            'pending_order_qty': pending_qty,
                            'pending_order_eta_days': float(po_info.get('eta_days', 999.0)),
                            'current_stock': state.current_stock,
                            'avg_daily_sales': state.avg_daily_sales,
                        })

        logger.info(f"Network optimization: {len(all_shortfalls)} shortfalls across "
                    f"{len(store_orders)} stores")

        # Step 2: Run fulfillment decisions
        decisions = self.decider.decide_batch(
            all_shortfalls,
            self.network_map,
            org_names=self.org_names,
            risk_scores=risk_scores,
        )
        plan.decisions = decisions

        # Step 3: Identify proactive transfers (Dead Stock rebalancing)
        proactive_transfers = self._identify_proactive_transfers(risk_scores)
        plan.transfers.extend(proactive_transfers)
        for t in proactive_transfers:
            self.tracker.register_transfer(t)
            plan.total_items_transferred += 1
            plan.total_units_transferred += t.qty

        # Step 4: Build adjusted orders + transfer list + donor additions
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
                    qty=_round_transfer_qty(decision.transfer_qty, original_rec.get('department', '')),
                    department=original_rec.get('department', ''),
                    urgency="HIGH" if decision.decision == "BOTH" else "MEDIUM",
                    cost_kes=self.transfer_cost_kes,
                )
                self.tracker.register_transfer(transfer)
                plan.transfers.append(transfer)
                plan.total_items_transferred += 1
                plan.total_units_transferred += decision.transfer_qty

                # Adjust recipient's order qty
                if org_cd not in plan.adjusted_orders:
                    plan.adjusted_orders[org_cd] = []
                    
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

        if self.registry_path:
            self.tracker.save_to_file(self.registry_path)

        return plan

    def _identify_proactive_transfers(self, risk_scores: Dict[str, float]) -> List[TransferRecord]:
        """Move idle capital to nodes that will sell it — via the PUSH pass.

        This used to run a SEPARATE engine, ``ProactiveRebalancer``, over the
        same stock for the same purpose, holding its own view of every rule:

            donor        cover > 60d                vs  dead, or past its
                                                        category's threshold
            protection   stock - safety x 2         vs  full for dead,
                                                        release fraction otherwise
            recipient    cover < 14d                vs  any ACTIVE store
            fill to      30 days                    vs  category threshold, or
                                                        the relief horizon
            bookkeeping  mutated current_stock      vs  the shared ledger

        Four constants and a private ledger, all disagreeing with the pass that
        does the same job on the other entry point. An operator running Smart
        Ordering and the Transfer Intelligence tab got two different answers
        about the same stock, and neither number came from the derived
        thresholds LATA and AMIT supply.

        Now there is one implementation. ``risk_scores`` is accepted for the
        caller's signature and unused: PUSH ranks on velocity x margin, which is
        measured, where the GNN score is a prior — see the risk-scoring review.
        """
        outbound, inbound = self._pending_flows([])
        item_coverage = self._coverage_index(outbound, inbound)
        opportunities = self._push_opportunities(
            item_coverage, self.donor_ledger, {})

        transfers = []
        for o in opportunities:
            # fresh lines are surfaced for a human to dispatch, never queued
            # automatically — the same rule the scan applies
            if o.manual_only:
                continue
            transfers.append(TransferRecord(
                from_org=o.from_org,
                to_org=o.to_org,
                itm_cd=o.itm_cd,
                product_name=o.product_name,
                qty=o.transfer_qty,
                department=o.department,
                urgency="LOW",
                cost_kes=self.transfer_cost_kes,
            ))
        return transfers

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
