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
    FulfillmentDecider,
    FulfillmentDecision,
    NetworkAvailabilityMap,
    StoreSkuState,
    _round_transfer_qty,
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
                 hot_node_days: int = 14):
        """
        Args:
            org_names: {org_cd: org_name} for all stores
            stock_data: {org_cd: [product dicts]} — current stock per store
            transfer_cost_kes: Fixed logistics cost per transfer
            min_shortfall_qty: Minimum shortfall to consider for transfer
            registry_path: Path to persistent transfer registry file
            distance_map: Optional {org_cd: {lat: L, lon: L}} for distance-aware selection
        """
        self.org_names = org_names
        self.stock_data = stock_data
        self.transfer_cost_kes = transfer_cost_kes
        self.min_shortfall_qty = min_shortfall_qty
        self.registry_path = registry_path
        self.distance_map = distance_map or {}
        self.cold_node_days = cold_node_days
        self.hot_node_days = hot_node_days

        self.tracker = TransferStateTracker()
        if self.registry_path:
            self.tracker.load_from_file(self.registry_path)

        # Detect warehouse hubs from distance_map (entries with is_warehouse_hub=True)
        warehouse_hubs = [
            org_cd for org_cd, info in self.distance_map.items()
            if isinstance(info, dict) and info.get('is_warehouse_hub', False)
        ]

        self.decider = FulfillmentDecider(
            transfer_cost_kes=transfer_cost_kes,
            distance_map=self.distance_map,
            warehouse_hubs=warehouse_hubs,
        )

        # Build network availability map from stock data
        self.network_map = self._build_network_map()

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
            for p in products:
                ads = float(p.get('avg_daily_sales', 0) or 0)
                current = float(p.get('current_stocks', 0) or 0)
                days_cover = (current / ads) if ads > 0 else 999.0
                dept = str(p.get('department', p.get('product_category', 'GENERAL'))).upper()
                is_fresh = any(k in dept for k in [
                    'MILK', 'DAIRY', 'FRESH', 'MEAT', 'BREAD', 'BAKERY'
                ]) or p.get('is_fresh', False)

                safety = ads * 14.0  # 14 days cover as minimum safety stock
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

    @staticmethod
    def _excess_units(ads: float, stock: float, is_fresh: bool) -> float:
        """Donor excess above safety stock — single definition for PULL and PUSH.

        Mirrors _build_network_map(): 14-day safety floor, overstock gate at
        14d (fresh) / 30d (dry), and a 7-day buffer above safety before any
        units count as excess. Zero-ADS items with stock are fully excess.
        """
        if ads > 0:
            days_cover = stock / ads
            safety = ads * 14.0
            overstock_threshold = 14.0 if is_fresh else 30.0
            if days_cover > overstock_threshold and (stock - safety) > (ads * 7.0):
                return stock - safety
            return 0.0
        return stock if stock > 0 else 0.0

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

    def scan_network_opportunities(self,
                                   moq_failures: Optional[Dict[str, set]] = None,
                                   pending_transfers: Optional[List[dict]] = None,
                                   pull_deficit_days: float = 7.0,
                                   max_pull_per_store: int = 50,
                                   max_push: int = 200,
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
            max_pull_per_store: cap on deficit items evaluated per store.
            max_push: cap on PUSH opportunities (highest value first).
        """
        moq_failures = moq_failures or {}
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
                ads = float(p.get('avg_daily_sales', 0) or 0)
                stock = float(p.get('current_stocks', p.get('current_stock', 0)) or 0)
                dept = str(p.get('department', p.get('product_category', 'GENERAL'))).upper()
                fresh = _fresh(p, dept)

                # Donor view: outbound commitments reduce what we can give.
                donor_stock = max(0.0, stock - outbound.get((org_cd, itm), 0.0))
                donor_excess = self._excess_units(ads, donor_stock, fresh)
                if donor_excess > 0:
                    n_excess += 1

                # Recipient view: inbound commitments count as supply.
                eff_stock = donor_stock + inbound.get((org_cd, itm), 0.0)
                days_cover = (eff_stock / ads) if ads > 0 else 999.0

                entry = {
                    'itm_cd': itm,
                    'product_name': str(p.get('product_name', '')),
                    'org_cd': org_cd,
                    'current_stock': eff_stock,
                    'avg_daily_sales': ads,
                    'days_cover': days_cover,
                    'donor_excess': donor_excess,
                    'sell_price': float(p.get('selling_price', p.get('sell_price', 0)) or 0),
                    'department': dept,
                    'supplier': str(p.get('supplier_name', '') or ''),
                    'uom': str(p.get('uom', 'EA')).upper(),
                    'is_fresh': fresh,
                }
                item_coverage.setdefault(itm, {})[org_cd] = entry

                rop = float(p.get('reorder_point', 0) or 0)
                org_moq = moq_failures.get(org_cd) or {}
                pull_trigger = (
                    (ads > 0 and (days_cover < pull_deficit_days or eff_stock <= rop))
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

            store_deficits[org_cd] = deficits
            result.store_stats[org_cd] = {
                'total_skus': len(products),
                'overstock': n_excess,
                'deficits': len(deficits),
                'push_from': 0,
            }

        # ── Pass 2: PULL — find donors for deficit items ──
        # booked tracks intra-scan donor allocations so one donor's excess is
        # not promised to several recipients; recip_booked tracks intra-scan
        # inbound so PUSH does not stack on top of a PULL already made.
        booked: Dict[tuple, float] = {}
        recip_booked: Dict[tuple, float] = {}

        for rec_org, deficits in store_deficits.items():
            for item in deficits[:max_pull_per_store]:
                itm = item['itm_cd']
                ads = item['avg_daily_sales']
                target_qty = max(ads * pull_deficit_days, 1.0) if ads > 0 else 2.0
                shortfall = max(0.0, target_qty - item['current_stock'],
                                item.get('moq_qty', 0.0))
                if shortfall < 0.1:
                    continue

                donors = self.network_map.find_donors(
                    itm, rec_org,
                    product_name=item['product_name'],
                    distance_calc=self.decider._calculate_distance_km,
                    warehouse_hubs=self.decider.warehouse_hubs,
                )
                best = None
                avail = 0.0
                for d in donors:
                    cov = item_coverage.get(itm, {}).get(d.org_cd)
                    base_excess = cov['donor_excess'] if cov else d.excess
                    avail = base_excess - booked.get((d.org_cd, itm), 0.0)
                    if avail > 0:
                        best = d
                        break
                if best is None:
                    continue

                xfer = _round_transfer_qty(min(avail * 0.5, shortfall), item['department'])
                if xfer < 1:
                    continue
                booked[(best.org_cd, itm)] = booked.get((best.org_cd, itm), 0.0) + xfer
                recip_booked[(rec_org, itm)] = recip_booked.get((rec_org, itm), 0.0) + xfer

                donor_cov = item_coverage.get(itm, {}).get(best.org_cd, {})
                result.opportunities.append(TransferOpportunity(
                    type="PULL",
                    itm_cd=itm,
                    product_name=item['product_name'],
                    from_org=best.org_cd,
                    to_org=rec_org,
                    transfer_qty=xfer,
                    donor_days_cover=round(donor_cov.get('days_cover', 999.0), 1),
                    recipient_days_cover=round(item['days_cover'], 1),
                    donor_excess=round(avail, 1),
                    value_kes=round(xfer * item['sell_price'], 0),
                    department=item['department'],
                    supplier=item['supplier'],
                    uom=item['uom'],
                    is_fresh=item['is_fresh'],
                    manual_only=item['is_fresh'],
                ))

        # ── Pass 3: PUSH — cold nodes (dead capital) → hot nodes ──
        push_opps: List[TransferOpportunity] = []
        for itm, cov_map in item_coverage.items():
            cold = [c for c in cov_map.values()
                    if c['days_cover'] > self.cold_node_days and c['donor_excess'] > 0]
            hot = [c for c in cov_map.values() if c['days_cover'] < self.hot_node_days]
            if not cold or not hot:
                continue
            for donor in cold:
                for recip in hot:
                    if donor['org_cd'] == recip['org_cd']:
                        continue
                    avail = donor['donor_excess'] - booked.get((donor['org_cd'], itm), 0.0)
                    if avail <= 0:
                        continue
                    # Recompute recipient cover including units already booked
                    # to it earlier in this scan (PULL or a previous PUSH).
                    recip_in = recip_booked.get((recip['org_cd'], itm), 0.0)
                    eff_recip_stock = recip['current_stock'] + recip_in
                    recip_ads = max(recip['avg_daily_sales'], 0.5)
                    eff_days = eff_recip_stock / recip_ads
                    if eff_days >= self.hot_node_days:
                        continue  # already topped up within this scan
                    need = max(1.0, (self.hot_node_days - eff_days) * recip_ads)
                    xfer = _round_transfer_qty(min(avail * 0.4, need), donor['department'])
                    if xfer < 1:
                        continue
                    booked[(donor['org_cd'], itm)] = booked.get((donor['org_cd'], itm), 0.0) + xfer
                    recip_booked[(recip['org_cd'], itm)] = recip_in + xfer
                    push_opps.append(TransferOpportunity(
                        type="PUSH",
                        itm_cd=itm,
                        product_name=donor['product_name'],
                        from_org=donor['org_cd'],
                        to_org=recip['org_cd'],
                        transfer_qty=xfer,
                        donor_days_cover=round(donor['days_cover'], 1),
                        recipient_days_cover=round(eff_days, 1),
                        donor_excess=round(avail, 1),
                        value_kes=round(xfer * donor['sell_price'], 0),
                        department=donor['department'],
                        supplier=donor['supplier'],
                        uom=donor['uom'],
                        is_fresh=donor['is_fresh'],
                        manual_only=donor['is_fresh'],
                    ))

        push_opps.sort(key=lambda o: -o.value_kes)
        for o in push_opps[:max_push]:
            result.store_stats.setdefault(o.from_org, {}).setdefault('push_from', 0)
            result.store_stats[o.from_org]['push_from'] += 1
        result.opportunities.extend(push_opps[:max_push])

        result.opportunities.sort(key=lambda o: -o.value_kes)
        logger.info(
            "Network scan: %d opportunities (%d pull / %d push), "
            "pending committed: %.0f out / %.0f in",
            len(result.opportunities),
            sum(1 for o in result.opportunities if o.type == "PULL"),
            sum(1 for o in result.opportunities if o.type == "PUSH"),
            result.pending_outbound_units, result.pending_inbound_units,
        )
        return result

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
        """
        Identify 'Dead Stock' to move to stores with 'Demand Spikes' or low stock.
        Proactive rebalancing doesn't wait for a shortfall PO.
        """
        from .fulfillment_decider import ProactiveRebalancer
        
        rebalancer = ProactiveRebalancer(
            cold_node_days=getattr(self, 'cold_node_days', 60),
            hot_node_days=getattr(self, 'hot_node_days', 14)
        )
        decisions = rebalancer.find_proactive_transfers(self.network_map)
        
        transfers = []
        for d in decisions:
            transfers.append(TransferRecord(
                from_org=d.donor_org,
                to_org=d.recipient_org,
                itm_cd=d.itm_cd,
                product_name=d.product_name,
                qty=d.transfer_qty,
                department=getattr(d, 'department', 'GENERAL'),
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
