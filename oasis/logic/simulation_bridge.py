import sys
import os
import hashlib
from datetime import datetime
from typing import List, Dict, Any

# Ensure we can import from the sibling modules
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine, apply_safety_guards
from oasis.data.supplier_calendar import SupplierCalendar


def _supplier_phase_offset(supplier: str, gap_days: int) -> int:
    """Deterministic per-supplier phase offset in [0, gap_days).

    A3 fix: the old fallback `current_day % gap_days == 0` landed every
    supplier sharing a gap on the same days, producing synchronized order
    spikes (and a year-end discontinuity). Hashing the supplier name spreads
    suppliers evenly across the cycle while keeping each supplier's days
    stable run-to-run. md5 (not Python's salted hash()) guarantees the
    offset is identical across processes.
    """
    if gap_days <= 1:
        return 0
    digest = hashlib.md5(str(supplier).encode("utf-8")).hexdigest()
    return int(digest, 16) % gap_days


def _find_calendar_path(data_dir: str) -> str:
    """Discover Supplier_Order_Calendar file from data_dir or cwd."""
    candidates = [
        os.path.join(data_dir, "..", "Supplier_Order_Calendar_2026.xlsx"),
        os.path.join(os.getcwd(), "Supplier_Order_Calendar_2026.xlsx"),
        os.path.join(data_dir, "Supplier_Order_Calendar_2026.xlsx"),
    ]
    for p in candidates:
        if os.path.exists(p):
            return os.path.abspath(p)
    return candidates[1]  # Fallback to cwd


class SimulationOrderUtil:
    """
    Bridge to use Oasis OrderEngine logic within a high-speed simulation.
    Replaces LLM calls with deterministic Python logic derived from the AI prompts.
    """
    
    def __init__(self, data_dir: str, thresholds: Dict[str, Any] = None, engine: OrderEngine = None):
        self.data_dir = data_dir
        if engine is not None:
            self.engine = engine
        else:
            self.engine = OrderEngine(data_dir)
            # Synchronous load for simulation speed
            self.engine.load_local_databases()
        
        # Calendar Integration (G2 Fix: relative path discovery)
        cal_path = _find_calendar_path(data_dir)
        self.calendar = SupplierCalendar(cal_path)
        self.calendar_loaded = False

        # F3: LATA Supplier Shield — per-supplier lead-time-variance safety
        # multipliers from supplier_patterns (written by run_lata). Unreliable
        # suppliers (>30% LT variance) inflate the buffer up to 2.0x; rock-solid
        # ones trim it toward 0.8x. Missing file/entry → neutral 1.0.
        self._lata_multipliers = self._load_lata_multipliers(data_dir)

        # F4: ROP source gate. 'heuristic' (default) keeps the flat fallback
        # ADS×(LT+safety). 'newsvendor' computes the statistically-correct
        # reorder point (μ_LTD + z·σ_LTD at OASIS_SERVICE_LEVEL) when no stored
        # ROP exists; 'newsvendor-all' also overrides stored ROPs (A/B mode).
        self._rop_mode = os.getenv("OASIS_ROP_MODE", "heuristic").lower().strip()
        self._service_level = float(os.getenv("OASIS_SERVICE_LEVEL", "0.95"))

        # G4 Fix: Configurable thresholds (can be overridden from Settings/DB)
        self.thresholds = thresholds or {
            'fresh_stale_days': 120,
            'dry_dead_days': 200,
            'dry_dead_min_sales': 5,
            'key_sku_boost_pct': 0.20,
            'critical_stockout_days': 2.0,
            # Phase C: Minimum Order Threshold to prevent micro-orders
            'min_order_units': 10,
            'min_order_value_kes': 5000,
        }
        
    @staticmethod
    def _load_lata_multipliers(data_dir: str) -> Dict[str, float]:
        """{SUPPLIER_UPPER: lata_variance_multiplier} from supplier_patterns_*.json."""
        import glob
        import json
        import os
        out: Dict[str, float] = {}
        try:
            candidates = sorted(glob.glob(os.path.join(data_dir, "supplier_patterns_*.json")),
                                reverse=True)
            if not candidates:
                return out
            with open(candidates[0], "r", encoding="utf-8") as f:
                patterns = json.load(f)
            for supplier, d in (patterns or {}).items():
                if isinstance(d, dict) and d.get("lata_variance_multiplier") is not None:
                    try:
                        out[str(supplier).upper().strip()] = float(d["lata_variance_multiplier"])
                    except (TypeError, ValueError):
                        continue
        except Exception:
            return {}
        return out

    def prepare_sku_data(self, sku_list: List[Dict[str, Any]], skip_enrichment: bool = False) -> List[Dict[str, Any]]:
        """
        Enrich raw SKU data using Oasis Intelligence.
        
        Performance: In simulation context (reordering), SKU data is already enriched
        from initialization. skip_enrichment=True bypasses the heavy 23k-item enrichment
        pipeline which was the #1 performance bottleneck (~10+ min per reorder cycle).
        """
        if skip_enrichment:
            # SKUs are already enriched — just ensure minimum required fields exist
            for sku in sku_list:
                if 'target_coverage_days' not in sku:
                    sku['target_coverage_days'] = 7 if not sku.get('is_fresh') else 2
                if 'reorder_point' not in sku:
                    ads = float(sku.get('avg_daily_sales', 0))
                    sku['reorder_point'] = ads * sku.get('target_coverage_days', 7)
                if 'safety_stock' not in sku:
                    sku['safety_stock'] = float(sku.get('avg_daily_sales', 0)) * 2
                if 'estimated_delivery_days' not in sku:
                    sku['estimated_delivery_days'] = 7
            return sku_list
        return self.engine.enrich_product_data(sku_list)

        
    def calculate_order_quantity(self, enriched_skus: List[Dict[str, Any]], 
                                 store_config: Dict[str, Any] = None,
                                 current_day: int = 1,
                                 gnn_risk_score: float = 0.0,
                                 use_real_date: bool = False) -> List[Dict[str, Any]]:
        """
        Deterministic implementation of the "AI Prompt Logic" for Replenishment.
        
        Rules ported from `analyze_batch_ai` prompt:
        1. Slow Mover Checks (Dead Stock > 200d)
        2. Freshness Checks (Stale Fresh > 120d)
        3. Net Requirement Calculation: (Target - Current - OnOrder)
        4. Key SKU Boost (Top 500 get 20% buffer)
        
        Args:
            use_real_date: If True, use today's actual weekday for schedule checks
                           instead of the simulation day counter. Use True for
                           dashboard/scheduler context, False for simulation.
        """
        # G2 Fix: Map to real calendar day when in dashboard context
        if use_real_date:
            current_day = datetime.today().timetuple().tm_yday  # Day-of-year (1-366)
        
        # Load Chapter 11 Engine Caches for daily replenishment enforcement
        amit_enabled = self.engine.is_engine_enabled('amit')
        amit_blacklist = self.engine.databases.get('amit_enforcement', set()) if amit_enabled else set()
        
        mande_enabled = self.engine.is_engine_enabled('mande')
        mande_purge = self.engine.databases.get('mande_purge_list', set()) if mande_enabled else set()
        
        halo_list = self.engine.databases.get('halo_protection_list', set())
        
        recommendations = []
        
        for p in enriched_skus:
            # Create a recommendation object
            rec = p.copy()
            rec['recommended_quantity'] = 0
            rec['reasoning'] = ""
            
            # --- CHAPTER 11 ENFORCEMENT ---
            p_name = p.get('product_name', 'Unknown')
            p_name_norm = self.engine.normalize_product_name(p_name)
            
            # HALO Affinity Protection Check
            is_halo_protected = False
            if p_name_norm in halo_list:
                is_halo_protected = True
                p['is_key_sku'] = True
                p['is_top_sku'] = True
                rec['is_key_sku'] = True
                rec['is_top_sku'] = True
                rec['reasoning'] += " [HALO Protected]"

            # AMIT Blacklist Check (Low GMROI delisting)
            if amit_enabled and p_name_norm in amit_blacklist:
                rec['recommended_quantity'] = 0
                rec['reasoning'] = "Blocked: AMIT Blacklist (Low GMROI / Stranded Capital)"
                recommendations.append(rec)
                continue
                
            # MANDE Supplier Purge Check (Delisted Supplier Capital Trap)
            supplier_upper = str(p.get('supplier_name', '')).upper().strip()
            is_staple = p.get('is_staple', False) or p.get('is_top_sku', False) or (p.get('sales_rank', 999) < 500)
            is_essential = p.get('is_fresh', False) or any(x in p_name_norm for x in ['SUGAR', 'SALT', 'FLOUR', 'RICE', 'COOKING OIL', 'FRESH MILK', 'BREAD', 'EGGS'])
            
            if mande_enabled and supplier_upper in mande_purge and not (is_staple or is_essential):
                rec['recommended_quantity'] = 0
                rec['reasoning'] = "Blocked: MANDE Supplier Purge (Delisted Capital Trap)"
                recommendations.append(rec)
                continue

            # 1. DETERMINE IF WE CAN ORDER TODAY
            supplier = p.get('supplier_name', 'Unknown')
            gap_days = int(p.get('median_gap_days', 7))
            if gap_days < 1: gap_days = 1
            
            # CALENDAR CHECK
            if not self.calendar_loaded:
                 self.calendar.load()
                 self.calendar_loaded = True

            schedule = self.calendar.get_schedule(supplier)
            is_ordering_day = False
            
            if schedule == 'DAILY':
                is_ordering_day = True
            elif isinstance(schedule, set):
                is_ordering_day = current_day in schedule
            else:
                # A3: phase-staggered fallback. Each supplier gets a stable
                # offset so same-gap suppliers spread across the cycle instead
                # of all firing on the same day. Day 1 stays an ordering day
                # for every supplier (greenfield / first-run priming).
                offset = _supplier_phase_offset(supplier, gap_days)
                is_ordering_day = ((current_day + offset) % gap_days == 0) or (current_day == 1)
            
            # Check Critical Status (for Override)
            current_stock = p.get('current_stock', p.get('current_stocks', 0))
            avg_daily_sales = p.get('avg_daily_sales', 0)
            days_coverage = current_stock / avg_daily_sales if avg_daily_sales > 0 else 999
            
            # Critical Threshold: Dynamic based on Lead Time
            # We must order when Stock < Lead Time + Safety Buffer
            lead_time = p.get('lead_time_days', 1) or 1
            
            # --- DYNAMIC VOLATILITY BUFFERING (Unified) ---
            # Base Safety: 4.0 for Fresh (Robust), 1.5 for Dry
            cv = p.get('demand_cv', 0.2)
            base_safety = 4.0 if p.get('is_fresh', False) else 1.5
            
            # Volatility Factor: 2.0x penalty for variance
            vol_factor = 2.0
            
            # GNN Risk Multiplier: Increase safety stock if network predicts high vulnerability
            gnn_multiplier = 1.0
            if gnn_risk_score > 0.5:
                # scale from 1.0 at risk 0.5 to 1.3 at risk 1.0
                gnn_multiplier = 1.0 + ((gnn_risk_score - 0.5) * 0.6)

            # F3: LATA Supplier Shield — lead-time-variance multiplier for this
            # SKU's supplier (unreliable suppliers need deeper safety stock).
            lata_multiplier = self._lata_multipliers.get(supplier_upper, 1.0)

            safety_buffer = (base_safety * (1 + (vol_factor * cv))
                             * gnn_multiplier * lata_multiplier)

            critical_thresh = lead_time + safety_buffer

            if gnn_multiplier > 1.0:
                 rec['reasoning'] += f" [GNN Risk Burst: +{(gnn_multiplier-1.0)*100:.0f}% Safety]"
            if abs(lata_multiplier - 1.0) > 0.05:
                 rec['reasoning'] += f" [LATA Shield: x{lata_multiplier:.2f} supplier variance]"
                 
            is_critical = days_coverage < critical_thresh
            
            if not is_ordering_day and not is_critical:
                 rec['reasoning'] = f" [Schedule: Gap {gap_days}d, Next: Day {current_day + (gap_days - current_day % gap_days)}]"
                 recommendations.append(rec)
                 continue
            
            if is_critical and not is_ordering_day:
                 rec['reasoning'] += " [CRITICAL OVERRIDE: Schedule Bypass]"

            # --- LOGIC PORTED FROM AI PROMPT ---
            
            # 1. SLOW MOVER / FRESHNESS CHECKS
            days_since_delivery = p.get('days_since_delivery', 0)
            is_fresh = p.get('is_fresh', False)
            sales_90d = p.get('total_units_sold_last_90d', 0)

            # 1a. DISCONTINUED: no stock AND nothing sold in 90 days.
            # OPT-IN, default OFF — Golden Logic behaviour is unchanged unless a
            # client sets block_discontinued. Configured like every other
            # threshold here rather than hardcoded, so this is a tuning knob and
            # not a new branch in the default hot path.
            #
            # Why it exists: the dead-stock rule below keys on
            # days_since_delivery, which some POS backends cannot supply (RXL has
            # no SM_LAST_RECV_DT). When that field defaults to 0, `> 200` is never
            # true and the guard is silently inert. This rule holds on stock and
            # sales alone — data every POS has — so it cannot be disabled by a
            # missing column.
            if self.thresholds.get('block_discontinued', False):
                _stock_now = float(p.get('current_stock')
                                   if p.get('current_stock') is not None
                                   else p.get('current_stocks', 0) or 0)
                if _stock_now <= 0 and float(sales_90d or 0) <= 0 and not is_halo_protected:
                    rec['reasoning'] = "Blocked: Discontinued (no stock, no sales in 90d)"
                    recommendations.append(rec)
                    continue
            
            # G4 Fix: Configurable thresholds (can be loaded from DB/Settings)
            fresh_stale_days = self.thresholds.get('fresh_stale_days', 120)
            dry_dead_days = self.thresholds.get('dry_dead_days', 200)
            dry_dead_min_sales = self.thresholds.get('dry_dead_min_sales', 5)
            
            # Fresh Stale Logic
            if is_fresh and days_since_delivery > fresh_stale_days:
                if sales_90d == 0 and not is_halo_protected:
                    rec['reasoning'] = f"Blocked: Stale Fresh (>{fresh_stale_days}d, No Sales)"
                    recommendations.append(rec)
                    continue 
                # Else: Cap logic handled in safety guards, but we can be proactive
            
            # Dry Dead Stock Logic
            if not is_fresh and days_since_delivery > dry_dead_days:
                if sales_90d < dry_dead_min_sales and not is_halo_protected:
                    if sales_90d == 0:
                        rec['reasoning'] = f"Blocked: Dead Stock (>{dry_dead_days}d, No Sales)"
                        recommendations.append(rec)
                        continue

            # 2. NET REQUIREMENT CALCULATION
            # Target Stock = Reorder Point (Coverage Days * Velocity)
            # But we might want to respect the STORE CONFIG for "Safety Days" if provided?
            
            avg_daily_sales = p.get('avg_daily_sales', 0)
            
            # G3 Fix: ROP Fallback — if reorder_point is 0 or missing (no intelligence data),
            # calculate a dynamic fallback instead of treating 0 as real ROP.
            # F4: in newsvendor mode the fallback (or, in 'newsvendor-all', every
            # ROP) is the statistically-correct μ_LTD + z·σ_LTD instead of the
            # flat heuristic — demand-variance-aware at the service level.
            reorder_point = p.get('reorder_point', 0)
            use_newsvendor = (self._rop_mode == "newsvendor-all"
                              or (self._rop_mode == "newsvendor" and reorder_point <= 0))
            if use_newsvendor and avg_daily_sales > 0:
                from math import sqrt

                from . import risk_baseline as RB
                mu_ltd = avg_daily_sales * lead_time
                sigma_ltd = cv * avg_daily_sales * sqrt(max(1.0, lead_time))
                reorder_point = RB.reorder_point(mu_ltd, sigma_ltd,
                                                 self._service_level)
                rec['reasoning'] += (f" [ROP Newsvendor: SL{self._service_level:.0%}]")
            elif reorder_point <= 0 and avg_daily_sales > 0:
                # Fallback ROP = ADS * (lead_time + base_safety)
                fallback_rop = avg_daily_sales * (lead_time + (base_safety * (1 + cv)))
                reorder_point = fallback_rop
                rec['reasoning'] += " [ROP Fallback: intelligence data missing]"
            
            current_stock = p.get('current_stock', p.get('current_stocks', 0))
            on_order = p.get('on_order_qty', 0)
            
            # Check reorder trigger
            if current_stock <= reorder_point:
                # Calculate Target Stock
                target_coverage_days = p.get('target_coverage_days', 7)
                
                # --- CYCLE STOCK CORRECTION & DOUBLE-STACKING FIX ---
                # For fresh items, we trust the highly optimized DDoS precision target computed during enrichment.
                # Daily fresh items (weekly schedule/rhythm <= 1.5d) should be strictly limited to 1.2 days coverage.
                if is_fresh:
                    if target_coverage_days <= 0:
                        target_coverage_days = 1.2
                    rec['reasoning'] += f" [Fresh DDoS Target: {target_coverage_days:.2f}d]"
                else:
                    gap_days = int(p.get('median_gap_days', 7))
                    if gap_days < 1: gap_days = 1
                    lead_time = int(p.get('lead_time_days', 1) or 1)
                    
                    min_cycle_coverage = gap_days + lead_time + safety_buffer
                    target_coverage_days = max(target_coverage_days, min_cycle_coverage)
                    rec['reasoning'] += f" [DDoS Target: {target_coverage_days:.2f}d]"

                target_stock = avg_daily_sales * target_coverage_days
                
                # Net Requirement
                net_req = target_stock - (current_stock + on_order)
                
                if net_req > 0:
                    # 3. KEY SKU BOOST
                    is_top_sku = p.get('is_top_sku', False) or p.get('sales_rank', 999) < 500
                    if is_top_sku:
                        net_req *= 1.20
                        rec['reasoning'] += " [Key SKU Boost +20%]"
                    
                    rec['recommended_quantity'] = net_req
                    rec['reasoning'] += f" [Net Req: {net_req:.1f} (Tgt {target_stock:.1f} - Cur {current_stock} - Ord {on_order})]"
                else:
                     rec['reasoning'] += " [Adequate Coverage]"
            else:
                 rec['reasoning'] = f" [Above ROP {reorder_point:.1f}]"

            recommendations.append(rec)
            
        return recommendations

    def finalize_orders(self, recommendations: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Apply strict safety guards (Rounding, Caps, Etc.)
        """
        # Need to rebuild product map for guards
        products_map = {r['product_name']: r for r in recommendations}
        return apply_safety_guards(recommendations, products_map, allocation_mode="replenishment")

    def apply_minimum_order_gate(self, finalized_recs: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Two-stage Minimum Order Gate.
        
        Stage 1: SKU-level MOQ/MOP screening.
                 Filters out individual items that do not meet the minimum SKU-level
                 order quantity or price. Routes them to transfers (TRANSFER_FIRST).
                 
        Stage 2: Supplier-level MOT screening.
                 Groups remaining items by supplier and verifies if the entire PO meets
                 the supplier-level unit or value threshold. Below-threshold suppliers
                 are routed to transfers (TRANSFER_FIRST).
                 
        Returns:
            {
                'po_recs': [items passing both stages — keep on PO],
                'transfer_recs': [items routed to transfers — Stage 1 or 2],
                'no_order': [items with quantity <= 0],
                'supplier_summary': {supplier: {units, value, status, item_count}}
            }
        """
        min_units = self.thresholds.get('min_order_units', 10)
        min_value = self.thresholds.get('min_order_value_kes', 5000)
        
        po_candidate_recs = []
        transfer_recs = []
        
        # --- STAGE 1: SKU-level MOQ/MOP gate ---
        for rec in finalized_recs:
            qty = rec.get('recommended_quantity', 0)
            if qty <= 0:
                continue
                
            is_fresh = rec.get('is_fresh', False)
            pack_size = max(1, int(rec.get('pack_size', 1)))
            cost_price = float(rec.get('cost_price', rec.get('sell_price', rec.get('selling_price', 0))))
            
            # Determine Item MOQ (Minimum Order Quantity)
            # Default fallback MOQ: pack_size for fresh, max(pack_size, 5) for dry
            db_moq = rec.get('moq_floor', 0)
            if db_moq > 0:
                item_moq = max(pack_size, db_moq)
            else:
                item_moq = pack_size if is_fresh else max(pack_size, 2)
                
            # Determine Item MOP (Minimum Order Price / Order Value)
            # Default fallback MOP: KES 200 for fresh, KES 100 for dry
            item_mop = 200.0 if is_fresh else 100.0
            
            # Evaluate individual item eligibility
            order_val = qty * cost_price
            meets_moq = qty >= item_moq
            meets_mop = order_val >= item_mop
            
            if not meets_moq or not meets_mop:
                # Routed to transfers at Stage 1 (SKU Gate)
                rec['fulfillment'] = 'TRANSFER_FIRST'
                fail_reason = []
                if not meets_moq:
                    fail_reason.append(f"{qty:.0f} units < MOQ {item_moq}")
                if not meets_mop:
                    fail_reason.append(f"value KES {order_val:,.0f} < MOP KES {item_mop:.0f}")
                
                rec['reasoning'] = (
                    rec.get('reasoning', '') +
                    f" [Item MOQ/MOP Gate: {', '.join(fail_reason)} — routed to transfer]"
                )
                transfer_recs.append(rec)
            else:
                # Eligible for PO grouping
                po_candidate_recs.append(rec)
                
        # --- STAGE 2: Supplier-level MOT gate ---
        supplier_groups: Dict[str, List[Dict[str, Any]]] = {}
        for rec in po_candidate_recs:
            supplier = rec.get('supplier_name', rec.get('supplier', 'UNKNOWN'))
            if supplier not in supplier_groups:
                supplier_groups[supplier] = []
            supplier_groups[supplier].append(rec)
            
        po_recs = []
        supplier_summary = {}
        
        for supplier, recs in supplier_groups.items():
            total_units = sum(r.get('recommended_quantity', 0) for r in recs)
            total_value = sum(
                r.get('recommended_quantity', 0) * r.get('cost_price', r.get('sell_price', r.get('selling_price', 0)))
                for r in recs
            )
            
            meets_units = total_units >= min_units
            meets_value = total_value >= min_value
            
            if meets_units or meets_value:
                # Meets supplier MOT — remains on PO
                for r in recs:
                    r['fulfillment'] = 'SUPPLIER_PO'
                po_recs.extend(recs)
                supplier_summary[supplier] = {
                    'units': total_units, 'value': total_value,
                    'status': 'PO', 'item_count': len(recs)
                }
            else:
                # Fails supplier MOT — routed to transfer at Stage 2
                for r in recs:
                    r['fulfillment'] = 'TRANSFER_FIRST'
                    r['reasoning'] = (
                        r.get('reasoning', '') +
                        f" [Below MOT: {total_units:.0f} units / KES {total_value:,.0f}"
                        f" — threshold {min_units} units or KES {min_value:,.0f}"
                        f" — routed to transfer]"
                    )
                transfer_recs.extend(recs)
                supplier_summary[supplier] = {
                    'units': total_units, 'value': total_value,
                    'status': 'TRANSFER', 'item_count': len(recs)
                }
                
        # Items with 0 quantity (no order needed) pass through unchanged
        no_order = [r for r in finalized_recs if r.get('recommended_quantity', 0) <= 0]
        
        return {
            'po_recs': po_recs,
            'transfer_recs': transfer_recs,
            'no_order': no_order,
            'supplier_summary': supplier_summary,
        }
