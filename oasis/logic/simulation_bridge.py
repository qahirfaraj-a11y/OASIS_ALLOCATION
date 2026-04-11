import sys
import os
from datetime import datetime
from typing import List, Dict, Any

# Ensure we can import from the sibling modules
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine, apply_safety_guards
from oasis.data.supplier_calendar import SupplierCalendar


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
    
    def __init__(self, data_dir: str, thresholds: Dict[str, Any] = None):
        self.data_dir = data_dir
        self.engine = OrderEngine(data_dir)
        # Synchronous load for simulation speed
        self.engine.load_local_databases()
        
        # Calendar Integration (G2 Fix: relative path discovery)
        cal_path = _find_calendar_path(data_dir)
        self.calendar = SupplierCalendar(cal_path)
        self.calendar_loaded = False
        
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
        
    def prepare_sku_data(self, sku_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich raw SKU data using Oasis Intelligence.
        """
        # Oasis expects a specific format, ensure mapping
        # sku_list should be list of dicts with 'product_name', 'barcode', etc.
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
        
        recommendations = []
        
        for p in enriched_skus:
            # Create a recommendation object
            rec = p.copy()
            rec['recommended_quantity'] = 0
            rec['reasoning'] = ""
            
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
                # Fallback to heuristic
                is_ordering_day = (current_day % gap_days == 0) or (current_day == 1)
            
            # Check Critical Status (for Override)
            current_stock = p.get('current_stock', 0)
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
                
            safety_buffer = base_safety * (1 + (vol_factor * cv)) * gnn_multiplier
            
            critical_thresh = lead_time + safety_buffer
            
            if gnn_multiplier > 1.0:
                 rec['reasoning'] += f" [GNN Risk Burst: +{(gnn_multiplier-1.0)*100:.0f}% Safety]"
                 
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
            
            # G4 Fix: Configurable thresholds (can be loaded from DB/Settings)
            fresh_stale_days = self.thresholds.get('fresh_stale_days', 120)
            dry_dead_days = self.thresholds.get('dry_dead_days', 200)
            dry_dead_min_sales = self.thresholds.get('dry_dead_min_sales', 5)
            
            # Fresh Stale Logic
            if is_fresh and days_since_delivery > fresh_stale_days:
                if sales_90d == 0:
                    rec['reasoning'] = f"Blocked: Stale Fresh (>{fresh_stale_days}d, No Sales)"
                    recommendations.append(rec)
                    continue 
                # Else: Cap logic handled in safety guards, but we can be proactive
            
            # Dry Dead Stock Logic
            if not is_fresh and days_since_delivery > dry_dead_days:
                if sales_90d < dry_dead_min_sales:
                    if sales_90d == 0:
                        rec['reasoning'] = f"Blocked: Dead Stock (>{dry_dead_days}d, No Sales)"
                        recommendations.append(rec)
                        continue

            # 2. NET REQUIREMENT CALCULATION
            # Target Stock = Reorder Point (Coverage Days * Velocity)
            # But we might want to respect the STORE CONFIG for "Safety Days" if provided?
            
            avg_daily_sales = p.get('avg_daily_sales', 0)
            
            # G3 Fix: ROP Fallback — if reorder_point is 0 or missing (no intelligence data),
            # calculate a dynamic fallback instead of treating 0 as real ROP
            reorder_point = p.get('reorder_point', 0)
            if reorder_point <= 0 and avg_daily_sales > 0:
                # Fallback ROP = ADS * (lead_time + base_safety)
                fallback_rop = avg_daily_sales * (lead_time + (base_safety * (1 + cv)))
                reorder_point = fallback_rop
                rec['reasoning'] += " [ROP Fallback: intelligence data missing]"
            
            current_stock = p.get('current_stock', 0)
            on_order = p.get('on_order_qty', 0)
            
            # Check reorder trigger
            if current_stock <= reorder_point:
                # Calculate Target Stock
                target_coverage_days = p.get('target_coverage_days', 7)
                
                # --- CYCLE STOCK CORRECTION ---
                gap_days = int(p.get('median_gap_days', 7))
                if gap_days < 1: gap_days = 1
                lead_time = int(p.get('lead_time_days', 1) or 1)
                
                min_cycle_coverage = gap_days + lead_time + safety_buffer
                target_coverage_days = max(target_coverage_days, min_cycle_coverage)

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
        Phase C: Minimum Order Threshold (MOT) gate.
        
        Groups finalized PO lines by supplier and checks whether the
        supplier's total picking list meets the minimum threshold.
        Items below MOT are tagged TRANSFER_FIRST so the dashboard
        can route them to inter-store transfers instead of micro-orders.
        
        Returns:
            {
                'po_recs': [items above MOT — keep as supplier PO],
                'transfer_recs': [items below MOT — route to transfers],
                'supplier_summary': {supplier: {units, value, status}}
            }
        """
        min_units = self.thresholds.get('min_order_units', 10)
        min_value = self.thresholds.get('min_order_value_kes', 5000)
        
        # Group by supplier
        supplier_groups: Dict[str, List[Dict[str, Any]]] = {}
        for rec in finalized_recs:
            qty = rec.get('recommended_quantity', 0)
            if qty <= 0:
                continue  # Skip non-order items
            supplier = rec.get('supplier_name', rec.get('supplier', 'UNKNOWN'))
            if supplier not in supplier_groups:
                supplier_groups[supplier] = []
            supplier_groups[supplier].append(rec)
        
        po_recs = []
        transfer_recs = []
        supplier_summary = {}
        
        for supplier, recs in supplier_groups.items():
            total_units = sum(r.get('recommended_quantity', 0) for r in recs)
            total_value = sum(
                r.get('recommended_quantity', 0) * r.get('selling_price', r.get('sell_price', r.get('cost_price', 0)))
                for r in recs
            )
            
            meets_units = total_units >= min_units
            meets_value = total_value >= min_value
            
            if meets_units or meets_value:
                # Above MOT — keep as supplier PO
                for r in recs:
                    r['fulfillment'] = 'SUPPLIER_PO'
                po_recs.extend(recs)
                supplier_summary[supplier] = {
                    'units': total_units, 'value': total_value,
                    'status': 'PO', 'item_count': len(recs)
                }
            else:
                # Below MOT — route to transfer
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
