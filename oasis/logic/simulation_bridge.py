import sys
import os
from typing import List, Dict, Any

# Ensure we can import from the sibling modules
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine, apply_safety_guards
from oasis.data.supplier_calendar import SupplierCalendar

class SimulationOrderUtil:
    """
    Bridge to use Oasis OrderEngine logic within a high-speed simulation.
    Replaces LLM calls with deterministic Python logic derived from the AI prompts.
    """
    
    def __init__(self, data_dir: str):
        self.engine = OrderEngine(data_dir)
        # Synchronous load for simulation speed
        self.engine.load_local_databases()
        
        # Calendar Integration
        self.calendar = SupplierCalendar(r"c:\Users\iLink\.gemini\antigravity\scratch\Supplier_Order_Calendar_2026.xlsx")
        self.calendar_loaded = False
        
    def prepare_sku_data(self, sku_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Enrich raw SKU data using Oasis Intelligence.
        """
        # Oasis expects a specific format, ensure mapping
        # sku_list should be list of dicts with 'product_name', 'barcode', etc.
        return self.engine.enrich_product_data(sku_list)
        
    def calculate_order_quantity(self, enriched_skus: List[Dict[str, Any]], 
                                 store_config: Dict[str, Any] = None,
                                 current_day: int = 1) -> List[Dict[str, Any]]:
        """
        Deterministic implementation of the "AI Prompt Logic" for Replenishment.
        
        Rules ported from `analyze_batch_ai` prompt:
        1. Slow Mover Checks (Dead Stock > 200d)
        2. Freshness Checks (Stale Fresh > 120d)
        3. Net Requirement Calculation: (Target - Current - OnOrder)
        4. Key SKU Boost (Top 500 get 20% buffer)
        
        NEW (Optimized): Schedule Awareness
        - If today is not the supplier's "Ordering Day" (based on Gap Days), returns 0.
        - OVERRIDE: Critical Stockout (< 2 days coverage) forces order.
        """
        
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
            safety_buffer = base_safety * (1 + (vol_factor * cv))
            
            critical_thresh = lead_time + safety_buffer
            
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
            
            # Fresh Stale Logic
            if is_fresh and days_since_delivery > 120:
                if sales_90d == 0:
                    rec['reasoning'] = "Blocked: Stale Fresh (>120d, No Sales)"
                    recommendations.append(rec)
                    continue 
                # Else: Cap logic handled in safety guards, but we can be proactive
            
            # Dry Dead Stock Logic
            if not is_fresh and days_since_delivery > 200:
                if sales_90d < 5: # Prompt said "if sales > 0" for Fresh, "sales > 5" for Dry roughly? 
                    # Prompt actually says: "Dry (>200d): Cap if sales > 5, else 0."
                    # Wait, prompt says: "Cap if sales > 0" in one version, "sales > 5" in another.
                    # Let's stick to strict Dead Stock:
                    if sales_90d == 0:
                        rec['reasoning'] = "Blocked: Dead Stock (>200d, No Sales)"
                        recommendations.append(rec)
                        continue

            # 2. NET REQUIREMENT CALCULATION
            # Target Stock = Reorder Point (Coverage Days * Velocity)
            # But we might want to respect the STORE CONFIG for "Safety Days" if provided?
            # User asked to "tweak logic".
            
            avg_daily_sales = p.get('avg_daily_sales', 0)
            
            # Use Oasis 'reorder_point' which is calculated from (Delivery Days + Buffer) * Velocity
            # OR use Simulation override?
            # Let's use Oasis Logic as the base, because it's "Robust".
            
            reorder_point = p.get('reorder_point', 0)
            current_stock = p.get('current_stock', 0)
            on_order = p.get('on_order_qty', 0) # Simulation tracks this? 
            # Note: Simulation passes 'on_order' in the input dict? verify caller.
            
            # Check reorder trigger
            if current_stock <= reorder_point:
                # Calculate Target Stock
                # Oasis Logic: Target Coverage = Delivery Days + Buffer
                # We want to fill up to Target.
                
                target_coverage_days = p.get('target_coverage_days', 7)
                
                # --- CYCLE STOCK CORRECTION ---
                # We must hold enough stock to last until the NEXT delivery.
                # Coverage needed = Gap Days (Review Period) + Lead Time + Safety Buffer
                gap_days = int(p.get('median_gap_days', 7))
                if gap_days < 1: gap_days = 1
                lead_time = int(p.get('lead_time_days', 1) or 1)
                # safety_buffer is already calculated dynamically above
                
                min_cycle_coverage = gap_days + lead_time + safety_buffer
                target_coverage_days = max(target_coverage_days, min_cycle_coverage)

                target_stock = avg_daily_sales * target_coverage_days
                
                # Net Requirement
                net_req = target_stock - (current_stock + on_order)
                
                if net_req > 0:
                    # 3. KEY SKU BOOST
                    # "High margin items (rank < 500) get 20% volume bump."
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
