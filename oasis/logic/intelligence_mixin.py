import json
import logging
import statistics
from datetime import datetime
from typing import List, Dict, Optional, Any, Tuple
from anthropic import AsyncAnthropic
from textwrap import dedent


from .department_constants import ESSENTIAL_DEPARTMENTS, FAST_FIVE_DEPARTMENTS, FRESH_DEPARTMENTS
from .order_logic_guards import apply_safety_guards

logger = logging.getLogger("OrderEngine.Intelligence")

class IntelligenceMixin:
    """
    IntelligenceMixin handles all analytical logic, including demand forecasting,
    product matching, and complex inventory allocation strategies.
    """
    profile_manager: Any
    budget_manager: Any
    total_budget: float
    grn_db: Dict[str, Any]
    _po_history_dates: Dict[str, List[datetime]]
    databases: Dict[str, Any]
    no_grn_suppliers: List[str]
    _brand_index_cache: Dict[str, Any] = {}
    _sales_index_cache: Dict[str, Any] = {}
    _prof_index_cache: Dict[str, Any] = {}
    _brand_index_source_id: Any = None
    grn_frequency_map: Dict[str, float]
    # FIX H1: Declare engine config contract for mixin interop
    engines_config: Dict[str, Any]
    def is_engine_enabled(self, engine_name: str) -> bool: ...
    
    def get_grn_cycle_days(self, product_name: str, is_fresh: bool = False) -> float:
        """Helper to get historical order frequency from OrderEngine state."""
        if not hasattr(self, 'grn_frequency_map') or not self.grn_frequency_map:
            return 1.0 if is_fresh else 7.0
        freq = self.grn_frequency_map.get(str(product_name).upper(), 0.0)
        return 1.0 / freq if freq > 0 else (1.0 if is_fresh else 7.0)
        
    def staple_priority_sort(self, x: Dict[str, Any]) -> Tuple[int, float]:
        """Priority tiers: 0=Fast Five Staple, 1=Other Staple, 2=Essential Dept, 3=Discretionary."""
        p_name = str(x.get('product_name', ''))
        dept = str(x.get('product_category', 'GENERAL')).upper()
        ads = float(x.get('avg_daily_sales', 0.0))
        
        # Safe access to budget_manager
        is_staple = False
        if hasattr(self, 'budget_manager') and self.budget_manager:
            is_staple = self.budget_manager.is_staple(p_name, dept, ads)
        
        # v10.1: Honor manual override from scorecard/rec
        if x.get('is_staple_override') or x.get('is_staple'):
            is_staple = True
        
        priority = 3
        if is_staple and dept in FAST_FIVE_DEPARTMENTS:
            priority = 0
        elif is_staple:
            priority = 1
        elif any(k in p_name.upper() for k in ['SUGAR', 'SALT', 'FLOUR', 'RICE', 'COOKING OIL', 'FRESH MILK', 'BREAD', 'EGGS']):
            priority = 2
        
        return (priority, -ads)
    
    def normalize_product_name(self, name: str) -> str:
        if not name: return ""
        return name.upper().strip().replace('  ', ' ')

    def _calculate_cv(self, monthly_sales: dict) -> float:
        """Calculate Coefficient of Variation (CV) from monthly sales data."""
        if not monthly_sales: return 0.5
        values = [float(v) for v in monthly_sales.values() if v is not None]
        if len(values) < 2: return 0.4
        mean = statistics.mean(values)
        if mean <= 0: return 1.0
        stdev = statistics.stdev(values) if len(values) > 1 else 0.0
        return float(round(float(stdev / mean), 3))

    def _find_lookalike_demand(self, product_name: str, sales_database: dict) -> float:
        """Find lookalike SKU demand based on brand and category."""
        if not hasattr(self, '_brand_index_cache') or self._brand_index_cache is None:
             self._brand_index_cache = {}
             self._brand_index_source_id = None

        if self._brand_index_source_id != id(sales_database):
             self._brand_index_source_id = id(sales_database)
             if not hasattr(self, '_brand_index_cache') or self._brand_index_cache is None:
                 self._brand_index_cache = {}
             self._brand_index_cache.clear()
             for name, data in sales_database.items():
                 brand = name.split()[0].strip().upper()
                 if brand:
                     if brand not in self._brand_index_cache: self._brand_index_cache[brand] = []
                     val = float(data.get('avg_daily_sales', 0.0))
                     if val > 0: self._brand_index_cache[brand].append(val)

        # v10.9: Hardened safety for malformed names
        tokens = str(product_name).split()
        if not tokens:
            return 0.0
            
        brand = tokens[0].strip().upper()
        similar_sales = self._brand_index_cache.get(brand, [])
        return float(statistics.median(similar_sales)) if similar_sales else 0.0

    _normalized_db_cache: Dict[int, Dict[str, str]] = {}  # {id(db): {normalized_name: original_key}}

    def _get_normalized_index(self, database: dict) -> Dict[str, str]:
        """Build or retrieve a normalized key index for a database (O(N) once, O(1) lookup)."""
        db_id = id(database)
        if db_id not in self._normalized_db_cache:
            index = {}
            for key in database:
                norm = self.normalize_product_name(key)
                if norm not in index:  # First match wins
                    index[norm] = key
            self._normalized_db_cache[db_id] = index
        return self._normalized_db_cache[db_id]

    def find_best_match(self, item_code: Optional[str], barcode: Optional[str], product_name: str, database: dict) -> Optional[dict]:
        """Matches product against database using Item Code, Barcode, or Name.
        
        Performance: Uses pre-computed normalized index instead of difflib.get_close_matches
        which was O(N) per call and the #1 CPU bottleneck (23k products × 20k keys × 2 calls).
        """
        if item_code:
            s_code = str(item_code).strip()
            for key in database:
                if key.startswith(s_code + " ") or key.startswith(s_code + "\t"): return database[key]
        if barcode:
            s_barcode = str(barcode).strip()
            for key, val in database.items():
                if s_barcode in key: return val
                if isinstance(val, dict) and str(val.get('barcode', '')) == s_barcode: return val
        if product_name in database: return database[product_name]
        
        # Fast O(1) normalized lookup (replaces O(N) loop)
        normalized = self.normalize_product_name(product_name)
        norm_index = self._get_normalized_index(database)
        if normalized in norm_index:
            return database[norm_index[normalized]]
        
        # Token-based partial match (fast fallback, replaces O(N) difflib)
        # Only try if the product name has enough tokens to be meaningful
        tokens = normalized.split()
        if len(tokens) >= 2:
            prefix = " ".join(tokens[:2])
            for norm_key, orig_key in norm_index.items():
                if norm_key.startswith(prefix):
                    return database[orig_key]
        
        return None


    def _get_actual_cost_price(self, product_rec: dict, selling_price: float) -> float:
        """Calculate actual cost price using GRN history or margin estimates."""
        p_name = product_rec.get('product_name', '')
        p_barcode = str(product_rec.get('barcode', '')).strip()
        grn_key = p_barcode if p_barcode else self.normalize_product_name(p_name)
        grn_stat = getattr(self, 'grn_db', {}).get(grn_key)
        if grn_stat and isinstance(grn_stat, dict) and grn_stat.get('avg_cost'):
            return float(grn_stat['avg_cost'])
        margin_pct = product_rec.get('margin_pct')
        if margin_pct is not None and -200 < margin_pct < 100:
            return float(selling_price) * (1 - float(margin_pct) / 100.0)
        return float(selling_price) * 0.75

    def calculate_replenishment_target_stock(self, product: dict, tier_profile: dict) -> float:
        """
        v9.5 PRECISION ALLOCATION: Smart Greenfield & Replenishment Logic.
        Formula: Target = (LeadTime + Safety + Cycle) * 1.15 * VelocityMultiplier
        """
        avg_sales = float(product.get('avg_daily_sales', 0.0))
        if avg_sales <= 0: return 0.0
        
        # 1. Supply Chain Parameters
        lead_time = int(product.get('estimated_delivery_days', 7))
        if lead_time < 1: lead_time = 1
        
        is_fresh = product.get('is_fresh', False)
        p_name_upper = str(product.get('product_name', '')).upper()

        if is_fresh:
            # v10.12: Fresh items now use the unified DDoS pipeline but with tighter safety
            # to prevent spoilage while respecting the ordering rhythm.
            pass
             
        # === CHAPTER 12: Dynamic Days of Stock (DDoS) ===
        # v10.12: This replaces hardcoded caps with rhythm-aware replenishment targets.
        # Formula: Target = Next_Order_Interval + Lead_Time + Safety_Buffer
        
        # 1. Determine Next Order Interval (Cycle)
        supplier_name = str(product.get('supplier_name', 'UNKNOWN')).upper().strip()
        rhythm_db = getattr(self, 'rhythm_db', {})
        s_rhythm = rhythm_db.get(supplier_name, {})
        
        # ONE ORDER CYCLE, ONE SOURCE.
        #
        # This read `s_rhythm.get('median_gap', 7.0)` — the PO-rhythm file — and
        # fell back to a literal 7 whenever a supplier was missing from it. The
        # ordering stage meanwhile floors coverage at the product's OWN
        # median_gap_days, measured from real goods receipts. So for a supplier
        # absent from the rhythm file the two stages used 7 and 30 for the same
        # quantity, enrichment computed a target the order then overrode, and
        # the queue displayed a number the order never honoured.
        #
        # Precedence: the PO rhythm file where it actually covers this
        # supplier, then the receipt-derived gap on the product, then 7 as the
        # last resort. 7 is now what it always claimed to be — a default for
        # "no history at all" — rather than a value that quietly outranks
        # measured history.
        order_cycle = s_rhythm.get('median_gap')
        if order_cycle is None:
            order_cycle = product.get('median_gap_days')
        order_cycle = float(order_cycle) if order_cycle else 7.0
        
        # 2. Safety Buffer (LATA & Simulation Aware)
        # Fresh items get a lean 1.2-day TOTAL BASE if supplied daily, 
        # otherwise Dry goods get 3.0 days safety buffer.
        
        if is_fresh and order_cycle <= 1.5:
            # v10.12: Strict 1.2 Day Total Coverage for Daily Fresh (Bread/Milk)
            # This covers the immediate 24h gap + 0.2 morning rush buffer.
            target_days = 1.2
            trace_tag = "DAILY_FRESH_JIT"
        else:
            base_safety = 3.0
            # Apply LATA Variance Multiplier
            lata_multiplier = 1.0
            if getattr(self, 'is_engine_enabled', lambda x: False)('lata'):
                supplier_patterns = self.databases.get('supplier_patterns', {})
                sp = supplier_patterns.get(supplier_name, {})
                lata_multiplier = float(sp.get('lata_variance_multiplier', 1.0))
            
            safety_buffer = base_safety * lata_multiplier
            target_days = (order_cycle + lead_time + safety_buffer)
            trace_tag = "STANDARD_REPLENISHMENT"

        # Apply Simulation Feedback Multiplier (v10.12: Unified)
        sim_multiplier = 1.0
        sim_feedback = self.databases.get('simulation_feedback', {}).get('sku_feedback', {})
        if p_name_upper in sim_feedback or product.get('product_name') in sim_feedback:
            fb = sim_feedback.get(product.get('product_name')) or sim_feedback.get(p_name_upper)
            if fb.get('stockout_days', 0) > 0:
                sim_multiplier = 1.2
        
        target_days *= sim_multiplier
        
        # Apply Velocity-Based Depth Scaling (v10.0 authoritative)
        if avg_sales > 10:
            velocity_multiplier = 1.4  # Very high velocity
        elif avg_sales > 5:
            velocity_multiplier = 1.3  # High velocity  
        elif avg_sales > 2:
            velocity_multiplier = 1.2  # Medium-high velocity
        elif avg_sales > 1:
            velocity_multiplier = 1.0  # Medium velocity
        else:
            velocity_multiplier = 0.8  # Low velocity (reduce overstocking)
        
        target_days *= velocity_multiplier
        
        # 4. Strategic Guardrails & Department Caps
        if any(x in p_name_upper for x in ['UHT', 'ESL', 'LONG LIFE']):
            target_days = min(target_days, max(7.0, order_cycle + lead_time))
        elif is_fresh:
            # Fresh cap is tighter to prevent spoilage
            # v10.12: Strict 1.2 Day Total Coverage for Daily Fresh items
            if order_cycle <= 1.5:
                target_days = min(target_days, 1.2)
                target_days = max(target_days, 1.0)
            else:
                target_days = min(target_days, order_cycle + lead_time + 1.2)
                target_days = max(target_days, 2.0)
        else:
            # Dynamic Cap for dry goods: Cycle + 14 days (or max 60)
            dynamic_cap = min(60.0, order_cycle + lead_time + 14.0)
            target_days = min(target_days, dynamic_cap)
             
        return float(round(target_days, 2))


    def extract_brand(self, name: str) -> str:
        """Simple brand extraction from product name."""
        if not name: return "GENERIC"
        parts = name.split(' ')
        if len(parts) > 0:
            if len(parts) > 1 and parts[0] in ['TROPICAL', 'PEE', 'PEE BEE', 'GOLDEN', 'FRESH']:
                 return " ".join(parts[:2]).upper()
            return parts[0].upper()
        return "GENERIC"

    def get_brand_strength(self, brand: str) -> float:
        """Returns a score 0.0-1.0 based on brand visibility/sales volume."""
        top_brands = ['TROPICAL HEAT', 'INDOMIE', 'KENSALT', 'NDOVU', 'EXE', 'SUNGOLD', 'DAIMA', 'BIO', 'COKE', 'PEPSI', 'CROWN', 'MACCOFFEE']
        if brand.upper() in top_brands: return 0.95
        return 0.5

    def enrich_product_data(self, products: List[Dict[str, Any]], is_greenfield: bool = False):
        """Phase 3: Product Enrichment. Maps all intelligence metrics."""
        supplier_patterns = self.databases.get('supplier_patterns', {})
        sales_forecasting = self.databases.get('sales_forecasting', {})
        sales_profitability = self.databases.get('sales_profitability', {})
        supplier_quality = self.databases.get('supplier_quality', {})  # R2: Golden Parity
        supp_map = self.databases.get('product_supplier_map', {})
        dept_map = self.databases.get('product_department_map', {})
        sim_feedback = self.databases.get('simulation_feedback', {}).get('sku_feedback', {})
        
        logger.info(f"Phase 3: Enriching {len(products)} products...")
        
        # Determine tier for replenishment logic (Pass empty profile if not in ordering context)
        profile = getattr(self, 'current_profile', {})

        for p in products:
            p_name = str(p.get('product_name', ''))
            p_code = p.get('item_code')
            p_barcode = str(p.get('barcode', '')).strip()
            p_upper = p_name.upper()

            # 0. Department Resolution
            if p_name in dept_map:
                p['product_category'] = dept_map[p_name]
            elif p_upper in dept_map:
                p['product_category'] = dept_map[p_upper]

            # 1. Supplier Resolution
            if not p.get('supplier_name') or p.get('supplier_name') == 'Unknown':
                 found = supp_map.get(p_name) or supp_map.get(p_upper)
                 if found: p['supplier_name'] = found

            supplier = str(p.get('supplier_name', 'UNKNOWN')).upper().strip()
            p['is_consignment'] = (supplier in self.no_grn_suppliers) or ("PLU" in p_upper)

            # 2. Timing & Rhythm
            pattern = supplier_patterns.get(supplier) or supplier_patterns.get(self.normalize_product_name(supplier))
            if pattern:
                p['estimated_delivery_days'] = float(pattern.get('estimated_delivery_days', 4))
                p['supplier_reliability'] = float(pattern.get('reliability_score', 0.8))
                p['supplier_frequency'] = pattern.get('order_frequency', 'weekly')
                
                # v6.3 FIX: Data-Driven Freshness
                median_gap = pattern.get('median_gap_days', 7)
                p['median_gap_days'] = median_gap
                if median_gap <= 2 or p['supplier_frequency'] == 'daily':
                     p['supplier_frequency'] = 'daily' 
                     p['is_fresh'] = True
            else:
                p['estimated_delivery_days'] = 7.0
                p['supplier_reliability'] = 0.9
                p['supplier_frequency'] = 'weekly'
                p['median_gap_days'] = 7

            # 2.5 GRN Intelligence (Move to start of loop for anchoring)
            grn_stat = self.grn_db.get(p_barcode) or self.grn_db.get(self.normalize_product_name(p_name))
            last_delivery_days = 0
            if grn_stat and isinstance(grn_stat, dict):
                # v10.10: Handle both 'total/count' and 'avg_cost/frequency' cache formats
                cnt = self._safe_int(grn_stat.get('count') or grn_stat.get('frequency') or 0)
                tot = self._safe_float(grn_stat.get('total') or 0.0)
                
                if cnt > 0:
                    if tot > 0:
                        p['historical_avg_order_qty'] = int(round(tot / cnt))
                    else:
                        # Fallback: if we only have frequency, use a derived estimate
                        # Golden Logic: Default to 14 days of current ADS as historical baseline
                        p['historical_avg_order_qty'] = 0 # Will be patched after ADS enrichment
                    
                    p['confidence_grn'] = 'HIGH' if cnt >= 100 else 'MEDIUM'
                    p['order_cycle_count'] = cnt
                
                # Aging Check (v9.1 Discontinued Logic)
                last_delivery_days = float(grn_stat.get('days_since_last_grn', 0))
            else:
                p['historical_avg_order_qty'] = 0
                p['order_cycle_count'] = 0 
                last_delivery_days = float(p.get('last_days_since_last_delivery', 0))

            # Cost Price Tracking
            selling_price = float(p.get('selling_price', 0.0))
            p['cost_price'] = self._get_actual_cost_price(p, selling_price)

            # Fresh Keywords & Rhythm Overrides
            # GOLDEN PARITY FIX: Use OR-logic to preserve supplier-driven freshness
            # (supplier pattern may have already set is_fresh=True at line 288)
            # R5: Golden Parity — broadened fresh keywords to match original scope
            has_fresh_keywords = any(x in p_upper for x in [
                'MILK', 'DAIRY', 'BREAD', 'VEG', 'FRUIT', 'MEAT', 'YOGURT',
                'YOGHURT', 'CHEESE', 'JUICE', 'BUTTER', 'MAZIWA', 'BAKERY', 'BIO ', 'DAIMA'
            ])
            is_fresh_dept = any(str(p.get('department', '')).upper() == d.upper() for d in FRESH_DEPARTMENTS)
            p['is_fresh'] = p.get('is_fresh', False) or has_fresh_keywords or is_fresh_dept
            
            # UHT/Long Life exclusion overrides both supplier AND keyword freshness
            if any(x in p_upper for x in ["UHT", "LONG LIFE", "LONGLIFE", "ESL", "TETRA"]):
                 p['is_fresh'] = False
                 # Golden logic: revert daily to weekly for UHT
                 if p.get('supplier_frequency') == 'daily':
                     p['supplier_frequency'] = 'weekly'
            
            if p['is_fresh']:
                p['supplier_frequency'] = 'daily'
                p['estimated_delivery_days'] = min(float(p['estimated_delivery_days']), 2.0)
                
            # v9.1 Discontinued Logic (Golden Parity)
            is_discontinued = False
            if p['is_fresh'] and last_delivery_days > 120:
                is_discontinued = True
                p['discontinued_reason'] = f"Discontinued Fresh (Last GRN {last_delivery_days:.0f}d > 120d)"
            elif not p['is_fresh'] and last_delivery_days > 200:
                is_discontinued = True
                p['discontinued_reason'] = f"Discontinued Dry (Last GRN {last_delivery_days:.0f}d > 200d)"
            
            if is_discontinued and not is_greenfield:
                p['is_discontinued'] = True
                p['reasoning'] = p['discontinued_reason']
                p['exclude_from_allocation'] = True
            
            # v10.1: Greenfield initialization (Clean Slate)
            if is_greenfield:
                p['current_stocks'] = 0
                p['historical_order_count'] = 0
                p['days_since_delivery'] = 0
                p['last_days_since_last_delivery'] = 0
                p['exclude_from_allocation'] = False # Re-enable for fresh load

            # 3. Sales Forecasting
            sales_data = sales_forecasting.get(p_name)
            if not sales_data:
                norm_name = self.normalize_product_name(p_name)
                if not hasattr(self, '_sales_index_cache') or not self._sales_index_cache:
                    self._sales_index_cache = {self.normalize_product_name(k): k for k in sales_forecasting}
                found_key = self._sales_index_cache.get(norm_name)
                if found_key: sales_data = sales_forecasting[found_key]
            
            if not sales_data: sales_data = self.find_best_match(p_code, p_barcode, p_name, sales_forecasting)

            if sales_data:
                hist_ads = float(round(float(sales_data.get('avg_daily_sales', 0.1)), 3))
                p['sales_trend'] = sales_data.get('trend', 'stable')
                p['sales_trend_pct'] = float(sales_data.get('trend_pct', 0.0))
                p['months_active'] = sales_data.get('months_active', 6)  # R14: Golden Parity
                
                # v10.10: Blended Velocity (Prefer Live Ingestion if available)
                live_ads = p.get('live_ads_30d', 0.0)
                if live_ads > 0:
                     # 70/30 Blend: Favor recent volatility but anchor with history
                     p['avg_daily_sales'] = round((0.7 * live_ads) + (0.3 * hist_ads), 3)
                     p['is_velocity_blended'] = True
                else:
                     p['avg_daily_sales'] = hist_ads

                p['demand_cv'] = self._calculate_cv(sales_data.get('monthly_sales', {}))
                monthly_sales = sales_data.get('monthly_sales', {})
                if monthly_sales:
                    sorted_months = sorted([(str(k), float(v)) for k, v in monthly_sales.items()], key=lambda x: x[0], reverse=True)
                    p['days_since_last_sale'] = 999
                    for month_str, qty in sorted_months:
                        if qty > 0:
                            try:
                                p['days_since_last_sale'] = (datetime.now() - datetime.strptime(month_str + "-01", "%Y-%m-%d")).days
                                break
                            except: pass
                    p['total_units_sold_last_90d'] = sum(float(qty) for _, qty in sorted_months[:3] if qty)
                    if sorted_months and len(sorted_months) > 0 and sorted_months[0][1] > 0:
                        p['avg_daily_sales_last_30d'] = float(round(float(sorted_months[0][1]) / 30.0, 3))
                    else:
                        p['avg_daily_sales_last_30d'] = 0.0
            else:
                p['avg_daily_sales'] = float(round(float(p.get('avg_daily_sales', p.get('estimated_daily_sales', 0.0))), 3))
                p['demand_cv'] = 0.5  # Golden standard default
                p['days_since_last_sale'] = 999
                p['total_units_sold_last_90d'] = 0
                p['avg_daily_sales_last_30d'] = 0.0

            # === CHAPTER 11: DHARAM Ghost Demand Patching ===
            # If DHARAM engine is enabled and this SKU has a demand recovery patch,
            # override the historical ADS with the true unsuppressed demand.
            # FIX C3: Normalize keys to prevent silent mismatches from casing/whitespace.
            dharam_patches = self.databases.get('dharam_demand_patch', {})
            if dharam_patches:
                p_name_norm = p_name.strip().upper()
                patched_ads_val = dharam_patches.get(p_name)
                if patched_ads_val is None:
                    patched_ads_val = dharam_patches.get(p_name_norm)
                if patched_ads_val is None:
                    # Try normalized key matching as last resort
                    p_name_canonical = self.normalize_product_name(p_name)
                    for dk, dv in dharam_patches.items():
                        if self.normalize_product_name(dk) == p_name_canonical:
                            patched_ads_val = dv
                            break
                if patched_ads_val is not None:
                    original_ads = p['avg_daily_sales']
                    patched_ads = float(patched_ads_val)
                    # Only apply if patch is higher (Ghost Demand = suppressed sales)
                    if patched_ads > original_ads:
                        p['avg_daily_sales'] = patched_ads
                        p['dharam_original_ads'] = original_ads
                        p['dharam_patched'] = True
                        p['dharam_recovery_pct'] = round(((patched_ads - original_ads) / max(original_ads, 0.001)) * 100, 1)

            # Last Order Date (from PO patterns)
            p['days_since_last_order'] = 999
            if supplier in supplier_patterns:
                po_history = getattr(self, '_po_history_dates', {})
                if supplier in po_history:
                    last_date = max(po_history[supplier])
                    p['days_since_last_order'] = (datetime.now() - last_date).days

            # PHASE 3: RELIABLE FORECASTING PARAMETERS (Refined with Gold Standard data)
            p['current_stock'] = p.get('current_stocks', 0)
            p['days_since_delivery'] = p.get('last_days_since_last_delivery', 0)
            
            # v10.10: Correct Velocity Ingestion (Golden Parity)
            # Use units_sold_last_month if available (Picking list), otherwise fallback to ADS
            units_last_month = float(p.get('units_sold_last_month', 0.0))
            if units_last_month > 0:
                p['sales_velocity'] = float(round(units_last_month / 30.0, 2))
            else:
                p['sales_velocity'] = float(round(p.get('avg_daily_sales', 0.0), 2))
            
            # Patch Historical Avg if it was missing from GRN Cache
            if p.get('historical_avg_order_qty', 0) == 0:
                # Golden Logic Fallback: Standard replenishment cycle (14 days)
                p['historical_avg_order_qty'] = int(round(p['sales_velocity'] * 14))

            is_fresh = bool(p['is_fresh'])
            
            # GOLDEN PARITY: reliability_score field (Regression #8)
            p['reliability_score'] = p.get('supplier_reliability', 0.9) * 100
            p['supplier_frequency_days'] = p.get('estimated_delivery_days', 7)
            
            # GAP 1 FIX: explicitly set lead_time_days
            p['lead_time_days'] = 1 if is_fresh else p.get('estimated_delivery_days', 3)
            
            # GOLDEN PARITY: Category branching with safety_stock_pct (Regression #2, #3)
            # Use department map first, then keyword fallback
            if not p.get('product_category') or p.get('product_category') == 'general':
                if is_fresh:
                    p['product_category'] = "FRESH"
                    p['safety_stock_pct'] = 20
                elif any(x in p_upper for x in ['PET', '300ML', '330ML', '500ML', '2LT', 'SODA', 'PEPSI', 'MIRINDA', '7UP', 'MOUNTAIN DEW', 'JUICE', 'WATER']):
                    p['product_category'] = "BEVERAGES"
                    p['safety_stock_pct'] = 15
                else:
                    p['product_category'] = "GENERAL"
                    p['safety_stock_pct'] = 10
            else:
                # Preserve dept-map category, still set safety_stock_pct
                if is_fresh:
                    p['safety_stock_pct'] = 20
                elif any(x in p.get('product_category', '').upper() for x in ['BEER', 'BEVERAGE', 'DRINK', 'JUICE', 'WATER', 'SODA']):
                    p['safety_stock_pct'] = 15
                else:
                    p['safety_stock_pct'] = 10

            # Calculate precision target coverage (Strategic Depth v8.0)
            p['target_coverage_days'] = self.calculate_replenishment_target_stock(p, profile)
            
            # GOLDEN PARITY: Missing default fields (Regression #10, #11)
            # DEFAULT it, do not CLOBBER it. PosErpAdapter.fetch_enriched_products
            # sets on_order_qty from open POs (PENDING + APPROVED) so the net
            # requirement is (target - current - on_order); assigning 0 here
            # threw that away on every run, and calculate_order_quantity then
            # re-ordered everything already in transit. Ordering once looked
            # fine — the damage only appears on day 2, compounding daily.
            p['on_order_qty'] = float(p.get('on_order_qty') or 0)
            p['expiry_risk'] = 'high' if is_fresh else 'low'
            p['moq_floor'] = 0
            p['min_presentation_stock'] = 0
            p['is_key_sku'] = p.get('is_top_sku', False)
            p['shelf_life_days'] = 7 if is_fresh else 365
            if is_fresh and p.get('supplier_frequency') == 'daily':
                p['upper_coverage_days'] = 1.2
            elif is_fresh:
                p['upper_coverage_days'] = 3.0
            elif any(x in p_upper for x in ['UHT', 'ESL', 'LONG LIFE']):
                p['upper_coverage_days'] = 7.0
            else:
                p['upper_coverage_days'] = 45.0
            
            # GOLDEN PARITY: last_delivery_quantity (Regression #11)
            if p.get('historical_avg_order_qty', 0) > 0:
                p['last_delivery_quantity'] = p['historical_avg_order_qty']
            else:
                p['last_delivery_quantity'] = max(50, p.get('current_stocks', 0) * 2)
            
            # R2: Supplier Quality Enrichment (Golden Parity)
            supplier_upper = str(p.get('supplier_name', 'UNKNOWN')).upper().strip()
            sq = supplier_quality.get(supplier_upper, {})
            p['supplier_expiry_returns'] = sq.get('expiry_returns', 0)
            p['quality_score'] = sq.get('quality_score', 100)
            
            # 4.5 Quality & Trend Tracking
            p['supplier_quality_score'] = p.get('supplier_reliability', 0.9) * 100
            if not sales_data or 'trend' not in sales_data:
                p['sales_trend'] = 'stable'
                p['sales_trend_pct'] = 0.0
            
            # --- GOLDEN STATE CATEGORY BOOSTS (v3.2) ---
            base_coverage = float(p['target_coverage_days'])
            dept_upper = str(p.get('department', p.get('product_category', 'GENERAL'))).upper()
            
            # Bread/Bakery: 2.0x boost (R9: added FESTIVE, NATURES from golden)
            if 'BREAD' in p_upper or 'FESTIVE' in p_upper or 'NATURES' in p_upper or any(x in p_upper for x in ['800G', '600G', '400G']):
                if 'BAKERY' in dept_upper or 'BREAD' in dept_upper:
                    p['target_coverage_days'] = int(base_coverage * 2.0)
                    p['category_boost'] = 2.0
                    p['category_boost_reason'] = 'Bread/bakery high-velocity perishable'
            
            # Dairy/Fresh Milk: Strict 1.2 day cap (no boost)
            elif any(x in p_upper for x in ['DAIMA', 'BIO ', 'FRESH MILK', 'MAZIWA']):
                p['category_boost'] = 1.0
                p['category_boost_reason'] = 'Strict 1.2 day cap for dairy'
            
            # High-velocity staples: 1.3x boost (identified from feedback)
            elif any(x in p_upper for x in ['GOLD 500ML', 'CROWN TFA', 'MACCOFFEE', 'INDOMIE']):
                p['target_coverage_days'] = int(base_coverage * 1.3)
                p['category_boost'] = 1.3
                p['category_boost_reason'] = 'High-velocity staple'

            # Beverages/Juice: 1.5x boost
            elif any(x in p_upper for x in ['DEL 1L', 'JUICE', 'BERRY', 'QUENCHER']):
                p['target_coverage_days'] = int(base_coverage * 1.5)
                p['category_boost'] = 1.5
                p['category_boost_reason'] = 'Beverage high demand'

            # Confectionery/Impulse: 2.5x boost (R10: added GIANT from golden)
            elif any(x in p_upper for x in ['LOLLIPOP', 'LOLLYPOP', 'CHUPA', 'CANDY', 'GIANT', 'ORBIT', 'WRIGLEY']):
                p['target_coverage_days'] = int(base_coverage * 2.5)
                p['category_boost'] = 2.5
                p['category_boost_reason'] = 'Impulse confectionery high-risk'
            
            # Staple Commodities: 1.4x boost (bulk household essentials)
            elif any(x in p_upper or x in dept_upper for x in ['KENSALT', 'NDOVU', 'MAIZE MEAL', 'ATTA', ' SALT', ' FLOUR', 'SUGAR']):
                p['target_coverage_days'] = base_coverage * 1.4
                p['category_boost'] = 1.4
                p['category_boost_reason'] = 'Staple commodity bulk'

            # v10.0 Essentials: 1.25x boost
            elif dept_upper in ESSENTIAL_DEPARTMENTS:
                p['target_coverage_days'] = float(round(float(base_coverage * 1.25), 2))
                p['category_boost'] = 1.25
                p['category_boost_reason'] = 'Essential household item'

            # Specialty Baking: 1.3x boost
            elif any(x in p_upper for x in ['YEAST', 'ANGEL 10G']):
                p['target_coverage_days'] = int(base_coverage * 1.3)
                p['category_boost'] = 1.3
                p['category_boost_reason'] = 'Specialty baking ingredient'

            # GAP-L: Simulation Feedback Adjustment (v8.2)
            if p_name in sim_feedback:
                fb = sim_feedback[p_name]
                stockout_freq = float(fb.get('stockout_frequency', 0))
                avg_stockout_day = float(fb.get('avg_first_stockout_day', 14))
                
                if stockout_freq > 0.3:
                    avg_day = avg_stockout_day
                    # Severity Formula: Ranges from 1.2 to 2.5 based on stockout timing
                    if avg_day < 5.0: severity = 2.5
                    elif avg_day < 7.0: severity = 2.0
                    elif avg_day < 10.0: severity = 1.5
                    else: severity = 1.2
                    
                    depth_multiplier = min(3.5, 1.0 + (stockout_freq * severity))
                    p['target_coverage_days'] = int(float(p['target_coverage_days']) * depth_multiplier)
                    p['simulation_adjusted'] = True
                    p['sim_stockout_frequency'] = stockout_freq
                    p['sim_avg_stockout_day'] = avg_day
                    p['sim_depth_multiplier'] = float(round(float(depth_multiplier), 2))
                    p['sim_severity_factor'] = float(severity)
                    
                    if p.get('reorder_point'):
                        p['reorder_point'] = int(float(p['reorder_point']) * depth_multiplier)
                
                # v9.1: MDQ Adjustment from Feedback
                if stockout_freq > 0.5:
                    p['mdq'] = max(float(p.get('mdq', 6)), float(fb.get('suggested_min_display', 12)))
                    p['reasoning'] = str(p.get('reasoning', '')) + " [SIM MDQ BOOST]"

            # Minimum Depth Floors (v9.1 Parity)
            if ('BREAD' in p_upper or 'BAKERY' in dept_upper) and float(p.get('target_coverage_days', 0.0)) < 3.0:
                p['target_coverage_days'] = 3.0
                p['floor_applied'] = True

            # --- FINAL STAGE STRATEGIC CAPS ---
            #
            # OPERATOR RULING (2026-08-25): an order must ALWAYS aim to cover
            # until the next delivery. The ceilings are a guard against
            # over-ordering lines that arrive on a DAILY cadence — they were
            # never meant to cut an order below the horizon it has to survive.
            #
            # They were doing exactly that. A 7-day ceiling on UHT whose
            # supplier comes every 14 days does not prevent overstock; it
            # guarantees a 7-day hole on the shelf. The ordering stage then
            # quietly overrode the ceiling anyway (simulation_bridge's
            # min_cycle_coverage floor), so the two stages disagreed on 61% of
            # ordered lines and the queue displayed a target the order never
            # honoured. One rule, applied once, in the open.
            #
            # FIX 7 retained: a category boost is a floor, so a ceiling never
            # cuts below the boosted value either.
            target_days = float(p['target_coverage_days'])
            category_boost_applied = p.get('category_boost', 1.0)
            boosted_floor = float(base_coverage * category_boost_applied) if category_boost_applied > 1.0 else 0.0

            # The horizon this line actually has to survive: the wait until the
            # next delivery lands. Same quantity the ordering stage computes,
            # so the two agree by construction rather than by luck.
            _gap = float(p.get('median_gap_days', 7) or 7)
            _lead = float(p.get('estimated_delivery_days', 7) or 7)
            _safety = 1.2 if is_fresh else 3.0
            next_delivery_days = _gap + _lead + _safety

            # DAILY CADENCE is what the ceilings are for. Where a supplier
            # genuinely comes every day, the next-delivery horizon is ~2 days
            # and the ceiling binds — which is the intended guard against
            # holding a week of something that arrives tomorrow.
            daily_cadence = _gap <= 1.5

            def _ceiling(nominal):
                """A ceiling that guards daily lines without starving slow ones."""
                c = max(float(nominal), boosted_floor)
                if not daily_cadence:
                    c = max(c, next_delivery_days)
                return c

            if is_fresh:
                cap = 1.2
                if p.get('sim_stockout_frequency', 0) > 0.3:
                    cap = 1.5
                is_dairy = any(x in p_upper for x in ['DAIMA', 'BIO ', 'FRESH MILK', 'MAZIWA'])
                # Dairy on a daily round keeps the strict JIT ceiling: it is the
                # shortest-lived thing in the shop and it arrives tomorrow.
                effective_cap = cap if (is_dairy and daily_cadence) else _ceiling(cap)
                if target_days > effective_cap:
                    p['target_coverage_days'] = effective_cap
                    p['cap_applied'] = True
                    p['cap_reason'] = f'Fresh Ceiling ({effective_cap:.1f}d)'
            elif any(x in p_upper for x in ['UHT', 'ESL', 'LONG LIFE']):
                effective_cap = _ceiling(7.0)
                p['target_coverage_days'] = min(target_days, effective_cap)
                if effective_cap > 7.0:
                    p['cap_reason'] = (f'UHT ceiling raised 7d -> {effective_cap:.0f}d '
                                       f'to reach a delivery every {_gap:.0f}d')
            else:
                effective_cap = _ceiling(25.0)
                p['target_coverage_days'] = min(target_days, effective_cap)

            # NEVER ORDER PAST SPOILAGE. Where the wait for the next delivery
            # is longer than the item survives, no quantity is correct: cover
            # the horizon and it rots, respect the shelf life and it runs out.
            # That is a buying-cadence problem, and it is flagged rather than
            # silently resolved in either direction — it is also the strongest
            # possible case for a transfer from another branch.
            _shelf = float(p.get('shelf_life_days', 0) or 0)
            if _shelf > 0 and float(p['target_coverage_days']) > _shelf:
                p['target_coverage_days'] = _shelf
                p['cap_applied'] = True
                p['cap_reason'] = f'Shelf life ({_shelf:.0f}d)'
                if next_delivery_days > _shelf:
                    p['cadence_conflict'] = True
                    p['cadence_conflict_note'] = (
                        f'next delivery is {next_delivery_days:.0f}d away but this '
                        f'line only lasts {_shelf:.0f}d — it cannot be covered by '
                        f'ordering, only by ordering more often or transferring in')

            # Re-calculate ROP/Safety after ALL boosts and caps
            # R4: Golden Parity — ROP uses sales_velocity (historical), safety_stock uses avg_daily_sales (forecasted)
            # ROP should only cover lead time + safety, NOT the full order cycle
            _lead_time = p.get('estimated_delivery_days', 7)
            # safety_stock in units is (target_coverage_days - _lead_time - 7) roughly, but we can just use safety_stock_pct
            _safety_days = 3.0 if not is_fresh else 1.2
            p['reorder_point'] = float(round(float(p['sales_velocity'] * (_lead_time + _safety_days)), 2))
            p['target_stock'] = float(round(float(p['sales_velocity'] * p['target_coverage_days']), 2))
            p['safety_stock'] = float(round(float(_safety_days * p.get('avg_daily_sales', 0)), 2))

            # 5. Finalize Statistics
            p['confidence'] = "HIGH" if p.get('historical_avg_order_qty', 0) > 0 else "MEDIUM"
            if grn_stat and isinstance(grn_stat, dict):
                p['confidence_grn'] = 'HIGH' if grn_stat.get('count', 0) >= 100 else 'MEDIUM'
            else:
                p['confidence_grn'] = 'LOW'
                if float(p.get('avg_daily_sales', 0.0)) == 0.0:
                    p['avg_daily_sales'] = self._find_lookalike_demand(p_name, sales_forecasting)
                    p['is_lookalike_forecast'] = True
                    p['new_item_aggression_cap'] = 7 if is_fresh else 21

            # 6. Profitability & Rank
            # GOLDEN PARITY: Fast-path lookup using _prof_index_cache (Regression #5)
            prof_data = sales_profitability.get(p_name)
            if not prof_data:
                norm_name = self.normalize_product_name(p_name)
                if norm_name not in self._prof_index_cache:
                    for k in sales_profitability.keys():
                        self._prof_index_cache[self.normalize_product_name(k)] = k
                
                found_key = self._prof_index_cache.get(norm_name)
                if found_key:
                    prof_data = sales_profitability[found_key]
            
            if not prof_data:
                prof_data = self.find_best_match(p_code, p_barcode, p_name, sales_profitability)

            if prof_data:
                p['sales_rank'] = prof_data.get('sales_rank', 999)
                # Preserve existing margin if prof_data doesn't have one
                p['margin_pct'] = float(prof_data.get('margin_pct', p.get('margin_pct', 0.0)))
                p['revenue'] = float(prof_data.get('revenue', 0.0))  # R15: Golden Parity
                p['is_top_sku'] = True
                p['is_key_sku'] = True
            else:
                p['sales_rank'] = 999
                # Only default to 0.0 if the metric is completely missing
                if p.get('margin_pct') is None:
                    p['margin_pct'] = 0.0
                p['is_top_sku'] = False

            # v10.0: Explicit ABC Classification based on rank
            rank = p.get('sales_rank', 999)
            if rank <= 200: p['ABC_Class'] = 'A'
            elif rank <= 1000: p['ABC_Class'] = 'B'
            else: p['ABC_Class'] = 'C'
            
            # Default to 'B' if rank is missing but demand exists (prevents accidental scrubbing)
            if not prof_data and p.get('avg_daily_sales', 0) > 0.1:
                p['ABC_Class'] = 'B'

            if p_name.upper().startswith('CFB '): p['exclude_from_allocation'] = True
            
            # Injection 4 / Asymmetric Service Levels
            p['velocity_weekly'] = float(p.get('avg_daily_sales', 0)) * 7.0
            vel = p['velocity_weekly']
            cost = p.get('cost_price', 0)
            high_cost_threshold = 5000.0
            if vel >= 10.0:
                p['target_sl'] = 0.98
                p['z_score'] = 2.05
            elif vel < 2.0 and cost > high_cost_threshold:
                p['target_sl'] = 0.85
                p['z_score'] = 1.04
            else:
                p['target_sl'] = 0.95
                p['z_score'] = 1.64
            
            # Use dynamic Z-score for safety stock calculation if demand_cv is present
            cv = float(p.get('demand_cv', 0.5))
            # Fallback reorder point / safety stock
            if p.get('reorder_point', 0) == 0:
                _lt = p.get('lead_time_days', 3)
                _sd = 3.0 if not is_fresh else 1.2
                p['reorder_point'] = float(round(float(p.get('avg_daily_sales', 0) * (_lt + _sd)), 2))

        # GAP 1 FIX: Compute missing sales_rank
        missing_rank = [p for p in products if p.get('sales_rank', 999) == 999]
        if missing_rank:
            sorted_by_ads = sorted(missing_rank, key=lambda x: float(x.get('avg_daily_sales', 0)), reverse=True)
            for idx, p in enumerate(sorted_by_ads):
                # Only give meaningful rank if it has sales
                if float(p.get('avg_daily_sales', 0)) > 0:
                    p['sales_rank'] = idx + 1
                    if p['sales_rank'] < 500:
                        p['is_top_sku'] = True
                        p['is_key_sku'] = True

        return products

    async def analyze_batch_ai(self, products: list[dict[str, Any]], batch_num: int, total_batches: int, allocation_mode: str = "replenishment") -> list[dict[str, Any]]:
        """Phase 4: AI Analysis (Claude Sonnet 3.7). Analyzes a single batch with specialized retail logic."""
        logger.info(f"Phase 4: AI Analysis - Batch {batch_num}/{total_batches} ({allocation_mode} mode)")
        client = AsyncAnthropic()
        
        products_summary = json.dumps(products, indent=2)
        prompt = dedent("""
            You are an elite retail inventory analyst with comprehensive 2025 historical intelligence.
            
            MODE: {mode}
            
            CRITICAL: ALWAYS PRIORITIZE HISTORICAL DATA OVER CALCULATIONS!
            The 'product_name' in your output MUST MATCH the input 'product_name' EXACTLY.

            {strategy_instructions}

            PRODUCT DATA TO ANALYZE:
            {products}

            OUTPUT FORMAT (JSON list, exactly 13 fields):
            [
              {{
                "product_name": "EXACT_NAME",
                "supplier_name": "SUPPLIER",
                "current_stock": 0,
                "recommended_quantity": 0,
                "days_since_delivery": 0,
                "last_delivery_quantity": 0,
                "product_category": "general",
                "sales_velocity": 0.0,
                "estimated_delivery_days": 1,
                "supplier_frequency": "daily",
                "reorder_point": 0.0,
                "safety_stock_pct": 20,
                "reasoning": "Detailed logic trace..."
              }}
            ]
            """).format(
                mode=allocation_mode.upper(),
                products=products_summary,
                strategy_instructions=dedent("""
                    1. **STRATEGY: REPLENISHMENT (Default)**
                       - Goal: Survival Coverage. Maintain shelves based on usage.
                       - PHASE 1: SLOW MOVER & FRESH CHECK
                       - PHASE 2: TOP 500 / KEY SKU. Never stockout. Increase by 20% if stock < reorder.
                       - PHASE 3: DEMAND & NET REQUIREMENT ((forecast + safety) - (current + on_order)).
                    
                    2. **STRATEGY: INITIAL LOAD (Greenfield)**
                       - Goal: Shelf Presentation & Assortment Fill.
                       - **BYPASS AGING**: Ignore 'days_since_delivery'. Buy fresh stock for all SKUs.
                       - **MDQ (Minimum Display Quantity)**: Recommended Order = MAX(Forecasted Demand, shelf_fill_target).
                       - **VOLUME BUMP**: High margin items (rank < 500) get 20% volume bump.
                       - In Greenfield mode, assume current_stock is effectively 0 for the requirement calculation.
                       - If demand > 0.1, ALWAYS recommend at least 1 Pack.
                """)
            )

        try:
            response = await client.messages.create(model="claude-3-7-sonnet-20250219", max_tokens=4000, temperature=0.1, messages=[{"role": "user", "content": prompt}])
            text = response.content[0].text.strip()
            if "```" in text: text = text.split("```")[1].strip()
            if text.startswith("json"): text = text[4:].strip()
            recommendations = json.loads(text)
            products_map = {p['product_name']: p for p in products}
            return apply_safety_guards(recommendations, products_map, allocation_mode)
        except Exception as e:
            logger.error(f"AI batch error: {e}")
            return []

    async def update_all_intelligence(self):
        """Phase 8: Intelligence Refresh (The Brain Update)."""
        logger.info("Phase 8: Refreshing Global Intelligence Databases...")
        
        # 1. Update Patterns from PO History
        if hasattr(self, 'update_supplier_patterns'):
            self.update_supplier_patterns()
        
        # 2. Update Lead Times from GRN vs PO
        if hasattr(self, 'update_lead_time_intelligence'):
            self.update_lead_time_intelligence()
            
        # 3. Update Quality from Returns
        if hasattr(self, 'update_supplier_quality_scores'):
            self.update_supplier_quality_scores()
            
        # 4. Update Demand Intelligence (POS Sales & Transfers)
        if hasattr(self, 'update_demand_intelligence'):
            self.update_demand_intelligence()
            
        # 5. Update Profitability Intelligence
        if hasattr(self, 'scan_sales_profitability'):
            self.scan_sales_profitability()
            
        logger.info("Intelligence Refresh Complete. Databases persistent in session memory.")
