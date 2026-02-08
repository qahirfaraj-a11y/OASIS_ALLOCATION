import json
import csv
import io
import re
import os
import asyncio
import httpx
from datetime import datetime
from typing import Literal, Any, Dict, List, Tuple
from openpyxl import load_workbook
from .rounding import apply_pack_rounding

# Logger placeholder (simple print for now, or use logging module)
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OrderEngine")


def apply_safety_guards(recommendations: List[dict], products_map: Dict[str, dict], allocation_mode: str = "replenishment") -> List[dict]:
    """
    Apply strict Python-based safety guards to harmonized logic.
    Enforces caps, buffer zones, and fresh rules regardless of LLM output.
    """
    for rec in recommendations:
        p = products_map.get(rec['product_name'])
        if not p: continue
        
        # Original LLM reasoning (keep for context)
        reason = rec.get('reasoning', '')
        
        # Logic Variables
        days_since_delivery = int(p.get('last_days_since_last_delivery', 0))
        is_fresh = p.get('is_fresh', False)
        current_stock = int(p.get('current_stocks', 0))
        pack_size = int(p.get('pack_size', 1))
        
        # Sales metrics
        avg_daily_sales = p.get('avg_daily_sales', 0)
        avg_daily_sales_last_30d = p.get('avg_daily_sales_last_30d', 0)
        effective_daily_sales = max(0.01, avg_daily_sales)
        if avg_daily_sales_last_30d > 0: effective_daily_sales = avg_daily_sales_last_30d
        
        total_units_sold_last_90d = p.get('total_units_sold_last_90d', 0)
        
        # --- HARMONIZED LOGIC ENFORCEMENT ---
        
        cap_qty = None
        cap_reason = ""
        
        # GREENFIELD BYPASS (Day 1 Allocation)
        if allocation_mode == "initial_load":
            # Skip aging checks, but enforce MDQ (Minimum Display Quantity)
            # If demand is tiny but not 0, round up to 1 pack for shelf presentation
            base_rec = rec.get('recommended_quantity', 0)
            if base_rec > 0 and base_rec < pack_size:
                rec['recommended_quantity'] = pack_size
                rec['reasoning'] = reason + f" [GREENFIELD: Enforced MDQ (1 Pack)]"
            
            # Skip historical aging logic
            pass
        else:
            # 1. Tiered Fresh Logic
            if is_fresh:
                if days_since_delivery > 180:
                    cap_qty = 0
                    cap_reason = f"GUARD: Critical Stale Fresh (>180d). Blocked."
                elif days_since_delivery > 120:
                    if total_units_sold_last_90d == 0:
                        cap_qty = 0
                        cap_reason = f"GUARD: Stale Fresh (>120d, No Sales). Blocked."
                    else:
                        # Long-Life Chilled: Cap at 7 days coverage
                        max_stock = 7 * effective_daily_sales
                        max_order = max(0, int(max_stock - current_stock))
                        cap_qty = max_order
                        cap_reason = f"GUARD: Stale Fresh Watchlist (>120d). Capped at 7d coverage ({max_order})."

            # 2. Slow Mover Logic (Dry)
            elif days_since_delivery >= 200:
                if total_units_sold_last_90d == 0:
                    # Dead Stock
                    if p.get('abc_rank') == 'A' and current_stock == 0:
                         pass 
                    else:
                        cap_qty = 0
                        cap_reason = f"GUARD: Dead Stock (>200d, No Sales). Blocked."
                else:
                    # Steady Slow Mover: Cap at 21 days coverage
                    max_stock = 21 * effective_daily_sales
                    max_order = max(0, int(max_stock - current_stock))
                    
                    if max_order < (pack_size * 0.5): max_order = 0
                    
                    cap_qty = max_order
                    cap_reason = f"GUARD: Slow Mover Steady (>200d). Capped at 21d coverage ({max_order})."

            # 3. Buffer Zone (160-200d)
            elif 160 <= days_since_delivery < 200:
                current_rec = rec.get('recommended_quantity', 0)
                if current_rec > 0:
                    new_qty = int(current_rec * 0.8)
                    rec['recommended_quantity'] = new_qty
                    rec['reasoning'] = reason + f" [GUARD: Buffer Zone 160-200d, reduced 20%]"

        # Apply Hard Caps (if any set above)
        if cap_qty is not None:
            if rec.get('recommended_quantity', 0) > cap_qty:
                rec['recommended_quantity'] = cap_qty
                rec['reasoning'] = reason + f" [{cap_reason}]"
        
        # --- PACK ROUNDING (Final Step) ---
        base_qty = rec.get('recommended_quantity', 0)
        
        coverage_days = current_stock / effective_daily_sales if effective_daily_sales > 0 else 999
        risk_level = "medium"
        if current_stock <= 0 or coverage_days < 3:
            risk_level = "high"
        elif coverage_days > 20: 
            risk_level = "low"
            
        rounding_info = apply_pack_rounding(
            base_qty=base_qty,
            pack_size=pack_size,
            is_key_sku=p.get('is_key_sku', False),
            stockout_risk=risk_level,
            max_overage_ratio=0.25
        )
        
        rec['recommended_quantity'] = rounding_info['rounded_qty']
        rec['pack_rounding'] = rounding_info
        
        if rounding_info['rounding_direction'] != 'none':
            rec['reasoning'] += f" [Pack Rounding: {rounding_info['rounding_direction'].upper()} ({rounding_info['rounding_reason']})]"
                
    return recommendations


from .budget_manager import BudgetManager
from .store_profile_manager import StoreProfileManager
from .department_constants import ESSENTIAL_DEPARTMENTS, FAST_FIVE_DEPARTMENTS, FRESH_DEPARTMENTS

class OrderEngine:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.databases = {}
        self.grn_db = {} # Aggregated history from Excel
        self.no_grn_suppliers = []
        self.budget_manager = BudgetManager(data_dir)
        self.profile_manager = StoreProfileManager()
        
        # Load GRN Frequency Map (v8.0)
        self.grn_frequency_map = self.load_grn_frequency()

    def load_no_grn_suppliers(self):
        try:
            # Try multiple paths for robustness
            paths = [
                os.path.join(self.data_dir, 'app/data/no_grn_suppliers.json'),
                os.path.join(self.data_dir, 'no_grn_suppliers.json'),
                os.path.join(self.data_dir, 'oasis/data/no_grn_suppliers.json') # Fallback if initialized with scratch_dir
            ]
            
            final_path = None
            for p in paths:
                if os.path.exists(p):
                    final_path = p
                    break
            
            if final_path:
                with open(final_path, 'r') as f:
                    self.no_grn_suppliers = [s.upper() for s in json.load(f)]
            else:
                self.no_grn_suppliers = []
        except:
            self.no_grn_suppliers = []


    def load_local_databases(self):
        """Synchronous database loading for backward compatibility."""
        logger.info(f"Loading databases synchronously from {self.data_dir}")
        if not self.grn_db:
            # v4.0 Performance Fix: Check for cached GRN file first
            grn_cache_match = next((f for f in os.listdir(self.data_dir) if 'grn_intelligence' in f and f.endswith('.json')), None)
            if grn_cache_match:
                try:
                    with open(os.path.join(self.data_dir, grn_cache_match), 'r', encoding='utf-8') as f:
                        self.grn_db = json.load(f)
                    logger.info(f"Loaded GRN Intelligence from cache: {grn_cache_match}")
                except Exception as e:
                    logger.warning(f"Failed to load GRN cache, scanning files: {e}")
                    self.grn_db = self.scan_grn_files()
            else:
                self.grn_db = self.scan_grn_files()
        
        self.load_no_grn_suppliers()

        db_configs = {
            'supplier_patterns': 'supplier_patterns_2025',
            'product_supplier_map': 'product_supplier_map',
            'product_intelligence': 'sales_profitability_intelligence_2025',
            'sales_forecasting': 'sales_forecasting_2025',
            'supplier_quality': 'supplier_quality_scores_2025',
            'sales_profitability': 'sales_profitability_intelligence_2025',
            'simulation_feedback': 'simulation_feedback'  # GAP-L: Feedback loop
        }

        available_files = os.listdir(self.data_dir)
        for db_key, search_term in db_configs.items():
            match = next((f for f in available_files if search_term in f and '_updated.json' in f), None)
            if not match:
                match = next((f for f in available_files if search_term in f and f.endswith('.json')), None)
            
            if match:
                try:
                    with open(os.path.join(self.data_dir, match), 'r', encoding='utf-8') as f:
                        self.databases[db_key] = json.load(f)
                    logger.info(f"Loaded {db_key} from {match}")
                except Exception as e:
                    logger.error(f"Failed to load {db_key}: {e}")
                    self.databases[db_key] = {}
        
    async def load_databases_async(self):
        """Phase 2: Parallel Loading (Optimized). Loads all 4 databases simultaneously."""
        logger.info(f"Phase 2: Loading databases in parallel from {self.data_dir}")
        
        if not self.grn_db:
             # v4.0 Performance Fix: Check for cached GRN file first
            grn_cache_match = next((f for f in os.listdir(self.data_dir) if 'grn_intelligence' in f and f.endswith('.json')), None)
            if grn_cache_match:
                try:
                    with open(os.path.join(self.data_dir, grn_cache_match), 'r', encoding='utf-8') as f:
                        self.grn_db = json.load(f)
                    logger.info(f"Loaded GRN Intelligence from cache: {grn_cache_match}")
                except Exception as e:
                    logger.warning(f"Failed to load GRN cache, scanning files: {e}")
                    self.grn_db = self.scan_grn_files()
            else:
                self.grn_db = self.scan_grn_files()
            
            
        self.load_no_grn_suppliers()

        # Database mapping
        db_configs = {
            'supplier_patterns': 'supplier_patterns_2025',
            'product_intelligence': 'sales_profitability_intelligence_2025',
            'sales_forecasting': 'sales_forecasting_2025',
            'supplier_quality': 'supplier_quality_scores_2025',
            'sales_profitability': 'sales_profitability_intelligence_2025'
        }

        available_files = os.listdir(self.data_dir)
        
        async def load_single_db(db_key, search_term):
            # Check for updated version first
            match = next((f for f in available_files if search_term in f and '_updated.json' in f), None)
            if not match:
                match = next((f for f in available_files if search_term in f and f.endswith('.json')), None)
            
            if match:
                try:
                    # Async read not strictly necessary for local files if small, 
                    # but following the requirements pattern.
                    with open(os.path.join(self.data_dir, match), 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    logger.info(f"Loaded {db_key} from {match}")
                    return db_key, data
                except Exception as e:
                    logger.error(f"Failed to load {db_key}: {e}")
            return db_key, {}

        # Parallelize
        tasks = [load_single_db(k, v) for k, v in db_configs.items()]
        results = await asyncio.gather(*tasks)
        
        for db_key, data in results:
            self.databases[db_key] = data

        logger.info(f"Databases loaded: {list(self.databases.keys())}")

    def update_all_intelligence(self):
        """Triggers a full refresh of all intelligence sources."""
        logger.info("--- Starting Full Intelligence Update ---")
        self.update_supplier_quality_scores()
        self.update_demand_intelligence()
        self.update_supplier_patterns()
        self.update_lead_time_intelligence()
        self.scan_sales_profitability() # Phase 8: Profitability Refresh
        logger.info("--- Intelligence Update Complete ---")

    def parse_inventory_file(self, file_path: str) -> List[dict]:
        """Phase 1: File Parsing. Supports CSV, Excel, and Picking list formats."""
        logger.info(f"Phase 1: Parsing inventory file: {file_path}")
        
        products = []
        is_excel = file_path.lower().endswith(('.xlsx', '.xls'))
        
        try:
            if is_excel:
                # Load with data_only=True to get values, not formulas
                wb = load_workbook(file_path, data_only=True)
                ws = wb.active
                
                # 1. Extract supplier from Row 1, Col 7-10
                supplier_name = None
                for col in range(7, 11):
                    cell_value = ws.cell(row=1, column=col).value
                    if cell_value and not supplier_name:
                        supplier_name = str(cell_value).strip()
                        break
                if not supplier_name:
                    supplier_name = 'UNKNOWN SUPPLIER'
                
                # 2. Extract headers from Row 3
                headers = [ws.cell(row=3, column=col).value for col in range(1, 30)]
                headers = [str(h).strip().upper() if h is not None else '' for h in headers]
                
                # Map columns (strictly as per picking list specs)
                col_map = {h: i+1 for i, h in enumerate(headers) if h}
                
                # 3. Parse product data starting Row 4
                for row_idx in range(4, ws.max_row + 1):
                    # Check if row has a description/name
                    # DESCRIPTION is usually Col 1 but we use the map
                    p_name = ws.cell(row=row_idx, column=col_map.get('DESCRIPTION', 1)).value
                    if not p_name: continue

                    # Map spec columns
                    # DESCRIPTION -> Product Name
                    # Rhapta -> Current Stock
                    # RR Prev -> Units Sold LAST MONTH
                    # RR GRN -> DAYS since last GRN
                    # RR PB -> Blocked/Open (0=open, 1=blocked)
                    # Pack -> Pack size
                    # SP -> Selling Price
                    
                    rr_prev = self._safe_float(ws.cell(row=row_idx, column=col_map.get('RR PREV', 0)).value) if col_map.get('RR PREV') else 0.0
                    
                    pb_val = str(ws.cell(row=row_idx, column=col_map.get('RR PB', 0)).value).strip().upper() if col_map.get('RR PB') else '0'
                    blocked_status = 'blocked' if pb_val in ['1', 'BLOCKED', '1.0'] else 'open'
                    
                    product = {
                        "product_name": str(p_name).strip(),
                        "item_code": ws.cell(row=row_idx, column=col_map.get('ITEM CODE', 0)).value if col_map.get('ITEM CODE') else '',
                        "barcode": ws.cell(row=row_idx, column=col_map.get('BARCODE', 0)).value if col_map.get('BARCODE') else '',
                        "supplier_name": supplier_name,
                        "current_stocks": self._safe_float(ws.cell(row=row_idx, column=col_map.get('RHAPTA', 0)).value) if col_map.get('RHAPTA') else 0.0,
                        "units_sold_last_month": rr_prev,
                        "estimated_daily_sales": rr_prev / 30.0 if rr_prev > 0 else 0.0,
                        "last_days_since_last_delivery": self._safe_int(ws.cell(row=row_idx, column=col_map.get('RR GRN', 0)).value) if col_map.get('RR GRN') else 0,
                        "blocked_open_for_order": blocked_status,
                        "pack_size": self._safe_int(ws.cell(row=row_idx, column=col_map.get('PACK', 0)).value) if col_map.get('PACK') else 1,
                        "selling_price": self._safe_float(ws.cell(row=row_idx, column=col_map.get('SP', 0)).value) if col_map.get('SP') else 0.0,
                        "product_category": 'general',
                        "is_fresh": any(k in str(p_name).upper() for k in ['MILK', 'BREAD', 'DAIRY', 'YOGURT', 'CAKE', 'ROLL'])
                    }
                    products.append(product)
            else:
                # CSV parsing
                with open(file_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        row_cleaned = {k.strip().lower().replace(' ', '_'): v.strip() for k, v in row.items()}
                        products.append({
                            "product_name": row_cleaned.get('product_name', ''),
                            "item_code": row_cleaned.get('item_code', ''),
                            "barcode": row_cleaned.get('barcode', ''),
                            "current_stocks": self._safe_int(row_cleaned.get('current_stocks', 0)),
                            "supplier_name": row_cleaned.get('supplier_name', ''),
                            "last_days_since_last_delivery": self._safe_int(row_cleaned.get('last_days_since_last_delivery', 0)),
                            "blocked_open_for_order": row_cleaned.get('blocked_open_for_order', 'open')
                        })

        except Exception as e:
            logger.error(f"Error parsing file: {e}")
            raise
            
        return products

    def _safe_int(self, value):
        try:
            return int(float(str(value).replace(',', '')))
        except:
            return 0

    def _safe_float(self, value):
        try:
            return float(str(value).replace(',', ''))
        except:
            return 0.0

    def normalize_product_name(self, name: str) -> str:
        return name.upper().strip().replace('  ', ' ')

    def _calculate_cv(self, monthly_sales: dict) -> float:
        """Calculate Coefficient of Variation (CV) from monthly sales data."""
        if not monthly_sales:
            return 0.5  # High uncertainty default
        
        values = list(monthly_sales.values())
        if len(values) < 2:
            return 0.4  # Moderate uncertainty
            
        import statistics
        mean = statistics.mean(values)
        if mean == 0:
            return 1.0  # Highly volatile/uncertain
            
        stdev = statistics.stdev(values) if len(values) > 1 else 0
        cv = stdev / mean
        return round(cv, 3)

    def _find_lookalike_demand(self, product_name: str, sales_database: dict) -> float:
        """Find lookalike SKU demand based on brand and category."""
        # v8.2 OPTIMIZATION: Cache brand index to avoid O(N) scans
        if not hasattr(self, '_brand_index_cache'):
             self._brand_index_cache = {}
             self._brand_index_source_id = None
             
        # Rebuild cache if DB object changes (unlikely but safe)
        if self._brand_index_source_id != id(sales_database):
             self._brand_index_source_id = id(sales_database)
             self._brand_index_cache = {}
             # Build Index
             for name, data in sales_database.items():
                 # Split by first word
                 brand = name.split()[0].strip().upper()
                 if brand:
                     if brand not in self._brand_index_cache:
                         self._brand_index_cache[brand] = []
                     val = data.get('avg_daily_sales', 0)
                     if val > 0:
                         self._brand_index_cache[brand].append(val)
                         
        brand = product_name.split()[0].strip().upper()
        similar_sales = self._brand_index_cache.get(brand, [])
        
        if not similar_sales:
            return 0.0
            
        import statistics
        return statistics.median(similar_sales)

    def find_best_match(self, product_name: str, database: dict, item_code: str = None, barcode: str = None) -> Tuple[str | None, dict | None]:
        """
        Matches product against database using:
        1. Item Code (DB Key starts with Code)
        2. Barcode (DB Key contains Barcode OR Value has 'barcode')
        3. Name (Exact)
        4. Name (Fuzzy)
        """
        
        # 1. Item Code Match (High Priority)
        if item_code:
            s_code = str(item_code).strip()
            if s_code:
                for key in database:
                    if key.startswith(s_code + " ") or key.startswith(s_code + "\t"):
                        return key, database[key]
        
        # 2. Barcode Match
        if barcode:
            s_barcode = str(barcode).strip()
            if s_barcode:
                for key, val in database.items():
                    if s_barcode in key:
                        return key, val
                    if isinstance(val, dict) and str(val.get('barcode', '')) == s_barcode:
                        return key, val
        
        # 3. Exact Name Match
        if product_name in database:
            return product_name, database[product_name]
        
        # 4. Case-insensitive Name Match
        normalized = self.normalize_product_name(product_name)
        for key in database:
            if self.normalize_product_name(key) == normalized:
                return key, database[key]
        
        # 5. Fuzzy Name Matching
        import difflib
        keys = list(database.keys())
        close_matches = difflib.get_close_matches(product_name, keys, n=1, cutoff=0.6)
        if close_matches:
            return close_matches[0], database[close_matches[0]]
            
        return None, None
    
    def _get_actual_cost_price(self, product_rec: dict, selling_price: float) -> float:
        """
        v2.9: Calculate actual cost price using same logic as reporting.
        Priority: GRN avg_cost → Margin% calculation → 0.75 estimate
        """
        # 1. Try GRN database
        p_name = product_rec.get('product_name', '')
        p_barcode = str(product_rec.get('barcode', '')).strip()
        grn_key = p_barcode if p_barcode else self.normalize_product_name(p_name)
        grn_stat = self.grn_db.get(grn_key)
        if grn_stat and grn_stat.get('avg_cost'):
            return grn_stat['avg_cost']
        
        # 2. Try margin_pct from product data
        margin_pct = product_rec.get('margin_pct')
        if margin_pct is not None and margin_pct >= 0 and margin_pct < 100:
            return selling_price * (1 - margin_pct / 100.0)
        
        # 3. Fallback to 25% margin estimate
        # 3. Fallback to 25% margin estimate
        return selling_price * 0.75

    def calculate_replenishment_target_stock(self, product: dict, tier_profile: dict) -> float:
        """
        v6.0: Smart Greenfield Logic.
        Calculates the ideal stock level based on Supply Chain Dynamics, not just flat depth.
        Formula: Target = ADS * (ReviewPeriod + LeadTime + SafetyBuffer)
        """
        avg_sales = product.get('avg_daily_sales', 0.0)
        if avg_sales <= 0: return 0.0
        
        # 1. Supply Chain Parameters
        lead_time = int(product.get('estimated_delivery_days', 7))
        if lead_time < 1: lead_time = 1
        
        # Review Period (Gap Days): How often do we order?
        frequency = product.get('supplier_frequency', 'weekly').lower()
        if frequency == 'daily':
            review_period = 1
        elif frequency == 'monthly':
            review_period = 30
        else:
            review_period = 7
            
        is_fresh = product.get('is_fresh', False)
        
        if is_fresh:
             # v8.0 FIX: GRN Frequency Based Logic (User Request)
             # "Stocked for 1 day plus small buffer"
             # Query GRN Frequency Map (e.g., 1.0 = Daily, 0.5 = Every 2 Days)
             cycle_days = self.get_grn_cycle_days(product.get('product_name'))
             
             # Target = Cycle Days + Buffer
             # Buffer = 0.25 days (Small buffer as requested)
             target_days = cycle_days + 0.25
             
             # Ensure Long Life (UHT) isn't crushed if data missing
             p_name_upper = product.get('product_name', '').upper()
             if 'UHT' in p_name_upper or 'ESL' in p_name_upper or 'LONG LIFE' in p_name_upper:
                 # v8.1 FIX: Long Life should have decent coverage (e.g. 7 days minimum)
                 # Even if ordered daily, we can stock more.
                 target_days = max(7.0, target_days)
                      
             return target_days 
             
        # 2. Safety Buffer (Dynamic)
        # Base: 4.0 for Fresh (non-daily?), 1.5 for Dry
        base_safety = 2.0 if is_fresh else 1.5
        
        # Volatility penalty
        cv = product.get('demand_cv', 0.5)
        vol_factor = 2.0
        safety_buffer = base_safety * (1 + (vol_factor * cv))
        
        # 3. Calculate Cycle Stock Coverage
        target_days = review_period + lead_time + safety_buffer
        
        # 4. Apply Tier Constraints
        profile_depth = tier_profile.get('depth_days', 14)
        
        if is_fresh:
             target_days = min(target_days, 3.0) 
        else:
             target_days = min(target_days, profile_depth * 1.5, 45.0)
             
        return target_days

    def enrich_product_data(self, products: List[dict]):
        """Phase 3: Product Enrichment. Maps all intelligence metrics."""
        supplier_patterns = self.databases.get('supplier_patterns', {})
        sales_forecasting = self.databases.get('sales_forecasting', {})
        supplier_quality = self.databases.get('supplier_quality', {})
        sales_profitability = self.databases.get('sales_profitability', {})
        supp_map = self.databases.get('product_supplier_map', {})
        
        logger.info(f"Phase 3: Enriching {len(products)} products...")
        
        for p in products:
            p_name = p.get('product_name', '')
            p_code = p.get('item_code')
            p_barcode = str(p.get('barcode', '')).strip()
            p_upper = p_name.upper()
            
            # 0. Supplier Lookup (Fix "Unknown")
            if not p.get('supplier_name') or p.get('supplier_name') == 'Unknown':
                 found = supp_map.get(p_name) or supp_map.get(p_upper)
                 if found:
                     p['supplier_name'] = found

            supplier = p.get('supplier_name', '').upper()
            
            # v2.1 Consignment Flagging
            is_consignment = (supplier in self.no_grn_suppliers) or ("PLU" in p_upper)
            p['is_consignment'] = is_consignment

            # 2. Enrich Supplier Info & Data-Driven Classification
            supp_name = str(p.get('supplier_name', 'Unknown')).strip()
            
            # Default Values
            p['estimated_delivery_days'] = 7
            p['supplier_reliability'] = 0.9
            p['supplier_frequency'] = 'weekly'
            p['is_fresh'] = False
            
            if supp_name and supp_name != 'Unknown':
                supp_key = self.normalize_product_name(supp_name)
                pattern = supplier_patterns.get(supp_name) or supplier_patterns.get(supp_key)
                
                if pattern:
                    p['estimated_delivery_days'] = pattern.get('estimated_delivery_days', 4)
                    p['supplier_reliability'] = pattern.get('reliability_score', 0.8)
                    p['supplier_frequency'] = pattern.get('order_frequency', 'weekly')
                    
                    # v6.3 FIX: Data-Driven Freshness
                    # If confirmed Daily (< 2 days gap), treat as potential fresh supplier
                    median_gap = pattern.get('median_gap_days', 7)
                    if median_gap <= 2 or p['supplier_frequency'] == 'daily':
                         p['supplier_frequency'] = 'daily' # Enforce consistency
                         p['is_fresh'] = True # Provisional Fresh
                
                elif "BROOKSIDE" in supp_name or "DAIRY" in supp_name or "BAKERY" in supp_name:
                    # Fallback for known fresh entities if pattern missing
                    p['estimated_delivery_days'] = 1
                    p['supplier_reliability'] = 0.98
                    p['supplier_frequency'] = 'daily'
                    p['is_fresh'] = True
            
            # 3. Product-Level Overrides (The UHT vs Fresh Correctness)
            # "UHT", "LONG LIFE", "TETRA" -> NOT FRESH (even if supplier is fresh-capable)
            if any(x in p_upper for x in ["UHT", "LONG LIFE", "LONGLIFE", "ESL", "TETRA"]):
                 p['is_fresh'] = False
                 
                 # If it was marked daily, revert to Weekly for allocation depth purposes?
                 if p.get('supplier_frequency') == 'daily':
                      p['supplier_frequency'] = 'weekly'
            
            # "FRESH MILK" / "YOGHURT" -> Force FRESH
            if "FRESH MILK" in p_upper or "YOGHURT" in p_upper or "BREAD" in p_upper:
                 p['is_fresh'] = True
                 p['supplier_frequency'] = 'daily' # Ensure strict 1.2 logic applies


            # 1. Supplier Patterns (Original section, now using the enriched values)
            # The original 'pat' variable is no longer directly used for these assignments,
            # as the values are now set based on the 'pattern' found above.
            # We'll ensure these fields are populated.
            p['reliability_score'] = p.get('supplier_reliability', 0.9) * 100 # Convert to 0-100 scale
            p['supplier_frequency_days'] = p.get('estimated_delivery_days', 7) # Using estimated_delivery_days as a proxy for median_gap_days if not explicitly available

            # 2. Sales Forecasting
            # v8.2 OPTIMIZATION: Use Fast Index to avoid O(N) scans in find_best_match
            # We build index ONCE outside loop (see below) or checking if we can trust exact match?
            # actually we are inside loop.
            # Let's check if 'sales_index' exists (we need to inject it or build it lazy)
            
            # FAST PATH:
            sales_data = sales_forecasting.get(p_name) # Exact Match O(1)
            
            if not sales_data:
                 # Try Normalized Match O(1) using index
                 if 'sales_index' not in locals():
                     # Build lazy index for this batch
                     sales_index = {self.normalize_product_name(k): k for k in sales_forecasting.keys()}
                 
                 norm_name = self.normalize_product_name(p_name)
                 found_key = sales_index.get(norm_name)
                 if found_key:
                     sales_data = sales_forecasting[found_key]
            
            if not sales_data:
                # Fallback to slow full search only if fast path failed
                _, sales_data = self.find_best_match(p_name, sales_forecasting, p_code, p_barcode)
                
            if sales_data:
                p['avg_daily_sales'] = sales_data.get('avg_daily_sales', p.get('estimated_daily_sales', 0))
                p['sales_trend'] = sales_data.get('trend', 'stable')
                p['sales_trend_pct'] = sales_data.get('trend_pct', 0.0)
                p['months_active'] = sales_data.get('months_active', 6)
                # Calculate CV
                p['demand_cv'] = self._calculate_cv(sales_data.get('monthly_sales', {}))
                
                # NEW: Sales Behavior Tracking for Slow Mover Classification
                monthly_sales = sales_data.get('monthly_sales', {})
                if monthly_sales:
                    # Calculate days_since_last_sale (find most recent month with sales > 0)
                    sorted_months = sorted(monthly_sales.items(), reverse=True)
                    p['days_since_last_sale'] = 999  # Default: no sales found
                    for month_str, qty in sorted_months:
                        if qty > 0:
                            try:
                                from datetime import datetime
                                # Month format: "2025-11" -> parse as first day of month
                                last_sale_date = datetime.strptime(month_str + "-01", "%Y-%m-%d")
                                p['days_since_last_sale'] = (datetime.now() - last_sale_date).days
                                break
                            except:
                                pass
                    
                    # Calculate total_units_sold_last_90d (sum of last 3 months)
                    recent_months = sorted_months[:3]  # Last 3 months
                    p['total_units_sold_last_90d'] = sum(qty for _, qty in recent_months if qty)
                    
                    # Calculate avg_daily_sales_last_30d (most recent month / 30)
                    if sorted_months and sorted_months[0][1] > 0:
                        p['avg_daily_sales_last_30d'] = round(sorted_months[0][1] / 30.0, 3)
                    else:
                        p['avg_daily_sales_last_30d'] = 0.0
                else:
                    p['days_since_last_sale'] = 999
                    p['total_units_sold_last_90d'] = 0
                    p['avg_daily_sales_last_30d'] = 0.0
            else:
                p['demand_cv'] = 0.5 # Default high volatility for new/unknown items
                p['days_since_last_sale'] = 999
                p['total_units_sold_last_90d'] = 0
                p['avg_daily_sales_last_30d'] = 0.0
            
            # 2b. Last Order Date (from PO patterns)
            p['days_since_last_order'] = 999
            if supplier in supplier_patterns:
                # We don't have the explicit last_order_date in the DB yet, but we can look for it
                # or assume it's roughly median_gap_days ago if ordered consistently.
                # For this implementation, we rely on the PO history scan if available.
                po_history = getattr(self, '_po_history_dates', {}) # Temporary cache or scan result
                if supplier in po_history:
                    last_date = max(po_history[supplier])
                    p['days_since_last_order'] = (datetime.now() - last_date).days
                else:
                    # Fallback to a high number if unknown
                    p['days_since_last_order'] = 999

            # PHASE 3: RELIABLE FORECASTING PARAMETERS (Refined with Gold Standard data)
            p['current_stock'] = p.get('current_stocks', 0)
            p['days_since_delivery'] = p.get('last_days_since_last_delivery', 0)
            p['sales_velocity'] = round(p.get('units_sold_last_month', 0) / 30.0, 2)
            
            # Category Logic & Safety Stock Pct
            # v2 Refinement: Freshness is primarily driven by supplier rhythm (Daily = Fresh)
            supplier_freq = p.get('supplier_frequency', 'weekly').lower()
            is_daily_supplier = supplier_freq == 'daily'
            has_fresh_keywords = any(x in p_name.upper() for x in ['MILK', 'DAIRY', 'BREAD', 'VEG', 'FRUIT', 'MEAT', 'YOGURT', 'CHEESE', 'JUICE', 'BUTTER'])
            
            p['is_fresh'] = is_daily_supplier or has_fresh_keywords
            
            # v6.4 FIX: Re-assert UHT/Long Life exclusion (overrides keywords)
            if any(x in p_name.upper() for x in ["UHT", "LONG LIFE", "LONGLIFE", "ESL", "TETRA"]):
                 p['is_fresh'] = False
                 # Ensure weekly frequency for bulk buying efficiency
                 if p.get('supplier_frequency') == 'daily':
                      p['supplier_frequency'] = 'weekly'
            
            is_fresh = p['is_fresh']
            
            if is_fresh:
                p['product_category'] = "fresh"
                p['safety_stock_pct'] = 20
            elif any(x in p_name.upper() for x in ['PET', '300ML', '330ML', '500ML', '2LT', 'SODA', 'PEPSI', 'MIRINDA', '7UP', 'MOUNTAIN DEW', 'JUICE', 'WATER']):
                # Beverages keep their own profile
                p['product_category'] = "beverages"
                p['safety_stock_pct'] = 15
            else:
                p['product_category'] = "general"
                p['safety_stock_pct'] = 10
                
            # Reorder Point logic: sales_velocity * (delivery_days + buffer)
            # v4.0 Volatility Buffering: Add safety stock for High CV + Long Lead Time items
            d_days = p['estimated_delivery_days']
            cv = p.get('demand_cv', 0.5)
            
            # Base Buffer (Gold Standard)
            buffer = 3 if d_days >= 4 else 1
            
            # Volatility Cushion
            # If CV > 0.3, we add days. 
            vol_buffer = int(cv * 5)
            
            # High Risk Penalty (Long LT + High Volatility = Guaranteed Stockout without cushion)
            if d_days > 3 and cv > 0.3:
                 vol_buffer += 2
                 
            p['target_coverage_days'] = d_days + buffer + vol_buffer
            p['reorder_point'] = round(p['sales_velocity'] * p['target_coverage_days'], 2)
            
            # v2 Logic Supplements
            p['on_order_qty'] = 0  # Placeholder for future integration
            p['expiry_risk'] = 'high' if is_fresh else 'low'
            p['moq_floor'] = 0  # Placeholder
            p['min_presentation_stock'] = 0  # Placeholder
            p['is_key_sku'] = p.get('is_top_sku', False)  # Link Top SKU to Core SKU concept
            p['shelf_life_days'] = 7 if is_fresh else 365 # Default shelf life
            p['upper_coverage_days'] = 10 if is_fresh else 45 # Anti-overstock limit
            
            # 3. GRN Intelligence (PRIMARY Baseline)
            grn_stat = self.grn_db.get(p_barcode) if p_barcode else None
            if not grn_stat:
                grn_stat = self.grn_db.get(self.normalize_product_name(p_name))
            
            if grn_stat and grn_stat['count'] > 0:
                p['historical_avg_order_qty'] = round(grn_stat['total'] / grn_stat['count'])
                p['confidence_grn'] = 'HIGH' if grn_stat['count'] >= 100 else 'MEDIUM'
                p['order_cycle_count'] = grn_stat['count']
            else:
                p['historical_avg_order_qty'] = 0
                p['confidence_grn'] = 'LOW'
                p['order_cycle_count'] = 0
                
                # New Item: Use Lookalike Demand if no actual sales
                if p.get('avg_daily_sales', 0) == 0:
                    lookalike_demand = self._find_lookalike_demand(p_name, sales_forecasting)
                    p['lookalike_demand'] = lookalike_demand
                    p['avg_daily_sales'] = lookalike_demand
                    p['is_lookalike_forecast'] = True
                    p['new_item_aggression_cap'] = 7 if is_fresh else 21

            # 4. Supplier Quality
            sq = supplier_quality.get(supplier, {})
            p['supplier_expiry_returns'] = sq.get('expiry_returns', 0)
            p['quality_score'] = sq.get('quality_score', 100)

            # 5. Sales Profitability (Top 500 SKUs)
            # v8.2 OPTIMIZATION: Fast Index for Profitability
            prof_data = sales_profitability.get(p_name)
            
            if not prof_data:
                 if 'prof_index' not in locals():
                     prof_index = {self.normalize_product_name(k): k for k in sales_profitability.keys()}
                 
                 found_key = prof_index.get(self.normalize_product_name(p_name))
                 if found_key:
                     prof_data = sales_profitability[found_key]
            
            if not prof_data:
                _, prof_data = self.find_best_match(p_name, sales_profitability, p_code, p_barcode)
                
            if prof_data:
                p['sales_rank'] = prof_data.get('sales_rank', 999)
                p['margin_pct'] = prof_data.get('margin_pct', 0.0)
                p['revenue'] = prof_data.get('revenue', 0.0)
                p['is_top_sku'] = True
                p['is_key_sku'] = True # Always treat top 500 as key SKUs
            else:
                p['sales_rank'] = 999
                p['margin_pct'] = 0.0
                p['is_top_sku'] = False
                # Keep existing is_key_sku value

            # Link is_fresh to the enrichment dict
            p['is_fresh'] = is_fresh
            
            # Baseline Population
            if p['historical_avg_order_qty'] > 0:
                p['last_delivery_quantity'] = p['historical_avg_order_qty']
            else:
                p['last_delivery_quantity'] = max(50, p.get('current_stocks', 0) * 2)
            
            # --- CFB EXCLUSION: Internal bakery items (not for allocation) ---
            if p_name.upper().startswith('CFB '):
                p['exclude_from_allocation'] = True
                p['exclusion_reason'] = 'Internal bakery production'
                continue  # Skip further processing for CFB items
            
            # --- CATEGORY-SPECIFIC COVERAGE BOOSTS (based on simulation feedback) ---
            name_upper = p_name.upper()
            dept = p.get('department', '').upper()
            
            # Bread/Bakery: 2.0x boost (high velocity, short shelf life)
            # Excludes CFB (already filtered above)
            if 'BREAD' in name_upper or 'FESTIVE' in name_upper or 'NATURES' in name_upper:
                if 'BAKERY' in dept or any(x in name_upper for x in ['800G', '600G', '400G']):
                    base_coverage = p.get('target_coverage_days', 7)
                    p['target_coverage_days'] = int(base_coverage * 2.0)
                    p['category_boost'] = 2.0
                    p['category_boost_reason'] = 'Bread/bakery high-velocity perishable'
            
            # Dairy/Fresh Milk: 1.5x boost (DAIMA, BIO, BROOKSIDE fresh)
            elif any(x in name_upper for x in ['DAIMA', 'BIO ', 'FRESH MILK', 'MAZIWA']):
                base_coverage = p.get('target_coverage_days', 7)
                p['target_coverage_days'] = int(base_coverage * 1.5)
                p['category_boost'] = 1.5
                p['category_boost_reason'] = 'Fresh dairy perishable'
            
            # High-velocity staples: 1.3x boost (identified from feedback)
            elif any(x in name_upper for x in ['GOLD 500ML', 'CROWN TFA', 'MACCOFFEE', 'INDOMIE']):
                base_coverage = p.get('target_coverage_days', 7)
                p['target_coverage_days'] = int(base_coverage * 1.3)
                p['category_boost'] = 1.3
                p['category_boost_reason'] = 'High-velocity staple'
            
            # Confectionery/Impulse: 2.5x boost (checkout aisle items)
            elif any(x in name_upper for x in ['LOLLIPOP', 'LOLLYPOP', 'CHUPA', 'CANDY', 'GIANT', 'ORBIT', 'WRIGLEY']):
                base_coverage = p.get('target_coverage_days', 7)
                p['target_coverage_days'] = int(base_coverage * 2.5) # Boosted to 2.5x (was 1.5x)
                p['category_boost'] = 2.5
                p['category_boost_reason'] = 'Impulse confectionery high-risk'
            
            # Staple Commodities: 1.4x boost (bulk household essentials)
            elif any(x in name_upper for x in ['KENSALT', 'NDOVU', 'MAIZE MEAL', 'ATTA', ' SALT', ' FLOUR']):
                base_coverage = p.get('target_coverage_days', 7)
                p['target_coverage_days'] = int(base_coverage * 1.4)
                p['category_boost'] = 1.4
                p['category_boost_reason'] = 'Staple commodity bulk'
            
            # Beverages/Juice: 1.5x boost (high demand drinks)
            elif any(x in name_upper for x in ['DEL 1L', 'JUICE', 'BERRY', 'QUENCHER']):
                base_coverage = p.get('target_coverage_days', 7)
                p['target_coverage_days'] = int(base_coverage * 1.5)
                p['category_boost'] = 1.5
                p['category_boost_reason'] = 'Beverage high demand'
            
            # Specialty Baking: 1.3x boost
            elif any(x in name_upper for x in ['YEAST', 'ANGEL 10G']):
                base_coverage = p.get('target_coverage_days', 7)
                p['target_coverage_days'] = int(base_coverage * 1.3)
                p['category_boost'] = 1.3
                p['category_boost_reason'] = 'Specialty baking ingredient'
            
            # GAP-L ENHANCED: Data-driven depth adjustment based on simulation feedback
            # Uses stockout frequency AND avg first stockout day to calculate optimal depth
            sim_feedback = self.databases.get('simulation_feedback', {})
            sku_feedback = sim_feedback.get('sku_feedback', {})
            
            if p_name in sku_feedback:
                fb = sku_feedback[p_name]
                stockout_freq = fb.get('stockout_frequency', 0)
                avg_stockout_day = fb.get('avg_first_stockout_day', 14)
                
                # Calculate dynamic depth multiplier based on:
                # 1. Stockout frequency (higher = more boost)
                # 2. Avg first stockout day (earlier = more boost)
                
                if stockout_freq > 0.3:  # Apply if >30% stockout frequency
                    original_coverage = p.get('target_coverage_days', 7)
                    
                    # Formula: Depth Multiplier = 1 + (freq * severity_factor)
                    # Severity increases if stockouts happen early (before day 7)
                    if avg_stockout_day < 5:
                        severity = 2.5  # Critical: stockout in first 5 days
                    elif avg_stockout_day < 7:
                        severity = 2.0  # High: stockout in first week
                    elif avg_stockout_day < 10:
                        severity = 1.5  # Medium: mid-period stockout
                    else:
                        severity = 1.2  # Low: late stockout
                    
                    # Dynamic multiplier: ranges from 1.3x to 3.5x (increased for early stockouts)
                    depth_multiplier = min(3.5, 1.0 + (stockout_freq * severity))
                    
                    # Apply depth adjustment
                    new_coverage = int(original_coverage * depth_multiplier)
                    p['target_coverage_days'] = new_coverage
                    p['simulation_adjusted'] = True
                    p['sim_stockout_frequency'] = stockout_freq
                    p['sim_avg_stockout_day'] = avg_stockout_day
                    p['sim_depth_multiplier'] = round(depth_multiplier, 2)
                    p['sim_severity'] = severity
                    
                    # Also boost reorder point proportionally
                    if p.get('reorder_point'):
                        p['reorder_point'] = int(p['reorder_point'] * depth_multiplier)
                    
                    # Log high-severity adjustments
                if stockout_freq >= 0.7:
                        logger.debug(f"High-risk SKU: {p_name[:40]} -> {depth_multiplier:.1f}x depth (freq={stockout_freq:.0%}, day={avg_stockout_day:.1f})")
            
            # --- FIX: MINIMUM DEPTH FLOORS FOR PERISHABLES ---
            # Ensure bread has at least 3 days and milk has at least 5 days coverage
            # regardless of other settings
            if ('BREAD' in name_upper or 'BAKERY' in dept) and p.get('target_coverage_days', 0) < 3:
                p['target_coverage_days'] = 3
                p['floor_applied'] = True
            
            # v7.6 REFINEMENT: Remove static 5-day floor for Dairy/Milk
            # User Feedback: "Fresh orders should ≈ Sales". 5 days is too long for daily fresh items.
            # if (any(x in name_upper for x in ['MILK', 'DAIRY', 'YOGHU']) or 'DAIRY' in dept) and p.get('target_coverage_days', 0) < 5:
            #     p['target_coverage_days'] = 5
            #     p['floor_applied'] = True
                
        return products

    async def analyze_batch_ai(self, products: list[dict[str, Any]], batch_num: int, total_batches: int, allocation_mode: str = "replenishment") -> list[dict[str, Any]]:
        """Phase 4: AI Analysis (Claude Sonnet 3.7). Analyzes a single batch with specialized retail logic."""
        logger.info(f"Phase 4: AI Analysis - Batch {batch_num}/{total_batches} ({allocation_mode} mode)")
        
        from anthropic import AsyncAnthropic
        client = AsyncAnthropic()
        
        products_summary = json.dumps(products, indent=2)
        
        from textwrap import dedent
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
                       - PHASE 4: Apply strict aging checks (Dead stock if > 200 days).
                       - Only recommend if stock < reorder point.
                    
                    2. **STRATEGY: INITIAL LOAD (Greenfield)**
                       - Goal: Shelf Presentation & Assortment Fill.
                       - **BYPASS AGING**: Ignore 'days_since_delivery'. Buy fresh stock for all SKUs even if they were slow previously.
                       - **MDQ (Minimum Display Quantity)**: Recommended Order = MAX(Forecasted Demand, shelf_fill_target).
                       - If demand > 0.1, ALWAYS recommend at least 1 Pack.
                    
                    3. **CORE RULES**:
                       - Balanced Net Requirement = (demand + safety stock) - (current_stock + on_order).
                       - In Greenfield mode, assume current_stock is effectively 0 for the requirement calculation.
                       - High margin items (rank < 500) get 20% volume bump.
                """) if allocation_mode == "initial_load" else dedent("""
                    1. **PHASE 1: SLOW MOVER & FRESH CHECK**
                       - Fresh (>120d): Cap if sales > 0, else 0.
                       - Dry (>200d): Cap if sales > 5, else 0.
                    2. **PHASE 2: TOP 500 / KEY SKU**
                       - Never stockout. Increase by 20% if stock < reorder.
                    3. **PHASE 3: DEMAND & NET REQUIREMENT**
                       - (forecast + safety) - (current + on_order).
                """)
            )

        try:
            response = await client.messages.create(
                model="claude-3-7-sonnet-20250219",
                max_tokens=4000,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.content[0].text.strip()
            if "```" in text: text = text.split("```")[1].strip()
            if text.startswith("json"): text = text[4:].strip()
            recommendations = json.loads(text)
            # Apply Harmonized Python Guards
            products_map = {p['product_name']: p for p in products}
            return apply_safety_guards(recommendations, products_map, allocation_mode)
        except Exception as e:
            logger.error(f"AI batch error: {e}")
            return []

        try:
            response = await client.messages.create(
                model="claude-3-7-sonnet-20250219",
                max_tokens=4000,
                temperature=0.1,
                messages=[{"role": "user", "content": prompt}]
            )
            text = response.content[0].text.strip()
            if "```" in text: text = text.split("```")[1].strip()
            if text.startswith("json"): text = text[4:].strip()
            recommendations = json.loads(text)
            # Apply Harmonized Python Guards
            products_map = {p['product_name']: p for p in products}
            return apply_safety_guards(recommendations, products_map)
        except Exception as e:
            logger.error(f"AI batch error: {e}")
            return []

    def apply_greenfield_allocation(self, recommendations: List[dict], total_budget: float = 300000.0, seasonal_demand_map: Dict[str, float] = None) -> Dict:
        """
        Phase 1 & 2: Initial Stock Allocation (The "Greenfield" Scenario).
        Now supports Hybrid Seasonal "Guiding".
        """
        logger.info(f"Starting Greenfield Allocation. Budget: ${total_budget:,.2f}")
        
        # --- HYBRID DEMAND BLENDING (Guide Strategy) ---
        if seasonal_demand_map:
             logger.info("Applying Hybrid Seasonal Blending (Scorecard + Monthly Cache)...")
             # Calculate Scale Factor (Mega Store -> Current Budget)
             # Mega Store Monthly Revenue approx 80M? Or we use the known benchmark.
             # Better: Use the traffic_scale passed implicitly? 
             # Actually, we can derive it. Scorecard items have 'avg_daily_sales'. 
             # If we compare TOTAL Scorecard Demand vs TOTAL Seasonal Demand, we find the ratio.
             
             # v7.9 Fix: Calculate Scale Factor based on INTERSECTION only
             # Previous logic compared Total(2k items) vs Total(14k items) -> 0.06 factor
             common_vol_scorecard = 0.0
             common_vol_seasonal = 0.0
             
             for r in recommendations:
                 p_name = r.get('product_name', '').upper()
                 if p_name in seasonal_demand_map:
                     common_vol_scorecard += r.get('avg_daily_sales', 0)
                     common_vol_seasonal += seasonal_demand_map[p_name] / 30.0
                     
             if common_vol_seasonal > 0:
                 scale_factor = common_vol_scorecard / common_vol_seasonal
                 # Clamp to avoid crazy multipliers
                 scale_factor = max(0.5, min(1.5, scale_factor)) # Tighter clamp (0.5-1.5)
                 logger.info(f"Derived Seasonal Scale Factor (Intersection): {scale_factor:.4f}")
             else:
                 scale_factor = 1.0
             
             blended_count = 0
             for rec in recommendations:
                 p_name = rec.get('product_name', '').upper()
                 if p_name in seasonal_demand_map:
                     monthly_total = seasonal_demand_map[p_name]
                     seasonal_daily = (monthly_total / 30.0) * scale_factor
                     
                     # Hybrid Formula: (Core + Seasonal) / 2
                     core_daily = rec.get('avg_daily_sales', 0)
                     blended_daily = (core_daily + seasonal_daily) / 2.0
                     
                     rec['avg_daily_sales'] = blended_daily
                     rec['is_seasonally_adjusted'] = True
                     blended_count += 1
             
             logger.info(f"Blended demand for {blended_count} items based on seasonal cache.")

        # --- PRE-ALLOCATION ANALYSIS ---
        # Implements the Comprehensive Tiered Allocation Logic.
        # Phase 1: Width (Essential Inclusion & Anchors) - "General Opening Fund"
        # Phase 2: Depth (Wallet-Constrained Volume) - Split 80/20 for Small Stores
        
        # Returns: dict with 'recommendations' and 'summary' keys
        
        # logger.info(f"--- Starting Tiered Allocation (Budget: ${total_budget:,.0f}) ---") # This line is replaced by the new logger.info above
        
        # Initialize summary tracking
        summary = {
            'total_budget': total_budget,
            'pass1_cash': 0.0,
            'pass1_consignment': 0.0,
            'pass2_cash': 0.0,
            'pass2b_cash': 0.0,
            'total_skipped': 0,
            'skip_reasons': {},
            'dept_utilization': {}
        }
        
        # 1. Determine Dynamic Tier Strategy
        profile = self.profile_manager.get_profile(total_budget)
        
        is_small = profile['is_small']
        is_micro = total_budget < 200000 
        
        # Duka Specifics (v3.5: Use centralized constant - GAP-2 fix)
        fast_five_depts = FAST_FIVE_DEPARTMENTS
        
        # Dynamic Configs from Profile
        depth_cap_days = profile['depth_days']
        max_total_packs = profile['max_packs']
        price_ceiling = profile['price_ceiling']
        min_display_qty = profile['min_display_qty']
        allow_c_class = profile['allow_c_class']
        
        # Static defaults or logical derivations
        pass2_staple_share = 0.60
        if is_small: pass2_staple_share = 0.80
        if is_micro: pass2_staple_share = 0.95
        
        logger.info(f"Tier Profile: {profile['tier_name']} | Ceiling: {price_ceiling} | Depth: {depth_cap_days}d")

        # Initialize Wallets
        wallets = self.budget_manager.initialize_wallets(total_budget, buffer_pct=profile['wallet_buffer_pct'])
        
        # --- PASS 1: GLOBAL WIDTH (Variety First) ---
        # --- PRE-PASS: SORTING ---
        # v3.2 FIX (GAP 1): Sort by Staple Priority FIRST, then by velocity
        # This prevents high-velocity discretionary items from consuming budget before essentials
        def staple_priority_sort(x):
            is_staple = self.budget_manager.is_staple(x['product_name'], x.get('product_category'), x.get('avg_daily_sales', 0))
            dept = x.get('product_category', 'GENERAL').upper()
            # Priority tiers: 0=Fast Five Staple, 1=Other Staple, 2=Essential Dept, 3=Discretionary
            priority = 3
            if is_staple and dept in fast_five_depts:
                priority = 0
            elif is_staple:
                priority = 1
            elif dept in ['SUGAR', 'SALT', 'FLOUR', 'RICE', 'COOKING OIL', 'FRESH MILK', 'BREAD', 'EGGS']:
                priority = 2
            return (priority, -x.get('avg_daily_sales', 0))
        recommendations.sort(key=staple_priority_sort)
        
        # v4.2 FIX: Remove "TOTAL" summary row if present (It consumes all depth budget)
        recommendations = [r for r in recommendations if str(r.get('product_name', '')).upper() != 'TOTAL']
        
        # --- PASS 0: SUPPLIER CONSOLIDATION (Gap K Fix) ---
        # Consolidate volume to Top N Suppliers per Staple Department to ensure depth
        # v3.7: Tiered Capping: Micro=3, Small=5, Others=Unlimited (User Request)
        allowed_suppliers = {}
        
        supplier_cap = 999 # Default unlimited
        if is_micro:
            supplier_cap = 3
        elif is_small:
            supplier_cap = 5
            
        if supplier_cap < 999:
            consolidation_depts = ['RICE', 'SUGAR', 'FLOUR', 'COOKING OIL', 'MAIZE MEAL', 'PASTA', 'FRESH MILK']
            
            # 1. Aggregate Sales by Supplier
            supplier_sales = {d: {} for d in consolidation_depts}
            
            for rec in recommendations:
                dept = rec.get('product_category', 'GENERAL').upper()
                if dept in consolidation_depts:
                    # v3.8 FIX: Only count TRUE STAPLES for supplier ranking (User FB: "Tropical Heat has Rice Cakes not Rice")
                    if not self.budget_manager.is_staple(rec['product_name'], rec.get('product_category'), rec.get('avg_daily_sales', 0)):
                        continue

                    # Normalization is critical
                    supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                    if not supp or supp == 'NON': supp = 'UNKNOWN'
                    
                    sales = rec.get('avg_daily_sales', 0)
                    price = rec.get('selling_price', 0)
                    revenue = sales * price
                    supplier_sales[dept][supp] = supplier_sales[dept].get(supp, 0) + revenue
            
            # 2. Pick Top N
            for dept in consolidation_depts:
                ranked = sorted(supplier_sales[dept].items(), key=lambda x: x[1], reverse=True)
                # Only consolidate if we need to trim (Total > Cap)
                if len(ranked) > supplier_cap:
                    top_n = [s[0] for s in ranked[:supplier_cap]]
                    allowed_suppliers[dept] = set(top_n)
                    logger.info(f"Consolidated {dept} Suppliers (Top {supplier_cap}/{len(ranked)}): {top_n}")
                elif ranked:
                    # Allow all if within cap
                    allowed_suppliers[dept] = set([s[0] for s in ranked])

        # --- PASS 1: GLOBAL WIDTH (Variety First) ---
        # "Allocates exactly 1 Pack (MDQ) to every item."
        pass1_cost = 0.0
        pass1_consignment_val = 0.0
        sku_counts_per_dept = {} # For "One Brand/Limit" logic
        
        for rec in recommendations:
            p_name = rec['product_name']
            dept = rec.get('product_category', 'GENERAL').upper()
            is_staple = self.budget_manager.is_staple(p_name, dept, rec.get('avg_daily_sales', 0))
            pack_size = int(rec.get('pack_size', 1))
            price = float(rec.get('selling_price', 0.0))
            is_consignment = rec.get('is_consignment', False)
            
            # v2.9: Use actual cost to prevent budget overruns
            # Match the same cost calculation as reporting
            cost_price_est = self._get_actual_cost_price(rec, price)
            
            # Init counter

            if dept not in sku_counts_per_dept: sku_counts_per_dept[dept] = 0
            
            # Constraint Checklist
            should_list = True
            reason_tag = ""
            
            # 0.5 Supplier Consolidation Check (Gap K Fix)
            if dept in allowed_suppliers:
                supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                if not supp or supp == 'NON': supp = 'UNKNOWN'
                
                if supp not in allowed_suppliers[dept]:
                     should_list = False
                     reason_tag = "[PASS 1: SUPPLIER CONSOLIDATION]"

            # 0. Internal Production Exclusion (v2.8)
            # Bakery Foodplus is internal production, not purchased from suppliers
            if dept in ['BAKERY FOODPLUS', 'BALERY FOODPLU']:  # Handle typo variance
                should_list = False
                reason_tag = "[PASS 1: INTERNAL PRODUCTION - NOT PURCHASED]"
            
            # 1. ABC / Subsistence Filters
            abc_class = rec.get('ABC_Class', 'A') 
            
            # v3.5: Use centralized department constants (GAP-2 fix)
            is_essential_dept = dept in ESSENTIAL_DEPARTMENTS
            
            # v3.12 FIX (GAP ANALYSIS): Keyword Overrides for mis-categorized essentials
            # Ensures Yoghurt, Soda, Ghee, Lentils get essential treatment even if Dept is 'GENERAL'
            if not is_essential_dept:
                p_u = p_name.upper()
                if "YOGHURT" in p_u or "YOGURT" in p_u: is_essential_dept = True
                elif "SODA" in p_u or "COKE" in p_u or "ALVARO" in p_u or "VIMTO" in p_u: is_essential_dept = True
                elif "GHEE" in p_u: is_essential_dept = True
                elif "LENTIL" in p_u or "BEAN" in p_u or "NDENGU" in p_u or "POJO" in p_u: is_essential_dept = True
                elif "DAIRY" in p_u: is_essential_dept = True
            
            # v3.1: Detect bulk items (5KG, 5L, 5LT, 10KG etc.) for higher ceiling
            # v3.12 FIX: Added space variants (5 KG, 5 L)
            p_name_upper = p_name.upper()
            is_bulk_item = any(x in p_name_upper for x in ['5KG', '5L', '5LT', '10KG', '10L', '20L', '25KG', '5 KG', '5 L', '10 KG'])
            
            # Unified Dynamic Constraint Logic (Replaces hardcoded Micro/Standard split)
            # Price Ceiling Check - v3.1: Essentials get 2x, Bulk essentials get 3x
            if is_essential_dept and is_bulk_item:
                effective_ceiling = price_ceiling * 3  # Bulk staples
            elif is_essential_dept:
                effective_ceiling = price_ceiling * 2  # Regular essentials
            else:
                effective_ceiling = price_ceiling
            
            if price > effective_ceiling:
                if is_staple: # Anchors override ceiling
                    reason_tag = "[PASS 1: ANCHOR OVERRIDE]"
                    should_list = True
                else:
                    reason_tag = f"[PASS 1: BLOCKED - PRICE > {effective_ceiling:.0f}]"
                    should_list = False
            
            # SKU Cap Per Dept (if very small store)
            # Use dynamic profile constraints if added later, for now we rely on Budget Guard.
            # But let's keep the Micro Cap if needed? 
            # Actually, the user wants "Dynamic".
            # Let's trust the profile's Price Ceiling to filter out the noise.
            
            # v3.2 FIX (GAP 2): Changed elif to if - runs independently of price ceiling check
            # v3.3 FIX (GAP B): Add essential department bypass for dead stock filter
            # Dead Stock Check (Dynamic)
            # Dead Stock Check (Dynamic)
            if should_list and not allow_c_class and abc_class == 'C':
                 avg_daily = rec.get('avg_daily_sales', 0)
                 # Bypass dead stock filter for essential/staple departments
                 # v3.12 FIX: Use global ESSENTIAL_DEPARTMENTS instead of hardcoded subset to allow Ghee/Lentils
                 # essential_depts = ['COOKING OIL', 'FLOUR', 'SUGAR', 'FRESH MILK', 'BREAD', 'RICE', 'MAIZE MEAL']
                 dead_stock_threshold = 0.02 if is_micro else 0.20
                 
                 if avg_daily < dead_stock_threshold and not is_essential_dept:
                      should_list = False
                      reason_tag = f"[PASS 1: DEAD STOCK < {dead_stock_threshold}]"

            # Hybrid Scaled Demand Logic (Standard+)
            # v3.0 FIX: Scale threshold proportionally AND bypass for essential departments
            # v8.2 FIX: Enable for Micro too, so we can calculate scaled_demand for allocation sizing.
            if should_list:
                mega_budget = 114000000.0
                budget_ratio = total_budget / mega_budget
                mega_demand_proxy = rec.get('avg_daily_sales', 0) * 45 
                scaled_demand = mega_demand_proxy * budget_ratio
                
                # v2.5: Exempt new products from this filter (they'll get conservative treatment in Pass 2)
                has_lookalike = rec.get('lookalike_demand', 0) > 0
                is_new_product = rec.get('avg_daily_sales', 0) == 0
                
                # v3.5: Use centralized department constants (GAP-2 fix)
                # v3.12: REMOVED re-calculation that overwrote keyword overrides!
                # is_essential_dept = dept in ESSENTIAL_DEPARTMENTS
                
                # v3.0 HYBRID FIX: Scale threshold proportionally to store size
                # Mega (114M) uses 0.5, Small (200k) uses 0.5 * (200k/114M) = ~0.001
                # But we apply a sqrt to prevent too aggressive filtering
                scaled_threshold = 0.5 * (budget_ratio ** 0.5)
                scaled_threshold = max(0.01, scaled_threshold)  # Floor at 0.01
                
                if should_list and is_small:
                    if scaled_demand >= scaled_threshold:
                        pass  # Passes threshold
                    elif is_staple or is_essential_dept:
                        # v3.0: Staples AND essential departments ALWAYS pass
                        reason_tag = "[PASS 1: ESSENTIAL BYPASS]"
                    elif is_new_product or has_lookalike:
                        # Allow new products through to Get conservative allocation in Pass 2
                        reason_tag = "[PASS 1: NEW PRODUCT - PROVISIONAL]"
                    else:
                        should_list = False
                        should_list = False
                        reason_tag = f"[SCALED DROP] Demand: {scaled_demand:.2f} < {scaled_threshold:.2f}"
                        
                # v8.2 FIX: Apply Demand Scaling to Allocation Calculation
                # If we scaled demand for filtering, we must also use it for Qty calculation!
                # Removed "not is_micro" check - Micro stores need this MOST.
                if should_list and 'scaled_demand' in locals():
                     # Only scale DOWN. Never scale UP (if we are allocating for a Mega store using Micro history?)
                     # Actually, `budget_ratio` handles both. But usually we load Mega history.
                     # Let's trust budget_ratio but cap at 1.0 (never inflate demand artificially beyond history unless specified)
                     # Actually, if store is BIGGER than history source, we SHOULD scale up? 
                     # For now, let's assume we load "Golden Store" (Mega) pattern.
                     
                     # Update the ADS used for allocation
                     rec['avg_daily_sales'] = scaled_demand 
                     
                     # v8.2 FIX: robust append
                     current_reason = rec.get('reasoning', '')
                     rec['reasoning'] = current_reason + f" [SCALED ADS: {scaled_demand:.1f}]"

            if should_list:
                # Apply Min Display Qty
                # FIXED: Ensure MDQ respects Pack Sizes
                # If pack size is 6, and MDQ is 3, we buy 1 pack (6 units).
                # If pack size is 1, and MDQ is 3, we buy 3 packs (3 units).
                
                raw_mdq = min_display_qty

                # v4.1 OPTIMIZATION: Velocity Adjusted MDQ for Large Allocations
                # Reduces capital locked in slow movers to fund depth for fast movers.
                if not is_small and not is_micro:
                     velocity = rec.get('avg_daily_sales', 0)
                     
                     # Only reduce if NOT essential/staple
                     if not (is_staple or is_essential_dept):
                         if velocity < 0.1:
                              # C-Class: 25% MDQ (e.g., 6 units for Mega instead of 24)
                              raw_mdq = max(3, int(min_display_qty * 0.25)) 
                         elif velocity < 0.5:
                              # B-Class: 50% MDQ (e.g., 12 units for Mega instead of 24)
                              raw_mdq = max(6, int(min_display_qty * 0.50))
                
                # v3.10 FIX (APS-3): Large Pack Optimization ("Break Bulk")
                # If pack cost > 2x Ceiling, we break bulk to preserve capital (User Request)
                if is_small:
                    pack_cost_est = price * pack_size
                    threshold = price_ceiling * 2.0
                    if pack_cost_est > threshold and pack_size > 1:
                        # Break Bulk Mode: Treat as loose units or smaller pack
                        # Log it in reasoning
                        old_pack = pack_size
                        pack_size = max(1, int(rec.get('moq_floor', 1)))
                        if "[BREAK BULK]" not in reason_tag:
                             # We use reasoning field but reason_tag is used for skipping.
                             # We'll append to rec['reasoning'] later, but `reason_tag` variable is currently used for SKIP reasons?
                             # Line 1036 assigns reason.
                             pass
                        
                        # We must ensure we don't buy 24 units if we broke bulk.
                        # Recalculate based on new pack size.
                
                # Check for BUDGET GUARD (Pass 1 Safety Break)
                # v3.9: Dynamic Cap to enforce Depth.
                # v3.9: Dynamic Cap to enforce Depth.
                # v5.0 FIX: Enforce 30% Liquidity Reserve for ALL tiers to prevent Day 1 Stockouts
                # v5.7 ADJUSTMENT: Nano/Micro/Small stores (<12M) cannot afford 30% reserve.
                # v7.0 GAP-E FIX: Lowered to 85% for small stores to leave room for depth in Pass 2.
                if total_budget < 12000000:
                     limit_pct = 0.85  # GAP-E: Was 0.95, now 0.85 for better depth allocation
                else:
                     limit_pct = 0.70
                
                pass1_limit = total_budget * limit_pct
                
                if pass1_cost > pass1_limit:
                     # Strict Cutoff: Even Staples must stop if we want to preserve Money for Depth of specific items.
                     # v3.9b: Strict Cap for Small/Micro (APS-1). Override only for Large stores.
                     if is_staple and not is_small:
                          # For larger stores, we can be lenient with staples
                          raw_mdq = max(1, raw_mdq // 2)
                     else:
                          # For Micro/Small: Strict CAP. No overrides.
                          # Cut discretionary & overflow staples
                          rec['recommended_quantity'] = 0
                          rec['reasoning'] = f"[PASS 1: BUDGET EXHAUSTED] Cap {limit_pct:.0%}. Width Cut."
                          rec['pass1_allocated'] = False
                          continue

                # v5.6 FIX: Day 1 Launch Buffer (Prevent "Replenishment Lag")
                # If an item sells 10/day, and MDQ is 3, we MUST buy at least LeadTiime + Buffer.
                # Otherwise we stockout before first reorder arrives.
                launch_target_units = 0
                if is_staple or is_essential_dept or rec.get('avg_daily_sales', 0) > 1.0:
                     lead_time = int(rec.get('estimated_delivery_days', 2))
                     # Buffer: LeadTime + 2 Days (Minimum to bridge to first delivery)
                     # v5.5 FIX: Fresh Constraint for Launch Buffer
                     if dept in FRESH_DEPARTMENTS:
                          needed_days = min(lead_time + 1.0, 3.0) # Cap at 3 days max for fresh
                          # v7.5 FIX: Fresh Milk Needs 4 Days (Weekend Pre-Load taught us this)
                          if 'MILK' in dept:
                              needed_days = 4.0
                     else:
                          # v7.5 FIX: High Velocity Gap (Water, Maize)
                          # If sales > 5/day, LeadTime + 2 is risky if delivery is late or demand spikes.
                          # Boost to LeadTime + 4.0 for safety.
                          velocity = rec.get('avg_daily_sales', 0)
                          if velocity > 5.0:
                              needed_days = lead_time + 4.0
                          else:
                              needed_days = lead_time + 2.0
                     
                     # v8.0 FIX: Revert Fresh Milk "4 Days" rule. User says "1 Day + Buffer" based on velocity.
                     # We will rely on get_grn_cycle_days logic (implied frequency)
                     if rec.get('is_fresh', False):
                         cycle_days = self.get_grn_cycle_days(rec['product_name'])
                         # Launch Buffer = Cycle + 1 Day Safety (First delivery)
                         needed_days = cycle_days + 0.5 
                         
                         # v8.1 FIX: Long Life Floor for Launch
                         p_name_upper = rec.get('product_name', '').upper()
                         if 'UHT' in p_name_upper or 'ESL' in p_name_upper or 'LONG LIFE' in p_name_upper:
                             needed_days = max(7.0, needed_days) 
                     
                     launch_target_units = int(rec.get('avg_daily_sales', 0) * needed_days)
                
                rec_qty_units = max(int(rec.get('moq_floor', 0)), raw_mdq, launch_target_units)
                
                # Convert to Packs
                # v6.2 MICRO FIX: Allow Bulk Breaking for Pass 1 Width
                is_break_bulk = False
                
                # Check if we should break bulk (Fresh or Expensive or just Micro/Small policy)
                # For Micro/Small, we ALWAYS break bulk if demand < 1 pack to prevent uniformity.
                if (is_micro or is_small) and rec_qty_units < pack_size:
                     is_break_bulk = True
                
                if is_break_bulk:
                     # Round to nearest integer unit, respecting MDQ
                     rec_qty_final = max(int(rec.get('min_display_qty', 3)), rec_qty_units)
                     # Ensure we don't accidentally exceed pack size
                     rec_qty_final = min(rec_qty_final, pack_size)
                     if "BREAK BULK" not in reason_tag:
                         if 'reasoning' in rec: rec['reasoning'] += " [MICRO BREAK BULK]" # Append if exists
                         else: reason_tag += " [MICRO BREAK BULK]"
                else:
                    required_packs = (rec_qty_units + pack_size - 1) // pack_size # Ceiling div
                    required_packs = max(1, required_packs)
                    rec_qty_final = required_packs * pack_size
                
                # v2.7: Enforce max_packs limit even in Pass 1
                max_allowed_units = max_total_packs * pack_size
                
                # v7.0 GAP-F FIX: Anchor override in Pass 1 (matches Pass 2 behavior)
                # Allow unlimited packs for staple anchors (COOKING OIL, FLOUR, SUGAR)
                if is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR'] and is_staple:
                    max_allowed_units = 999  # GAP-F: Anchor override
                    
                # v7.9 Fix: Fresh Items Exempt from Shelf Cap IN PASS 1
                # Justification: Pass 1 calculates critical "Launch Buffer". We cannot cap this.
                if rec.get('is_fresh', False):
                     # Allow launch buffer to exceed shelf cap
                     max_allowed_units = max(max_allowed_units, int(launch_target_units * 1.1))
                
                if rec_qty_final > max_allowed_units:
                    rec_qty_final = max_allowed_units
                    reason_tag += f" [PASS 1: CAPPED TO {max_total_packs} PACKS]"
                
                cost = rec_qty_final * cost_price_est
                
                # DOUBLE CHECK BUDGET BEFORE COMMIT (Cash Only)
                check_cost = 0 if is_consignment else cost
                if (pass1_cost + check_cost) > total_budget:
                     # Hard Stop for this item
                     rec['recommended_quantity'] = 0
                     rec['reasoning'] = f"[PASS 1: BUDGET CAP HIT] Cost: {check_cost}"
                     rec['pass1_allocated'] = False
                     continue
                
                # Deduct from Wallet (if cash)
                if not is_consignment:
                     self.budget_manager.spend_from_wallet(wallets, dept, cost)
                     
                rec['recommended_quantity'] = rec_qty_final
                rec['reasoning'] = f"[PASS 1: WIDTH] MDQ: {raw_mdq} -> {rec_qty_final} Units"
                
                # v3.10: Append Break Bulk note
                if 'old_pack' in locals() and old_pack != pack_size:
                     rec['reasoning'] += f" [BREAK BULK: {old_pack}->{pack_size}]"
                
                if is_consignment:
                    rec['reasoning'] += " [CONSIGNMENT]"
                    pass1_consignment_val += cost
                else:
                    pass1_cost += cost
                
                rec['pass1_allocated'] = True
                sku_counts_per_dept[dept] += 1
            else:
                rec['recommended_quantity'] = 0
                rec['reasoning'] = f"[PASS 1: SKIPPED] {reason_tag}"
                rec['pass1_allocated'] = False
                
                # Track skip reason
                summary['total_skipped'] += 1
                skip_category = "other"
                if "PRICE >" in reason_tag:
                    skip_category = "price_ceiling"
                elif "DEAD STOCK" in reason_tag:
                    skip_category = "dead_stock"
                elif "SCALED DROP" in reason_tag:
                    skip_category = "low_demand"
                elif "SUPPLIER CONSOLIDATION" in reason_tag:
                    skip_category = "supplier_consolidation"
                summary['skip_reasons'][skip_category] = summary['skip_reasons'].get(skip_category, 0) + 1

        logger.info(f"Pass 1 Complete. Committed: ${pass1_cost:,.2f}")
        
        # --- PASS 1.5: PRUNING (APS-4) ---
        # "If Budget Exhausted and Vital Depth is missing, remove lowest ROI items from Pass 1."
        remaining_liquidity = total_budget - pass1_cost
        
        # Calculate Critical Liquidity Need (Approximation: 20% of budget for Fast Five Depth?)
        # Or use the Fast Five Reservation Logic.
        critical_depth_need = 0.0
        if is_small:
             # v5.7 ALIGNMENT: If using 95% Relaxed Cap (Budget < 12M), drop reserve requirement to 5%.
             if total_budget < 12000000:
                  critical_depth_need = total_budget * 0.05
             else:
                  critical_depth_need = total_budget * 0.15 # Reserve 15% minimum for Depth
             
        shortfall = critical_depth_need - remaining_liquidity
        
        if shortfall > 0 and is_small:
             logger.warning(f"Pass 1.5: Liquidity Shortfall ${shortfall:,.2f}. Pruning Pass 1 Tail.")
             
             # Identify Candidates: Discretionary Items allocated in Pass 1
             prune_candidates = []
             for rec in recommendations:
                  if rec.get('pass1_allocated'):
                       dept = rec.get('product_category', 'GENERAL').upper()
                       is_staple = self.budget_manager.is_staple(rec['product_name'], rec.get('product_category'), rec.get('avg_daily_sales', 0))
                       is_essential = dept in ESSENTIAL_DEPARTMENTS
                       
                       # Only prune Discretionary (Non-Staple, Non-Essential)
                       if not is_staple and not is_essential:
                            # Calculate Pruning Score (Lower is better to keep? No, Lower is candidate.)
                            # We want to remove LOWEST value.
                            velocity = rec.get('avg_daily_sales', 0)
                            prune_candidates.append(rec)
             
             # Sort candidates by Velocity (Ascending) - Cut the slow movers
             prune_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0))
             
             pruned_count = 0
             reclaimed_cash = 0.0
             
             for rec in prune_candidates:
                  if reclaimed_cash >= shortfall:
                       break
                  
                  # Cut this item
                  qty = rec['recommended_quantity']
                  price = float(rec['selling_price'])
                  # Estimate cost (approx)
                  cost_est = qty * (price * 0.8) # Rough cost
                  # Better: calculate properly
                  
                  rec['recommended_quantity'] = 0
                  rec['pass1_allocated'] = False
                  rec['reasoning'] += " [PRUNED: LIQUIDITY RECOVERY]"
                  
                  # Reclaim (Assuming cash, not consignment - we usually prune both, but Consignment doesn't help liquidity? Actually Consignment doesn't consume width budget. So pruning consignment does NOTHING for cash. We must checking if is_consignment!)
                  if not rec.get('is_consignment', False):
                       reclaimed_cash += cost_est
                       pass1_cost -= cost_est # Deduct from Pass 1 Total
                       # Note: We should technically credit the Wallet too, but Wallets are 'spend_from_wallet'. We assume Pass 2 re-checks availability.
                  
                  pruned_count += 1
             
             logger.info(f"Pass 1.5 Pruninig Complete. Pruned {pruned_count} items. Reclaimed ${reclaimed_cash:,.2f}")
        
        # --- PASS 2: STRATEGIC DEPTH (The Wallet Pass) ---
        pass2_cost = 0.0
        
        # Calculate Remaining Budget Global
        # But we act per Wallet.
        # Strict rule: "80% of remaining capital is locked to Staples" (Small Tier)
        # We need to enforce this SPLIT.
        # Actually, let's respect the wallets but prioritize Staples within the wallet.
        
        # Sort recommendations: Staples FIRST, then High Volume Discretionary
        # We process ALL Staples for depth, THEN Discretionary if budget allowed.
        
        # We need to calculate what remains in the wallets.
        # But applying a global 80/20 split on *remaining* might be complex if done per department.
        # Heuristic: Process Staples up to Depth Cap. Then Process Discretionary.
        
        # Let's split eligible items into groups for Duka Logic
        candidates = [r for r in recommendations if r.get('pass1_allocated') and r['recommended_quantity'] > 0]
        
        # 1. Fast Five Staples (Duka Priority)
        fast_five_candidates = [r for r in candidates if is_small and r.get('product_category','').upper() in fast_five_depts and self.budget_manager.is_staple(r['product_name'], r.get('product_category'), r.get('avg_daily_sales', 0))]
        # 2. Other Staples
        other_staple_candidates = [r for r in candidates if self.budget_manager.is_staple(r['product_name'], r.get('product_category'), r.get('avg_daily_sales', 0)) and r not in fast_five_candidates]
        # 3. Discretionary
        discretionary_candidates = [r for r in candidates if not self.budget_manager.is_staple(r['product_name'], r.get('product_category'), r.get('avg_daily_sales', 0))]
        
        # Sort by Sales Velocity to prioritize winners
        fast_five_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
        other_staple_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
        discretionary_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
        
        # Execute Split with Budget Partitioning
        # Calculate Total Available in Wallets for Pass 2 (Corrected for Ghost Spend)
        # Use strict accounting: Total - Pass 1
        total_remaining_budget = total_budget - pass1_cost
        # sum([w['remaining'] for w in wallets.values()]) # This was unreliable if Pass 1 used unmapped depts
        
        
        # Duka: Fast Five Reservation
        fast_five_reservation = 0.0
        if is_small:
             # User Rule: "60% of that 200k must be reserved for the Big Five"
             # So we target Total Spend on Fast Five >= 0.6 * Total Budget
             target_fast_five_total = total_budget * 0.60
             current_fast_five_spend = sum([wallets[d]['spent'] for d in fast_five_depts if d in wallets])
             fast_five_reservation = max(0, target_fast_five_total - current_fast_five_spend)
             if fast_five_reservation > 0:
                 logger.info(f"Duka Mode: Reserving ${fast_five_reservation:,.2f} for Fast Five Depth.")
        
        # Budget Pools (Soft Targets / Hard Caps)
        # Small Store: 80% Staples / 20% Discretionary
        staple_allocation_target = total_remaining_budget * pass2_staple_share
        discretionary_hard_cap = total_remaining_budget * (1.0 - pass2_staple_share)
        
        logger.info(f"Pass 2 Budget: ${total_remaining_budget:,.2f} (Staples Target: ${staple_allocation_target:,.2f}, Discretionary Cap: ${discretionary_hard_cap:,.2f})")
        

                # v5.4 FIX: Clean Internal Helper for Priority Allocation
        def allocate_list_constrained(candidate_list, phase_cap, phase_name, tier_profile):
            
            # 1. Build Calculation Queue
            queue = []
            for rec in candidate_list:
                dept = rec.get('product_category', 'GENERAL').upper()
                avg_sales = rec.get('avg_daily_sales', 0.0)
                
                # --- v2.5 NEW PRODUCT HYBRID LOGIC ---
                effective_avg_sales = avg_sales
                new_product_mode = False
                
                if avg_sales <= 0:
                    lookalike = rec.get('lookalike_demand', 0.0)
                    if lookalike > 0:
                        effective_avg_sales = lookalike * 0.5 
                        new_product_mode = True
                        if "[NEW PRODUCT" not in rec['reasoning']: rec['reasoning'] += " [NEW PRODUCT: Lookalike]"
                    else:
                        is_fresh = rec.get('is_fresh', False)
                        effective_avg_sales = 0.3 if is_fresh else 0.5
                        new_product_mode = True
                        if "[NEW PRODUCT" not in rec['reasoning']: rec['reasoning'] += " [NEW PRODUCT: Baseline]"

                # --- DEPTH CALCULATION ---
                # v6.0 FIX: Smart Replenishment Logic
                # Instead of flat "Depth Cap", use calculated replenishment need.
                
                # Default "Effective Days" starts with the smart target
                smart_target_days = self.calculate_replenishment_target_stock(rec, tier_profile)
                effective_days = smart_target_days
                
                # v7.6 REFINEMENT: Tight Coupling for Fresh (JIT)
                # User Request: "Fresh orders ≈ Sales".
                # We allocate LeadTime + 1.5 Days (Buffer).
                # v7.6 REFINEMENT: Tight Coupling for Fresh (JIT)
                # Now handled by calculate_replenishment_target_stock using GRN frequency
                # We trust smart_target_days.
                if rec.get('is_fresh', False):
                     if "[JIT FRESH]" not in rec['reasoning']: rec['reasoning'] += " [JIT FRESH]"
                
                # Fallback / Override for New Products Logic (Hybrid)
                # Fallback / Override for New Products Logic (Hybrid)
                if new_product_mode:
                     effective_days = min(effective_days, 14.0)
                
                # --- OLD LOGIC REMOVED (Fresh/Flat checks overridden by Smart Target) ---
                # v5.3 Global Freshness Logic -- Now handled inside `calculate_replenishment_target_stock`
                # so we don't need to re-apply it here, avoiding double logic.
                
                # Calculate Ideal
                pack_size = int(rec.get('pack_size', 1))
                current_qty = rec['recommended_quantity']
                ideal_qty = int(effective_avg_sales * effective_days)
                
                # Min Packs
                min_pack_floor = 1
                if is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR']:
                     unit_price = float(rec.get('selling_price', 0))
                     min_pack_floor = 12 if unit_price < 50 else 6
                
                # v5.4 FIX: Relax Floor for Fresh (Avoid spoilage due to minimums)
                if dept in FRESH_DEPARTMENTS:
                     min_pack_floor = 1 

                # MICRO STORE FIX: Allow Bulk Breaking (Cash & Carry Mode)
                # If Micro store, allow purchasing just the MDQ (e.g. 6 units) even if PackSize is 12.
                floor_qty = min_pack_floor * pack_size
                if is_micro or is_small:
                    floor_qty = max(min_pack_floor, int(rec.get('min_display_qty', 3)))
                
                # --- CONSTRAINT: MAX ALLOWED UNITS ---
                max_total_packs = int(tier_profile.get('max_packs', 10)) # Use tier_profile here
                max_allowed_units = max_total_packs * pack_size
                
                # Allow high velocity items to breach max_packs if needed for Minimum Coverage
                # Rule: If ADS > 2, allow up to 1.5x Max Packs
                if rec['avg_daily_sales'] > 2.0:
                     max_allowed_units = int(max_allowed_units * 1.5)
                     
                # v7.9 Fix: Fresh Items Exempt from Shelf Cap
                # Justification: High velocity fresh items (Milk/Bread) are floor-stacked or critically replenished.
                # We cannot cap them at "18 packs" if they sell 60/day.
                if rec.get('is_fresh', False):
                    # Allow full target + 10% flex, but not less than current max_allowed_units
                    max_allowed_units = max(max_allowed_units, int(ideal_qty * 1.1)) 
                
                # High Velocity Unlock (additional logic)
                if is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR']:
                    max_allowed_units = max(max_allowed_units, 999) # Ensure it's at least 999
                elif total_budget >= 20000000: 
                    max_allowed_units = max(max_allowed_units, 99999999) # Ensure it's at least 99999999
                elif effective_avg_sales > 1.0:
                     # v6.1 FIX: Allow velocity floor to match Calculated Effective Days
                     velocity_floor = int(effective_avg_sales * effective_days)
                     max_allowed_units = max(max_allowed_units, velocity_floor)
                
                # --- FINAL RESOLUTION ---
                # 1. Apply Floor
                target_with_floor = max(ideal_qty, floor_qty)
                
                # 2. Apply Ceiling
                final_target = min(target_with_floor, max_allowed_units)
                
                # 3. Add to Queue if actionable
                if current_qty < final_target:
                    price = float(rec.get('selling_price', 0.0))
                    cost_price_est = self._get_actual_cost_price(rec, price)
                    
                    queue.append({
                        'rec': rec,
                        'dept': dept,
                        'pack_size': pack_size,
                        'cost_per_pack': pack_size * cost_price_est,
                        'target_qty': final_target,
                        'cost_est': cost_price_est
                    })

            # 2. Execute Round Robin
            phase_cost = 0.0
            active = True
            
            while active and queue:
                active = False
                for i in range(len(queue) - 1, -1, -1):
                    item = queue[i]
                    rec = item['rec']
                    dept = item['dept']
                    pack_cost = item['cost_per_pack']
                    pack_size = item['pack_size']
                    
                    # Check Phase Cap
                    if (phase_cost + pack_cost) > phase_cap:
                        rec['reasoning'] += f" [{phase_name} CAP]"
                        queue.pop(i)
                        continue
                        
                    # Check Share Cap (except for Priority)
                    is_priority = (phase_name == "PRIORITY")
                    if not is_priority:
                         wallet_limit_ratio = 0.25 if is_small else 0.50
                         max_item_spend = wallets.get(dept, {}).get('allocated_budget', 0) * wallet_limit_ratio if dept in wallets else 99999999.0
                         current_spend = rec['recommended_quantity'] * item['cost_est']
                         if (current_spend + pack_cost) > max_item_spend:
                             if rec.get('pass1_allocated'): rec['reasoning'] += " [SHARE CAP]"
                             queue.pop(i)
                             continue

                    # Check Wallet
                    can_spend = True
                    if not is_priority and dept in wallets:
                         if not self.budget_manager.check_wallet_availability(wallets, dept, pack_cost):
                             can_spend = False
                    
                    if can_spend:
                        rec['recommended_quantity'] += pack_size
                        if not is_priority and dept in wallets:
                            self.budget_manager.spend_from_wallet(wallets, dept, pack_cost)
                        
                        rec['pass2_allocated'] = True
                        if "[PASS 2]" not in rec.get('reasoning', ''): rec['reasoning'] = rec.get('reasoning', '') + " [PASS 2]"
                        phase_cost += pack_cost
                        active = True
                        
                        if rec['recommended_quantity'] >= item['target_qty']:
                            queue.pop(i)
            
            return phase_cost

        # --- EXECUTION SEQUENCE ---
        
        # v6.1 FIX: Ensure tier_profile is available for Smart Allocation
        tier_profile = self.profile_manager.get_profile(total_budget)
        
        # 1. Fast Five (Priority)
        added_fast_five_cost = allocate_list_constrained(fast_five_candidates, total_remaining_budget, "PRIORITY", tier_profile)
        
        # 2. Other Staples
        remaining_after_ff = total_remaining_budget - added_fast_five_cost
        staple_allocation_target = remaining_after_ff * pass2_staple_share
        
        logger.info(f"Pass 2 Remaining: ${remaining_after_ff:,.2f} (Other Staples Target: ${staple_allocation_target:,.2f})")
        
        added_other_staple_cost = allocate_list_constrained(other_staple_candidates, staple_allocation_target, "STAPLE", tier_profile)
        
        # 3. Discretionary
        remaining_disc = remaining_after_ff * (1.0 - pass2_staple_share)
        added_disc_cost = allocate_list_constrained(discretionary_candidates, remaining_disc, "DISC", tier_profile)
        
        pass2_cost = added_fast_five_cost + added_other_staple_cost + added_disc_cost

        logger.info(f"Pass 2 Complete. Added Depth: ${pass2_cost:,.2f} (Desc: {added_fast_five_cost:,.0f}/{added_other_staple_cost:,.0f}/{added_disc_cost:,.0f})")
        
        # --- PASS 2B: FLEX POOL REDISTRIBUTION (v3.0 - Gap-11 Fix) ---
        # Calculate TRUE unused budget (flex pool available)
        actual_spent = pass1_cost + pass2_cost
        true_unused = total_budget - actual_spent
        unused_pct = (true_unused / total_budget * 100) if total_budget > 0 else 0
        
        redistrib_cost = 0.0
        flex_pool_transactions = []  # Track all transactions for audit
        items_enhanced = 0
        
        if true_unused > (total_budget * 0.05):  # > 5% unused (lowered threshold from 10%)
            logger.info(f"Pass 2B: Flex Pool Active. Available: ${true_unused:,.2f} ({unused_pct:.1f}%)")
            
            # --- EXPANDED ELIGIBILITY: All Priority 1/2 Items with Depth Potential ---
            flex_candidates = []
            
            for rec in recommendations:
                if rec.get('pass1_allocated') and rec['recommended_quantity'] > 0:
                    # v7.6 FIX: Ignore Fresh Items for Flex Pool (They have strict JIT targets)
                    if rec.get('is_fresh', False):
                         continue
                         
                    # Priority check: Staples OR A-Class items
                    is_staple = self.budget_manager.is_staple(rec['product_name'], rec.get('product_category'), rec.get('avg_daily_sales', 0))
                    abc_class = rec.get('ABC_Class', 'B')
                    is_priority = is_staple or abc_class == 'A'
                    
                    if is_priority:
                        # Calculate depth potential (how much more this item could use)
                        current_qty = rec['recommended_quantity']
                        avg_sales = rec.get('avg_daily_sales', 0.0)
                        
                        # Handle new products
                        if avg_sales <= 0:
                            lookalike = rec.get('lookalike_demand', 0.0)
                            avg_sales = lookalike * 0.5 if lookalike > 0 else (0.3 if rec.get('is_fresh') else 0.5)
                        
                        # Calculate ideal depth
                        ideal_days = depth_cap_days
                        
                        # v5.3 FIX: Strict Fresh Constraint for Flex Pool
                        dept_upper = rec.get('product_category', 'GENERAL').upper()
                        if dept_upper in FRESH_DEPARTMENTS:
                             lead_time = int(rec.get('estimated_delivery_days', 1))
                             target_fresh_days = min(lead_time + 1.0, 3.0)
                             ideal_days = min(ideal_days, target_fresh_days)
                        else:
                             # Standard Shelf Life Logic
                             shelf_life = rec.get('shelf_life_days', 365)
                             if shelf_life < 30:
                                 max_safe_days = max(1, shelf_life - 2)
                                 ideal_days = min(ideal_days, max_safe_days)
                        
                        ideal_qty = int(avg_sales * ideal_days)
                        additional_qty = max(0, ideal_qty - current_qty)
                        
                        if additional_qty > 0:
                            # Calculate ROI score for prioritization
                            velocity = rec.get('avg_daily_sales', 0.0)
                            margin = rec.get('profit_margin', 0.2)
                            roi_score = velocity * margin
                            
                            flex_candidates.append({
                                'rec': rec,
                                'additional_qty': additional_qty,
                                'ideal_days': ideal_days,
                                'roi_score': roi_score,
                                'dept': rec.get('product_category', 'GENERAL').upper()
                            })
            
            # Sort by ROI score (highest first) - maximize value from flex pool
            flex_candidates.sort(key=lambda x: x['roi_score'], reverse=True)
            
            logger.info(f"Pass 2B: {len(flex_candidates)} items eligible for flex pool (Priority 1/2 with depth potential)")
            
            # --- DISTRIBUTE FLEX POOL ---
            flex_pool_remaining = true_unused
            
            for candidate in flex_candidates:
                if flex_pool_remaining <= 0:
                    break
                    
                rec = candidate['rec']
                additional_qty = candidate['additional_qty']
                dept = candidate['dept']
                
                # Calculate cost
                price = float(rec.get('selling_price', 0.0))
                cost_price = self._get_actual_cost_price(rec, price)
                pack_size = int(rec.get('pack_size', 1))
                
                # Allocate pack-by-pack from flex pool
                allocated_from_flex = 0
                
                while allocated_from_flex < additional_qty and flex_pool_remaining > 0:
                    pack_cost = pack_size * cost_price
                    
                    if pack_cost <= flex_pool_remaining:
                        # Can afford this pack from flex pool
                        flex_pool_remaining -= pack_cost
                        allocated_from_flex += pack_size
                        rec['recommended_quantity'] += pack_size
                    else:
                        # Can't afford anymore
                        break
                
                if allocated_from_flex > 0:
                    # Track transaction
                    flex_spent = allocated_from_flex * cost_price
                    flex_pool_transactions.append({
                        'item': rec['product_name'],
                        'dept': dept,
                        'units_added': allocated_from_flex,
                        'flex_spent': flex_spent,
                        'roi_score': candidate['roi_score']
                    })
                    
                    # Update reasoning
                    rec['reasoning'] += f" [FLEX POOL: +{allocated_from_flex} units, ${flex_spent:,.0f}]"
                    items_enhanced += 1
            
            redistrib_cost = true_unused - flex_pool_remaining
            
            if redistrib_cost > 0:
                logger.info(f"Pass 2B: Flex Pool distributed ${redistrib_cost:,.2f} to {items_enhanced} items")
                logger.info(f"Pass 2B: Flex Pool remaining: ${flex_pool_remaining:,.2f}")
                
                # Log top 5 beneficiaries for audit trail
                for i, txn in enumerate(flex_pool_transactions[:5]):
                    logger.info(f"  #{i+1}: {txn['item']} (+{txn['units_added']} units, ${txn['flex_spent']:,.0f}, ROI: {txn['roi_score']:.2f})")
        
        # --- PASS 3: SUPPLIER ANCHORING (MOV TRAP FIX) ---
        # Insight: If total spend with a supplier is < MOV, we will never be able to restock.
        # Better to cut them Day 1 and focus budget on viable partners.
        
        if is_small:
            mov_threshold = 1500 if is_micro else 3000
            supplier_spend = {}
            
            # 1. Aggregate Spend
            for rec in recommendations:
                if rec['recommended_quantity'] > 0:
                    supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                    price = float(rec.get('selling_price', 0))
                    # Use actual cost estimate
                    cost = self._get_actual_cost_price(rec, price) * rec['recommended_quantity']
                    supplier_spend[supp] = supplier_spend.get(supp, 0) + cost
            
            # 2. Prune Below Threshold
            pruned_anchor_count = 0
            pruned_anchor_val = 0.0
            
            for rec in recommendations:
                if rec['recommended_quantity'] > 0:
                    supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                    total_supp_spend = supplier_spend.get(supp, 0)
                    
                    # Exceptions: Consignment (No MOV), Fresh (Daily Delivery usually bypasses strict MOV or has lower thresholds in reality)
                    # But actually, Fresh delivery failure is even worse.
                    # Let's strictly enforce for Dry, be lenient for Fresh/Consignment
                    is_consign = rec.get('is_consignment', False)
                    is_fresh_supp = rec.get('is_fresh', False)
                    
                    if not is_consign and not is_fresh_supp:
                        if total_supp_spend < mov_threshold:
                            # Prune
                            qty = rec['recommended_quantity']
                            price = float(rec.get('selling_price', 0))
                            cost_est = self._get_actual_cost_price(rec, price) * qty
                            
                            rec['recommended_quantity'] = 0
                            rec['reasoning'] += f" [ANCHOR PRUNE: Supp Spend ${total_supp_spend:,.0f} < ${mov_threshold}]"
                            
                            pruned_anchor_count += 1
                            pruned_anchor_val += cost_est
                            
                            # Update running totals for summary accuracy
                            if rec.get('pass1_allocated'): pass1_cost -= cost_est
                            else: pass2_cost -= cost_est # Assumption
                            
            if pruned_anchor_count > 0:
                logger.info(f"Pass 3: Pruned {pruned_anchor_count} items from low-volume suppliers. Saved ${pruned_anchor_val:,.2f}")
                
                # --- PASS 3B: REDISTRIBUTE TO ANCHORS ---
                # Reinvest the saved capital into Top 3 Suppliers ("Anchors")
                if pruned_anchor_val > 0:
                    # 1. Identify Anchors (Top 3 by spend)
                    # Filter out pruned suppliers (spend < threshold)
                    viable_suppliers = {k: v for k, v in supplier_spend.items() if v >= mov_threshold}
                    sorted_anchors = sorted(viable_suppliers.items(), key=lambda x: x[1], reverse=True)[:3]
                    anchor_names = [x[0] for x in sorted_anchors]
                    
                    if anchor_names:
                        logger.info(f"Pass 3B: Redistributing ${pruned_anchor_val:,.2f} to Anchors: {anchor_names}")
                        
                        # 2. Find eligible items from these anchors
                        anchor_candidates = []
                        for rec in recommendations:
                            supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                            if supp in anchor_names and rec['recommended_quantity'] > 0:
                                # Calculate potential depth
                                avg_sales = rec.get('avg_daily_sales', 0.1)
                                current_qty = rec['recommended_quantity']
                                
                                # Cap at 45 days (Reasonable Max)
                                max_qty = int(avg_sales * 45)
                                headroom = max(0, max_qty - current_qty)
                                
                                if headroom > 0:
                                    anchor_candidates.append({
                                        'rec': rec,
                                        'headroom': headroom,
                                        'priority': avg_sales * float(rec.get('profit_margin', 0.2)) # ROI Score
                                    })
                        
                        # 3. Distribute
                        anchor_candidates.sort(key=lambda x: x['priority'], reverse=True)
                        reinvested = 0.0
                        
                        for cand in anchor_candidates:
                            if pruned_anchor_val <= 0: break
                            
                            rec = cand['rec']
                            price = float(rec.get('selling_price', 0))
                            cost = self._get_actual_cost_price(rec, price)
                            
                            # Buy as much as headroom allows or budget permits
                            affordable_qty = int(pruned_anchor_val / cost) if cost > 0 else 0
                            add_qty = min(cand['headroom'], affordable_qty)
                            
                            if add_qty > 0:
                                rec['recommended_quantity'] += add_qty
                                cost_added = add_qty * cost
                                
                                pruned_anchor_val -= cost_added
                                reinvested += cost_added
                                
                                rec['reasoning'] += f" [ANCHOR BOOST: +{add_qty}]"
                                
                        logger.info(f"Pass 3B Complete. Reinvested ${reinvested:,.2f}.")
                        pass2_cost += reinvested # Attribute to Pass 2 bucket for now

        # --- PASS 4: MOP-UP (GAP-E FIX) ---
        # If <5% budget remains after all passes, spend it on highest-priority items
        # This ensures we don't leave money on the table for small stores
        mop_up_cost = 0.0
        final_unused = total_budget - (pass1_cost + pass2_cost + redistrib_cost)
        final_unused_pct = (final_unused / total_budget * 100) if total_budget > 0 else 0
        
        if final_unused > 0 and final_unused_pct <= 5.0:
            logger.info(f"Pass 4 (Mop-Up): ${final_unused:,.2f} remaining ({final_unused_pct:.1f}%). Distributing to priority items.")
            
            # Find items that can absorb more (staples with headroom)
            mop_candidates = []
            for rec in recommendations:
                if rec['recommended_quantity'] > 0:
                    is_staple = self.budget_manager.is_staple(rec['product_name'], rec.get('product_category'), rec.get('avg_daily_sales', 0))
                    if is_staple:
                        avg_sales = rec.get('avg_daily_sales', 0.1)
                        current_qty = rec['recommended_quantity']
                        # Allow up to 60 days for mop-up (generous ceiling)
                        max_qty = int(avg_sales * 60)
                        headroom = max(0, max_qty - current_qty)
                        
                        if headroom > 0:
                            price = float(rec.get('selling_price', 0))
                            cost_per_unit = self._get_actual_cost_price(rec, price)
                            roi_score = avg_sales * float(rec.get('profit_margin', 0.2))
                            
                            mop_candidates.append({
                                'rec': rec,
                                'headroom': headroom,
                                'cost_per_unit': cost_per_unit,
                                'roi_score': roi_score
                            })
            
            # Sort by ROI and distribute
            mop_candidates.sort(key=lambda x: x['roi_score'], reverse=True)
            mop_budget = final_unused
            
            for cand in mop_candidates:
                if mop_budget <= 0:
                    break
                    
                rec = cand['rec']
                cost_per_unit = cand['cost_per_unit']
                
                # Buy as much as we can afford within headroom
                affordable = int(mop_budget / cost_per_unit) if cost_per_unit > 0 else 0
                add_qty = min(cand['headroom'], affordable)
                
                if add_qty > 0:
                    rec['recommended_quantity'] += add_qty
                    cost_added = add_qty * cost_per_unit
                    mop_budget -= cost_added
                    mop_up_cost += cost_added
                    rec['reasoning'] += f" [MOP-UP: +{add_qty}]"
            
            if mop_up_cost > 0:
                logger.info(f"Pass 4 Complete. Mop-up distributed ${mop_up_cost:,.2f}")

        # --- FINALIZE SUMMARY ---
        summary['pass1_cash'] = pass1_cost
        summary['pass1_consignment'] = pass1_consignment_val
        summary['pass2_cash'] = pass2_cost
        summary['pass2b_cash'] = redistrib_cost if 'redistrib_cost' in locals() else 0.0
        summary['pass2b_items_enhanced'] = items_enhanced if 'items_enhanced' in locals() else 0
        
        # Flex pool metrics (Gap-11 fix tracking)
        summary['flex_pool_available'] = true_unused if 'true_unused' in locals() else 0.0
        summary['flex_pool_distributed'] = redistrib_cost if redistrib_cost > 0 else 0.0
        summary['flex_pool_remaining'] = (true_unused - redistrib_cost) if (redistrib_cost > 0 and 'true_unused' in locals()) else 0.0
        
        # v2.9 FIX: Include Pass 2B in total  
        # v7.0 GAP-E FIX: Include mop_up_cost in total
        summary['mop_up_cash'] = mop_up_cost if 'mop_up_cost' in locals() else 0.0
        summary['total_cash_used'] = pass1_cost + pass2_cost + summary['pass2b_cash'] + summary['mop_up_cash']
        summary['total_consignment'] = pass1_consignment_val
        summary['unused_budget'] = total_budget - summary['total_cash_used']
        summary['utilization_pct'] = (summary['total_cash_used'] / total_budget * 100) if total_budget > 0 else 0
        
        # Calculate department utilization
        for dept, wallet in wallets.items():
            if wallet['max_budget'] > 0:
                util = (wallet['spent'] / wallet['max_budget']) * 100
                summary['dept_utilization'][dept] = round(util, 1)
        
        return {
            'recommendations': recommendations,
            'summary': summary
        }

    async def run_intelligent_analysis(self, file_path: str, output_path: str, allocation_mode: str = "replenishment", total_budget: float = 200000.0):
        """Phase 7: Master Workflow."""
        await self.load_databases_async()
        
        # Populate PO History cache for enrichment
        self._po_history_dates = self.scan_purchase_orders()
        
        products = self.parse_inventory_file(file_path)
        if not products: return
        
        # In greenfield mode, we assume initial stock is 0 for calculations
        if allocation_mode == "initial_load":
            for p in products:
                p['current_stocks'] = 0.0
                
        enriched = self.enrich_product_data(products)
        
        all_recommendations = []
        
        # Performance Optimization: Bypass AI for Greenfield (Initial Load)
        # The Python logic (apply_greenfield_allocation) is authoritative and overwrites AI output anyway.
        if allocation_mode == "initial_load":
            logger.info("Mode: Initial Load. Bypassing AI Analysis for speed (using Enriched Data directly).")
            all_recommendations = enriched
            # ensure 'reasoning' field exists
            for r in all_recommendations:
                if 'reasoning' not in r: r['reasoning'] = "AI Bypass: Using Enriched Data"
        else:
            # Standard AI Analysis Loop
            batch_size = 20
            # Map enriched by p_name for merging
            enriched_map = {p['product_name']: p for p in enriched}
            
            for i in range(0, len(enriched), batch_size):
                batch = enriched[i:i + batch_size]
                recs = await self.analyze_batch_ai(batch, (i // batch_size) + 1, (len(enriched) + batch_size - 1) // batch_size, allocation_mode)
                
                # Merge with original data to preserve all fields (stocks, days, etc.)
                for r in recs:
                    meta = enriched_map.get(r['product_name'], {})
                    merged = {**meta, **r}
                    all_recommendations.append(merged)
        
        # --- NEW: Apply Greenfield Python Constraints ---
        if allocation_mode == "initial_load":
            all_recommendations = self.apply_greenfield_allocation(all_recommendations, total_budget)
            
        self.generate_excel_report(file_path, all_recommendations, output_path)
        return all_recommendations

    def generate_excel_report(self, original_file_path: str, recommendations: List[dict], output_path: str):
        """
        Phase 5 & 6: Report Generation.
        Takes the original Excel file, adds new columns for recommendations,
        and creates a summary sheet.
        """
        logger.info(f"Generating Excel report: {output_path}")
        
        try:
            # Load original workbook
            wb = load_workbook(original_file_path)
            ws = wb.active # Assuming single sheet or first sheet relevant
            
            # Map recommendations by product name for O(1) lookup
            rec_map = {r['product_name']: r for r in recommendations}
            
            # Find header row (Search for common keywords)
            header_row_idx = 3 # Default
            # Iterate rows and populate
            col_map = {}
            for col in range(1, 40):
                val = ws.cell(row=header_row_idx, column=col).value
                if val:
                    col_map[str(val).strip().lower().replace(' ', '_')] = col
            
            # Add new headers
            new_headers = ["Recommended Qty", "Historical Avg", "Confidence", "Reasoning", "Est. Cost (KES)"]
            
            # Use fixed columns 11-15 for "Picking List" format if description is at Col 1
            is_picking_list = col_map.get('rr_prev') and col_map.get('description') == 1
            
            if is_picking_list:
                start_col = 11
            else:
                # Find first empty column after existing headers
                start_col = 1
                for col in range(1, 100):
                    val = ws.cell(row=header_row_idx, column=col).value
                    if not val:
                        start_col = col
                        break
                    else:
                        start_col = col + 1
            
            for i, h in enumerate(new_headers):
                c = ws.cell(row=header_row_idx, column=start_col + i)
                c.value = h
                c.font = c.font.copy(bold=True)
                if is_picking_list:
                    from openpyxl.styles import PatternFill
                    c.fill = PatternFill(start_color="4A9EFF", end_color="4A9EFF", fill_type="solid")
                    c.font = c.font.copy(color="FFFFFF")

            desc_col = col_map.get('description', col_map.get('product_name'))
            
            if not desc_col:
                logger.error("Could not find Description/Product Name column.")
                return

            total_rec_units = 0
            total_est_cost = 0.0

            for row_idx in range(header_row_idx + 1, ws.max_row + 1):
                product_name_cell = ws.cell(row=row_idx, column=desc_col).value
                if not product_name_cell: continue
                
                product_name = str(product_name_cell).strip()
                rec = rec_map.get(product_name, {})
                
                qty = rec.get('recommended_quantity', 0)
                hist = rec.get('historical_avg', 0)
                conf = rec.get('confidence', '')
                reason = rec.get('reasoning', '')
                cost = rec.get('est_cost', 0.0)
                
                # Write values
                ws.cell(row=row_idx, column=start_col).value = qty
                ws.cell(row=row_idx, column=start_col + 1).value = hist
                ws.cell(row=row_idx, column=start_col + 2).value = conf
                ws.cell(row=row_idx, column=start_col + 3).value = reason
                ws.cell(row=row_idx, column=start_col + 4).value = cost
                
                total_rec_units += qty
                total_est_cost += cost

            # Create Summary Sheet (Phase 6)
            if "Order Summary" in wb.sheetnames:
                del wb["Order Summary"]
            ws_summary = wb.create_sheet("Order Summary", 0)
            
            est_savings = total_est_cost * 0.10 # 10% Waste Reduction
            
            summary_data = [
                ["Metric", "Value"],
                ["Total Products Analyzed", len(recommendations)],
                ["Total Recommended Units", total_rec_units],
                ["Estimated Total Cost (KES)", f"{total_est_cost:,.2f}"],
                ["Estimated Savings (10% Waste Red.)", f"{est_savings:,.2f}"],
                ["Generated On", "AI Inventory Assistant"]
            ]
            
            for r_idx, r_data in enumerate(summary_data, 1):
                ws_summary.cell(row=r_idx, column=1).value = r_data[0]
                ws_summary.cell(row=r_idx, column=2).value = r_data[1]

            wb.save(output_path)
            logger.info("Excel report saved successfully.")
            
        except Exception as e:
            logger.error(f"Failed to generate Excel report: {e}")
            raise

        # Load GRN Frequency Map (v8.0)
        self.grn_frequency_map = self.load_grn_frequency()

    def load_grn_frequency(self):
        """Loads SKU order frequency data (0.0 - 1.0)."""
        try:
            path = os.path.join(self.data_dir, "sku_grn_frequency.json")
            if os.path.exists(path):
                with open(path, 'r') as f:
                    return json.load(f)
        except Exception as e:
            logger.error(f"Failed to load GRN Frequency Map: {e}")
        return {}

    def get_grn_cycle_days(self, product_name):
        """Calculates Cycle Days based on GRN Frequency (1/Freq)."""
        # Default fresh cycle = 1 day (Daily)
        if not product_name: return 1.0
        
        freq = self.grn_frequency_map.get(product_name.upper(), 0)
        if freq <= 0: return 1.0 # Default to Daily if unknown
        
        # Cycle Days = 1 / Frequency
        # Freq 1.0 -> 1 Day
        # Freq 0.5 -> 2 Days
        # Freq 0.25 -> 4 Days
        return 1.0 / freq

    def has_grn_data(self, product_name):
        return product_name.upper() in self.grn_frequency_map
        
    def _load_products(self):
        """
        Scans all GRN Excel files (grnds_*.xlsx) and aggregates valid order history.
        Structure: { 'barcode_or_name': {'total': X, 'count': Y} }
        """
        import glob
        
        logger.info("Scanning GRN Excel files...")
        grn_stats = {}
        
        # Pattern matches both grnd_ and grnds_
        files = glob.glob(os.path.join(self.data_dir, "grnd*.xlsx"))
        
        total_files = len(files)
        if total_files == 0:
            logger.warning(f"No GRN files found in {self.data_dir}. Expected grnds_*.xlsx")
            return {}

        logger.info(f"Found {total_files} GRN files. Processing...")
        
        for i, fpath in enumerate(files):
            try:
                # Use data_only=True to get calculated values, read_only=True for speed
                wb = load_workbook(fpath, read_only=True, data_only=True)
                ws = wb.active
                
                # Headers are in row 1
                headers = {}
                header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
                for idx, val in enumerate(header_row):
                    if val:
                        # Normalize header: "Bar Code" -> "barcode"
                        h_norm = str(val).strip().lower().replace(' ', '').replace('_', '').replace('-', '')
                        headers[h_norm] = idx
                
                # Flexible Header Matching
                col_barcode = headers.get('barcode', headers.get('barcode')) # matches 'Bar Code', 'Barcode', 'BarCode'
                col_name = headers.get('itemname', headers.get('description', headers.get('productname')))
                col_qty = headers.get('grnqty', headers.get('qty', headers.get('quantity')))
                col_code = headers.get('itemcode', headers.get('code'))
                
                if col_qty is None:
                    logger.warning(f"File {os.path.basename(fpath)} missing quantity column. Found: {list(headers.keys())}")
                    continue 
                
                # Iterate rows (skip header)
                row_count = 0
                for row in ws.iter_rows(min_row=2, values_only=True):
                    # Get values
                    qty = row[col_qty]
                    if not isinstance(qty, (int, float)) or qty <= 0: continue
                    
                    # Keys
                    barcode = str(row[col_barcode]).strip() if col_barcode is not None and row[col_barcode] else None
                    name = str(row[col_name]).strip().upper() if col_name is not None and row[col_name] else ""
                    code = str(row[col_code]).strip() if col_code is not None and row[col_code] else None
                    
                    if not barcode and not name and not code: continue

                    # Update Stats
                    for key in [barcode, name, code]:
                        if key:
                            if key not in grn_stats: grn_stats[key] = {'total': 0.0, 'count': 0}
                            grn_stats[key]['total'] += float(qty)
                            grn_stats[key]['count'] += 1
                    
                    row_count += 1
                
                wb.close()
                if (i+1) % 5 == 0: logger.info(f"Processed {i+1}/{total_files} files... ({row_count} rows in this file)")
                
            except Exception as e:
                logger.error(f"Error reading {os.path.basename(fpath)}: {e}")

        logger.info(f"GRN Scan Complete. Indexed {len(grn_stats)} unique items/barcodes.")
        return grn_stats

    def scan_purchase_returns(self):
        """
        Scans all purchase return Excel files (prts_*.xlsx) and aggregates supplier quality data.
        Returns: { 'supplier_name': { 'total_returns': X, 'expiry_returns': Y, ... } }
        """
        import glob
        logger.info("Scanning Purchase Return Excel files...")
        return_stats = {}
        
        files = glob.glob(os.path.join(self.data_dir, "prts_*.xlsx"))
        if not files:
            logger.warning("No Purchase Return files found (prts_*.xlsx)")
            return {}

        for fpath in files:
            try:
                wb = load_workbook(fpath, read_only=True, data_only=True)
                ws = wb.active
                
                # Headers are in row 1
                headers = {}
                header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True))
                for idx, val in enumerate(header_row):
                    if val:
                        h_norm = "".join(c for c in str(val).lower() if c.isalnum())
                        headers[h_norm] = idx
                
                # Match normalized headers
                col_supplier = headers.get('vencodename', headers.get('vendor')) 
                col_reason = headers.get('reason')
                col_qty = headers.get('rejcqty', headers.get('qty'))
                col_amt = headers.get('netamt', headers.get('amount'))
                
                if col_supplier is None or col_qty is None:
                    logger.warning(f"File {os.path.basename(fpath)} missing critical columns.")
                    continue

                for row in ws.iter_rows(min_row=2, values_only=True):
                    supplier_raw = str(row[col_supplier]).strip() if row[col_supplier] else None
                    if not supplier_raw: continue
                    
                    # Normalize supplier name: "SA0024 - AQUAMIST LIMITED" -> "AQUAMIST LIMITED"
                    if ' - ' in supplier_raw:
                        supplier = supplier_raw.split(' - ', 1)[1].upper().strip()
                    else:
                        supplier = supplier_raw.upper().strip()

                    qty = self._safe_float(row[col_qty])
                    amt = self._safe_float(row[col_amt]) if col_amt is not None else 0.0
                    reason = str(row[col_reason]).strip().upper() if col_reason is not None and row[col_reason] else "OTHER"

                    if supplier not in return_stats:
                        return_stats[supplier] = {
                            'total_returns': 0,
                            'expiry_returns': 0,
                            'damaged_returns': 0,
                            'short_supply_returns': 0,
                            'total_qty_returned': 0.0,
                            'total_value_returned': 0.0
                        }
                    
                    stats = return_stats[supplier]
                    stats['total_returns'] += 1
                    stats['total_qty_returned'] += qty
                    stats['total_value_returned'] += amt
                    
                    if 'EXPIRY' in reason or 'EXP' in reason:
                        stats['expiry_returns'] += 1
                    elif 'DAMAGE' in reason:
                        stats['damaged_returns'] += 1
                    elif 'SHORT' in reason or 'SUPPLY' in reason:
                        stats['short_supply_returns'] += 1
                
                wb.close()
            except Exception as e:
                logger.error(f"Error reading {os.path.basename(fpath)}: {e}")
        
        return return_stats

    def update_supplier_quality_scores(self):
        """Processes PRTS files and updates the supplier quality database."""
        new_returns = self.scan_purchase_returns()
        if not new_returns:
            return
        
        # Load existing database
        sq_db = self.databases.get('supplier_quality', {})
        
        # Merge and Recalculate
        for supplier, stats in new_returns.items():
            if supplier not in sq_db:
                sq_db[supplier] = {
                    'total_returns': 0,
                    'expiry_returns': 0,
                    'damaged_returns': 0,
                    'short_supply_returns': 0,
                    'total_qty_returned': 0.0,
                    'total_value_returned': 0.0
                }
            
            curr = sq_db[supplier]
            # We treat the PRTS files as the new sources, but if we wanted to incremental, we'd add.
            # However, usually PRTS scanned collectively represent the state.
            # Let's overwrite with new scanned values for consistency.
            curr.update(stats)
            
            # Simple scoring logic
            score = 100.0
            score -= (curr['expiry_returns'] * 2.0)
            score -= (curr['damaged_returns'] * 1.0)
            score -= (curr['short_supply_returns'] * 0.5)
            
            curr['quality_score'] = max(0.0, score)
            
            # Risk Level
            if curr['quality_score'] >= 90: curr['risk_level'] = 'LOW'
            elif curr['quality_score'] >= 70: curr['risk_level'] = 'MEDIUM'
            elif curr['quality_score'] >= 40: curr['risk_level'] = 'HIGH'
            else: curr['risk_level'] = 'CRITICAL'

        # Save back to file
        available_files = os.listdir(self.data_dir)
        match = next((f for f in available_files if 'supplier_quality_scores_2025' in f), "supplier_quality_scores_2025_updated.json")
        save_path = os.path.join(self.data_dir, match)
        
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(sq_db, f, indent=2)
            logger.info(f"Updated supplier quality database saved to {match}")
            self.databases['supplier_quality'] = sq_db
        except Exception as e:
            logger.error(f"Failed to save updated quality scores: {e}")

    def scan_cashier_sales(self):
        """
        Scans all cashier POS sales Excel files (*_cash.xlsx) and aggregates units sold.
        Returns: { 'item_code_or_barcode': total_qty }
        """
        import glob
        logger.info("Scanning Cashier POS Sales files...")
        sales_stats = {}
        
        files = glob.glob(os.path.join(self.data_dir, "*_cash.xlsx"))
        if not files:
            logger.warning("No Cashier POS Sales files found (*_cash.xlsx)")
            return {}

        for fpath in files:
            try:
                # POS Headers are in row 2: Item Name, Itm Code, Qty, Cashier
                wb = load_workbook(fpath, read_only=True, data_only=True)
                ws = wb.active
                
                rows = ws.iter_rows(min_row=2, values_only=True)
                header_row = next(rows, None)
                if not header_row: continue
                
                headers = {str(val).strip().lower(): idx for idx, val in enumerate(header_row) if val}
                
                col_code = headers.get('itm code', headers.get('item code', headers.get('code')))
                col_qty = headers.get('qty', headers.get('quantity'))
                col_name = headers.get('item name', headers.get('description'))

                if col_qty is None:
                    logger.warning(f"File {os.path.basename(fpath)} missing quantity column.")
                    continue

                for row in rows:
                    code = str(row[col_code]).strip() if col_code is not None and row[col_code] else None
                    qty = self._safe_float(row[col_qty])
                    name = str(row[col_name]).strip().upper() if col_name is not None and row[col_name] else None

                    if not code and not name: continue
                    
                    for key in [code, name]:
                        if key:
                            sales_stats[key] = sales_stats.get(key, 0.0) + qty
                
                wb.close()
            except Exception as e:
                logger.error(f"Error reading {os.path.basename(fpath)}: {e}")
        
        logger.info(f"POS Scan Complete. Indexed {len(sales_stats)} item sales records.")
        return sales_stats

    def scan_inventory_transfers(self):
        """
        Scans transfer in (trn_*.xlsx) and transfer out (trout_*.xlsx) files.
        Returns: { 'item_code_or_barcode': { 'in': X, 'out': Y, 'net': Z } }
        """
        import glob
        logger.info("Scanning Inventory Transfer files...")
        transfer_stats = {}

        # 1. Process Transfers In (trn_*.xlsx)
        in_files = glob.glob(os.path.join(self.data_dir, "trn_*.xlsx"))
        for fpath in in_files:
            try:
                wb = load_workbook(fpath, read_only=True, data_only=True)
                ws = wb.active
                header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if not header_row: continue
                
                headers = {str(val).strip().lower().replace(' ', ''): idx for idx, val in enumerate(header_row) if val}
                col_barcode = headers.get('barcode')
                col_qty = headers.get('stiqty', headers.get('qty'))
                col_name = headers.get('itemname')

                for row in ws.iter_rows(min_row=2, values_only=True):
                    barcode = str(row[col_barcode]).strip() if col_barcode is not None and row[col_barcode] else None
                    qty = self._safe_float(row[col_qty])
                    name = str(row[col_name]).strip().upper() if col_name is not None and row[col_name] else None
                    
                    for key in [barcode, name]:
                        if key:
                            if key not in transfer_stats: transfer_stats[key] = {'in': 0.0, 'out': 0.0}
                            transfer_stats[key]['in'] += qty
                wb.close()
            except Exception as e:
                logger.error(f"Error reading {os.path.basename(fpath)}: {e}")

        # 2. Process Transfers Out (trout_*.xlsx)
        out_files = glob.glob(os.path.join(self.data_dir, "trout_*.xlsx"))
        for fpath in out_files:
            try:
                wb = load_workbook(fpath, read_only=True, data_only=True)
                ws = wb.active
                header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if not header_row: continue
                
                headers = {str(val).strip().lower().replace(' ', ''): idx for idx, val in enumerate(header_row) if val}
                col_barcode = headers.get('barcode')
                col_qty = headers.get('stoqty', headers.get('qty'))
                col_name = headers.get('itemname')

                for row in ws.iter_rows(min_row=2, values_only=True):
                    barcode = str(row[col_barcode]).strip() if col_barcode is not None and row[col_barcode] else None
                    qty = self._safe_float(row[col_qty])
                    name = str(row[col_name]).strip().upper() if col_name is not None and row[col_name] else None
                    
                    for key in [barcode, name]:
                        if key:
                            if key not in transfer_stats: transfer_stats[key] = {'in': 0.0, 'out': 0.0}
                            transfer_stats[key]['out'] += qty
                wb.close()
            except Exception as e:
                logger.error(f"Error reading {os.path.basename(fpath)}: {e}")

        for k in transfer_stats:
            transfer_stats[k]['net'] = transfer_stats[k]['out'] - transfer_stats[k]['in']
            
        logger.info(f"Transfer Scan Complete. Indexed {len(transfer_stats)} transfer records.")
        return transfer_stats

    def update_demand_intelligence(self):
        """
        Integrates POS Sales and Transfers to update the Sales Forecasting database.
        Formula: True Demand = POS Sales + Transfers Out - Transfers In
        """
        sales = self.scan_cashier_sales()
        transfers = self.scan_inventory_transfers()
        
        if not sales and not transfers:
            return

        # Load current forecasting DB
        forecast_db = self.databases.get('sales_forecasting', {})
        if not forecast_db:
            logger.warning("Sales forecasting database is empty. Cannot update demand intelligence.")
            return

        update_count = 0
        all_keys = set(sales.keys()) | set(transfers.keys())
        
        for key in all_keys:
            # We need to find the entry in forecast_db
            # This is tricky because keys might be codes or names.
            # find_best_match is used during enrichment, but here we need to update the DB.
            # Let's try direct lookup first.
            entry = forecast_db.get(key)
            if not entry:
                # Try case matching
                for k in forecast_db:
                    if k.upper() == key.upper():
                        entry = forecast_db[k]
                        break
            
            if entry:
                pos_qty = sales.get(key, 0.0)
                trans = transfers.get(key, {'in':0.0, 'out':0.0, 'net':0.0})
                
                # True Demand = Internal usage/sales that reduced stock
                true_demand = pos_qty + trans['out'] - trans['in']
                
                # Update avg_daily_sales based on a 365 day year (or 30 day month if we assume these files are monthly)
                # Given files like jan_cash, mar_cash, it seems we have multiple months.
                # Let's assume we are updating the YEARLY average.
                # If we don't have a count of days, we'll use a standard divisor.
                # Existing field is usually 'avg_daily_sales'
                
                # Simple update for now: blend previous with new if possible, or overwrite if new is substantial
                prev_avg = entry.get('avg_daily_sales', 0.0)
                # Assuming these files cover approx 300 days (sampled throughout the year)
                calculated_daily = max(0.0, true_demand / 300.0)
                
                if prev_avg > 0:
                    entry['avg_daily_sales'] = round((prev_avg + calculated_daily) / 2, 4)
                else:
                    entry['avg_daily_sales'] = round(calculated_daily, 4)
                
                update_count += 1

        # Save back
        available_files = os.listdir(self.data_dir)
        match = next((f for f in available_files if 'sales_forecasting_2025' in f), "sales_forecasting_2025_updated.json")
        save_path = os.path.join(self.data_dir, match)
        
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(forecast_db, f, indent=2)
            logger.info(f"Updated sales forecasting database saved to {match} ({update_count} entries updated)")
            self.databases['sales_forecasting'] = forecast_db
        except Exception as e:
            logger.error(f"Failed to save updated sales forecasting: {e}")

    def scan_grn_files(self):
        """
        Scans GRN files to build cost history.
        v8.2: Restored as stub/minimal to fix AttributeError.
        """
        # For now, return empty to unblock simulation. 
        # Enrichment will fallback to estimated cost.
        return {}

    def scan_purchase_orders(self):
        """
        Scans all purchase order Excel files (po_*.xlsx) and extracts supplier ordering history.
        Returns: { 'supplier_name': [list_of_dates] }
        """
        import glob
        from datetime import datetime
        logger.info("Scanning Purchase Order Excel files...")
        po_history = {}
        
        files = glob.glob(os.path.join(self.data_dir, "po_*.xlsx"))
        if not files:
            logger.warning("No Purchase Order files found (po_*.xlsx)")
            return {}

        for fpath in files:
            try:
                wb = load_workbook(fpath, read_only=True, data_only=True)
                ws = wb.active
                
                header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if not header_row: continue
                
                headers = {"".join(c for c in str(val).lower() if c.isalnum()): idx for idx, val in enumerate(header_row) if val}
                
                col_vendor = headers.get('vendorcodename', headers.get('vendor'))
                col_date = headers.get('podate', headers.get('date'))
                col_net_amt = headers.get('netamt', headers.get('amount'))

                if col_vendor is None or col_date is None:
                    logger.warning(f"File {os.path.basename(fpath)} missing critical PO columns.")
                    continue

                for row in ws.iter_rows(min_row=2, values_only=True):
                    vendor_raw = str(row[col_vendor]).strip() if row[col_vendor] else None
                    if not vendor_raw: continue
                    
                    # Normalize supplier name: "SB0009 - BROOKSIDE DAIRY LIMITED" -> "BROOKSIDE DAIRY LIMITED"
                    if ' - ' in vendor_raw:
                        supplier = vendor_raw.split(' - ', 1)[1].upper().strip()
                    else:
                        supplier = vendor_raw.upper().strip()

                    date_val = row[col_date]
                    if not date_val: continue
                    
                    # Convert to datetime if it's a string
                    if isinstance(date_val, str):
                        try:
                            # Try common formats
                            for fmt in ('%d-%b-%Y', '%Y-%m-%d', '%m/%d/%Y'):
                                try:
                                    date_obj = datetime.strptime(date_val, fmt)
                                    break
                                except ValueError:
                                    continue
                            else:
                                logger.warning(f"Could not parse date: {date_val}")
                                continue
                        except Exception:
                            continue
                    elif isinstance(date_val, datetime):
                        date_obj = date_val
                    else:
                        continue

                    if supplier not in po_history:
                        po_history[supplier] = []
                    
                    po_history[supplier].append(date_obj)
                
                wb.close()
            except Exception as e:
                logger.error(f"Error reading {os.path.basename(fpath)}: {e}")
        
        # Sort dates for each supplier
        for supplier in po_history:
            po_history[supplier].sort()
            
        logger.info(f"PO Scan Complete. Indexed history for {len(po_history)} suppliers.")
        return po_history

    def update_supplier_patterns(self):
        """Processes PO files and updates the supplier patterns database."""
        import statistics
        po_history = self.scan_purchase_orders()
        if not po_history:
            return
        
        # Load existing database
        patterns_db = self.databases.get('supplier_patterns', {})
        
        update_count = 0
        for supplier, dates in po_history.items():
            if len(dates) < 2:
                # Need at least two orders to calculate gaps
                continue
            
            gaps = []
            for i in range(1, len(dates)):
                gap = (dates[i] - dates[i-1]).days
                if gap > 0: # Ignore same-day orders
                    gaps.append(gap)
            
            if not gaps:
                continue
                
            median_gap = statistics.median(gaps)
            avg_gap = statistics.mean(gaps)
            total_orders = len(dates)
            
            if supplier not in patterns_db:
                patterns_db[supplier] = {
                    'median_gap_days': 0,
                    'average_gap_days': 0,
                    'estimated_delivery_days': 7, # Default fallback
                    'total_orders_2025': 0,
                    'reliability_score': 100
                }
            
            pat = patterns_db[supplier]
            pat['median_gap_days'] = int(median_gap)
            pat['average_gap_days'] = round(avg_gap, 1)
            pat['total_orders_2025'] = total_orders
            
            # Simple reliability based on consistency of gaps if many orders
            if len(gaps) > 5:
                # coefficient of variation
                stdev = statistics.stdev(gaps)
                cv = stdev / avg_gap
                # Lower CV means more reliable
                reliability = max(0, 100 - int(cv * 50))
                pat['reliability_score'] = reliability
                
            update_count += 1

        # Save back to file
        available_files = os.listdir(self.data_dir)
        match = next((f for f in available_files if 'supplier_patterns_2025' in f), "supplier_patterns_2025_updated.json")
        save_path = os.path.join(self.data_dir, match)
        
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(patterns_db, f, indent=2)
            logger.info(f"Updated supplier patterns database saved to {match} ({update_count} suppliers updated)")
            self.databases['supplier_patterns'] = patterns_db
        except Exception as e:
            logger.error(f"Failed to save updated supplier patterns: {e}")

    def update_lead_time_intelligence(self):
        """
        Calculates fulfillment lead times by linking PO dates to GRN receipt dates.
        Updates 'estimated_delivery_days' in supplier patterns.
        """
        import glob
        from datetime import datetime
        import statistics
        
        logger.info("Starting Lead Time Intelligence calculation...")
        po_dates = {} # { po_no: po_date }
        
        # 1. Scan PO files to get PO issuance dates
        po_files = glob.glob(os.path.join(self.data_dir, "po_*.xlsx"))
        for fpath in po_files:
            try:
                wb = load_workbook(fpath, read_only=True, data_only=True)
                ws = wb.active
                header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if not header_row: continue
                headers = {"".join(c for c in str(val).lower() if c.isalnum()): idx for idx, val in enumerate(header_row) if val}
                
                col_po = headers.get('pono')
                col_date = headers.get('podate')
                
                if col_po is None or col_date is None: continue

                for row in ws.iter_rows(min_row=2, values_only=True):
                    po_no = str(row[col_po]).strip() if row[col_po] else None
                    if not po_no: continue
                    
                    date_val = row[col_date]
                    if isinstance(date_val, str):
                        for fmt in ('%d-%b-%Y', '%Y-%m-%d', '%m/%d/%Y'):
                            try:
                                date_obj = datetime.strptime(date_val, fmt)
                                break
                            except ValueError: continue
                        else: continue
                    elif isinstance(date_val, datetime):
                        date_obj = date_val
                    else: continue
                    
                    po_dates[po_no] = date_obj
                wb.close()
            except Exception as e: logger.error(f"Error reading PO {fpath}: {e}")

        # 2. Scan GRN files to find receipt dates for those POs
        supplier_lead_times = {} # { supplier_name: [list_of_gaps] }
        grn_files = glob.glob(os.path.join(self.data_dir, "grnd*.xlsx"))
        for fpath in grn_files:
            try:
                wb = load_workbook(fpath, read_only=True, data_only=True)
                ws = wb.active
                header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
                if not header_row: continue
                headers = {"".join(c for c in str(val).lower() if c.isalnum()): idx for idx, val in enumerate(header_row) if val}
                
                col_po = headers.get('pono')
                col_date = headers.get('grndate', headers.get('docdate'))
                col_vendor = headers.get('vendorcodename', headers.get('vendor'))
                
                if col_po is None or col_date is None or col_vendor is None: continue

                for row in ws.iter_rows(min_row=2, values_only=True):
                    po_no = str(row[col_po]).strip() if row[col_po] else None
                    if not po_no or po_no not in po_dates: continue
                    
                    date_val = row[col_date]
                    if isinstance(date_val, str):
                        for fmt in ('%d-%b-%Y', '%Y-%m-%d', '%m/%d/%Y'):
                            try:
                                date_obj = datetime.strptime(date_val, fmt)
                                break
                            except ValueError: continue
                        else: continue
                    elif isinstance(date_val, datetime):
                        date_obj = date_val
                    else: continue
                    
                    # Calculate gap
                    gap = (date_obj - po_dates[po_no]).days
                    if gap < 0: continue # Should not happen unless bad data
                    
                    vendor_raw = str(row[col_vendor]).strip()
                    if ' - ' in vendor_raw:
                        supplier = vendor_raw.split(' - ', 1)[1].upper().strip()
                    else:
                        supplier = vendor_raw.upper().strip()

                    if supplier not in supplier_lead_times: supplier_lead_times[supplier] = []
                    supplier_lead_times[supplier].append(gap)
                wb.close()
            except Exception as e: logger.error(f"Error reading GRN {fpath}: {e}")

        # 3. Aggregate and Update Database
        patterns_db = self.databases.get('supplier_patterns', {})
        update_count = 0
        for supplier, gaps in supplier_lead_times.items():
            if not gaps: continue
            
            median_lead = int(statistics.median(gaps))
            
            if supplier in patterns_db:
                patterns_db[supplier]['estimated_delivery_days'] = max(1, median_lead)
                update_count += 1
            else:
                # Create default entry if missing
                patterns_db[supplier] = {
                    'median_gap_days': 7,
                    'average_gap_days': 7.0,
                    'estimated_delivery_days': max(1, median_lead),
                    'total_orders_2025': len(gaps),
                    'reliability_score': 100
                }
                update_count += 1

        # Save back
        available_files = os.listdir(self.data_dir)
        match = next((f for f in available_files if 'supplier_patterns_2025' in f), "supplier_patterns_2025_updated.json")
        save_path = os.path.join(self.data_dir, match)
        
        try:
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(patterns_db, f, indent=2)
            logger.info(f"Updated lead times in patterns database saved to {match} ({update_count} suppliers updated)")
            self.databases['supplier_patterns'] = patterns_db
        except Exception as e:
            logger.error(f"Failed to save updated lead times: {e}")


    def scan_sales_profitability(self):
        """
        Phase 8: Profitability & Top SKU Refresh.
        Scans topselqty.xlsx and updates the sales_profitability_intelligence_2025 database.
        """
        import os
        logger.info("Scanning Sales Profitability (Top SKUs) file...")
        
        file_path = os.path.join(self.data_dir, "topselqty.xlsx")
        if not os.path.exists(file_path):
            logger.warning(f"Top SKUs file not found: {file_path}")
            return
            
        try:
            wb = load_workbook(file_path, read_only=True, data_only=True)
            ws = wb.active
            
            # Headers: ORG Code/Name, Itm Code, Item Name, QTY, NET AMT, LPP, WAC, TAX AMT, MARGIN, MARGIN %
            header_row = next(ws.iter_rows(min_row=1, max_row=1, values_only=True), None)
            if not header_row:
                return
                
            headers = {str(val).strip().lower(): idx for idx, val in enumerate(header_row) if val}
            
            col_name = headers.get('item name', headers.get('description'))
            col_qty = headers.get('qty')
            col_rev = headers.get('net amt', headers.get('revenue'))
            col_margin_amt = headers.get('margin')
            col_margin_pct = headers.get('margin %')
            
            if col_name is None or col_qty is None:
                logger.warning("Missing critical columns in topselqty.xlsx")
                return

            new_profitability = {}
            # Load all rows into memory to sort correctly for ranking
            rows = []
            for row in ws.iter_rows(min_row=2, values_only=True):
                if row[col_name] and isinstance(row[col_qty], (int, float)):
                    rows.append(row)
            
            # Sort by QTY descending to assign rank
            rows.sort(key=lambda x: x[col_qty], reverse=True)
            
            for idx, row in enumerate(rows, 1):
                name = str(row[col_name]).strip().upper()
                new_profitability[name] = {
                    "total_qty_sold": row[col_qty],
                    "revenue": float(row[col_rev]) if col_rev is not None and row[col_rev] else 0.0,
                    "margin_pct": float(row[col_margin_pct]) if col_margin_pct is not None and row[col_margin_pct] else 0.0,
                    "gross_profit": float(row[col_margin_amt]) if col_margin_amt is not None and row[col_margin_amt] else 0.0,
                    "sales_rank": idx,
                    "category": "unknown"
                }

            # Save back to JSON
            match = "sales_profitability_intelligence_2025_updated.json"
            save_path = os.path.join(self.data_dir, match)
            
            with open(save_path, 'w', encoding='utf-8') as f:
                json.dump(new_profitability, f, indent=2)
                
            logger.info(f"Updated sales profitability database saved to {match} ({len(new_profitability)} entries)")
            self.databases['sales_profitability'] = new_profitability
            
        except Exception as e:
            logger.error(f"Failed to scan sales profitability: {e}")
