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

class OrderEngine:
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.databases = {}
        self.grn_db = {} # Aggregated history from Excel
        self.no_grn_suppliers = []
        self.budget_manager = BudgetManager(data_dir)
        self.profile_manager = StoreProfileManager()

    def load_no_grn_suppliers(self):
        try:
            path = os.path.join(self.data_dir, 'app/data/no_grn_suppliers.json')
            if not os.path.exists(path):
                path = os.path.join(self.data_dir, 'no_grn_suppliers.json')
            with open(path, 'r') as f:
                self.no_grn_suppliers = [s.upper() for s in json.load(f)]
        except:
            self.no_grn_suppliers = []




    def load_local_databases(self):
        """Synchronous database loading for backward compatibility."""
        logger.info(f"Loading databases synchronously from {self.data_dir}")
        if not self.grn_db:
            self.grn_db = self.scan_grn_files()
        
        self.load_no_grn_suppliers()

        db_configs = {
            'supplier_patterns': 'supplier_patterns_2025',
            'product_intelligence': 'sales_profitability_intelligence_2025',
            'sales_forecasting': 'sales_forecasting_2025',
            'supplier_quality': 'supplier_quality_scores_2025',
            'sales_profitability': 'sales_profitability_intelligence_2025'
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
        # Simple heuristic: Split by first word (usually brand)
        brand = product_name.split()[0].upper()
        similar_sales = []
        
        for name, data in sales_database.items():
            if name.startswith(brand):
                similar_sales.append(data.get('avg_daily_sales', 0))
        
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
        return selling_price * 0.75

    def enrich_product_data(self, products: List[dict]):
        """Phase 3: Product Enrichment. Maps all intelligence metrics."""
        supplier_patterns = self.databases.get('supplier_patterns', {})
        sales_forecasting = self.databases.get('sales_forecasting', {})
        supplier_quality = self.databases.get('supplier_quality', {})
        sales_profitability = self.databases.get('sales_profitability', {})
        
        logger.info(f"Phase 3: Enriching {len(products)} products...")
        
        for p in products:
            p_name = p.get('product_name', '')
            p_code = p.get('item_code')
            p_barcode = str(p.get('barcode', '')).strip()
            supplier = p.get('supplier_name', '').upper()
            
            # v2.1 Consignment Flagging
            # Check No GRN list OR "PLU" keyword in name
            is_consignment = (supplier in self.no_grn_suppliers) or ("PLU" in p_name.upper())
            p['is_consignment'] = is_consignment

            # 1. Supplier Patterns
            pat = supplier_patterns.get(supplier, {})
            p['estimated_delivery_days'] = pat.get('estimated_delivery_days', 7)
            p['supplier_frequency'] = pat.get('order_frequency', 'daily') # Map to the field name AI expect
            p['reliability_score'] = pat.get('reliability_score', 90)
            p['supplier_frequency_days'] = pat.get('median_gap_days', 7)

            # 2. Sales Forecasting
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
            # Gold Standard: Brookside (1d) uses +1 buffer, SBC/Towfiq (7d) uses +3 buffer.
            d_days = p['estimated_delivery_days']
            buffer = 3 if d_days >= 4 else 1
            p['target_coverage_days'] = d_days + buffer
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

    def apply_greenfield_allocation(self, recommendations: List[dict], total_budget: float) -> dict:
        """
        Implements the Comprehensive Tiered Allocation Logic.
        Phase 1: Width (Essential Inclusion & Anchors) - "General Opening Fund"
        Phase 2: Depth (Wallet-Constrained Volume) - Split 80/20 for Small Stores
        
        Returns: dict with 'recommendations' and 'summary' keys
        """
        logger.info(f"--- Starting Tiered Allocation (Budget: ${total_budget:,.0f}) ---")
        
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
        
        # Duka Specifics
        fast_five_depts = ['FRESH MILK', 'BREAD', 'COOKING OIL', 'FLOUR', 'SUGAR']
        
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
        # Ensure we pick the Best items for the limited slots
        recommendations.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
        
        # --- PASS 1: GLOBAL WIDTH (Variety First) ---
        # "Allocates exactly 1 Pack (MDQ) to every item."
        pass1_cost = 0.0
        pass1_consignment_val = 0.0
        sku_counts_per_dept = {} # For "One Brand/Limit" logic
        
        for rec in recommendations:
            p_name = rec['product_name']
            dept = rec.get('product_category', 'GENERAL').upper()
            is_staple = self.budget_manager.is_staple(p_name)
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
            
            # 0. Internal Production Exclusion (v2.8)
            # Bakery Foodplus is internal production, not purchased from suppliers
            if dept in ['BAKERY FOODPLUS', 'BALERY FOODPLU']:  # Handle typo variance
                should_list = False
                reason_tag = "[PASS 1: INTERNAL PRODUCTION - NOT PURCHASED]"
            
            # 1. ABC / Subsistence Filters
            abc_class = rec.get('ABC_Class', 'A') 
            
            # Unified Dynamic Constraint Logic (Replaces hardcoded Micro/Standard split)
            # Price Ceiling Check
            if price > price_ceiling:
                if is_staple: # Anchors override ceiling
                    reason_tag = "[PASS 1: ANCHOR OVERRIDE]"
                    should_list = True
                else:
                    reason_tag = f"[PASS 1: BLOCKED - PRICE > {price_ceiling:.0f}]"
                    should_list = False
            
            # SKU Cap Per Dept (if very small store)
            # Use dynamic profile constraints if added later, for now we rely on Budget Guard.
            # But let's keep the Micro Cap if needed? 
            # Actually, the user wants "Dynamic".
            # Let's trust the profile's Price Ceiling to filter out the noise.
            
            # Dead Stock Check (Dynamic)
            elif not allow_c_class and abc_class == 'C':
                 avg_daily = rec.get('avg_daily_sales', 0)
                 if avg_daily < 0.2: 
                      should_list = False
                      reason_tag = "[PASS 1: DEAD STOCK]"

            # Hybrid Scaled Demand Logic (Standard+)
            if should_list and not is_micro:
                mega_budget = 114000000.0
                budget_ratio = total_budget / mega_budget
                mega_demand_proxy = rec.get('avg_daily_sales', 0) * 45 
                scaled_demand = mega_demand_proxy * budget_ratio
                
                # v2.5: Exempt new products from this filter (they'll get conservative treatment in Pass 2)
                has_lookalike = rec.get('lookalike_demand', 0) > 0
                is_new_product = rec.get('avg_daily_sales', 0) == 0
                
                if should_list and is_small:
                    if scaled_demand >= 0.5:
                        pass 
                    elif is_staple:
                        reason_tag = "[PASS 1: STAPLE SAFETY NET]"
                    elif is_new_product or has_lookalike:
                        # Allow new products through to Get conservative allocation in Pass 2
                        reason_tag = "[PASS 1: NEW PRODUCT - PROVISIONAL]"
                    else:
                        should_list = False
                        reason_tag = f"[SCALED DROP] Demand: {scaled_demand:.2f} < 0.5"

            if should_list:
                # Apply Min Display Qty
                # FIXED: Ensure MDQ respects Pack Sizes
                # If pack size is 6, and MDQ is 3, we buy 1 pack (6 units).
                # If pack size is 1, and MDQ is 3, we buy 3 packs (3 units).
                
                raw_mdq = min_display_qty
                
                # Check for BUDGET GUARD (Pass 1 Safety Break)
                # If we have already spent 90% of budget just on WIDTH, we must stop adding non-essential items.
                if pass1_cost > (total_budget * 0.95):
                     if is_staple:
                          # Anchors get a pass, but maybe reduced
                          raw_mdq = max(1, raw_mdq // 2)
                     else:
                          # Cut discretionary width if budget is critical
                          rec['recommended_quantity'] = 0
                          rec['reasoning'] = f"[PASS 1: BUDGET EXHAUSTED] Width Cut."
                          rec['pass1_allocated'] = False
                          continue

                rec_qty_units = max(int(rec.get('moq_floor', 0)), raw_mdq)
                
                # Convert to Packs
                required_packs = (rec_qty_units + pack_size - 1) // pack_size # Ceiling div
                required_packs = max(1, required_packs)
                
                rec_qty_final = required_packs * pack_size
                
                # v2.7: Enforce max_packs limit even in Pass 1
                max_allowed_units = max_total_packs * pack_size
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
                summary['skip_reasons'][skip_category] = summary['skip_reasons'].get(skip_category, 0) + 1

        logger.info(f"Pass 1 Complete. Committed: ${pass1_cost:,.2f}")
        
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
        fast_five_candidates = [r for r in candidates if is_small and r.get('product_category','').upper() in fast_five_depts and self.budget_manager.is_staple(r['product_name'])]
        # 2. Other Staples
        other_staple_candidates = [r for r in candidates if self.budget_manager.is_staple(r['product_name']) and r not in fast_five_candidates]
        # 3. Discretionary
        discretionary_candidates = [r for r in candidates if not self.budget_manager.is_staple(r['product_name'])]
        
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
        

        def fill_depth_constrained(candidate_list, global_spending_cap=999999999.0):
            batch_cost = 0.0
            
            # Create a working queue of candidates that still need depth
            # Store calculated targets upfront to avoid re-calc
            queue = []
            for rec in candidate_list:
                dept = rec.get('product_category', 'GENERAL').upper()
                avg_sales = rec.get('avg_daily_sales', 0.0)
                
                # --- v2.5 NEW PRODUCT HYBRID LOGIC ---
                # Don't skip items with 0 sales; use lookalike or conservative baseline
                effective_avg_sales = avg_sales
                new_product_mode = False
                
                if avg_sales <= 0:
                    # Check for lookalike demand
                    lookalike = rec.get('lookalike_demand', 0.0)
                    if lookalike > 0:
                        effective_avg_sales = lookalike * 0.5  # Conservative: 50% of lookalike
                        new_product_mode = True
                        if "[NEW PRODUCT" not in rec['reasoning']:
                            rec['reasoning'] += f" [NEW PRODUCT: Lookalike-Based]"
                    else:
                        # Absolute minimum baseline for items that passed Pass 1
                        # Fresh: 0.3/day (conservative trial), Dry: 0.5/day
                        is_fresh = rec.get('is_fresh', False)
                        effective_avg_sales = 0.3 if is_fresh else 0.5
                        new_product_mode = True
                        if "[NEW PRODUCT" not in rec['reasoning']:
                            rec['reasoning'] += f" [NEW PRODUCT: Baseline Estimate]"
                
                # Skip only if effective sales is still zero (shouldn't happen after baseline)
                if effective_avg_sales <= 0:
                    continue
                
                # Depth Logic
                # Default: Use Tier Config
                effective_days = depth_cap_days
                
                # --- v2.4 SMART DEPTH (Risk-Based Buffer) ---
                # Strategy: Only Buffer A/B Class items. Do not buffer C-Class (Slow Movers).
                risk_multiplier = 1.0
                risk_reasons = []
                
                abc_class = rec.get('ABC_Class', 'A')
                
                if abc_class != 'C':
                    # Reliability Check
                    rel_score = rec.get('reliability_score', 90)
                    if rel_score < 70:
                        risk_multiplier += 0.25
                        risk_reasons.append(f"Unreliable Supp({rel_score}%)")
                        
                    # Volatility Check
                    volatility = rec.get('demand_cv', 0.5)
                    if volatility > 0.8:
                        risk_multiplier += 0.15
                        risk_reasons.append(f"Volatile({volatility})")
                        
                    if risk_multiplier > 1.0:
                        # Cap at 1.5x to prevent explosion
                        risk_multiplier = min(risk_multiplier, 1.5)
                        effective_days = int(effective_days * risk_multiplier)
                        
                        if "[RISK BUFFER" not in rec['reasoning']:
                             rec['reasoning'] += f" [RISK BUFFER: +{int((risk_multiplier-1)*100)}% ({', '.join(risk_reasons)})]"

                # Duka "Perfect Initial Allocation" Boost
                # For Non-Perishable Fast Five, we ignore liquidity constraints and Stock Up (30 Days)
                # But we MUST respect spoilage for Milk/Bread.
                min_pack_floor = 0
                if is_small:
                    if dept in ['COOKING OIL', 'FLOUR', 'SUGAR']:
                        # Anchor boost overrides standard depth, but we keep the risk buffer if it pushes it higher?
                        # Let's say Anchor Boost is the floor.
                        effective_days = max(effective_days, 30) 
                        
                        # Case Quantity Logic (Presentation Minimums)
                        # Don't buy 3 units. Buy a Half-Case (6) or Strip (12).
                        unit_price = float(rec.get('selling_price', 0))
                        if unit_price < 50:
                             min_pack_floor = 12 # Sachet Strip
                        else:
                             min_pack_floor = 6  # Half-Case / Row
                             
                    elif dept in ['FRESH MILK', 'BREAD']:
                        effective_days = min(effective_days, 2) # Strict fresh clamp
                        # No min floor boost for fresh (spoilage risk)
                        min_pack_floor = 1 
                
                # New Product Conservative Cap
                if new_product_mode:
                    # Cap new products to conservative depth regardless of tier
                    is_fresh = rec.get('is_fresh', False)
                    max_new_product_days = 7 if is_fresh else 14
                    effective_days = min(effective_days, max_new_product_days)
                
                # v2.6 EXPIRY/SHELF-LIFE ENFORCEMENT
                # Prevent ordering more than can be sold before expiry
                shelf_life = rec.get('shelf_life_days', 365)
                if shelf_life < 30:  # Only enforce for perishables
                    # Leave 2-day safety buffer for delivery + shelf display
                    max_safe_days = max(1, shelf_life - 2)
                    if effective_days > max_safe_days:
                        effective_days = max_safe_days
                        if "[EXPIRY CAP" not in rec['reasoning']:
                            rec['reasoning'] += f" [EXPIRY CAP: {shelf_life}d shelf-life]"
                        
                ideal_qty = int(effective_avg_sales * effective_days)
                # Apply Min Floor for Anchors
                ideal_qty = max(ideal_qty, min_pack_floor)
                
                current_qty = rec['recommended_quantity']
                pack_size = int(rec.get('pack_size', 1))
                max_allowed_units = max_total_packs * pack_size
                
                # Boost max packs for Anchors too? 
                # If we want 14 days of Oil (e.g. 14 units), but max_packs is 2, we fail.
                # Override Max Packs for Boosted items.
                if is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR']:
                    max_allowed_units = 999
                elif total_budget >= 20000000: # Mega Store - Unlimited Depth
                    max_allowed_units = 99999999
                
                final_target = min(ideal_qty, max_allowed_units)
                
                if current_qty < final_target:
                    price = float(rec.get('selling_price', 0.0))
                    # v2.9: Use actual cost
                    cost_price_est = self._get_actual_cost_price(rec, price)
                    queue.append({
                        'rec': rec,
                        'dept': dept,
                        'pack_size': pack_size,
                        'cost_per_pack': pack_size * cost_price_est,
                        'target_qty': final_target,
                        'cost_est': cost_price_est
                    })

            # Round-Robin Loop
            active = True
            while active and queue:
                active = False 
                
                # We iterate a copy to allow removal from main queue
                for i in range(len(queue) - 1, -1, -1):
                    item = queue[i]
                    rec = item['rec']
                    dept = item['dept']
                    pack_cost = item['cost_per_pack']
                    pack_size = item['pack_size']
                    
                    # 1. Global Cap Check
                    if (batch_cost + pack_cost) > global_spending_cap:
                         rec['reasoning'] += " [PASS 2: GLOBAL BUDGET PARTITION CAP]"
                         # Stop processing this item, and honestly stop everything
                         # But let's just remove this item to be safe, loops will finish
                         queue.pop(i)
                         continue

                    # Duka Priority Bypass Logic
                    is_priority_bypass = False
                    # Only bypass for Standard Duka (not Micro)
                    if is_small and not is_micro and dept in ['COOKING OIL', 'FLOUR', 'SUGAR', 'FRESH MILK', 'BREAD']:
                         is_priority_bypass = True

                    # 2. Check Item Limits (Share Cap)
                    # Bypass for Anchors (we want to stock them heavy)
                    if not is_priority_bypass:
                        wallet_limit_ratio = 0.25 if is_small else 0.50
                        if dept in wallets:
                            max_item_spend = wallets[dept]['allocated_budget'] * wallet_limit_ratio
                        else:
                            max_item_spend = 999999999.0
                        
                        current_qty = rec['recommended_quantity']
                        pack_cost = item['cost_per_pack'] # Need to re-extract or assume in scope?
                        # item is queue[i]. Loop context?
                        # Ah, we are in the loop. 'item' is defined.
                        current_item_spend = current_qty * item['cost_est']
                        
                        if (current_item_spend + pack_cost) > max_item_spend:
                            if rec.get('pass1_allocated', False):
                                 rec['reasoning'] += " [PASS 2: ITEM SHARE CAP]"
                                 queue.pop(i)
                                 continue
                    
                    # 3. Check Wallet Availability
                    # Bypass for Anchors (Use the "General Liquid Fund" essentially)
                    # Bypass for Consignment (Free Capital)
                    
                    is_consignment = rec.get('is_consignment', False)
                    can_afford = False
                    
                    if is_consignment:
                        can_afford = True # Always afford free stuff (subject to max packs)
                    elif is_priority_bypass:
                        can_afford = True
                    else:
                        can_afford = self.budget_manager.check_wallet_availability(wallets, dept, pack_cost)
                    
                    if can_afford:
                        # We track spend even if bypassing, wallets will go negative (which is fine, reflects reality of over-investment in anchors)
                        # BUT: Do NOT spend for Consignment (it's free)
                        if not is_consignment:
                            self.budget_manager.spend_from_wallet(wallets, dept, pack_cost)
                            batch_cost += pack_cost
                        
                        rec['recommended_quantity'] += pack_size
                        active = True # We did something!
                        
                        # Check if satisfied
                        if rec['recommended_quantity'] >= item['target_qty']:
                            # Done with this item
                            if "[PASS 2" not in rec['reasoning']:
                                rec['reasoning'] += " [PASS 2: DEPTH FILL]"
                            queue.pop(i)
                    else:
                        rec['reasoning'] += " [PASS 2: WALLET CAP]"
                        queue.pop(i) # Cannot buy anymore for this department
            
            return batch_cost

        # 1. Fill Fast Five (Priority)
        added_fast_five_cost = 0
        if fast_five_candidates:
             # Allow them to consume ALL remaining budget if needed
             added_fast_five_cost = fill_depth_constrained(fast_five_candidates, global_spending_cap=total_remaining_budget) 
        
        # Recalculate remaining for others
        remaining_after_ff = total_remaining_budget - added_fast_five_cost
        
        staple_allocation_target = remaining_after_ff * pass2_staple_share
        discretionary_hard_cap = remaining_after_ff * (1.0 - pass2_staple_share)
        
        logger.info(f"Pass 2 Remaining: ${remaining_after_ff:,.2f} (Other Staples Target: ${staple_allocation_target:,.2f}, Disc Cap: ${discretionary_hard_cap:,.2f})")
        
        # 2. Fill Other Staples (Capped by remaining)
        # We use 'remaining_after_ff' or 'staple_allocation_target' as the global cap?
        # User implies Fast Five takes precedence. Other Staples get the scraps.
        # So we cap at remaining_after_ff. 
        # But we also have Discretionary.
        # Implies we should split the scraps. 
        # But if remaining is ~0, cap is ~0.
        added_other_staple_cost = fill_depth_constrained(other_staple_candidates, global_spending_cap=staple_allocation_target)
        
        # 3. Fill Discretionary (Capped)
        added_disc_cost = fill_depth_constrained(discretionary_candidates, global_spending_cap=discretionary_hard_cap)
        
        pass2_cost = added_fast_five_cost + added_other_staple_cost + added_disc_cost

        logger.info(f"Pass 2 Complete. Added Depth: ${pass2_cost:,.2f} (Fast5: ${added_fast_five_cost:,.0f}, OtherStaples: ${added_other_staple_cost:,.0f}, Discretionary: ${added_disc_cost:,.0f})")
        
        # --- PASS 2B: BUDGET REDISTRIBUTION (v2.5) ---
        # v2.9 FIX: Calculate TRUE unused budget (not wallet balances which include buffers)
        actual_spent = pass1_cost + pass2_cost
        true_unused = total_budget - actual_spent
        total_allocated_budget = sum([w['max_budget'] for w in wallets.values()])
        unused_pct = (true_unused / total_budget * 100) if total_budget > 0 else 0
        
        redistrib_cost = 0.0  # Initialize
        
        if true_unused > (total_budget * 0.10):  # > 10% unused
            logger.info(f"Pass 2B: Budget Redistribution Active. Unused: ${true_unused:,.2f} ({unused_pct:.1f}%)")
            
            # Identify high-priority items that were capped (hit max_packs or wallet limit)
            realloc_candidates = []
            for rec in recommendations:
                if rec.get('pass1_allocated') and rec['recommended_quantity'] > 0:
                    # Check if item is a Staple or A-Class
                    is_staple = self.budget_manager.is_staple(rec['product_name'])
                    abc_class = rec.get('ABC_Class', 'B')
                    is_priority = is_staple or abc_class == 'A'
                    
                    if is_priority:
                        # Check if item was likely capped (reasoning contains "CAP" or quantity = max_packs)
                        reasoning = rec.get('reasoning', '')
                        was_capped = 'CAP' in reasoning or 'WALLET' in reasoning
                        
                        if was_capped:
                            realloc_candidates.append(rec)
            
            # Sort by sales velocity (prioritize winners)
            realloc_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
            
            # Attempt to add more depth using TRUE unused budget (not wallet balances)
            redistrib_cost = fill_depth_constrained(realloc_candidates, global_spending_cap=true_unused)
            
            if redistrib_cost > 0:
                logger.info(f"Pass 2B: Redistributed ${redistrib_cost:,.2f} to {len([r for r in realloc_candidates if '[PASS 2B' in r.get('reasoning', '')])} priority items")
                # Mark items that received redistribution
                for rec in realloc_candidates:
                    if '[PASS 2B' not in rec.get('reasoning', '') and rec['recommended_quantity'] > rec.get('_pre_2b_qty', 0):
                        rec['_pre_2b_qty'] = rec.get('_pre_2b_qty', rec['recommended_quantity'])
                        rec['reasoning'] = rec['reasoning'].replace('[PASS 2: DEPTH FILL]', '[PASS 2B: REDISTRIBUTED]')
        
        # --- FINALIZE SUMMARY ---
        summary['pass1_cash'] = pass1_cost
        summary['pass1_consignment'] = pass1_consignment_val
        summary['pass2_cash'] = pass2_cost
        summary['pass2b_cash'] = redistrib_cost if 'redistrib_cost' in locals() else 0.0
        # v2.9 FIX: Include Pass 2B in total  
        summary['total_cash_used'] = pass1_cost + pass2_cost + summary['pass2b_cash']
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

    def scan_grn_files(self):
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
