import json
import csv
import io
import re
import os
import asyncio
import httpx
import math
import logging
import logging
import textwrap
import sys
from datetime import datetime
from typing import Literal, Any, Dict, List, Tuple, Optional, Union
from openpyxl import load_workbook

# Add root to path for absolute imports if needed
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Import Mixins
from .data_mixin import DataMixin
from .intelligence_mixin import IntelligenceMixin
from .procurement_mixin import ProcurementMixin
from .maintenance_mixin import MaintenanceMixin
from .obsidian_mixin import ObsidianMixin

# Import Support Managers
from .budget_manager import BudgetManager
from .store_profile_manager import StoreProfileManager
from .order_logic_guards import apply_safety_guards
from .department_constants import ESSENTIAL_DEPARTMENTS, FAST_FIVE_DEPARTMENTS, FRESH_DEPARTMENTS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OrderEngine")

class OrderEngine(IntelligenceMixin, ProcurementMixin, MaintenanceMixin, DataMixin, ObsidianMixin):
    """
    The O.A.S.I.S. Order Engine.
    Inherits specialized logic from Mixins and orchestrates the end-to-end 
    inventory optimization process.
    """
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        ObsidianMixin.__init__(self)
        self.databases: Dict[str, Any] = {}
        self.grn_db: Dict[str, Any] = {} 
        self.no_grn_suppliers: List[str] = []
        
        # Core Managers
        self.budget_manager = BudgetManager(data_dir)
        self.profile_manager = StoreProfileManager()

        # Cache & Indexing Attributes for Mixins
        self.grn_frequency_map: Dict[str, float] = {}
        self._brand_index_cache: Dict[str, List[float]] = {}
        self._prof_index_cache: Dict[str, str] = {}
        self._sales_index_cache: Dict[str, str] = {}
        self._brand_index_source_id: Optional[int] = None
        self._po_history_dates: Dict[str, List[datetime]] = {}
        
        # Chapter 11: OASIS Engine Feature Flags
        self.engines_config: Dict[str, Any] = self._load_engines_config()
        
        # Initial Load
        self.grn_frequency_map = self.load_grn_frequency()
    
    def _load_engines_config(self) -> Dict[str, Any]:
        """Load the OASIS engines feature flag and parameter configuration."""
        config_paths = [
            os.path.join(self.data_dir, 'oasis_engines_config.json'),
            os.path.join(self.data_dir, '..', 'data', 'oasis_engines_config.json'),
            os.path.join(os.path.dirname(__file__), '..', 'data', 'oasis_engines_config.json'),
        ]
        for path in config_paths:
            if os.path.exists(path):
                try:
                    with open(path, 'r', encoding='utf-8') as f:
                        config = json.load(f)
                    
                    self.global_settings = config.get('global_settings', {})
                    self.category_rules = config.get('category_rules', {})
                    
                    engines = config.get('engines', {})
                    enabled_list = [k for k, v in engines.items() if v.get('enabled')]
                    logger.info(f"OASIS Engines Config loaded. Active engines: {enabled_list or 'NONE'}")
                    return config
                except Exception as e:
                    logger.warning(f"Failed to load engines config: {e}")
        
        # Fallback defaults
        self.global_settings = {"strict_mathematical_mode": False, "simulation_correction_multiplier": 1.15}
        self.category_rules = {}
        return {"engines": {}}
    
    def is_engine_enabled(self, engine_name: str) -> bool:
        """Check if a Chapter 11 engine is enabled via feature flags."""
        engines = self.engines_config.get('engines', {})
        return bool(engines.get(engine_name, {}).get('enabled', False))
    
    def _load_engine_caches(self):
        """Load pre-computed JSON caches for enabled Chapter 11 engines."""
        # AMIT: Load blacklist
        if self.is_engine_enabled('amit'):
            amit_path = os.path.join(self.data_dir, 'amit_enforcement.json')
            if not os.path.exists(amit_path):
                amit_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'amit_enforcement.json')
            if os.path.exists(amit_path):
                try:
                    with open(amit_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.databases['amit_enforcement'] = set(data.get('blacklist', []))
                    self.databases['amit_lowest_gmroi'] = data.get('lowest_gmroi_per_dept', {})
                    logger.info(f"[AMIT] Loaded blacklist: {len(self.databases['amit_enforcement'])} SKUs blocked.")
                except Exception as e:
                    logger.warning(f"[AMIT] Failed to load cache: {e}")
                    self.databases['amit_enforcement'] = set()
            else:
                logger.warning("[AMIT] Enabled but no amit_enforcement.json found. Run amit_gatekeeper.py first.")
                self.databases['amit_enforcement'] = set()
        
        # DHARAM: Load demand patches
        if self.is_engine_enabled('dharam'):
            dharam_path = os.path.join(self.data_dir, 'dharam_demand_patch.json')
            if not os.path.exists(dharam_path):
                dharam_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'dharam_demand_patch.json')
            if os.path.exists(dharam_path):
                try:
                    with open(dharam_path, 'r', encoding='utf-8') as f:
                        data = json.load(f)
                    self.databases['dharam_demand_patch'] = data.get('demand_patches', {})
                    logger.info(f"[DHARAM] Loaded demand patches: {len(self.databases['dharam_demand_patch'])} SKUs corrected.")
                except Exception as e:
                    logger.warning(f"[DHARAM] Failed to load cache: {e}")
                    self.databases['dharam_demand_patch'] = {}
            else:
                logger.warning("[DHARAM] Enabled but no dharam_demand_patch.json found. Run dharam_revenue.py first.")
                self.databases['dharam_demand_patch'] = {}

    def has_grn_data(self, key: str) -> bool:
        """v8.0: Quick check for historical presence."""
        return key in self.grn_db

    # FIX M2: load_grn_frequency() defined in DataMixin (single source of truth, inherited via MRO)

    def get_latest_inventory_file(self) -> Optional[str]:
        """[GOLDEN LOGIC v10.0] Identifies the most recent inventory file in data_dir."""
        files = [f for f in os.listdir(self.data_dir) if 'inventory' in f.lower() and (f.endswith('.csv') or f.endswith('.xlsx'))]
        if not files: return None
        full_paths = [os.path.join(self.data_dir, f) for f in files]
        return max(full_paths, key=os.path.getmtime)

    def load_no_grn_suppliers(self):
        """Robust search for supplier bypass list (v10.0 Pro)."""
        paths = [
            os.path.join(self.data_dir, 'app/data/no_grn_suppliers.json'),
            os.path.join(self.data_dir, 'no_grn_suppliers.json'),
            os.path.join(self.data_dir, 'oasis/data/no_grn_suppliers.json')
        ]
        
        final_path = next((p for p in paths if os.path.exists(p)), None)
        
        if final_path:
            try:
                with open(final_path, 'r', encoding='utf-8') as f:
                    self.no_grn_suppliers = [s.upper() for s in json.load(f)]
                logger.info(f"Loaded {len(self.no_grn_suppliers)} bypass suppliers from {final_path}")
                return self.no_grn_suppliers
            except Exception as e:
                logger.warning(f"Failed to load bypass JSON: {e}")

        # Authoritative Golden Fallback
        self.no_grn_suppliers = [
            "PLU", "LOCAL", "DIRECT", "CONSIGNMENT", "KEBS", "CITY COUNCIL",
            "JIKONI", "BAKERY FOODPLUS", "BAKERY", "FRESH", "WATER", "MILK", "EGGS",
            "VEGETABLES", "FRUITS", "MEAT", "POULTRY", "FISH", "CHANDARANA",
            "INTERNAL", "TRANSFER", "PROMO", "SAMPLE", "ADJUSTMENT", "CONSIGN"
        ]
        return self.no_grn_suppliers

    def scan_grn_files(self) -> Dict[str, Any]:
        """ autoritàative grn scanner from golden v10 """
        grn_dir = os.path.join(self.data_dir, 'grns')
        if not os.path.exists(grn_dir):
            # Fallback to base data_dir if /grns/ not present
            grn_dir = self.data_dir
            
        logger.info(f"Scanning GRNs from {grn_dir}...")
        # Redirect to DataMixin handle if already implemented there, 
        # but ensure it follows the "Golden" directory rules.
        return self._load_products(grn_dir)

    async def load_databases_async(self):
        """Phase 2: Parallel Database Loading (v10.0 Optimization)"""
        logger.info(f"Phase 2: Loading databases from {self.data_dir}...")
        
        # v4.0 Performance Fix: Check for cached GRN file first
        grn_cache_match = next((f for f in os.listdir(self.data_dir) if 'grn_intelligence' in f and f.endswith('.json')), None)
        if grn_cache_match:
            try:
                loop = asyncio.get_event_loop()
                def _do_load_grn():
                    with open(os.path.join(self.data_dir, grn_cache_match), 'r', encoding='utf-8') as f:
                        return json.load(f)
                self.grn_db = await loop.run_in_executor(None, _do_load_grn)
                logger.info(f"Loaded GRN Intelligence from cache: {grn_cache_match}")
            except Exception as e:
                logger.warning(f"Failed to load GRN cache, scanning files: {e}")
                self.grn_db = self.scan_grn_files()
        else:
            self.grn_db = self.scan_grn_files()
        
        available_files = os.listdir(self.data_dir)

        db_configs = {
            'supplier_patterns': 'supplier_patterns_2025',
            'product_supplier_map': 'product_supplier_map',
            'product_department_map': 'product_department_map',
            'product_intelligence': 'sales_profitability_intelligence_2025',
            'sales_forecasting': 'sales_forecasting_2025',
            'supplier_quality': 'supplier_quality_scores_2025',
            'sales_profitability': 'sales_profitability_intelligence_2025',
            'simulation_feedback': 'simulation_feedback'
        }
        
        async def load_single_db(db_key, search_term):
            # Prioritize updated/v10.0 versions
            match = next((f for f in available_files if search_term in f and '_updated.json' in f), None)
            if not match:
                match = next((f for f in available_files if search_term in f and f.endswith('.json')), None)
            
            if match:
                try:
                    fpath = os.path.join(self.data_dir, match)
                    # Use run_in_executor for heavy JSON loading
                    loop = asyncio.get_event_loop()
                    def _do_load_json():
                        with open(fpath, 'r', encoding='utf-8') as f:
                            return json.load(f)
                    self.databases[db_key] = await loop.run_in_executor(None, _do_load_json)
                    logger.info(f"Loaded {db_key} from {match}")
                except Exception as e:
                    logger.error(f"Failed to load {db_key}: {e}")
                    self.databases[db_key] = {}
            else:
                self.databases[db_key] = {}
                logger.warning(f"No database match found for {search_term}")

        # Parallelize database loads
        await asyncio.gather(*(load_single_db(k, v) for k, v in db_configs.items()))
        
        # Post-load sync: link simulation feedback to intelligence
        if 'simulation_feedback' in self.databases:
             logger.info("Syncing simulation feedback to intelligence layer...")
             # (Feedback is accessed dynamically by IntelligenceMixin)
        
        self.load_no_grn_suppliers()
        
        # Chapter 11: Load pre-computed engine caches (AMIT, DHARAM, LATA)
        self._load_engine_caches()
        
        logger.info("Phase 2: Database loading complete.")

    def load_local_databases(self):
        """Synchronous version of database loader for legacy/script compatibility."""
        data_dir = self.data_dir
        
        # Consistent with Golden Script: use grn_intelligence_cache.json if available
        grn_cache_path = os.path.join(data_dir, "grn_intelligence_cache.json")
        if os.path.exists(grn_cache_path):
            with open(grn_cache_path, 'r') as f:
                self.grn_db = json.load(f)
                logger.info("Loaded GRN Intelligence Cache (Sync)")
        else:
            self.grn_db = {}
            
        db_configs = {
            'supplier_patterns': 'supplier_patterns_2025',
            'product_supplier_map': 'product_supplier_map',
            'product_department_map': 'product_department_map',
            'product_intelligence': 'sales_profitability_intelligence_2025',  # R16: Golden Parity
            'sales_forecasting': 'sales_forecasting_2025',
            'supplier_quality': 'supplier_quality_scores_2025',
            'sales_profitability': 'sales_profitability_intelligence_2025',
            'simulation_feedback': 'simulation_feedback'
        }
        
        for key, search_term in db_configs.items():
            available_files = os.listdir(data_dir)
            match = next((f for f in available_files if search_term in f and '_updated.json' in f), None)
            if not match:
                match = next((f for f in available_files if search_term in f and f.endswith('.json')), None)
            
            if match:
                try:
                    with open(os.path.join(data_dir, match), 'r', encoding='utf-8') as f:
                        self.databases[key] = json.load(f)
                    logger.info(f"Loaded {key} from {match} (Sync)")
                except Exception as e:
                    logger.error(f"Failed to load {key} (Sync): {e}")
                    self.databases[key] = {}
            else:
                self.databases[key] = {}
        
        self.load_no_grn_suppliers()
        
        # Chapter 11: Load pre-computed engine caches (AMIT, DHARAM, LATA)
        self._load_engine_caches()

    async def update_demand_intelligence_async(self):
        """Phase 1: Online & Multi-Store Demand Sync (v10.1 Pro)"""
        logger.info("Phase 1: Syncing Online Demand Intelligence...")
        try:
            online_data = self.load_online_sales()
            if online_data:
                # Phase 1.1: Authoritative Online-to-Offline Demand Merge
                forecast_db = self.databases.get('sales_forecasting', {})
                if not forecast_db and 'sales_forecasting' not in self.databases:
                     # Lazy load if needed
                     await self.load_databases_async()
                     forecast_db = self.databases.get('sales_forecasting', {})
                
                merged_count = 0
                for sku, qty in online_data.items():
                    # Check barcode and name matches as well
                    if sku in forecast_db:
                        item = forecast_db[sku]
                        curr_ads = float(item.get('avg_daily_sales', 0))
                        # v10.1: Apply a 30-day spread for online velocity addition
                        online_daily = qty / 30.0
                        item['avg_daily_sales'] = round(curr_ads + online_daily, 3)
                        item['is_online_blended'] = True
                        merged_count += 1
                
                logger.info(f"Phase 1: Merged {merged_count} online sales records into demand layer.")
        except Exception as e:
            logger.warning(f"Demand sync failed: {e}")

    def load_online_sales(self) -> Dict[str, Any]:
        """Loads Shopify/Online sales CSV and merges [v10.1 Pro]"""
        path = os.path.join(self.data_dir, 'online_sales_export.csv')
        if not os.path.exists(path):
            # Try recursive search if not in root
            match = next((f for f in os.listdir(self.data_dir) if 'online_sales' in f.lower() and f.endswith('.csv')), None)
            if match: path = os.path.join(self.data_dir, match)
            else: return {}

        logger.info(f"Ingesting online sales from {path}...")
        results = {}
        try:
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    sku = row.get('Variant SKU') or row.get('SKU')
                    if sku:
                        qty = float(row.get('Net Quantity', 0))
                        results[sku.upper()] = results.get(sku.upper(), 0) + qty
            return results
        except Exception as e:
            logger.error(f"Failed to load online sales: {e}")
            return {}

    async def run_intelligent_analysis(self, file_path: str, output_path: str, allocation_mode: str = "replenishment", total_budget: float = 200000.0):
        """Master Workflow [v10.1 authoritative]."""
        self.total_budget = total_budget
        
        # Phase 1: Refresh Online Intelligence (NEW in v10.1)
        await self.update_demand_intelligence_async()
        
        # Phase 2: Parallel Database Loading
        await self.load_databases_async()
        
        # Phase 3: Synchronize Rhythm & History
        self._po_history_dates = self.scan_purchase_orders()
        
        # Phase 4: Parsing
        products = self.parse_inventory_file(file_path)
        if not products:
            logger.warning("No products found to analyze.")
            return []
            
        # Phase 5: Batch Enrichment
        all_enriched = []
        batch_size = 5000
        
        if allocation_mode == "initial_load":
            for p in products: p['current_stocks'] = 0.0

        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            logger.info(f"Phase 5: Enriching Batch {(i // batch_size) + 1} ({len(batch)} items)...")
            enriched_batch = self.enrich_product_data(batch, is_greenfield=(allocation_mode=="initial_load"))
            all_enriched.extend(enriched_batch)

        # Phase 6: Strategic Allocation & Inference
        final_recommendations = []
        if allocation_mode == "initial_load":
            logger.info("Mode: Initial Load. applying Greenfield Allocation...")
            result = self.apply_greenfield_allocation(all_enriched, total_budget)
            final_recommendations = result.get('recommendations', [])
            final_recommendations = apply_safety_guards(final_recommendations, {p['product_name']: p for p in all_enriched}, "initial_load")
        else:
            logger.info("Mode: Replenishment. Applying AI-Guided Logic...")
            if os.environ.get("ANTHROPIC_API_KEY"):
                try:
                    final_recommendations = await self.analyze_batch_ai(all_enriched, 1, 1, allocation_mode)
                except Exception as e:
                    logger.error(f"AI Analysis failed: {e}. Falling back to Rule Engine.")
                    final_recommendations = []

            if not final_recommendations:
                logger.info("Using Rule-Based Inference Engine...")
                try:
                    from ..llm.inference import RuleBasedLLM
                    llm = RuleBasedLLM()
                    final_recommendations = await llm.analyze(all_enriched)
                except Exception as e:
                    logger.error(f"Rule engine failed: {e}")
                final_recommendations = apply_safety_guards(final_recommendations, {p['product_name']: p for p in all_enriched}, allocation_mode)

        # Merge remaining metadata for reporting
        enriched_map = {p['product_name']: p for p in all_enriched}
        for r in final_recommendations:
            meta = enriched_map.get(r['product_name'], {})
            r['cost_price'] = meta.get('cost_price', r.get('cost_price', 0))
            r.update({k: v for k, v in meta.items() if k not in r})
                    
        # Log to Obsidian
        total_rec_qty = sum(float(r.get('recommended_quantity', 0)) for r in final_recommendations)
        total_cost = sum(float(r.get('recommended_quantity', 0)) * float(r.get('cost_price', 0)) for r in final_recommendations)
        total_value = sum(float(r.get('recommended_quantity', 0)) * float(r.get('selling_price', 0)) for r in final_recommendations)
        
        base_fname = os.path.basename(file_path)
        parts = base_fname.replace('.', '_').split('_')
        store_id = parts[0].capitalize() if parts and len(parts[0]) > 2 else "Main"
        
        supplier_name = "General"
        if final_recommendations:
             all_suppliers = [r.get('supplier_name') for r in final_recommendations if r.get('supplier_name')]
             if all_suppliers:
                  from collections import Counter
                  supplier_name = Counter(all_suppliers).most_common(1)[0][0]

        summary = {
            'mode': allocation_mode,
            'store_id': store_id,
            'supplier_name': supplier_name,
            'total_products': len(final_recommendations),
            'total_units': int(total_rec_qty),
            'total_cost': total_cost,
            'total_value': total_value,
            'est_savings': total_cost * 0.1,
            'insights': f"Relational analysis for {store_id} completed using {supplier_name} as primary context."
        }
        self.log_run_summary(summary)

        if output_path:
            try:
                self.generate_excel_report(file_path, final_recommendations, output_path)
            except Exception as e:
                logger.error(f"Failed to generate Excel report: {e}")
        
        return final_recommendations
