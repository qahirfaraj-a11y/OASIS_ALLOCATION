import json
import csv
import os
import asyncio
import logging
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

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
from .allocation_strategies import AllocationConfig

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
        ObsidianMixin.__init__(self, vault_path=os.path.join(self.data_dir, "Oasis"))
        self.databases: Dict[str, Any] = {}
        self.grn_db: Dict[str, Any] = {} 
        self.no_grn_suppliers: List[str] = []
        
        # Core Managers
        self.budget_manager = BudgetManager(data_dir)
        self.profile_manager = StoreProfileManager()
        self.allocation_config = AllocationConfig()

        # Cache & Indexing Attributes for Mixins
        self.grn_frequency_map: Dict[str, float] = {}
        self._brand_index_cache: Dict[str, List[float]] = {}
        self._prof_index_cache: Dict[str, str] = {}
        self._sales_index_cache: Dict[str, str] = {}
        self._brand_index_source_id: Optional[int] = None
        self._po_history_dates: Dict[str, List[datetime]] = {}
        
        # v10.12: Load Dynamic Rhythm & Schedule
        self.rhythm_db = {}
        self.schedule_db = {}
        r_path = os.path.join(self.data_dir, '..', 'supplier_rhythm_analysis.json')
        s_path = os.path.join(self.data_dir, '..', 'supplier_weekly_schedule.json')
        
        if os.path.exists(r_path):
            with open(r_path, 'r') as f:
                self.rhythm_db = json.load(f).get('po_rhythm', {})
                logger.info(f"Loaded PO Rhythm for {len(self.rhythm_db)} suppliers.")
        
        if os.path.exists(s_path):
            with open(s_path, 'r') as f:
                self.schedule_db = json.load(f)
                logger.info("Loaded Weekly Supplier Schedule.")
        
        # Chapter 11: OASIS Engine Feature Flags
        self.engines_config: Dict[str, Any] = self._load_engines_config()
        
        # Initial Load
        self.grn_frequency_map = self.load_grn_frequency()
    
    def _load_engines_config(self) -> Dict[str, Any]:
        """Load the OASIS engines feature flag and parameter configuration.

        Resolved through oasis.logic.engines_config: the tuned
        oasis_engines_config.json for this install if present, otherwise the
        SHIPPED oasis_engines_config.default.json. That second tier matters —
        without it a client install returned {"engines": {}}, is_engine_enabled()
        answered False for every engine, and the whole Chapter-11 layer sat
        dormant and silent (deep-analysis finding S1).
        """
        from .engines_config import load_engines_config, resolve_source

        tier, path = resolve_source(self.data_dir)
        config = load_engines_config(self.data_dir)
        if config:
            self.global_settings = config.get('global_settings', {})
            self.category_rules = config.get('category_rules', {})
            engines = config.get('engines', {})
            enabled_list = [k for k, v in engines.items()
                            if isinstance(v, dict) and v.get('enabled')]
            logger.info("OASIS Engines Config loaded (%s: %s). Active engines: %s",
                        tier, os.path.basename(path or '?'),
                        enabled_list or 'NONE')
            return config

        # No config file at all — the engine layer runs off, and says so loudly.
        logger.error("No engine config resolved — every Chapter-11 engine is "
                     "DISABLED. Expected oasis_engines_config.json or the "
                     "shipped oasis_engines_config.default.json in %s",
                     self.data_dir)
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
        
        # MANDE: Load purge report blacklist
        self.databases['mande_purge_list'] = set()
        mande_path = os.path.join(self.data_dir, 'mande_purge_report.json')
        if not os.path.exists(mande_path):
            mande_path = os.path.join(os.path.dirname(__file__), '..', 'data', 'mande_purge_report.json')
        if os.path.exists(mande_path):
            try:
                with open(mande_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    purge_cands = data.get('purge_candidates', [])
                    for s in purge_cands:
                        self.databases['mande_purge_list'].add(s.get('supplier', '').strip().upper())
                logger.info(f"[MANDE] Loaded purge list: {len(self.databases['mande_purge_list'])} suppliers flagged for delisting.")
            except Exception as e:
                logger.warning(f"[MANDE] Failed to load cache: {e}")
                
        # HALO / MASTERCLASS: Load affinity protections
        self.databases['halo_protection_list'] = set()
        halo_paths = [
            os.path.join(self.data_dir, 'alcohol_masterclass_intel.json'),
            os.path.join(os.path.dirname(__file__), '..', '..', 'alcohol_masterclass_intel.json')
        ]
        h_path = next((p for p in halo_paths if os.path.exists(p)), None)
        if h_path:
            try:
                with open(h_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    
                # Protect pareto leaders
                for category in ['beer_pareto', 'wine_pareto', 'spirit_insights']:
                    for item in data.get(category, []):
                        name = item.get('SKU Name', '')
                        if name and name != 'Unknown SKU':
                            self.databases['halo_protection_list'].add(self.normalize_product_name(name))
                            
                # Protect direct halo partners
                halo_partners = data.get('halo_partners', {})
                for k in halo_partners:
                    if k and not any(x in k for x in ['LTD', 'COMPANY', 'LIMITED']):
                        self.databases['halo_protection_list'].add(self.normalize_product_name(k))
                        
                logger.info(f"[HALO] Guardian logic active: {len(self.databases['halo_protection_list'])} key anchor/halo items protected.")
            except Exception as e:
                logger.warning(f"[HALO] Failed to load cache: {e}")


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
            "JIKONI", "BAKERY FOODPLUS", "BAKERY", "OWN BRAND",
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
            self.grn_db = self.scan_grn_files()
            
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
        
        # Calendar Analyzer: Enrich supplier patterns with PO/GRN lead time data
        try:
            from .calendar_analyzer import CalendarAnalyzer
            cal_analyzer = CalendarAnalyzer()
            cal_analyzer.load_data()
            cal_results = cal_analyzer.analyze()
            if cal_results:
                # Merge calendar-derived lead times into supplier_patterns
                sp = self.databases.get('supplier_patterns', {})
                enriched_count = 0
                for supplier_name, cal_data in cal_results.items():
                    s_upper = supplier_name.upper().strip()
                    if s_upper in sp:
                        # Only override if calendar has better data (more order history)
                        if cal_data.get('order_count', 0) >= 3:
                            sp[s_upper]['estimated_delivery_days'] = cal_data['lead_time_days']
                            sp[s_upper]['calendar_frequency_days'] = cal_data['frequency_days']
                            sp[s_upper]['calendar_category'] = cal_data['category']
                            enriched_count += 1
                    else:
                        # New supplier discovered from PO/GRN files
                        sp[s_upper] = {
                            'estimated_delivery_days': cal_data['lead_time_days'],
                            'median_gap_days': cal_data['frequency_days'],
                            'reliability_score': 0.85,
                            'order_frequency': cal_data['category'].lower(),
                            'calendar_frequency_days': cal_data['frequency_days'],
                            'calendar_category': cal_data['category'],
                        }
                        enriched_count += 1
                self.databases['supplier_patterns'] = sp
                logger.info(f"[CALENDAR] Enriched {enriched_count} suppliers with PO/GRN delivery rhythm data.")
        except Exception as e:
            logger.warning(f"[CALENDAR] CalendarAnalyzer integration skipped: {e}")

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
        
        # BUG 9 FIX: Only load databases if not already loaded (startup load_local_databases covers this).
        # Prevents redundant re-parsing of all JSON databases + GRN files on every click.
        if not self.databases:
            await self.load_databases_async()
        
        # BUG 10 FIX: Cache PO history — only scan on first run.
        if not self._po_history_dates:
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
                    final_recommendations = []

            # BUG 3 FIX: Apply safety guards AFTER both paths have resolved,
            # and only if we have valid recommendations to guard.
            if final_recommendations:
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

    def run_mop_up_engine(self, recommendations: List[dict], total_budget: float) -> List[dict]:
        """
        Phase A: K-Core Pruning. Prune isolated loose units based on affinity/velocity.
        Phase B: Knapsack Mop-Up. Consolidate high-velocity items to pack sizes using remaining budget.
        """
        logger.info("Executing Post-Allocation Mop-Up Engine...")
        
        # Calculate currently utilized budget
        current_utilized = sum(float(r.get('recommended_quantity', 0)) * float(r.get('cost_price', 0)) for r in recommendations)
        
        # PHASE A: K-Core Pruning (Heuristic based on ads vs pack size for isolated 1-units)
        reclaimed_capital = 0.0
        pruned_count = 0
        for r in recommendations:
            qty = float(r.get('recommended_quantity', 0))
            if qty <= 0: continue
            
            pack_size = float(r.get('pack_size', 1))
            ads = float(r.get('avg_daily_sales', 0))
            cost = float(r.get('cost_price', 0))
            
            # Prune ONLY dead/extremely slow singles to reclaim capital safely
            if qty == 1 and cost > 0:
                if ads < 0.05:
                    r['recommended_quantity'] = 0.0
                    r['reasoning'] = r.get('reasoning', '') + " [MOP-UP: Pruned Dead SKU]"
                    r['mop_up_action'] = "PRUNED"
                    reclaimed_capital += cost
                    pruned_count += 1
        
        current_utilized -= reclaimed_capital
        remaining_budget = total_budget - current_utilized
        logger.info(f"Mop-Up Phase A: Pruned {pruned_count} items. Reclaimed KES {reclaimed_capital:.2f}. Remaining budget: KES {remaining_budget:.2f}")
        
        # PHASE B: Knapsack Mop-Up
        if remaining_budget <= 0:
            return recommendations
            
        candidates = sorted([r for r in recommendations if r.get('recommended_quantity', 0) > 0], key=lambda x: float(x.get('avg_daily_sales', 0)), reverse=True)
        consolidated_count = 0
        
        # 1. Round up broken packs first
        for r in candidates:
            qty = float(r.get('recommended_quantity', 0))
            pack_size = float(r.get('pack_size', 1))
            cost = float(r.get('cost_price', 0))
            
            if cost > 0 and pack_size > 1 and qty > 0 and (qty % pack_size) != 0:
                needed_units = pack_size - (qty % pack_size)
                additional_cost = needed_units * cost
                
                if additional_cost <= remaining_budget:
                    r['recommended_quantity'] += needed_units
                    remaining_budget -= additional_cost
                    r['reasoning'] = r.get('reasoning', '') + f" [MOP-UP: Consolidated (+{needed_units})]"
                    r['mop_up_action'] = f"CONSOLIDATED (+{needed_units})"
                    consolidated_count += 1
                    
        # 2. Round-Robin Scale Up for high-velocity items
        high_velocity = [r for r in candidates if float(r.get('avg_daily_sales', 0)) >= 0.2 and float(r.get('cost_price', 0)) > 0]
        
        if high_velocity and remaining_budget > 0:
            scaled_amounts = {}
            # Limit to a single pass to avoid inflating quantities dangerously
            for r in high_velocity:
                name = r['product_name']
                pack_size = float(r.get('pack_size', 1))
                cost = float(r.get('cost_price', 0))
                ads = float(r.get('avg_daily_sales', 0))
                qty = float(r.get('recommended_quantity', 0))
                additional_cost = pack_size * cost
                
                # Safety Cap: Do not scale if it pushes coverage beyond 30 days
                if (qty + pack_size) <= max(ads * 30, pack_size * 2):
                    if additional_cost > 0 and additional_cost <= remaining_budget:
                        r['recommended_quantity'] += pack_size
                        remaining_budget -= additional_cost
                        scaled_amounts[name] = scaled_amounts.get(name, 0) + pack_size
                    
            for r in high_velocity:
                name = r['product_name']
                if name in scaled_amounts:
                    added = scaled_amounts[name]
                    r['reasoning'] = r.get('reasoning', '') + f" [MOP-UP: Scaled Up (+{added})]"
                    r['mop_up_action'] = f"SCALED (+{added})"
                    consolidated_count += 1
                    
        logger.info(f"Mop-Up Phase B: Consolidated/Scaled {consolidated_count} items. Remaining budget: KES {remaining_budget:.2f}")
        return recommendations
