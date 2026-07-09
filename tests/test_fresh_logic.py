import sys
import os

# Add scratch to python path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from oasis.logic.intelligence_mixin import IntelligenceMixin
from oasis.logic.procurement_mixin import ProcurementMixin
from typing import Dict, Any

class MockEngine(IntelligenceMixin, ProcurementMixin):
    def __init__(self):
        self.databases = {
            'sales_forecasting': {},
            'sales_profitability': {},
            'simulation_feedback': {'sku_feedback': {}},
            'supplier_patterns': {},
            'product_supplier_map': {},
            'product_department_map': {}
        }
        self.grn_db = {}
        self._po_history_dates = {}
        self.no_grn_suppliers = []
        self.grn_frequency_map = {}
        self.engines_config = {}
        
        class MockProfileManager:
            def get_profile(self, budget):
                return {
                    'tier_name': 'Test Tier',
                    'is_small': False,
                    'depth_days': 14,
                    'max_packs': 100,
                    'price_ceiling': 1000,
                    'min_display_qty': 3,
                    'allow_c_class': True,
                    'wallet_buffer_pct': 0.1
                }
        self.profile_manager = MockProfileManager()
        
        class MockBudgetManager:
            def initialize_wallets(self, budget, buffer_pct):
                return {'FRESH': {'allocated_budget': budget, 'spent': 0}, 'GENERAL': {'allocated_budget': budget, 'spent': 0}}
            def is_staple(self, p_name, dept, ads):
                return False
            def spend_from_wallet(self, wallets, dept, amount):
                pass
            def check_wallet_availability(self, wallets, dept, amount):
                return True
        self.budget_manager = MockBudgetManager()

    def is_engine_enabled(self, name): return False
    def get_grn_cycle_days(self, p_name, is_fresh=False): return 1.0

engine = MockEngine()

def run_tests():
    print("--- Test 1: Dairy Category Boost & Strict 1.2 Cap ---")
    product_dairy = {
        'product_name': 'BIO FRESH MILK 500ML',
        'is_fresh': True,
        'department': 'FRESH',
        'avg_daily_sales': 10.0,
        'estimated_delivery_days': 1.0
    }
    enriched = engine.enrich_product_data([product_dairy])
    res = enriched[0]
    print(f"Product: {res['product_name']}")
    print(f"Category Boost: {res.get('category_boost')}")
    print(f"Target Coverage Days: {res.get('target_coverage_days')}")
    print(f"Cap Reason: {res.get('cap_reason')}")
    assert res.get('category_boost') == 1.0, "Dairy should not have a category boost > 1.0"
    assert res.get('target_coverage_days') <= 1.2, "Dairy should be strictly capped at 1.2"
    
    print("\n--- Test 2: Sim Feedback Override (Relax to 1.5) ---")
    product_dairy_sim = {
        'product_name': 'DAIMA MILK 500ML',
        'is_fresh': True,
        'department': 'FRESH',
        'avg_daily_sales': 10.0,
        'estimated_delivery_days': 1.0
    }
    engine.databases['simulation_feedback'] = {
        'sku_feedback': {
            'DAIMA MILK 500ML': {'stockout_frequency': 0.5, 'stockout_days': 2}
        }
    }
    enriched_sim = engine.enrich_product_data([product_dairy_sim])
    res_sim = enriched_sim[0]
    print(f"Product: {res_sim['product_name']}")
    print(f"Stockout Freq: {res_sim.get('sim_stockout_frequency')}")
    print(f"Target Coverage Days: {res_sim.get('target_coverage_days')}")
    assert res_sim.get('target_coverage_days') <= 1.5, "Cap should be relaxed to 1.5"
    
    print("\n--- Test 3: Minimum Shelf Presence for Low-Demand Fresh ---")
    product_low_demand = {
        'product_name': 'FRESH MINT BUNCH',
        'is_fresh': True,
        'department': 'FRESH',
        'avg_daily_sales': 0.1,  # very low demand
        'selling_price': 50.0,
        'pack_size': 1.0
    }
    enriched_low = engine.enrich_product_data([product_low_demand])
    alloc_res = engine.apply_greenfield_allocation(enriched_low, total_budget=1000000.0)
    res_low = enriched_low[0]
    print(f"Product: {res_low['product_name']}")
    print(f"Recommended Quantity: {res_low.get('recommended_quantity')}")
    assert res_low.get('recommended_quantity') >= 1, "Low demand fresh item must get at least 1 unit"
    
    print("\n--- All tests passed! ---")

if __name__ == "__main__":
    run_tests()
