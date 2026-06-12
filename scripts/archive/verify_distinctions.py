import sys
import os
import json

# Add current directory to path so we can import oasis
sys.path.append(os.getcwd())

from oasis.logic.intelligence_mixin import IntelligenceMixin

class MockEngine(IntelligenceMixin):
    def __init__(self):
        self.databases = {'supplier_patterns': {}, 'simulation_feedback': {}}
        self.rhythm_db = {}
        self.grn_db = {}
        self.no_grn_suppliers = []

def verify_distinctions():
    engine = MockEngine()
    # Simulate daily rhythm for Brookside
    engine.rhythm_db = {'BROOKSIDE DAIRY LIMITED': {'median_gap': 1.0}}
    
    # Test cases
    products = [
        {
            'product_name': 'BROOKSIDE 1LT FRESH MILK',
            'is_fresh': True,
            'avg_daily_sales': 50.0,
            'supplier_name': 'BROOKSIDE DAIRY LIMITED',
            'supplier_frequency': 'daily',
            'estimated_delivery_days': 1
        },
        {
            'product_name': 'BROOKSIDE 1LT UHT WHOLE MILK TBA',
            'is_fresh': False, 
            'avg_daily_sales': 20.0,
            'supplier_name': 'BROOKSIDE DAIRY LIMITED',
            'supplier_frequency': 'daily',
            'estimated_delivery_days': 1
        }
    ]
    
    print("O.A.S.I.S. Distinction Verification")
    print("=" * 40)
    
    enriched = engine.enrich_product_data(products)
    
    for p in enriched:
        print(f"Product: {p['product_name']}")
        print(f"  Is Fresh: {p['is_fresh']}")
        print(f"  Target Coverage: {p['target_coverage_days']} days")
        print(f"  Upper Bound (Cap): {p['upper_coverage_days']} days")
        
        if 'FRESH' in p['product_name'].upper():
            if p['target_coverage_days'] <= 1.2 and p['upper_coverage_days'] == 1.2:
                print("  [SUCCESS] Fresh JIT (1.2d) applied.")
            else:
                print("  [FAILURE] Fresh JIT not applied correctly.")
        
        if 'UHT' in p['product_name'].upper():
            if not p['is_fresh'] and p['upper_coverage_days'] == 7.0:
                print("  [SUCCESS] Long Life (7.0d) applied.")
            else:
                print("  [FAILURE] Long Life logic not applied correctly.")
        print("-" * 20)

if __name__ == "__main__":
    verify_distinctions()
