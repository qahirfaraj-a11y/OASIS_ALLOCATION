import os
from oasis.logic.data_mixin import DataMixin

def test_parser():
    class DummyEngine(DataMixin):
        def __init__(self):
            pass
            
    engine = DummyEngine()
    products = engine.parse_inventory_file(r"C:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv")
    
    print(f"Parsed {len(products)} products")
    for p in products[:5]:
        print(p)

if __name__ == "__main__":
    test_parser()
