import os
import sys
import logging

# Add current directory to path so we can import oasis.logic
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from oasis.logic.data_mixin import DataMixin

logging.basicConfig(level=logging.INFO)

class MockEngine(DataMixin):
    def __init__(self, data_dir):
        self.data_dir = data_dir
        self.databases = {}

if __name__ == "__main__":
    data_dir = os.path.join(os.path.dirname(__file__), "oasis", "data")
    engine = MockEngine(data_dir)
    
    print("--- Generating LATA Telemetry (PO to GRN Gaps) ---")
    engine.update_lead_time_intelligence()
    print("--- Done. Check oasis/data/supplier_delivery_gaps.json ---")
