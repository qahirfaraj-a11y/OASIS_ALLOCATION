import os
import pandas as pd
from pitch_data_ingestor_v2 import ForensicOperationsIngestor

def verify_multi_file():
    base_dir = os.getcwd()
    data_dir = os.path.join(base_dir, "oasis", "data")
    pos_path = os.path.join(data_dir, "jan_cash.xlsx")
    
    ingestor = ForensicOperationsIngestor(base_dir)
    print("Testing Multi-File Ingestion (Simulating Jan + Feb)...")
    
    # Uploading the same file twice should be handled by deduplication logic
    ingestor.load_logs(pos_file=[pos_path, pos_path])
    
    if ingestor.pos_df is not None:
        print(f"[OK] Concatenated POS Rows: {len(ingestor.pos_df)}")
        if len(ingestor.pos_df) == 14412: # Original count should stay same due to drop_duplicates
             print("[SUCCESS] Temporal Deduplication Worked!")
        else:
             print(f"[WARN] Deduplication might have failed. Count: {len(ingestor.pos_df)}")

    ingestor.run_pos_analysis()
    ingestor.run_cycle_analysis()
    
    audit = ingestor.get_full_audit()
    cycle = audit.get('cycle', {})
    
    print("\n--- Cycle Intelligence Summary ---")
    print(f"Payday Multiplier: {cycle.get('payday_multiplier')}")
    print(f"Payday Sales Share: {cycle.get('payday_sales_share')}")
    
    # Check if demand wave is populated
    wave = cycle.get('demand_wave', {})
    if 30 in wave or '30' in wave:
        print("[OK] Demand Waveform (1-31) successfully generated.")

if __name__ == "__main__":
    verify_multi_file()
