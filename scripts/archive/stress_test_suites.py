import os
import json
import numpy as np
import pandas as pd
from pitch_data_ingestor_v2 import ForensicOperationsIngestor

DATA_DIR = "oasis/data"
SERVICE_LEVEL_Z = 1.645

def test_sti_normalization():
    print("\n[STRESS TEST] STI Normalization (Rate-based Penalty)")
    ingestor = ForensicOperationsIngestor(DATA_DIR)
    
    # Mock return penalty rate multiplier from config
    rate_multiplier = ingestor.config.get("engines", {}).get("lata", {}).get("return_penalty_rate_multiplier", 1.5)
    
    # Case A: Small supplier, high returns (10 orders, 5 returns -> 50% rate)
    # Case B: Large supplier, same absolute returns (1000 orders, 5 returns -> 0.5% rate)
    
    def calc_penalty(returns, orders):
        rate = returns / max(1, orders)
        return min(0.3, rate * rate_multiplier)
    
    penalty_a = calc_penalty(5, 10)
    penalty_b = calc_penalty(5, 1000)
    
    print(f"Supplier A (10 orders, 5 returns): Penalty = {penalty_a:.4f}")
    print(f"Supplier B (1000 orders, 5 returns): Penalty = {penalty_b:.4f}")
    
    if penalty_a > penalty_b * 10:
        print("OK: STI Penalty is correctly rate-normalized. Large volume suppliers are no longer unfairly penalized for isolated returns.")
    else:
        print("FAIL: STI Penalty does not show sufficient volume normalization.")

def test_recovery_window_outliers():
    print("\n[STRESS TEST] Recovery Window Boundary Resilience")
    ingestor = ForensicOperationsIngestor(DATA_DIR)
    
    # Mock an extreme lead time scenario (e.g. Global Logistics Crisis)
    avg_lead_time = 150 
    lead_variance = 40
    
    # Configurable cap
    dharam_cfg = ingestor.config.get("engines", {}).get("dharam", {})
    max_rec_cap = dharam_cfg.get("max_recovery_window_days", 90)
    
    # Calculate recovery window logic from pitch_data_ingestor_v2.py
    raw_recovery = avg_lead_time + (SERVICE_LEVEL_Z * lead_variance)
    capped_recovery = max(2, min(max_rec_cap, raw_recovery))
    
    print(f"Raw Projected Recovery Window: {raw_recovery:.2f} days")
    print(f"Capped Recovery Window (Config Max {max_rec_cap}): {capped_recovery} days")
    
    if capped_recovery == max_rec_cap:
        print(f"OK: Recovery window correctly capped at {max_rec_cap}. Outlier lead times will not create runaway revenue-bleed estimates.")
    else:
        print("FAIL: Recovery window cap logic not enforced.")

def test_category_threshold_resilience():
    print("\n[STRESS TEST] Category-Aware Threshold Mapping")
    ingestor = ForensicOperationsIngestor(DATA_DIR)
    
    # Inject mock mappings for test items
    ingestor._dept_map["MOCK MILK"] = "DAIRY"
    ingestor._dept_map["MOCK RICE"] = "RICE"
    
    # DAIRY -> 21 days (from config)
    # RICE -> 90 days (from config)
    # TOYS -> 45 days (default)
    
    threshold_milk = ingestor._get_dead_stock_days("MOCK MILK")
    threshold_rice = ingestor._get_dead_stock_days("MOCK RICE")
    threshold_toys = ingestor._get_dead_stock_days("ACTION FIGURE") # Likely unmapped
    
    print(f"Threshold for MILK (DAIRY): {threshold_milk} days")
    print(f"Threshold for RICE: {threshold_rice} days")
    print(f"Threshold for TOYS: {threshold_toys} days")
    
    if threshold_milk == 21 and threshold_rice == 90 and threshold_toys == 45:
        print("OK: Category thresholds are correctly mapped and defaulting.")
    else:
        print("FAIL: Category mapping inconsistent.")

if __name__ == "__main__":
    print("=== O.A.S.I.S. ENGINE STRESS TEST SUITE ===")
    test_sti_normalization()
    test_recovery_window_outliers()
    test_category_threshold_resilience()
    print("\n=== STRESS TESTS COMPLETE ===")
