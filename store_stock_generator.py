"""
Store Stock Generator
======================
Generates per-store stock profiles by scaling the master scorecard
based on each store's demand_scale_factor (relative to Rhapta Road).

KEY: All Chandarana stores carry the FULL product range (~23K SKUs).
The difference between stores is VOLUME (quantity per SKU), not variety.

Usage:
    python store_stock_generator.py
"""

import json
import os
import math
import random
import pandas as pd
from typing import List, Dict

# Path to master scorecard
SCORECARD_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"
NETWORK_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\stores_network.json"


def load_master_scorecard(path: str = SCORECARD_PATH) -> pd.DataFrame:
    """Load the 23K SKU master scorecard."""
    df = pd.read_csv(path)
    print(f"Loaded {len(df)} SKUs from scorecard")
    return df


def allocate_stock_for_store(store: dict, scorecard_df: pd.DataFrame, 
                              noise_pct: float = 0.15,
                              seed: int = None) -> List[Dict]:
    """
    Generate a stock profile for a single store.
    
    All stores carry the FULL SKU range. The demand_scale_factor
    controls volume (qty per SKU), not variety (number of SKUs).
    
    Rhapta Road (scale=1.0) uses the raw scorecard ADS values.
    Other stores scale those volumes up or down.
    """
    if seed is not None:
        random.seed(seed)
    
    demand_scale = store.get("demand_scale_factor", 1.0)
    
    df = scorecard_df.copy()
    
    # All stores stock everything — no SKU filtering
    stock_profile = []
    
    for _, row in df.iterrows():
        base_ads = float(row.get("Avg_Daily_Sales", 0) or 0)
        price = float(row.get("Unit_Price", 0) or 0)
        margin = float(row.get("Margin_Pct", 25) or 25)
        cost = price * (1 - margin / 100)
        
        if price <= 0:
            # Still stock it at minimum qty (full range policy)
            base_ads = max(base_ads, 0.1)
            cost = max(cost, 1.0)
        
        # Scale ADS to this store's volume (relative to Rhapta Road)
        scaled_ads = base_ads * demand_scale
        
        # Apply store-specific noise
        noise = 1.0 + random.uniform(-noise_pct, noise_pct)
        scaled_ads *= noise
        
        # Stock depth: 14 days coverage
        depth_days = 14
        qty = max(1, round(scaled_ads * depth_days))
        
        stock_profile.append({
            "sku": str(row.get("Product", "Unknown")),
            "qty": qty,
            "price": round(max(price, 0), 2),
            "cost": round(max(cost, 0), 2),
            "supplier": str(row.get("Supplier", "Unknown")),
            "department": str(row.get("Department", "GENERAL")),
            "ads_scaled": round(scaled_ads, 4),
        })
    
    return stock_profile


def generate_all_stock_profiles(network_path: str = NETWORK_PATH,
                                 scorecard_path: str = SCORECARD_PATH,
                                 output_path: str = None) -> str:
    """
    Generate stock profiles for all stores in the network.
    Saves back to the network JSON with stock_profile populated.
    """
    with open(network_path, "r") as f:
        network = json.load(f)
    
    scorecard_df = load_master_scorecard(scorecard_path)
    
    print(f"\nGenerating stock profiles for {network['store_count']} stores...")
    print(f"  All stores carry full range ({len(scorecard_df)} SKUs)")
    print("-" * 70)
    
    for i, store in enumerate(network["stores"]):
        profile = allocate_stock_for_store(
            store, 
            scorecard_df, 
            seed=42 + i
        )
        store["stock_profile"] = profile
        
        total_skus = len(profile)
        total_value = sum(p["qty"] * p["cost"] for p in profile)
        scale = store.get("demand_scale_factor", 1.0)
        ref = " *" if store.get("is_reference_store", False) else ""
        
        print(f"  {store['store_id']} | {store['name']:<40} | "
              f"Scale {scale:>5.2f}x | "
              f"{total_skus:>6} SKUs | "
              f"KES {total_value:>14,.0f}{ref}")
    
    print(f"\n  * = Reference store (Rhapta Road)")
    
    if output_path is None:
        output_path = network_path
    
    with open(output_path, "w") as f:
        json.dump(network, f, indent=2)
    
    print(f"\n[OK] Stock profiles saved to {output_path}")
    return output_path


if __name__ == "__main__":
    generate_all_stock_profiles()
