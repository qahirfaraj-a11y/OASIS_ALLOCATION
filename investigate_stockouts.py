"""
Investigate Early Stockout Issue
=================================
Analyze why stockouts occur in Days 1-8.
"""

import pandas as pd
from retail_simulator import RetailSimulator, STORE_UNIVERSES
from dataclasses import asdict

# Run simulation with detailed logging
print("=" * 70)
print("STOCKOUT INVESTIGATION: Initial Allocation vs Demand")
print("=" * 70)

# Test multiple store tiers
for tier in ["Small_200k", "Medium_1M", "Online_5M"]:
    print(f"\n{'='*70}")
    print(f"TIER: {tier}")
    print(f"{'='*70}")
    
    config = STORE_UNIVERSES[tier]
    sim = RetailSimulator(tier, config)
    
    # Get initial allocation stats
    initial_stock = {name: sku.current_stock for name, sku in sim.skus.items()}
    initial_demand = {name: sku.avg_daily_sales for name, sku in sim.skus.items()}
    
    # Check for SKUs with low stock vs demand
    print(f"\n[1] SKUs with < 3 days coverage (potential early stockouts):")
    risky_skus = []
    for name, sku in sim.skus.items():
        if sku.avg_daily_sales > 0:
            days_coverage = sku.current_stock / sku.avg_daily_sales
            if days_coverage < 3:
                risky_skus.append({
                    'Product': name[:40],
                    'Stock': sku.current_stock,
                    'Avg Daily Sales': sku.avg_daily_sales,
                    'Days Coverage': days_coverage,
                    'Department': sku.department
                })
    
    if risky_skus:
        df_risky = pd.DataFrame(risky_skus).sort_values('Days Coverage')
        print(df_risky.head(15).to_string(index=False))
        print(f"\nTotal risky SKUs: {len(risky_skus)} out of {len(sim.skus)}")
    else:
        print("No risky SKUs found - all have >= 3 days coverage")
    
    # Run simulation
    result = sim.run(days=14)
    
    # Analyze daily performance
    print(f"\n[2] Daily Performance:")
    print(f"{'Day':<5} {'Fill %':<10} {'Stockouts':<12} {'Lost Sales':<15}")
    print("-" * 42)
    for log in result.daily_logs:
        print(f"{log.day:<5} {log.fill_rate:<10.1f} {log.stockout_count:<12} KES {log.lost_sales:,.0f}")
    
    # Identify first stockout SKUs
    print(f"\n[3] First Stockout Analysis:")
    first_stockouts = []
    for name, sku in result.final_sku_states.items():
        if sku.first_stockout_day is not None:
            first_stockouts.append({
                'Product': name[:35],
                'First Stockout Day': sku.first_stockout_day,
                'Initial Stock': initial_stock.get(name, 0),
                'Avg Daily Sales': sku.avg_daily_sales,
                'Days Coverage': initial_stock.get(name, 0) / sku.avg_daily_sales if sku.avg_daily_sales > 0 else 999
            })
    
    if first_stockouts:
        df_stockout = pd.DataFrame(first_stockouts).sort_values('First Stockout Day')
        print(df_stockout.head(20).to_string(index=False))
    else:
        print("No stockouts occurred!")

print("\n" + "=" * 70)
print("ANALYSIS COMPLETE")
print("=" * 70)
