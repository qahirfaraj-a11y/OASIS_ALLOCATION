import pandas as pd
import os
import sys

def analyze_simulation_events():
    """
    Analyzes both Orders and IMPLIED Stockouts based on metrics.
    Since we don't have a granular stockout log CSV, we can infer stockouts
    if we see lost sales in the metrics or by checking order gaps.
    
    Actually, to be precise, we need the simulation to log Stockout Events.
    The current `run_simulation_scenario.py` logs aggregated metrics.
    
    However, we can look at the `orders_*.csv`.
    If an item is ordered, it means it hit ROP.
    
    Let's look at the "Avg First Order Day" again, but clearer.
    And let's try to detect if Reorder Day is too late.
    
    If Lead Time = 2 days.
    If First Order Day = 8.
    Arrival Day = 11.
    If Stockout happened on Day 9, then we have a gap.
    
    We don't have the granular daily stock level per SKU in the CSVs currently exported.
    We only have Aggregated Metrics (Total Lost Revenue).
    
    BUT, we can deduce "Survival Rate" more accurately.
    High Survival Rate of Week 1 = Good Day 1 Allocation.
    
    Let's stick to interpreting the Orders for now, but label it clearly.
    """
    # Load the orders file from the recent simulation
    file_path = "orders_Verification_v7_JAN.csv"
    
    if not os.path.exists(file_path):
        print(f"Error: {file_path} not found.")
        return

    print(f"Analyzing Stock Flow via Orders: {file_path}")
    df = pd.read_csv(file_path)
    
    # Filter for interest categories
    categories = {
        'Impulse/Confect': ['LOLLIPOP', 'LOLLYPOP', 'CHUPA', 'CANDY', 'GIANT', 'ORBIT', 'WRIGLEY'],
        'Staples': ['KENSALT', 'NDOVU', 'MAIZE MEAL', 'ATTA', ' SALT', ' FLOUR'],
        'Bakery': ['BREAD', 'FESTIVE', 'NATURES'],
        'Dairy': ['DAIMA', 'BIO', 'FRESH MILK', 'MAZIWA']
    }
    
    print("-" * 100)
    print(f"{'Category':<20} | {'Count':<6} | {'Avg 1st Reorder':<16} | {'Implied Coverage (Days)':<25} | {'Risk Status'}")
    print("-" * 100)
    
    for cat_name, keywords in categories.items():
        pattern = '|'.join(keywords)
        mask = df['sku'].str.contains(pattern, case=False, na=False)
        cat_orders = df[mask]
        
        if cat_orders.empty:
            print(f"{cat_name:<20} | {'0':<6} | {'N/A':<16} | {'> 30 Days (Excellent)':<25} | {'SAFE'}")
            continue
            
        # Group by SKU to find first order day
        first_orders = cat_orders.groupby('sku')['day_generated'].min()
        
        avg_first_day = first_orders.mean()
        
        # Implied Coverage is roughly the First Reorder Day + Remaining Buffer at ROP
        # ROP usually covers ~5 days.
        # So Coverage = Reorder Day + 5 (approx).
        
        # Risk Analysis
        # If Reorder Day < 3: Critical (Day 1 Allocation too small)
        # If Reorder Day 4-7: Watch (Might stockout on weekend if ROP logic is weak)
        # If Reorder Day > 7: Healthy (Survived first week cycle)
        
        risk = "SAFE"
        if avg_first_day < 3: risk = "CRITICAL (Day 1 Gap)"
        elif avg_first_day < 6: risk = "HIGH (Weekend Risk)"
        elif avg_first_day < 8: risk = "MODERATE"
        
        print(f"{cat_name:<20} | {len(first_orders):<6} | {f'Day {avg_first_day:.1f}':<16} | {f'~{avg_first_day+5:.1f} Days':<25} | {risk}")

    print("-" * 100)
    print("NOTE: 'Avg 1st Reorder' is when the engine *detected* low stock and placed an order.")
    print("      Actual stockout happens later (Reorder Day + Lead Time) if safety stock is insufficient.")
    print("      Day 8 Reorder -> Stock Arrives Day 11. Weekend is Day 6-7.")
    print("      This suggests Day 8 Reorders are likely SAFE from the *first* weekend spike.")
    print("-" * 100)

if __name__ == "__main__":
    analyze_simulation_events()
