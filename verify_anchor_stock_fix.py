"""
Verification: Anchor Stock Days Comparison
Shows the impact of changing from 30-day fixed rule to lead time-based calculation
"""

# Test scenarios with different supplier profiles
scenarios = [
    {
        "supplier": "Reliable Cooking Oil Supplier",
        "lead_time_days": 4,
        "reliability_score": 92
    },
    {
        "supplier": "Standard Flour Supplier",
        "lead_time_days": 4,
        "reliability_score": 85
    },
    {
        "supplier": "Unreliable Sugar Supplier",
        "lead_time_days": 7,
        "reliability_score": 65
    },
    {
        "supplier": "Fast Local Supplier",
        "lead_time_days": 2,
        "reliability_score": 95
    },
    {
        "supplier": "Slow Regional Supplier",
        "lead_time_days": 10,
        "reliability_score": 75
    }
]

print("=" * 80)
print("ANCHOR STOCK CALCULATION COMPARISON")
print("=" * 80)
print()

# Assumptions
avg_daily_sales = 5  # units per day
cost_per_unit = 150  # KES
mini_store_count = 10  # number of mini stores

print(f"Assumptions:")
print(f"  - Average Daily Sales: {avg_daily_sales} units/day")
print(f"  - Cost per Unit: {cost_per_unit} KES")
print(f"  - Number of Mini Stores: {mini_store_count}")
print()
print("-" * 80)

total_old_capital = 0
total_new_capital = 0

for scenario in scenarios:
    supplier = scenario['supplier']
    lead_time = scenario['lead_time_days']
    reliability = scenario['reliability_score']
    
    # OLD LOGIC: Fixed 30 days
    old_stock_days = 30
    old_stock_qty = avg_daily_sales * old_stock_days
    old_capital = old_stock_qty * cost_per_unit
    
    # NEW LOGIC: Hybrid lead time-based
    if reliability < 80:
        multiplier = 2.0
    else:
        multiplier = 1.5
    
    new_stock_days = int(lead_time * multiplier) + 2
    new_stock_qty = avg_daily_sales * new_stock_days
    new_capital = new_stock_qty * cost_per_unit
    
    # Calculate savings
    qty_reduction = old_stock_qty - new_stock_qty
    capital_saved = old_capital - new_capital
    savings_pct = (capital_saved / old_capital) * 100 if old_capital > 0 else 0
    
    total_old_capital += old_capital
    total_new_capital += new_capital
    
    print(f"\n{supplier}")
    print(f"  Lead Time: {lead_time} days | Reliability: {reliability}% | Multiplier: {multiplier}x")
    print(f"  OLD: {old_stock_days} days = {old_stock_qty} units = {old_capital:,} KES")
    print(f"  NEW: {new_stock_days} days = {new_stock_qty} units = {new_capital:,} KES")
    print(f"  SAVINGS: {qty_reduction} units ({savings_pct:.1f}%) = {capital_saved:,} KES per store")

print()
print("=" * 80)
print("TOTAL IMPACT (Per Anchor SKU)")
print("=" * 80)
print(f"Old Model (30 days):  {total_old_capital:,} KES per store")
print(f"New Model (dynamic):  {total_new_capital:,} KES per store")
print(f"Savings Per Store:    {total_old_capital - total_new_capital:,} KES")
print(f"Reduction:            {((total_old_capital - total_new_capital) / total_old_capital * 100):.1f}%")
print()
print(f"Fleet-Wide Savings ({mini_store_count} stores):")
print(f"  Total Capital Freed: {(total_old_capital - total_new_capital) * mini_store_count:,} KES")
print()

# Specific example for your 4-day typical case
print("=" * 80)
print("YOUR TYPICAL SCENARIO (4-day lead time, 90% reliability)")
print("=" * 80)
typical_lead = 4
typical_reliability = 90
typical_multiplier = 1.5  # Since reliability >= 80
typical_new_days = int(typical_lead * typical_multiplier) + 2

print(f"Old Rule: 30 days of stock")
print(f"New Rule: {typical_new_days} days of stock")
print(f"Reduction: {30 - typical_new_days} days ({((30 - typical_new_days) / 30 * 100):.1f}% less inventory)")
print()
print(f"For a 200K mini store with 3 anchor categories:")
print(f"  Estimated capital freed: ~40-50K KES")
print(f"  Can now stock: ~8-10 additional SKUs with freed capital")
print()
