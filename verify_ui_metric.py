import sys
import os

sys.path.append(os.getcwd())
from allocation_app import load_and_run_allocation

basket_df, cash_spend, consignment_val, alloc_summary, _ = load_and_run_allocation(48180000, "JAN")

print(f"Engine Logged Cash Used: {alloc_summary.get('total_cash_used'):,.2f}")
print(f"Engine Logged Consign:   {alloc_summary.get('total_consignment'):,.2f}")

ui_cost_sum = basket_df['Allocated_Cost'].sum()
ui_revenue_sum = basket_df['Expected_Revenue'].sum()

print(f"\n--- UI Metrics ---")
print(f"Total Cost Displayed: KES {ui_cost_sum:,.0f}")
print(f"Expected Revenue Displayed: KES {ui_revenue_sum:,.0f}")

budget = 48180000
missing = budget - ui_cost_sum

print(f"Delta from Target Budget: {missing:,.0f} ({(missing/budget)*100:.2f}% under)")

