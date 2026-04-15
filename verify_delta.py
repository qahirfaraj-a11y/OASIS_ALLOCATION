import sys
import os
import pandas as pd

sys.path.append(os.getcwd())
from allocation_app import load_and_run_allocation

for budget in [300000, 1900000, 31550000]:
    basket_df, cash_spend, consignment_val, alloc_summary, _ = load_and_run_allocation(budget, "JAN")
    print(f"\n==========================================")
    print(f"BUDGET: {budget}")
    print(f"Engine Cash Used: {alloc_summary.get('total_cash_used', 0):,.2f} ({alloc_summary.get('utilization_pct', 0):.2f}%)")
    print(f"App Cash Spend: {cash_spend:,.2f} ({(cash_spend/budget)*100:.2f}%)")
    print(f"App Consignment Value: {consignment_val:,.2f}")
    print(f"Total App Value: {cash_spend + consignment_val:,.2f} ({((cash_spend + consignment_val)/budget)*100:.2f}%)")
