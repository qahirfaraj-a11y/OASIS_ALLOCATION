import sys
import os

sys.path.append(os.getcwd())
from allocation_app import load_and_run_allocation

basket_df, cash_spend, consignment_val, alloc_summary, _ = load_and_run_allocation(48180000, "JAN")

df_filtered = basket_df[basket_df['Qty'] > 0]
df_filtered['Unit_Price'] = df_filtered['Expected_Revenue'] / df_filtered['Qty']
df_filtered['Unit_Cost'] = df_filtered['Allocated_Cost'] / df_filtered['Qty']

print(df_filtered[['Product', 'Unit_Cost', 'Unit_Price']].head(30))

print("\nWhere Cost != Price:")
print(len(df_filtered[df_filtered['Unit_Cost'] != df_filtered['Unit_Price']]))

