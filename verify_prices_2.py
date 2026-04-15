import sys
import os
import pandas as pd

sys.path.append(os.getcwd())
from allocation_app import load_and_run_allocation

basket_df, cash_spend, consignment_val, alloc_summary, _ = load_and_run_allocation(48180000, "JAN")

df_filtered = basket_df[basket_df['Qty'] > 0]
df_filtered['Unit_Price'] = df_filtered['Expected_Revenue'] / df_filtered['Qty']
df_filtered['Unit_Cost'] = df_filtered['Allocated_Cost'] / df_filtered['Qty']

print("Engine GRN values for first 5:")
engine = load_and_run_allocation.engine if hasattr(load_and_run_allocation, 'engine') else None
# Actually just dump 5 from the dataframe.
for i, row in df_filtered.head(5).iterrows():
    print(f"Product: {row['Product']}, Allocated_Cost: {row['Allocated_Cost']}, Qty: {row['Qty']}, Unit_Cost: {row['Unit_Cost']}, Unit_Price: {row['Unit_Price']}")

