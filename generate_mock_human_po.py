import pandas as pd
import numpy as np
import os
import json

base_dir = os.path.dirname(os.path.abspath(__file__))
scorecard_path = os.path.join(base_dir, 'Full_Product_Allocation_Scorecard_v7.csv')

# Load actual scorecard
df = pd.read_csv(scorecard_path)

# Normalize column names to find Item_Name
col_map = {}
for c in df.columns:
    cl = c.lower().strip()
    if ('product' in cl or 'item' in cl) and 'Item_Name' not in col_map.values(): col_map[c] = 'Item_Name'
    elif ('ads' in cl or 'avg_daily' in cl or 'velocity' in cl) and 'ADS' not in col_map.values(): col_map[c] = 'ADS'
    elif ('soh' in cl or 'stock_on_hand' in cl or 'stock' in cl) and 'SOH' not in col_map.values(): col_map[c] = 'SOH'

df = df.rename(columns=col_map)

df['SOH'] = pd.to_numeric(df['SOH'], errors='coerce').fillna(0)
df['ADS'] = pd.to_numeric(df.get('ADS', pd.Series([1]*len(df))), errors='coerce').fillna(0)

# We want to create a mock human PO based on this scorecard
# Let's say the human buyer looks at items with SOH < 10 and orders an arbitrary amount
# But they miss some items, and over-order others

# Subset to items the human might look at
human_po_items = []

# 1. Human Missed (Items with 0 SOH but ADS > 1 that they didn't order)
# -> We won't include these in the human PO!

# 2. Human Over-Ordered (Items with some SOH, but they panic ordered a huge amount)
over_order_pool = df[(df['SOH'] > 5) & (df['SOH'] < 20)].head(50)
for _, row in over_order_pool.iterrows():
    # Human orders widely varying amounts, e.g., 50-200
    qty = np.random.randint(50, 200)
    human_po_items.append({'Item_Name': row['Item_Name'], 'Human_Order_Qty': qty})

# 3. Aligned (Human ordered a reasonable amount)
aligned_pool = df[df['SOH'] < 5].head(50)
for _, row in aligned_pool.iterrows():
    # Human orders approx 10 days of stock
    qty = int(max(row.get('ADS', 1) * 10, 5))
    human_po_items.append({'Item_Name': row['Item_Name'], 'Human_Order_Qty': qty})

# 4. Human Under-Ordered (Human ordered tiny amount)
under_pool = df[(df['SOH'] < 2) & ((df.get('ADS', 0)) > 5)].head(20)
for _, row in under_pool.iterrows():
    human_po_items.append({'Item_Name': row['Item_Name'], 'Human_Order_Qty': 2})

human_po_df = pd.DataFrame(human_po_items)

po_path = os.path.join(base_dir, 'mock_human_po.csv')
human_po_df.to_csv(po_path, index=False)
print(f"Generated mock human PO at {po_path} with {len(human_po_df)} lines.")
