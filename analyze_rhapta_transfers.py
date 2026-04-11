import pandas as pd
import os

trn_file = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trn_1_12.xlsx"
trout_file = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trout_1_12.xlsx"

branch_filter = "Rhapta Road"

print(f"--- Analyzing Rhapta Road ({branch_filter}) ---")

# Transfer In
df_in = pd.read_excel(trn_file)
rhapta_in = df_in[df_in['To Org Code/ Name'].str.contains(branch_filter, na=False, case=False)]

# Transfer Out
df_out = pd.read_excel(trout_file)
rhapta_out = df_out[df_out['From Org Code/ Name'].str.contains(branch_filter, na=False, case=False)]

print(f"\nTransfer In (STI) - Rhapta Road:")
print(f"Total Records: {len(rhapta_in)}")
print(f"Total Quantity: {rhapta_in['STI Qty'].sum()}")
print(f"Total Value (Net): {rhapta_in['Net Amt'].sum():,.2f}")

print(f"\nTransfer Out (STO) - Rhapta Road:")
print(f"Total Records: {len(rhapta_out)}")
print(f"Total Quantity: {rhapta_out['STO Qty'].sum()}")
print(f"Total Value (Net): {rhapta_out['Net Amt'].sum():,.2f}")

# Top Products In
print("\nTop 5 Products - Transferred IN:")
top_in = rhapta_in.groupby('Item Name')['STI Qty'].sum().sort_values(ascending=False).head(5)
print(top_in)

# Top Products Out
print("\nTop 5 Products - Transferred OUT:")
top_out = rhapta_out.groupby('Item Name')['STO Qty'].sum().sort_values(ascending=False).head(5)
print(top_out)

# Partners
print("\nMost Frequent Sources (Transfer In):")
sources = rhapta_in['From Org Code/ Name'].value_counts().head(3)
print(sources)

print("\nMost Frequent Destinations (Transfer Out):")
destinations = rhapta_out['To Org Code/ Name'].value_counts().head(3)
print(destinations)

# Imbalance Check (In - Out for top items)
all_items = set(rhapta_in['Item Name'].unique()) | set(rhapta_out['Item Name'].unique())
imbalance = []
for item in all_items:
    qty_in = rhapta_in[rhapta_in['Item Name'] == item]['STI Qty'].sum()
    qty_out = rhapta_out[rhapta_out['Item Name'] == item]['STO Qty'].sum()
    if qty_in > 0 or qty_out > 0:
        imbalance.append({'Item Name': item, 'In': qty_in, 'Out': qty_out, 'Net': qty_in - qty_out})

imbalance_df = pd.DataFrame(imbalance).sort_values('Net', ascending=False)
print("\nTop Net IN Flow (High dependency on other branches):")
print(imbalance_df.head(5))

print("\nTop Net OUT Flow (Providing for others):")
print(imbalance_df.sort_values('Net').head(5))
