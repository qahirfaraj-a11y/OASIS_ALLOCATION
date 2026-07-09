import pandas as pd

f1 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1776958868.xlsx'
f2 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1777045005.xlsx'

df1 = pd.read_excel(f1, sheet_name='Report', header=2)
df2 = pd.read_excel(f2, sheet_name='Report', header=2)

merged = df1.merge(df2, on='DESCRIPTION', suffixes=('_old', '_new'))
merged['diff'] = merged['Recommended Qty_new'] - merged['Recommended Qty_old']
diffs = merged[merged['diff'] != 0].sort_values('diff', ascending=False)

for idx, row in diffs.head(5).iterrows():
    print(f"Product: {row['DESCRIPTION']}")
    print(f"  Old Hist Avg: {row['Historical Avg_old']} | Reason: {row['Reasoning_old']}")
    print(f"  New Hist Avg: {row['Historical Avg_new']} | Reason: {row['Reasoning_new']}")
    print("-" * 80)
