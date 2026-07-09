import pandas as pd
import numpy as np

f1 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1776958868.xlsx'
f2 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1777045005.xlsx'

df1 = pd.read_excel(f1, sheet_name='Report', header=1)
df2 = pd.read_excel(f2, sheet_name='Report', header=1)

# Find common columns
desc_col = [c for c in df1.columns if 'desc' in c.lower()][0]
rec_col = [c for c in df1.columns if 'rec' in c.lower() and 'qty' in c.lower()][0]
reason_col = [c for c in df1.columns if 'reason' in c.lower()][0]

merged = df1.merge(df2, on=desc_col, suffixes=('_old', '_new'))
merged['diff'] = merged[rec_col + '_new'] - merged[rec_col + '_old']
diffs = merged[merged['diff'] != 0].sort_values('diff', ascending=False)

print(f"Total differences: {len(diffs)}")
for idx, row in diffs.head(10).iterrows():
    print(f"Product: {row[desc_col]}")
    print(f"  Old: {row[rec_col + '_old']} | Reason: {row[reason_col + '_old']}")
    print(f"  New: {row[rec_col + '_new']} | Reason: {row[reason_col + '_new']}")
    print("-" * 60)
