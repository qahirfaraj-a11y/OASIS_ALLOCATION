import pandas as pd

f1 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1776958868.xlsx'
f2 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1777045005.xlsx'

df1 = pd.read_excel(f1)
df2 = pd.read_excel(f2)

print('Columns in F1:', list(df1.columns))

prod_col = next((c for c in df1.columns if 'name' in c.lower() or 'product' in c.lower() or 'item' in c.lower()), None)
qty_col = next((c for c in df1.columns if 'qty' in c.lower() or 'quantity' in c.lower() or 'rec' in c.lower()), None)
reason_col = next((c for c in df1.columns if 'reason' in c.lower()), None)

if prod_col and qty_col:
    merged = df1.merge(df2, on=prod_col, suffixes=('_old', '_new'))
    diffs = merged[merged[qty_col + '_old'] != merged[qty_col + '_new']]
    print(f'\nFound {len(diffs)} differences out of {len(merged)} matched items.')
    
    if not diffs.empty:
        print('\nSample Differences:')
        for _, row in diffs.head(10).iterrows():
            print(f"Product: {row[prod_col]}")
            print(f"  Old Qty: {row[qty_col + '_old']} | Reason: {row.get(reason_col + '_old', '')}")
            print(f"  New Qty: {row[qty_col + '_new']} | Reason: {row.get(reason_col + '_new', '')}")
            print('-'*80)
else:
    print('Could not find product or quantity columns')
