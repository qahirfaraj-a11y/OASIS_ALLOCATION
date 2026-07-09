import pandas as pd

f1 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1776958868.xlsx'
f2 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1777045005.xlsx'

print('--- F1 ---')
excel1 = pd.ExcelFile(f1)
print('Sheets in F1:', excel1.sheet_names)
for sheet in excel1.sheet_names:
    print(f'Sheet: {sheet}')
    df = pd.read_excel(f1, sheet_name=sheet)
    print(df.head(5))

print('\n--- F2 ---')
excel2 = pd.ExcelFile(f2)
print('Sheets in F2:', excel2.sheet_names)
for sheet in excel2.sheet_names:
    print(f'Sheet: {sheet}')
    df = pd.read_excel(f2, sheet_name=sheet)
    print(df.head(5))
