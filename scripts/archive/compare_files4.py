import pandas as pd

f1 = r'C:\Users\iLink\Desktop\Projects\processed_dpl__1776958868.xlsx'
df1 = pd.read_excel(f1, sheet_name='Report', header=2)
print("F1 columns:", df1.columns.tolist())
