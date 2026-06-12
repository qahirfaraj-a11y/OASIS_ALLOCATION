import pandas as pd

excel_path = r"C:\Users\iLink\Downloads\Kapa_Portfolio_Node_Intelligence.xlsx"
df_sum = pd.read_excel(excel_path, sheet_name='Executive Summary', header=None)
print("=== Executive Summary Columns ===")
for r_idx, row in df_sum.iterrows():
    if pd.notna(row[1]):
        print(f"Row {r_idx}: {row[1]} -> {row[2]}")
        
df_nodes = pd.read_excel(excel_path, sheet_name='Kapa Network Nodes')
print("\n=== Kapa Network Nodes Columns ===")
print(list(df_nodes.columns))
print("\nFirst 3 rows:")
print(df_nodes.head(3).to_string())

df_cat = pd.read_excel(excel_path, sheet_name='Catalog Audit')
print("\n=== Catalog Audit Columns ===")
print(list(df_cat.columns))
print("\nFirst 3 rows:")
print(df_cat.head(3).to_string())
