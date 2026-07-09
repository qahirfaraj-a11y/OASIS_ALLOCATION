import pandas as pd

excel_path = r"C:\Users\iLink\Downloads\Kapa_Portfolio_Node_Intelligence.xlsx"
df_sum = pd.read_excel(excel_path, sheet_name='Executive Summary', header=None)
print("=== Executive Summary KPI Metrics ===")
for r_idx, row in df_sum.iterrows():
    if pd.notna(row[1]) and pd.notna(row[2]):
        print(f"  {row[1]}: {row[2]}")
