import pandas as pd

excel_path = r"C:\Users\iLink\Downloads\Kapa_Portfolio_Node_Intelligence.xlsx"
df_nodes = pd.read_excel(excel_path, sheet_name='Kapa Network Nodes')

print("=== Top 5 highest ROI items ===")
print(df_nodes[['SKU Name', 'Selling Price (SP)', 'Cost Price (CP)', 'Daily Sales Velocity (ADS)', '30D ROI (%)']].head(5).to_string())

print("\n=== Bottom 5 lowest non-zero ROI items ===")
non_zero = df_nodes[df_nodes['30D ROI (%)'] > 0]
print(non_zero[['SKU Name', 'Selling Price (SP)', 'Cost Price (CP)', 'Daily Sales Velocity (ADS)', '30D ROI (%)']].tail(5).to_string())
