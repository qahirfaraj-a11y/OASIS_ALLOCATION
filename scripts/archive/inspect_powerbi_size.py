import pandas as pd

f = r"C:\Users\iLink\.gemini\antigravity\scratch\powerbi_data_inspection.xlsx"
df = pd.read_excel(f, sheet_name='Item Analysis')
print("Total rows in PowerBI Item Analysis:", len(df))
print("Distinct suppliers in PowerBI Item Analysis:", df['Vendor_Name'].nunique())
print("Sample rows:")
print(df.head(5).to_string())
