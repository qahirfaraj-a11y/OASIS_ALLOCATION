import pandas as pd
import os

path = r'c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv'
df = pd.read_csv(path)

print("--- Fresh Item Check ---")
fresh_depts = ["FRESH MILK", "BREAD"]
fresh_samples = df[df['Department'].isin(fresh_depts)].sort_values('Avg_Daily_Sales', ascending=False).head(5)

for _, row in fresh_samples.iterrows():
    print(f"Product: {row['Product']}")
    print(f"  Department: {row['Department']}")
    print(f"  ADS: {row['Avg_Daily_Sales']:.2f}")
    # In Mega, coverage is 45d but capped at 2d for fresh.
    # Order = max(MDQ, ADS * 1.5 * 2) = max(MDQ, ADS * 3)
    qty = row['Recommended_Qty']
    price = row['Unit_Price']
    print(f"  Recommended Qty: {qty:.2f}")
    print(f"  Unit Price: {price:.2f}")
    print(f"  Total Capital: {qty * price:.2f}")
    print("-" * 20)

print("\n--- Assortment Variety Check ---")
for scenario in ["Small_200k", "Med_2.5M", "Large_10M", "Mega_115M"]:
    count = df[df[scenario] == True].shape[0]
    depts = df[df[scenario] == True]['Department'].nunique()
    print(f"{scenario}: {count} SKUs across {depts} departments")
