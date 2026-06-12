import pandas as pd
import os

path = r'c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv'
if not os.path.exists(path):
    print("File not found")
    exit()

df = pd.read_csv(path)
print(f"Total Rows: {len(df)}")
eligible = df[df['Stocking_Notes'].str.contains("-> Eligible", na=False)]
print(f"Eligible Count: {len(eligible)}")
staples = df[df['Is_Staple'] == True]
print(f"Staple Count: {len(staples)}")
eligible_staples = staples[staples['Stocking_Notes'].str.contains("-> Eligible", na=False)]
print(f"Eligible Staple Count: {len(eligible_staples)}")

# Check some items that are NOT eligible
print("\nSample Ineligible Items:")
print(df[~df['Stocking_Notes'].str.contains("-> Eligible", na=False)][['Product', 'Stocking_Notes']].head(10))

# Check Department counts for Eligible items
print("\nEligible Items per Department (Top 10):")
print(eligible['Department'].value_counts().head(10))
