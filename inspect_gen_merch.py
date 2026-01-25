import pandas as pd

FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"
df = pd.read_csv(FILE)

# Filter for General Merchandise
gm = df[df["Department"] == "General Merchandise"]

print(f"Total General Merchandise Items: {len(gm)}")

if len(gm) > 0:
    print("\n--- Top 20 General Merchandise Items (by Revenue) ---")
    print(gm[["Product", "Supplier", "Total_Revenue"]].head(20).to_string())
    
    print("\n\n--- Sampling 20 Random General Merchandise Items ---")
    print(gm[["Product", "Supplier"]].sample(min(20, len(gm))).to_string())

# Check Valid Depts
count = len(df[df["Department"] != "General Merchandise"])
print(f"\nTotal Classified Items: {count}")
