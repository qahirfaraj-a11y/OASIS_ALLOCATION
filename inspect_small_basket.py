import pandas as pd

FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"
df = pd.read_csv(FILE)

# Filter for Small Basket
basket = df[df["Small_200k"] == True].copy()
disc_basket = basket[basket["Is_Staple"] == False]

print(f"Total Discretionary Items in Small Basket: {len(disc_basket)}")
if len(disc_basket) > 0:
    # Infer Pack Cost (Re-using logic)
    def get_pack_cost(p_name, unit_price):
        pack_size = 1
        p_name = str(p_name)
        if any(x in p_name for x in ["6PK", "6 PK"]): pack_size = 6
        elif any(x in p_name for x in ["12PK", "12 PK", "SODA", "WATER"]): pack_size = 12
        elif "24PK" in p_name or "24 PK" in p_name: pack_size = 24
        return unit_price * pack_size

    disc_basket["Pack_Cost_Est"] = disc_basket.apply(lambda r: get_pack_cost(r["Product"], r["Unit_Price"]), axis=1)
    
    # Check for Expensive Items
    expensive = disc_basket[disc_basket["Pack_Cost_Est"] > 1500]
    print(f"Expensive Discretionary Items (>1500): {len(expensive)}")
    if len(expensive) > 0:
        print(expensive[["Product", "Pack_Cost_Est", "Cost_Drag"]].head(10).to_string())
    else:
        print("No expensive items found in the current selection.")
        print("Top 5 Most Expensive Discretionary items:")
        print(disc_basket.sort_values(by="Pack_Cost_Est", ascending=False)[["Product", "Pack_Cost_Est"]].head(5))

print("\n--- Diagnostic Complete ---")
