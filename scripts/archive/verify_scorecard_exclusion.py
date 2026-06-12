
import pandas as pd
import sys

OUTPUT_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v2.csv"
EXCLUDED_SUPPLIERS = [
    "BIGCOLD KENYA SIMPLIFINE BAKERY NO GRN",
    "CARD GROUP EAST AFRICA LTD NO GRN",
    "CORNER SHOP PREPACK NO GRN",
    "HIDDEN TREASURES BOOK W S LIMITED NO GRN",
    "KENCHIC BUTCHERY NO GRN",
    "ROYAL BLOOMS S ENTERPRISES NO GRN",
    "NATION MEDIA GROUP LTD ACC 2 NO GRN",
    "SELECTIONS WORLD LIMITED NO GRN",
    "STENTOR ENTERPRISES LIMITED NO GRN",
    "SUNPOWER PRODUCTS LTD DELI NO GRN",
    "THE CORNER SHOP LIMITED NO GRN",
    "THE STANDARD GROUP PLC NO GRN",
    "THE STAR PUBLICATIONS LTD NO GRN"
]

def verify():
    print(f"Loading {OUTPUT_FILE}...")
    try:
        df = pd.read_csv(OUTPUT_FILE)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        sys.exit(1)
        
    print("Columns found:", list(df.columns))
    
    # 1. Verify Exclusion (Removal)
    print("\n--- Checking Consignment Removal ---")
    failures = 0
    excluded_rows = df[df["Supplier"].isin(EXCLUDED_SUPPLIERS)]
    if len(excluded_rows) > 0:
        print(f"FAIL: Found {len(excluded_rows)} rows belonging to excluded suppliers. They should have been removed.")
        print(excluded_rows[["Product", "Supplier"]].head())
        failures += 1
    else:
        print("PASS: No consignment suppliers found in output.")

    # 2. Verify Logic Strings
    print("\n--- Checking Logic Trace Strings ---")
    
    # Sample check for 'Eligible' pattern
    eligible_sample = df[df["Stocking_Notes"].str.contains("-> Eligible")]
    if len(eligible_sample) > 0:
        notes = eligible_sample.iloc[0]["Stocking_Notes"]
        print(f"Sample Eligible Note: '{notes}'")
        if "Rev:OK" not in notes or "Price:OK" not in notes:
             print("FAIL: Eligible note missing key trace elements.")
             failures += 1
        else:
             print("PASS: Eligible trace looks correct.")
    else:
        print("WARNING: No eligible items found?")

    # Sample check for 'Ineligible' pattern
    ineligible_sample = df[df["Stocking_Notes"].str.contains("-> Ineligible")]
    if len(ineligible_sample) > 0:
        notes = ineligible_sample.iloc[0]["Stocking_Notes"]
        print(f"Sample Ineligible Note: '{notes}'")
        if "Rev:NONE" in notes or "Price:ZERO" in notes or "Strat:DELIST" in notes:
             print("PASS: Ineligible trace contains failure reason.")
        else:
             print("FAIL: Ineligible note missing failure reason?")
             failures += 1
    else:
        print("WARNING: No ineligible items found?")

    if failures == 0:
        print("\nVerification Complete: SUCCESS")
    else:
        print(f"\nVerification Complete: FAILED with {failures} issues.")

if __name__ == "__main__":
    verify()
