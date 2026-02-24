import pandas as pd

FILE_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Order_Calendar_2026.xlsx"
SEARCH_TERMS = ["BROOKSIDE", "BIO FOOD", "COCA COLA", "KWAL", "FARMERS CHOICE"]

try:
    df = pd.read_excel(FILE_PATH)
    # The supplier column seems to be the last one, or named 'Suppliers' (based on previous inspect, it didn't show header clearly, it was raw data).
    # Previous inspect showed row 4 had many suppliers in the last column.
    
    # Let's iterate through all string columns
    found_map = {term: [] for term in SEARCH_TERMS}
    
    for col in df.columns:
        if df[col].dtype == 'object':
            for term in SEARCH_TERMS:
                matches = df[df[col].astype(str).str.contains(term, case=False, na=False)][col].unique()
                if len(matches) > 0:
                    found_map[term].extend(matches[:3]) # First 3 examples

    print("SEARCH RESULTS:")
    for term, matches in found_map.items():
        print(f"Term '{term}': {matches}")

except Exception as e:
    print(f"Error: {e}")
