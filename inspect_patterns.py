import json
import os

DATA_FILE = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\supplier_patterns_2025 (3).json"

def inspect_patterns():
    if not os.path.exists(DATA_FILE):
        print("File not found:", DATA_FILE)
        return

    with open(DATA_FILE, 'r') as f:
        data = json.load(f)

    print(f"Loaded {len(data)} suppliers.")
    print("Sample Keys:", list(data.keys())[:5])
    
    # Try fuzzy match or exact match
    sample_keys = ['BROOKSIDE', 'BIDCO', 'KENYA NUT']
    
    for s_part in sample_keys:
        # Find key containing s_part
        match = next((k for k in data.keys() if s_part in k), None)
        if match:
            print(f"\nSUPPLIER: {match}")
            print(json.dumps(data[match], indent=2))


if __name__ == "__main__":
    inspect_patterns()
