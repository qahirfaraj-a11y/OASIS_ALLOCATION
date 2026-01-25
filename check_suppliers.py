
import json
import os

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch"
FILENAME = "supplier_patterns_2025 (3).json"

path = os.path.join(DATA_DIR, FILENAME)
try:
    with open(path, 'r') as f:
        data = json.load(f)
        print("Supplier Names found in JSON:")
        for name in list(data.keys())[:50]: # Print first 50
            print(f"'{name}'")
            
        print("\nChecking for specific names from user list:")
        user_list = [
            "BIGCOLD KENYA SIMPLIFINE BAKERY", "CARD GROUP EAST AFRICA LTD", "CORNER SHOP PREPACK",
            "HIDDEN TREASURES BOOK W S LIMITED", "KENCHIC BUTCHERY", "ROYAL BLOOMS S ENTERPRISES",
            "NATION MEDIA GROUP LTD ACC 2", "SELECTIONS WORLD LIMITED", "STENTOR ENTERPRISES LIMITED",
            "SUNPOWER PRODUCTS LTD DELI", "THE CORNER SHOP LIMITED", "THE STANDARD GROUP PLC",
            "THE STAR PUBLICATIONS LTD"
        ]
        
        all_suppliers = set(data.keys())
        for user_prov in user_list:
            # simple fuzzy check
            match = [s for s in all_suppliers if user_prov in s or s in user_prov]
            print(f"User Input: '{user_prov}' -> Matches: {match}")

except Exception as e:
    print(f"Error: {e}")
