
import json
import os

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch"
FILENAME = "product_supplier_map.json"

path = os.path.join(DATA_DIR, FILENAME)
try:
    with open(path, 'r') as f:
        data = json.load(f)
        # data is product -> supplier
        suppliers = set(data.values())
        print(f"Found {len(suppliers)} unique suppliers.")
        
        user_list = [
            "BIGCOLD KENYA SIMPLIFINE BAKERY", "CARD GROUP EAST AFRICA LTD", "CORNER SHOP PREPACK",
            "HIDDEN TREASURES BOOK W S LIMITED", "KENCHIC BUTCHERY", "ROYAL BLOOMS S ENTERPRISES",
            "NATION MEDIA GROUP LTD ACC 2", "SELECTIONS WORLD LIMITED", "STENTOR ENTERPRISES LIMITED",
            "SUNPOWER PRODUCTS LTD DELI", "THE CORNER SHOP LIMITED", "THE STANDARD GROUP PLC",
            "THE STAR PUBLICATIONS LTD"
        ]
        
        print("\nChecking User List:")
        for user_prov in user_list:
            match = [s for s in suppliers if user_prov in s or s in user_prov]
            print(f"User Input: '{user_prov}' -> Matches: {match}")
            
except Exception as e:
    print(f"Error: {e}")
