import os
import json

vault_path = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis_vault\Nodes\SKUs"
master_map_path = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\master_product_dept_map.json"

def verify():
    with open(master_map_path, 'r') as f:
        master_map = json.load(f)
    
    # Audit cases (Exact names from Scorecard)
    test_cases = [
        "ACHAARI 230G CARIBBEAN CHILLI SAUCE",
        "REMIA 250ML FRENCH SALAD DRESSING",
        "254 NIAJE 330ML LAGER BEER"
    ]
    
    for case in test_cases:
        expected_dept = master_map.get(case)
        # Sanitize filename like the script does
        filename = case.replace("/", "-").replace("\\", "-").replace(":", "-").replace("*", "-").replace("?", "-").replace("\"", "-").replace("<", "-").replace(">", "-").replace("|", "-") + ".md"
        filepath = os.path.join(vault_path, filename)
        
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
                if f"department: \"[[{expected_dept}]]\"" in content:
                    print(f"VERIFIED: {case} -> {expected_dept}")
                else:
                    print(f"FAILED: {case} - Expected department not found in file.")
        else:
            print(f"FAILED: {case} - File not found: {filename}")

if __name__ == "__main__":
    verify()
