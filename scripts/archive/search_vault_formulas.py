import os
import glob

def find_formulas():
    vault_path = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis_vault"
    print("=== Searching for formulas in vault ===")
    for root, dirs, files in os.walk(vault_path):
        for f in files:
            if f.endswith(".md"):
                fp = os.path.join(root, f)
                try:
                    with open(fp, 'r', encoding='utf-8') as file_obj:
                        content = file_obj.read()
                        if any(x in content.upper() for x in ["FORMULA", "SAFETY STOCK", "ROP = ", "ROI = ", "GROSS PROFIT"]):
                            print(f"Match in: {f}")
                            lines = content.split('\n')
                            for i, line in enumerate(lines):
                                if any(x in line.upper() for x in ["FORMULA", "SAFETY STOCK", "ROP", "ROI", "GROSS PROFIT", "REORDER"]):
                                    print(f"  L{i+1}: {line.strip()[:100]}")
                except Exception as e:
                    pass

find_formulas()
