import os

vault_path = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis_vault"
for root, dirs, files in os.walk(vault_path):
    for f in files:
        if "2026-03-20_Audit_Refinement_Log" in f:
            fp = os.path.join(root, f)
            print("Found path:", fp)
            with open(fp, 'r', encoding='utf-8') as file_obj:
                print(file_obj.read()[:2000])
