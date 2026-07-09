import sys

file_path = r'C:\Users\iLink\.gemini\antigravity\scratch\verify_fixes.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Fix the risk scores printing
old_print = """    for org, score in risk_scores_map.items():
        if org.startswith("CFP-"):
            print(f"  {org}: {score:.3f}")"""
new_print = """    for org, score in risk_scores_map.items():
        if org.startswith("ORG") or org.startswith("CFP"):
            print(f"  {org}: {score:.3f}")"""
content = content.replace(old_print, new_print)

# 2. Fix the transfer record printing
old_transfer = """    print(f"From {t.from_org} to {t.to_org}: {t.qty}x {t.itm_cd} ({t.type})")"""
new_transfer = """    print(f"From {t.from_org} to {t.to_org}: {t.qty}x {t.itm_cd}")"""
content = content.replace(old_transfer, new_transfer)

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)
print("verify_fixes.py patched.")
