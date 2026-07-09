import os

overview_path = r"C:\Users\iLink\.gemini\antigravity\brain\1bac4dd1-6749-46b0-9be2-c691593f61e0\.system_generated\logs\overview.txt"
if not os.path.exists(overview_path):
    print("overview.txt not found")
else:
    with open(overview_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    print(f"Total lines: {len(lines)}")
    # Search for lines containing kapa.xlsx
    for i, line in enumerate(lines):
        if "kapa.xlsx" in line or "ROI" in line or "revenue" in line:
            print(f"L{i}: {line.strip()[:150]}")
