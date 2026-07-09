import os

def search_log():
    log_path = r"C:\Users\iLink\.gemini\antigravity\brain\1bac4dd1-6749-46b0-9be2-c691593f61e0\.system_generated\logs\overview.txt"
    if os.path.exists(log_path):
        print("Log found!")
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
        print(f"Total lines in log: {len(lines)}")
        
        # Search for recent mentions of kapa.xlsx
        matches = []
        for i, line in enumerate(lines):
            if "kapa.xlsx" in line or "ROI" in line or "revenue" in line:
                matches.append((i+1, line.strip()))
                
        print(f"Found {len(matches)} matches. Printing last 30:")
        for idx, line in matches[-30:]:
            print(f"Line {idx}: {line[:150]}")
    else:
        print("Log not found at:", log_path)

search_log()
