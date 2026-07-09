import os

log_path = r"C:\Users\iLink\.gemini\antigravity\brain\1bac4dd1-6749-46b0-9be2-c691593f61e0\.system_generated\logs\overview.txt"
with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
    lines = f.readlines()
print("Total lines in log:", len(lines))
for line in lines[-20:]:
    print(line.strip())
