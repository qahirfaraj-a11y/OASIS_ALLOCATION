import difflib
import os
import re

files = [
    r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\logic\order_engine.py",
    r"C:\Users\iLink\Downloads\paste_sanitized.py",
    r"C:\Users\iLink\Downloads\paste_fixed.py"
]

labels = ["Local", "Sanitized", "Fixed"]

def normalize_line(line):
    # Remove all whitespace for comparison
    return re.sub(r'\s+', '', line)

def compare_logic(f1, l1, f2, l2):
    print(f"\n--- LOGIC COMPARISON: {l1} vs {l2} ---")
    with open(f1, 'r', encoding='utf-8') as a, open(f2, 'r', encoding='utf-8') as b:
        lines1 = a.readlines()
        lines2 = b.readlines()
        
        found_diff = False
        # We assume they have the same number of lines for now, but let's be safe.
        max_len = max(len(lines1), len(lines2))
        for i in range(max_len):
            lineno = i + 1
            l1_content = lines1[i] if i < len(lines1) else ""
            l2_content = lines2[i] if i < len(lines2) else ""
            
            if normalize_line(l1_content) != normalize_line(l2_content):
                print(f"Diff at line {lineno}:")
                print(f"  {l1}: {l1_content.strip()}")
                print(f"  {l2}: {l2_content.strip()}")
                found_diff = True
        
        if not found_diff:
            print("No logic differences found (ignoring whitespace).")

compare_logic(files[0], labels[0], files[1], labels[1])
compare_logic(files[0], labels[0], files[2], labels[2])
compare_logic(files[1], labels[1], files[2], labels[2])
