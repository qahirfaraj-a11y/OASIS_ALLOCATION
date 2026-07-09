import os
import glob

def check_scripts():
    search_dir = r"C:\Users\iLink\.gemini\antigravity\scratch"
    files = glob.glob(os.path.join(search_dir, "*.py"))
    for f in files:
        if "kapa" in os.path.basename(f) or "docx" in os.path.basename(f) or "report" in os.path.basename(f):
            print("=== Script:", f)
            with open(f, 'r', encoding='utf-8', errors='ignore') as file_obj:
                print(file_obj.read()[:2000])

check_scripts()
