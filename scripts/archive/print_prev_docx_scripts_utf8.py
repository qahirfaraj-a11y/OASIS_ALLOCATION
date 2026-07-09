import os
import glob
import sys

sys.stdout.reconfigure(encoding='utf-8')

def check_scripts():
    search_dir = r"C:\Users\iLink\.gemini\antigravity\scratch"
    files = glob.glob(os.path.join(search_dir, "*.py"))
    for f in files:
        bf = os.path.basename(f)
        if "kapa" in bf or "docx" in bf or "report" in bf:
            print("=== Script:", f)
            with open(f, 'r', encoding='utf-8', errors='ignore') as file_obj:
                print(file_obj.read()[:2000])

check_scripts()
