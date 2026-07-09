import os
import glob

def find_script():
    search_dir = r"C:\Users\iLink\.gemini\antigravity\scratch"
    files = glob.glob(os.path.join(search_dir, "*.py")) + glob.glob(os.path.join(search_dir, "**", "*.py"), recursive=True)
    for f in files:
        if "generate_kapa" in os.path.basename(f):
            print("Found script:", f)
            with open(f, 'r', encoding='utf-8') as file_obj:
                print(file_obj.read()[:2000])

find_script()
