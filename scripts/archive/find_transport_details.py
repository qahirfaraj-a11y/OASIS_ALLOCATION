import os
import glob

def find_details():
    keywords = ["TRANSPORT", "FREIGHT", "CARRIER", "LOGISTICS", "TRUCK", "DELIVERY", "LANDED"]
    extensions = [".py", ".json", ".csv", ".md", ".txt"]
    search_dirs = [
        r"C:\Users\iLink\.gemini\antigravity\scratch",
        r"C:\Oasis"
    ]
    
    for sdir in search_dirs:
        for root, dirs, files in os.walk(sdir):
            if "venv" in root or ".git" in root or "node_modules" in root:
                continue
            for f in files:
                ext = os.path.splitext(f)[1].lower()
                if ext in extensions:
                    fp = os.path.join(root, f)
                    try:
                        with open(fp, 'r', encoding='utf-8', errors='ignore') as file_obj:
                            content = file_obj.read()
                            if "KAPA" in content.upper():
                                # Check if any keyword is also in the file
                                for kw in keywords:
                                    if kw in content.upper():
                                        print(f"Match for KAPA & {kw} in {fp}")
                                        # Print surrounding lines
                                        lines = content.split('\n')
                                        for i, line in enumerate(lines):
                                            if "KAPA" in line.upper() and any(k in line.upper() for k in keywords):
                                                print(f"  L{i+1}: {line.strip()[:150]}")
                    except Exception as e:
                        pass

find_details()
