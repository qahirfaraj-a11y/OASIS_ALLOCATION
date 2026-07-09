import os
import glob

def find_kw():
    keywords = ["TRANSPORT", "FREIGHT", "LOGISTICS", "DELIVERY_COST", "TRANSPORT_COST", "LANDED_COST"]
    # search .py, .json, .csv, .md, .txt
    extensions = [".py", ".json", ".csv", ".md", ".txt"]
    
    search_dirs = [
        r"C:\Users\iLink\.gemini\antigravity\scratch",
        r"C:\Oasis"
    ]
    
    for sdir in search_dirs:
        print(f"=== Searching in {sdir} ===")
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
                            # check if any keyword is in content
                            for kw in keywords:
                                if kw in content.upper():
                                    print(f"Match for {kw} in {fp}")
                                    # Print lines
                                    lines = content.split('\n')
                                    for i, line in enumerate(lines):
                                        if kw in line.upper():
                                            print(f"  L{i+1}: {line.strip()[:120]}")
                    except Exception as e:
                        pass

find_kw()
