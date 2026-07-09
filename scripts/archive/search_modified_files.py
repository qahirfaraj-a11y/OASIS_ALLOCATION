import os
import glob
import datetime

def search():
    dirs = [
        r"C:\Users\iLink\.gemini\antigravity\scratch",
        r"C:\Users\iLink\Downloads"
    ]
    cutoff = datetime.datetime.now() - datetime.timedelta(days=1)
    
    print("=== Files modified in the last 24 hours ===")
    for d in dirs:
        for root, subdirs, files in os.walk(d):
            if "venv" in root or ".git" in root or "node_modules" in root:
                continue
            for f in files:
                fp = os.path.join(root, f)
                try:
                    mtime = datetime.datetime.fromtimestamp(os.path.getmtime(fp))
                    if mtime > cutoff:
                        print(f"  {fp} (modified: {mtime})")
                except Exception as e:
                    pass

search()
