import os
import glob

def find_docx():
    dirs = [
        r"C:\Users\iLink\.gemini\antigravity\scratch",
        r"C:\Users\iLink\Downloads"
    ]
    for d in dirs:
        print(f"=== Docx files under {d} ===")
        files = glob.glob(os.path.join(d, "*.docx")) + glob.glob(os.path.join(d, "**", "*.docx"), recursive=True)
        for f in files:
            print(f"  {f}")

find_docx()
