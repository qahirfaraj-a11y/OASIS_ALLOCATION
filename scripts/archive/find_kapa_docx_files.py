import os
import glob

def search():
    dirs = [
        r"C:\Users\iLink\.gemini\antigravity\scratch",
        r"C:\Users\iLink\Downloads"
    ]
    for d in dirs:
        print(f"=== Docx files under {d} ===")
        files = glob.glob(os.path.join(d, "*.docx")) + glob.glob(os.path.join(d, "**", "*.docx"), recursive=True)
        for f in files:
            bf = os.path.basename(f).lower()
            if "kapa" in bf or "deep" in bf or "dive" in bf or "report" in bf or "summary" in bf:
                print(f"  {f}")

search()
