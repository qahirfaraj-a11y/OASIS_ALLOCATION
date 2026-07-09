import os
import glob

def find_kapa_files():
    dirs = [
        r"C:\Users\iLink\.gemini\antigravity\scratch",
        r"C:\Users\iLink\Downloads"
    ]
    for d in dirs:
        print(f"=== Kapa files under {d} ===")
        files = glob.glob(os.path.join(d, "*kapa*")) + glob.glob(os.path.join(d, "**", "*kapa*"), recursive=True)
        for f in files:
            print(f"  {f}")

find_kapa_files()
