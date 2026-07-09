import docx
import os
import glob

def search_docx_files():
    for f in glob.glob(r"C:\Users\iLink\.gemini\antigravity\scratch\*.docx"):
        try:
            doc = docx.Document(f)
            text = []
            for p in doc.paragraphs:
                text.append(p.text)
            full_text = "\n".join(text).upper()
            if "KAPA" in full_text and ("TRANSPORT" in full_text or "LOGISTICS" in full_text or "FREIGHT" in full_text or "DELIVERY" in full_text):
                print(f"Found Kapa transport/delivery info in: {os.path.basename(f)}")
                # Print some lines around Kapa
                for line in text:
                    if "KAPA" in line.upper() and ("TRANSPORT" in line.upper() or "LOGISTICS" in line.upper() or "FREIGHT" in line.upper() or "DELIVERY" in line.upper() or "LEAD" in line.upper() or "COST" in line.upper()):
                        print(f"  Line: {line}")
        except Exception as e:
            print(f"Error reading {f}: {e}")

if __name__ == "__main__":
    search_docx_files()
