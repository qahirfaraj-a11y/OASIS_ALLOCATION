import pandas as pd
import zipfile
import xml.etree.ElementTree as ET

def peek_docx(file_path):
    print(f"\n--- Peeking into {file_path} ---")
    try:
        with zipfile.ZipFile(file_path, 'r') as docx:
            xml_content = docx.read('word/document.xml')
            root = ET.fromstring(xml_content)
            paragraphs = []
            for p in root.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}p'):
                text = ''.join(node.text for node in p.iter('{http://schemas.openxmlformats.org/wordprocessingml/2006/main}t') if node.text)
                if text:
                    paragraphs.append(text)
            
            # Search for Kapa
            kapa_lines = [p for p in paragraphs if 'Kapa' in p]
            if kapa_lines:
                print("Found Kapa in document:")
                for line in kapa_lines[:10]:
                    print(f"- {line}")
            else:
                print("Kapa not found in document text.")
    except Exception as e:
        print(f"Error reading docx: {e}")

def peek_xlsx(file_path):
    print(f"\n--- Peeking into {file_path} ---")
    try:
        # Load without full data for quick scan
        xl = pd.ExcelFile(file_path)
        print(f"Sheets: {xl.sheet_names}")
        for sheet in xl.sheet_names:
            df = pd.read_excel(file_path, sheet_name=sheet, nrows=100)
            # Search for Kapa in all columns
            mask = df.apply(lambda x: x.astype(str).str.contains('Kapa', case=False)).any(axis=1)
            if mask.any():
                print(f"Found Kapa in sheet: {sheet}")
                print(df[mask].head())
    except Exception as e:
        print(f"Error reading xlsx: {e}")

if __name__ == "__main__":
    peek_docx(r'C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Master_Intelligence_Report.docx')
    peek_xlsx(r'C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Fulfillment_Summary.xlsx')
    peek_xlsx(r'C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Intelligence_Report_2025_v3.xlsx')
