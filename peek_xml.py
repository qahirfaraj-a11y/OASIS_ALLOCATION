import zipfile
import re

doc_path = r"C:\Users\iLink\Downloads\OASIS_All_SKUs_Full_Query.docx"

with zipfile.ZipFile(doc_path) as z:
    xml_content = z.read('word/document.xml').decode('utf-8')
    # Peek at first 10k chars
    print(xml_content[:10000])
    
    # Try to find headers if it's a table
    headers = re.findall(r'<w:t>(.*?)</w:t>', xml_content[:50000])
    print(f"\nPotential headers (found {len(headers)}):")
    print(headers[:20])
