import zipfile
import re

doc_path = r"C:\Users\iLink\Downloads\OASIS_All_SKUs_Full_Query.docx"

with zipfile.ZipFile(doc_path) as z:
    xml_content = z.read('word/document.xml').decode('utf-8')
    # Let's find a few blocks and see the text around them
    # Find the first few <w:t> tags
    texts = re.findall(r'<w:t>(.*?)</w:t>', xml_content)
    print("PEEKING AT FIRST 50 TEXT FRAGMENTS:")
    for i, t in enumerate(texts[:100]):
        print(f"{i}: {t}")
