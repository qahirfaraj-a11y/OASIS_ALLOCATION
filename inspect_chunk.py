import zipfile
import re

doc_path = r"C:\Users\iLink\Downloads\OASIS_All_SKUs_Full_Query.docx"

with zipfile.ZipFile(doc_path) as z:
    xml_content = z.read('word/document.xml').decode('utf-8')
    chunks = re.split(r'---', xml_content)
    
    # Let's find a SKU identity chunk
    for i in range(1, len(chunks)):
        chunk = chunks[i]
        if 'Macro-Habitat' in chunk:
            print(f"CHUNK {i} PEEK:")
            print(repr(chunk[:500])) # Use repr to see newlines/tags
            break
