import zipfile
import xml.etree.ElementTree as ET
import sys
import os

def read_docx(path):
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    try:
        with zipfile.ZipFile(path) as docx:
            xml_content = docx.read('word/document.xml')
            
        tree = ET.fromstring(xml_content)
        
        # Word namespaces
        WORD_NAMESPACE = '{http://schemas.openxmlformats.org/wordprocessingml/2006/main}'
        PARA = WORD_NAMESPACE + 'p'
        TEXT = WORD_NAMESPACE + 't'
        
        text_lines = []
        for paragraph in tree.iter(PARA):
            texts = [node.text for node in paragraph.iter(TEXT) if node.text]
            if texts:
                text_lines.append(''.join(texts))
            else:
                text_lines.append('') # keep empty lines for paragraph breaks
                
        output_path = "Algorithmic_retail_extracted.txt"
        with open(output_path, "w", encoding="utf-8") as f:
            f.write("\n".join(text_lines))
        
        print(f"Extracted to {output_path}")
    except Exception as e:
        print(f"Error reading docx: {e}")

if __name__ == "__main__":
    docx_path = r"C:\Users\iLink\Documents\Algorithmic_retail.docx"
    read_docx(docx_path)
