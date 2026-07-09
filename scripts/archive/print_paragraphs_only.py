import docx

doc = docx.Document(r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Master_Intelligence_Report.docx")
print("=== Supplier_Master_Intelligence_Report.docx Paragraphs ===")
for i, p in enumerate(doc.paragraphs):
    if p.text.strip():
        print(f"Paragraph {i}: {p.text}")
