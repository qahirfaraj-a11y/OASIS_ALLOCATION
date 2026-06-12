from pypdf import PdfReader
import os

target_file = r"C:\Users\iLink\.gemini\antigravity\brain\de5dfb48-01e4-4e89-b189-535b50375359\CSL3973_2004_25_Oct_24_Nov.pdf"

try:
    reader = PdfReader(target_file)
    print(f"Number of pages: {len(reader.pages)}")
    
    page = reader.pages[0]
    text = page.extract_text()
    
    print("--- Extracted Text ---")
    print(text)
    print("--- End Extracted Text ---")

    # Try to get some font info if possible (though text extraction is usually just text)
    # pypdf doesn't always give easy access to font styles per character, but we can infer from raw content sometimes.
    
except Exception as e:
    print(f"Error reading PDF: {e}")
