import os

def try_ocr():
    img_path = r"C:\Users\iLink\Downloads\1778226447843.jpg"
    print(f"Checking if image exists: {os.path.exists(img_path)}")
    
    # 1. Try easyocr
    try:
        import easyocr
        print("easyocr is installed. Loading reader...")
        reader = easyocr.Reader(['en'])
        print("Reading image text...")
        results = reader.readtext(img_path)
        print("\n=== easyocr Extracted Text ===")
        for r in results:
            print(r[1])
        return
    except Exception as e:
        print(f"easyocr failed or not installed: {e}")
        
    # 2. Try pytesseract
    try:
        import pytesseract
        from PIL import Image
        print("pytesseract and PIL are installed. Reading image text...")
        text = pytesseract.image_to_string(Image.open(img_path))
        print("\n=== pytesseract Extracted Text ===")
        print(text)
        return
    except Exception as e:
        print(f"pytesseract failed or not installed: {e}")
        
    # 3. If no OCR, let's see if we have other files in the Downloads folder or scratch
    print("\nListing related files in Downloads...")
    downloads = r"C:\Users\iLink\Downloads"
    for f in os.listdir(downloads):
        if 'kapa' in f.lower() or 'job' in f.lower() or 'role' in f.lower() or 'apply' in f.lower():
            print(f"  {f}")

try_ocr()
