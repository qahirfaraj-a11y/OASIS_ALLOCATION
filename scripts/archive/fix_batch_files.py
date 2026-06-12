import os

def fix_file(path):
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return
    
    with open(path, 'rb') as f:
        content = f.read()
    
    # Remove BOM if present
    if content.startswith(b'\xef\xbb\xbf'):
        content = content[3:]
    elif content.startswith(b'\xff\xfe'):
        content = content.decode('utf-16').encode('utf-8')
    elif content.startswith(b'\xfe\xff'):
        content = content.decode('utf-16-be').encode('utf-8')
    
    # Normalize line endings to CRLF
    # First, convert all to LF
    text = content.decode('utf-8', errors='ignore')
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    # Then convert to CRLF
    fixed_content = text.replace('\n', '\r\n')
    
    with open(path, 'wb') as f:
        f.write(fixed_content.encode('utf-8'))
    print(f"Fixed {path}")

fix_file(r'c:\Users\iLink\.gemini\antigravity\scratch\dist_release\install_oasis.bat')
fix_file(r'c:\Users\iLink\.gemini\antigravity\scratch\dist_release\update_oasis.bat')
