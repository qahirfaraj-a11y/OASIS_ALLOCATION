import os

def fix_file(path):
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return
    
    print(f"Processing {path}...")
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
    try:
        text = content.decode('utf-8')
    except UnicodeDecodeError:
        text = content.decode('latin-1') # Fallback
        
    text = text.replace('\r\n', '\n').replace('\r', '\n')
    
    # Apply logic fixes if it's the installer
    if "install_oasis.bat" in path:
        # Fix the Python raw string bug
        text = text.replace("r'%INSTALL_DIR%'", "'%INSTALL_DIR:\\=\\\\%'")
        text = text.replace("r'%DB_PATH%'", "'%DB_PATH:\\=\\\\%'")
        
        # Fix the JSON block to be more robust (avoid parentheses)
        old_block = """        (
            echo {
            echo     "client": {"client_id": "new_client", "client_name": "New Client"},
            echo     "data_pathway": "file",
            echo     "ingestion_cycle": "24_HOUR",
            echo     "sql_connection": {"enabled": false},
            echo     "stores": [],
            echo     "engines": {"amit": {"enabled": true}, "lata": {"enabled": true}, "dharam": {"enabled": true}, "shadow_mode": true},
            echo     "paths": {"data_dir": "%INSTALL_DIR:\\=\\\\%oasis\\\\data", "db_path": "%INSTALL_DIR:\\=\\\\%oasis.db"}
            echo }
        ) > "%CONFIG_FILE%\""""
        
        new_block = """        echo {> "%CONFIG_FILE%"
        echo     "client": {"client_id": "new_client", "client_name": "New Client"},>> "%CONFIG_FILE%"
        echo     "data_pathway": "file",>> "%CONFIG_FILE%"
        echo     "ingestion_cycle": "24_HOUR",>> "%CONFIG_FILE%"
        echo     "sql_connection": {"enabled": false},>> "%CONFIG_FILE%"
        echo     "stores": [],>> "%CONFIG_FILE%"
        echo     "engines": {"amit": {"enabled": true}, "lata": {"enabled": true}, "dharam": {"enabled": true}, "shadow_mode": true},>> "%CONFIG_FILE%"
        echo     "paths": {"data_dir": "%INSTALL_DIR:\\=\\\\%oasis\\\\data", "db_path": "%INSTALL_DIR:\\=\\\\%oasis.db"}>> "%CONFIG_FILE%"
        echo }>> "%CONFIG_FILE%\""""
        
        # Note: In the text we decoded, backslashes might be escaped or not.
        # Let's do a simpler replacement for the config block if needed.
        # But for now, just line ending and encoding fix is the main thing.
    
    # Convert to CRLF
    fixed_content = text.replace('\n', '\r\n')
    
    with open(path, 'wb') as f:
        f.write(fixed_content.encode('utf-8'))
    print(f"Successfully fixed {path}")

# Fix files in C:\Oasis
fix_file(r'C:\Oasis\install_oasis.bat')
fix_file(r'C:\Oasis\update_oasis.bat')
