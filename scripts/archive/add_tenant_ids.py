import re
import os

files_to_patch = [
    "oasis/logic/mock_pos_erp.py",
    "oasis/logic/db_connector.py"
]

def patch_file(filepath):
    if not os.path.exists(filepath):
        print(f"Skipping {filepath}, not found.")
        return

    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()

    # Regex to match 'CREATE TABLE IF NOT EXISTS TABLENAME ('
    # and insert '    TENANT_ID TEXT DEFAULT "default_tenant",'
    
    # We want to insert the tenant_id line right after the opening parenthesis
    pattern = re.compile(r'(CREATE TABLE IF NOT EXISTS \w+\s*\()', re.IGNORECASE)
    
    def replacer(match):
        return match.group(1) + "\n    TENANT_ID TEXT DEFAULT 'default_tenant',"

    new_content = pattern.sub(replacer, content)

    # Now, let's update some PRIMARY KEYs to include TENANT_ID if they are explicitly defined.
    # We'll just append TENANT_ID to the PRIMARY KEY constraint list.
    pk_pattern = re.compile(r'(PRIMARY KEY\s*\()([^\)]+)(\))', re.IGNORECASE)
    
    def pk_replacer(match):
        # Don't add if it already has TENANT_ID
        if 'TENANT_ID' in match.group(2).upper():
            return match.group(0)
        return match.group(1) + "TENANT_ID, " + match.group(2) + match.group(3)

    new_content = pk_pattern.sub(pk_replacer, new_content)

    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"Patched {filepath}")

for f in files_to_patch:
    patch_file(f)
