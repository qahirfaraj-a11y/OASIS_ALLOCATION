import re
import os

def patch_data_gateway():
    path = "oasis/logic/data_gateway.py"
    if not os.path.exists(path): return
    with open(path, 'r', encoding='utf-8') as f: content = f.read()

    # Add tenant_id to __init__
    content = content.replace(
        "def __init__(self, config_path: Optional[str] = None):",
        "def __init__(self, config_path: Optional[str] = None, tenant_id: str = 'default_tenant'):\n        self.tenant_id = tenant_id"
    )
    
    # Filter SQL queries in DataGateway if it had raw SQL (mostly it relies on db_connector/SQLBridge)
    # But for file stock, we can inject a mock filter
    content = content.replace(
        "df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})",
        "df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})\n        # Tenant isolation mock\n        if 'TENANT_ID' in df.columns:\n            df = df[df['TENANT_ID'] == self.tenant_id]"
    )

    with open(path, 'w', encoding='utf-8') as f: f.write(content)
    print("Patched data_gateway.py")

def patch_auth_manager():
    path = "oasis/logic/auth_manager.py"
    if not os.path.exists(path): return
    with open(path, 'r', encoding='utf-8') as f: content = f.read()

    # Ensure TENANT_ID is added to the user dictionary
    content = content.replace(
        '"session_token": session_token',
        '"session_token": session_token,\n            "tenant_id": row.get("TENANT_ID", "default_tenant")'
    )
    content = content.replace(
        '"permissions": get_user_permissions(row["ROLE"])',
        '"permissions": get_user_permissions(row["ROLE"]),\n            "tenant_id": row.get("TENANT_ID", "default_tenant")'
    )

    with open(path, 'w', encoding='utf-8') as f: f.write(content)
    print("Patched auth_manager.py")

patch_data_gateway()
patch_auth_manager()
