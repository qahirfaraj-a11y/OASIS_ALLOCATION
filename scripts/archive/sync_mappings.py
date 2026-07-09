import sys, json, os, shutil
from datetime import datetime

sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')
DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data'

from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter

def backup_file(filepath):
    if os.path.exists(filepath):
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(filepath, f"{filepath}.bak_{ts}")
        print(f"Backed up {os.path.basename(filepath)}")

print("1. Initializing DB connection...")
uri = 'sqlite:///C:/Users/iLink/.gemini/antigravity/scratch/oasis/data/mock_pos_erp_showcase.db'
mapper = SchemaMapper.for_pos_erp()
conn = UniversalConnector(uri, mapper)
adapter = PosErpAdapter(conn)

print("2. Loading existing mapping files...")
supp_map_file = os.path.join(DATA_DIR, 'product_supplier_map.json')
dept_map_file = os.path.join(DATA_DIR, 'product_department_map.json')
master_dept_file = os.path.join(DATA_DIR, 'master_product_dept_map.json')
barcode_dept_file = os.path.join(DATA_DIR, 'barcode_department_map.json')

with open(supp_map_file, 'r', encoding='utf-8') as f:
    supp_map = json.load(f)
with open(dept_map_file, 'r', encoding='utf-8') as f:
    dept_map = json.load(f)
with open(master_dept_file, 'r', encoding='utf-8') as f:
    master_dept = json.load(f)
with open(barcode_dept_file, 'r', encoding='utf-8') as f:
    bc_dept = json.load(f)

orgs = adapter.fetch_all_organizations()
org_cds = [o["ORG_CD"] for o in orgs]

print(f"3. Scanning {len(org_cds)} organizations for enriched product data...")
added_supp = 0
added_dept = 0

for org_cd in org_cds:
    prods = adapter.fetch_enriched_products(org_cd)
    for p in prods:
        name = p.get('product_name', '')
        if not name:
            continue
            
        # ── SUPPLIER MAPPING ──────────────────────────────────
        if name not in supp_map or not supp_map[name] or str(supp_map[name]).strip().upper() in ["", "NULL", "NONE"]:
            db_supp = p.get('supplier_name', '')
            if db_supp and str(db_supp).strip().upper() not in ["", "NULL", "NONE"]:
                supp_map[name] = str(db_supp).strip()
                added_supp += 1
                
        # ── DEPARTMENT MAPPING ────────────────────────────────
        if name not in dept_map or not dept_map[name] or str(dept_map[name]).strip().upper() in ["", "NULL", "NONE", "GENERAL"]:
            # Fallback cascade
            final_dept = ""
            
            # 1. Master Map
            if name in master_dept and master_dept[name]:
                final_dept = master_dept[name]
            # 2. Barcode Map
            elif p.get('barcode', '') in bc_dept and bc_dept[p.get('barcode', '')]:
                final_dept = bc_dept[p.get('barcode', '')]
            # 3. DB Department
            elif p.get('department', ''):
                final_dept = p.get('department', '')
            # 4. DB Category
            elif p.get('category', ''):
                final_dept = p.get('category', '')
                
            if final_dept and str(final_dept).strip().upper() not in ["", "NULL", "NONE"]:
                dept_map[name] = str(final_dept).strip()
                added_dept += 1

print("\n4. Scan complete!")
print(f"   Added {added_supp} new supplier mappings.")
print(f"   Added {added_dept} new department mappings.")

print("\n5. Saving updated maps...")
backup_file(supp_map_file)
with open(supp_map_file, 'w', encoding='utf-8') as f:
    json.dump(supp_map, f, indent=4)
    
backup_file(dept_map_file)
with open(dept_map_file, 'w', encoding='utf-8') as f:
    json.dump(dept_map, f, indent=4)

print("\nSync completed successfully.")
