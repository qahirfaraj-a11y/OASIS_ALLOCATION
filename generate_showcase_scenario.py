"""
generate_showcase_scenario.py
============================
Creates a 'Perfect Showcase' database state for demos.
Narrative: Rhapta Road is facing supply chain gaps in Staples; Oasis identifies and fixes them.
"""

import os
import sys
import sqlite3
import random
from datetime import datetime, timedelta

# Add project root
sys.path.insert(0, os.getcwd())

from oasis.logic.mock_pos_erp import MockPosErpBuilder, DEFAULT_DB_PATH

def create_showcase():
    print("Initializing Perfect Showcase Scenario...")
    
    # 1. Check if DB exists and is accessible
    env_db_path = os.getenv("OASIS_DB_PATH")
    db_path = os.path.abspath(env_db_path if env_db_path else DEFAULT_DB_PATH)
    builder = MockPosErpBuilder(db_path=db_path, fast_mode=True)
    
    db_exists = os.path.exists(db_path)
    needs_full_rebuild = True
    
    if db_exists:
        try:
            conn = sqlite3.connect(db_path)
            # Check if ITEM_MST is already populated to skip rebuild
            res = conn.execute("SELECT COUNT(*) FROM ITEM_MST").fetchone()
            if res and res[0] > 100:
                print(f"[*] Baseline SKU Universe detected ({res[0]} items). Skipping rebuild.")
                needs_full_rebuild = False
            conn.close()
        except Exception:
            needs_full_rebuild = True

    if needs_full_rebuild:
        print("[!] Building fresh baseline database (Full Ecosystem)...")
        builder.build(reset=True)
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # 2. Identify Rhapta (ORG001/002)
        cursor.execute("SELECT ORG_CD FROM ORGANIZATION_MST WHERE ORG_NAME LIKE '%Rhapta%' OR ORG_NAME LIKE '%Flagship%' LIMIT 1")
        res = cursor.fetchone()
        org_cd = res[0] if res else 'ORG002'
        
        print(f"[*] Targeting Store: {org_cd}")

        # 3. Identify Staples to 'break' (Fuzzy matching for demo consistency)
        search_terms = ['MILK', 'SUGAR', 'BREAD', 'FLOUR', 'MAIZE']
        staple_ids = []
        for term in search_terms:
            cursor.execute("SELECT ITM_CD, ITM_LONG_NAME FROM ITEM_MST WHERE ITM_LONG_NAME LIKE ? LIMIT 1", (f'%{term}%',))
            row = cursor.fetchone()
            if row: staple_ids.append(row)
            
        if staple_ids:
            print(f"[*] Injecting 'Stockout Crisis' into {len(staple_ids)} refined staples at {org_cd}...")
            for itm_cd, name in staple_ids:
                cursor.execute("UPDATE STOCK_MASTER SET SM_QTY = 0 WHERE SM_ORG_CD = ? AND SM_ITM_CD = ?", (org_cd, itm_cd))
        else:
            print("[!] Warning: Fuzzy matching failed to find baseline staples.")
            
        # 4. Inject 'Capital Inefficiency' (Overstock in non-priority)
        cursor.execute("""
            SELECT ITM_CD FROM ITEM_MST 
            WHERE DEPARTMENT = 'GENERAL MERCHANDISE' 
               OR ITM_LONG_NAME LIKE '%WINE%' 
               OR ITM_LONG_NAME LIKE '%SPIRIT%' 
               OR ITM_LONG_NAME LIKE '%TOY%' 
               OR ITM_LONG_NAME LIKE '%GIFT%' 
               OR ITM_LONG_NAME LIKE '%CANDY%' 
               OR ITM_LONG_NAME LIKE '%HOUSEHOLD%'
            LIMIT 10
        """)
        fillers = cursor.fetchall()
        
        if not fillers:
            # Fallback: Just pick any 10 items that aren't the staples we just broke
            staple_item_cds = [s[0] for s in staple_ids]
            placeholders = ','.join(['?'] * len(staple_item_cds)) if staple_item_cds else "'NONE'"
            cursor.execute(f"SELECT ITM_CD FROM ITEM_MST WHERE ITM_CD NOT IN ({placeholders}) LIMIT 10", staple_item_cds)
            fillers = cursor.fetchall()

        if fillers:
            print(f"Identifying {len(fillers)} 'Idle Capital' items for recovery...")
            for (itm_cd,) in fillers:
                cursor.execute("UPDATE STOCK_MASTER SET SM_QTY = 500 WHERE SM_ORG_CD = ? AND SM_ITM_CD = ?", (org_cd, itm_cd))

        # 5. Populate Audit Log with a 'Story'
        print("Writing the Demo Narrative to Audit Logs...")
        # Clear existing demo logs to avoid clutter if re-running
        cursor.execute("DELETE FROM OASIS_AUDIT_LOG WHERE DETAILS LIKE '%Oasis Precision PO%' OR DETAILS LIKE '%Stockout detected%'")
        
        narrative_logs = [
            ('ops_admin', 'LOGIN', 'SESSION', 'user_001', org_cd, 'Standard Morning Check', datetime.now() - timedelta(minutes=60)),
            ('ops_admin', 'DB_SYNC', 'SYSTEM', 'sync_001', org_cd, 'Synced Store POS with Oasis Intelligence', datetime.now() - timedelta(minutes=55)),
            ('ops_admin', 'ALERT', 'NOTIF', 'alt_001', org_cd, 'CRITICAL: Stockout detected in Staple Food (Milk/Sugar)', datetime.now() - timedelta(minutes=50)),
            ('ops_admin', 'PO_GENERATED', 'PO', 'po_showcase_01', org_cd, 'Oasis Precision PO: Refilled staples using reclaimed capital from GM.', datetime.now() - timedelta(minutes=10)),
        ]
        
        for user, action, ent_type, ent_id, org, details, dt in narrative_logs:
            cursor.execute("""
                INSERT INTO OASIS_AUDIT_LOG (USERNAME, ACTION, ENTITY_TYPE, ENTITY_ID, ORG_CD, DETAILS, CREATED_DT)
                VALUES (?,?,?,?,?,?,?)
            """, (user, action, ent_type, ent_id, org, details, dt.strftime("%Y-%m-%d %H:%M:%S")))

        # 6. Inject ROI win
        cursor.execute("""
            INSERT OR REPLACE INTO OASIS_SYSTEM_CONFIG (CONFIG_KEY, CONFIG_VALUE, CONFIG_GROUP, DESCRIPTION)
            VALUES ('SHOWCASE_ROI_SAVINGS', '42,500', 'demo', 'Projected daily savings from optimized replenishment')
        """)

        conn.commit()
        print("Showcase Scenario Ready! Launch 'run_showcase.bat' to view.")
        
    except Exception as e:
        print(f"Failed to create showcase: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    create_showcase()
