"""
Test to ensure Data Integrity between DB initialization paths.
This prevents regression of D-1, D-2, and D-4, where mock_pos_erp.py
and db_connector.py (ensure_oasis_tables) diverged on the definitions
of OASIS_USERS, OASIS_SESSIONS, and INTEGRATION_PURCHASE_ORDERS.
"""

import sqlite3
import pytest
from oasis.logic.mock_pos_erp import SCHEMA_SQL
from oasis.logic.db_connector import ensure_oasis_tables

def get_table_schema(db_path, table_name):
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    cursor = conn.execute(f"PRAGMA table_info({table_name})")
    columns = [dict(row) for row in cursor.fetchall()]
    conn.close()
    
    return {col['name']: col['type'] for col in columns}

def test_schema_integrity(tmp_path):
    # 1. Init via SCHEMA_SQL (mock_pos_erp path)
    db1 = tmp_path / "schema_sql.db"
    conn1 = sqlite3.connect(str(db1))
    conn1.executescript(SCHEMA_SQL)
    conn1.close()
    
    # 2. Init via ensure_oasis_tables (UniversalConnector runtime path)
    db2 = tmp_path / "ensure_tables.db"
    ensure_oasis_tables(str(db2))
    
    tables_to_check = [
        "OASIS_USERS",
        "OASIS_SESSIONS",
        "INTEGRATION_PURCHASE_ORDERS"
    ]
    
    for table in tables_to_check:
        schema1 = get_table_schema(str(db1), table)
        schema2 = get_table_schema(str(db2), table)
        
        # Ensure the table actually exists
        assert schema1, f"{table} missing from SCHEMA_SQL"
        assert schema2, f"{table} missing from ensure_oasis_tables"
        
        # Compare columns and types
        missing_in_db2 = set(schema1.keys()) - set(schema2.keys())
        missing_in_db1 = set(schema2.keys()) - set(schema1.keys())
        
        assert not missing_in_db2, f"{table} in ensure_oasis_tables missing columns: {missing_in_db2}"
        assert not missing_in_db1, f"{table} in SCHEMA_SQL missing columns: {missing_in_db1}"
        
        for col in schema1:
            # We don't strictly assert type case exact match if SQLite treats them the same,
            # but usually they match string-wise.
            type1 = schema1[col].upper()
            type2 = schema2[col].upper()
            
            # INTEGER vs INT etc. For simplicity, just check they are generally compatible
            if type1 != type2:
                # Handle SQLite type aliases if necessary, but ideally they match perfectly.
                assert type1 == type2, f"Column {col} type mismatch in {table}: {type1} vs {type2}"

