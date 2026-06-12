"""
OASIS Audit Logger
==================
Records all significant user actions (PO generation, transfer execution,
config changes, login/logout) with full traceability.

Every audit entry includes: who, what, when, where (store), and details.
"""

import json
import sqlite3
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List

logger = logging.getLogger("OasisAudit")

# ---------------------------------------------------------------------------
# Action Constants
# ---------------------------------------------------------------------------

ACTION_LOGIN = "LOGIN"
ACTION_LOGOUT = "LOGOUT"
ACTION_PO_GENERATED = "PO_GENERATED"
ACTION_PO_APPROVED = "PO_APPROVED"
ACTION_PO_EXPORTED = "PO_EXPORTED"
ACTION_TRANSFER_EXECUTED = "TRANSFER_EXECUTED"
ACTION_FILE_PROCESSED = "FILE_PROCESSED"
ACTION_CONFIG_CHANGED = "CONFIG_CHANGED"

ENTITY_PO = "PO"
ENTITY_TRANSFER = "TRANSFER"
ENTITY_FILE = "FILE"
ENTITY_CONFIG = "CONFIG"
ENTITY_SESSION = "SESSION"


# ---------------------------------------------------------------------------
# Core Logging
# ---------------------------------------------------------------------------

def log_action(
    db_path: str = None,
    username: str = "",
    action: str = "",
    entity_type: str = None,
    entity_id: str = None,
    org_cd: str = None,
    details: Dict[str, Any] = None,
):
    """
    Insert an audit log entry.

    Args:
        db_path:     Path to the database (None = use OASIS_DB_URL)
        username:    Who performed the action
        action:      Action constant (e.g. PO_GENERATED)
        entity_type: Category (PO, TRANSFER, FILE, CONFIG, SESSION)
        entity_id:   Specific entity identifier
        org_cd:      Store code (if applicable)
        details:     Dict with additional context (serialized to JSON)
    """
    try:
        if db_path:
            conn = sqlite3.connect(db_path)
        else:
            from . import db as oasis_db
            conn = oasis_db.get_raw_connection()
        details_json = json.dumps(details) if details else None
        conn.execute(
            """INSERT INTO OASIS_AUDIT_LOG 
               (USERNAME, ACTION, ENTITY_TYPE, ENTITY_ID, ORG_CD, DETAILS, CREATED_DT)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (username, action, entity_type, entity_id, org_cd,
             details_json, datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        logger.info(f"AUDIT: {username} | {action} | {entity_type}:{entity_id} | {org_cd}")
    except Exception as e:
        logger.error(f"Failed to write audit log: {e}")


# ---------------------------------------------------------------------------
# Query Functions
# ---------------------------------------------------------------------------

def get_recent_logs(
    db_path: str = None,
    limit: int = 50,
    org_cd: str = None,
    username: str = None,
    action: str = None,
) -> pd.DataFrame:
    """
    Retrieve recent audit log entries as a DataFrame.

    Args:
        db_path:  Path to the database (None = use OASIS_DB_URL)
        limit:    Max rows to return
        org_cd:   Filter by store (optional)
        username: Filter by user (optional)
        action:   Filter by action type (optional)
    """
    try:
        if db_path:
            conn = sqlite3.connect(db_path)
        else:
            from . import db as oasis_db
            conn = oasis_db.get_raw_connection()
        query = "SELECT * FROM OASIS_AUDIT_LOG WHERE 1=1"
        params: List[Any] = []

        if org_cd:
            query += " AND ORG_CD = ?"
            params.append(org_cd)
        if username:
            query += " AND USERNAME = ?"
            params.append(username)
        if action:
            query += " AND ACTION = ?"
            params.append(action)

        query += " ORDER BY CREATED_DT DESC LIMIT ?"
        params.append(limit)

        df = pd.read_sql_query(query, conn, params=params)
        conn.close()
        return df
    except Exception as e:
        logger.error(f"Failed to read audit logs: {e}")
        return pd.DataFrame()


def get_action_summary(db_path: str = None, days: int = 7) -> Dict[str, int]:
    """Get a count of actions by type for the last N days."""
    try:
        if db_path:
            conn = sqlite3.connect(db_path)
        else:
            from . import db as oasis_db
            conn = oasis_db.get_raw_connection()
        cursor = conn.execute(
            """SELECT ACTION, COUNT(*) as cnt 
               FROM OASIS_AUDIT_LOG 
               WHERE CREATED_DT >= datetime('now', ?)
               GROUP BY ACTION ORDER BY cnt DESC""",
            (f"-{days} days",)
        )
        result = {row[0]: row[1] for row in cursor.fetchall()}
        conn.close()
        return result
    except Exception as e:
        logger.error(f"Failed to get action summary: {e}")
        return {}
