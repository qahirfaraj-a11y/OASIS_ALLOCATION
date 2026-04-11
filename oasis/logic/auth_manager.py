"""
OASIS Authentication & Role Manager
====================================
Handles user authentication, session management, and role-based
permission checks for the Retail Manager App.

Roles:
    - branch_manager:   Single-store view, no transfers, no config
    - regional_manager: Multi-store view, transfers, PO approval
    - ops_admin:        Full access including settings and audit log
"""

import hashlib
import secrets
import sqlite3
import json
import logging
from datetime import datetime
from typing import Optional, Dict, Any

logger = logging.getLogger("OasisAuth")

# ---------------------------------------------------------------------------
# Password Hashing
# ---------------------------------------------------------------------------

def hash_password(password: str, salt: str = None) -> str:
    """Hash a password with SHA-256 + salt. Returns 'salt:hash' string."""
    if salt is None:
        salt = secrets.token_hex(16)
    pw_hash = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
    return f"{salt}:{pw_hash}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a password against a stored 'salt:hash' string."""
    if ":" not in stored_hash:
        return False
    salt, _ = stored_hash.split(":", 1)
    return hash_password(password, salt) == stored_hash


# ---------------------------------------------------------------------------
# Role Permissions
# ---------------------------------------------------------------------------

ROLE_PERMISSIONS = {
    "branch_manager": {
        "tabs": {
            "live_sales": True,
            "transfer_intelligence": False,
            "stock_review": True,
            "smart_ordering": True,
            "oasis_processor": True,
            "allocation_engine": True,
            "simulation_validation": False,
            "analytics": False,
            "settings": False,
        },
        "can_view_all_stores": False,
        "can_approve_po": False,
        "can_execute_transfers": False,
        "can_view_audit_log": False,
        "can_edit_config": False,
    },
    "regional_manager": {
        "tabs": {
            "live_sales": True,
            "transfer_intelligence": True,
            "stock_review": True,
            "smart_ordering": True,
            "oasis_processor": True,
            "allocation_engine": True,
            "simulation_validation": True,
            "analytics": True,
            "settings": False,
        },
        "can_view_all_stores": True,
        "can_approve_po": True,
        "can_execute_transfers": True,
        "can_view_audit_log": True,
        "can_edit_config": False,
    },
    "ops_admin": {
        "tabs": {
            "live_sales": True,
            "transfer_intelligence": True,
            "stock_review": True,
            "smart_ordering": True,
            "oasis_processor": True,
            "allocation_engine": True,
            "simulation_validation": True,
            "analytics": True,
            "settings": True,
        },
        "can_view_all_stores": True,
        "can_approve_po": True,
        "can_execute_transfers": True,
        "can_view_audit_log": True,
        "can_edit_config": True,
    },
}



def get_user_permissions(role: str) -> Dict[str, Any]:
    """Get permissions dict for a given role."""
    return ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS["branch_manager"])


# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def authenticate(username: str, password: str, db_path: str) -> Optional[Dict[str, Any]]:
    """
    Authenticate a user against the OASIS_USERS table.
    
    Returns:
        User dict with keys: user_id, username, display_name, role, 
        assigned_org, email, permissions
        
        None if authentication fails.
    """
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        cursor = conn.execute(
            "SELECT * FROM OASIS_USERS WHERE USERNAME = ? AND ACTIVE_FLAG = 'Y'",
            (username,)
        )
        row = cursor.fetchone()
        conn.close()

        if row is None:
            logger.warning(f"Login failed: user '{username}' not found")
            return None

        if not verify_password(password, row["PASSWORD_HASH"]):
            logger.warning(f"Login failed: invalid password for '{username}'")
            return None

        # Update last login
        record_login(username, db_path)

        user = {
            "user_id": row["USER_ID"],
            "username": row["USERNAME"],
            "display_name": row["DISPLAY_NAME"],
            "role": row["ROLE"],
            "assigned_org": row["ASSIGNED_ORG"],
            "email": row["EMAIL"],
            "permissions": get_user_permissions(row["ROLE"]),
        }
        logger.info(f"Login successful: {username} ({row['ROLE']})")
        return user

    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return None


def record_login(username: str, db_path: str):
    """Update LAST_LOGIN_DT for a user."""
    try:
        conn = sqlite3.connect(db_path)
        conn.execute(
            "UPDATE OASIS_USERS SET LAST_LOGIN_DT = ? WHERE USERNAME = ?",
            (datetime.now().isoformat(), username)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to record login: {e}")


def get_all_users(db_path: str) -> list:
    """Get all users (for admin view). Excludes password hashes."""
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """SELECT USER_ID, USERNAME, DISPLAY_NAME, ROLE, ASSIGNED_ORG, 
                      EMAIL, ACTIVE_FLAG, CREATED_DT, LAST_LOGIN_DT 
               FROM OASIS_USERS ORDER BY ROLE, USERNAME"""
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to fetch users: {e}")
        return []


# ---------------------------------------------------------------------------
# Default Users (for seeding)
# ---------------------------------------------------------------------------

DEFAULT_USERS = [
    {
        "username": "ops_admin",
        "password": "oasis2026",
        "display_name": "Operations Admin",
        "role": "ops_admin",
        "assigned_org": None,
        "email": "ops@chandarana.co.ke",
    },
    {
        "username": "regional_mgr",
        "password": "oasis2026",
        "display_name": "Regional Manager",
        "role": "regional_manager",
        "assigned_org": None,
        "email": "regional@chandarana.co.ke",
    },
    {
        "username": "branch_mgr",
        "password": "oasis2026",
        "display_name": "Rhapta Road Manager",
        "role": "branch_manager",
        "assigned_org": "ORG001",
        "email": "rhapta.mgr@chandarana.co.ke",
    },
    {
        "username": "branch_mgr2",
        "password": "oasis2026",
        "display_name": "Lavington Manager",
        "role": "branch_manager",
        "assigned_org": "ORG002",
        "email": "lavington.mgr@chandarana.co.ke",
    },
    {
        "username": "demo_user",
        "password": "demo",
        "display_name": "Demo User",
        "role": "branch_manager",
        "assigned_org": "ORG001",
        "email": "demo@chandarana.co.ke",
    },
]


def seed_users(db_path: str):
    """Seed default users into OASIS_USERS table."""
    conn = sqlite3.connect(db_path)
    now = datetime.now().isoformat()
    
    for user in DEFAULT_USERS:
        pw_hash = hash_password(user["password"])
        try:
            conn.execute(
                """INSERT OR IGNORE INTO OASIS_USERS 
                   (USERNAME, PASSWORD_HASH, DISPLAY_NAME, ROLE, ASSIGNED_ORG, EMAIL, CREATED_DT)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user["username"], pw_hash, user["display_name"], 
                 user["role"], user["assigned_org"], user["email"], now)
            )
        except Exception as e:
            logger.error(f"Failed to seed user {user['username']}: {e}")
    
    conn.commit()
    conn.close()
    logger.info(f"Seeded {len(DEFAULT_USERS)} default users")
