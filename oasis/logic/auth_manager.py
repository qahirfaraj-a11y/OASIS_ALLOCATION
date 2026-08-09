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
import logging
import bcrypt
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

logger = logging.getLogger("OasisAuth")

def get_auth_db_conn(db_path: str = None) -> sqlite3.Connection:
    """Return a database connection for authentication storage."""
    if db_path:
        conn = sqlite3.connect(db_path, timeout=30.0)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn
    from . import db as oasis_db
    return oasis_db.get_raw_connection()

# ---------------------------------------------------------------------------
# Password Hashing
# ---------------------------------------------------------------------------

def hash_password(password: str) -> str:
    """Hash a password using bcrypt."""
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')


def verify_password(password: str, stored_hash: str, username: str = None, db_path: str = None) -> bool:
    """
    Verify a password against a stored hash.
    Gracefully migrates legacy SHA-256 hashes to bcrypt upon successful login.
    """
    # Check if this is a legacy SHA-256 hash (format: salt:hash)
    if not stored_hash.startswith('$2b$'):
        if ":" not in stored_hash:
            return False
            
        salt, legacy_hash = stored_hash.split(":", 1)
        test_hash = hashlib.sha256(f"{salt}{password}".encode()).hexdigest()
        
        if test_hash == legacy_hash:
            # Login successful with old hash! Gracefully migrate to bcrypt.
            if username and db_path:
                try:
                    new_hash = hash_password(password)
                    conn = get_auth_db_conn(db_path)
                    conn.execute(
                        "UPDATE OASIS_USERS SET PASSWORD_HASH = ? WHERE USERNAME = ?",
                        (new_hash, username)
                    )
                    conn.commit()
                    conn.close()
                    logger.info(f"Successfully migrated password hash to bcrypt for user: {username}")
                except Exception as e:
                    logger.error(f"Failed to migrate password hash for {username}: {e}")
            return True
        return False
        
    # Standard bcrypt verification
    try:
        return bcrypt.checkpw(password.encode('utf-8'), stored_hash.encode('utf-8'))
    except ValueError:
        return False


# ---------------------------------------------------------------------------
# Role Permissions
# ---------------------------------------------------------------------------

ROLE_PERMISSIONS = {
    "branch_manager": {
        "tabs": {
                        # Absent from this table until 2026-08-08, so user_perms["tabs"].get()
            # returned None for every role and the console's Executive ROI
            # tab was unreachable outside showcase mode.
            "executive_roi": False,
            "supplier_intelligence": False,
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
                        "executive_roi": True,
            "supplier_intelligence": True,
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
                        "executive_roi": True,
            "supplier_intelligence": True,
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
    # ── Journey role model (Customer Journey doc) — added alongside the
    # legacy roles above, which are retained for backward compatibility. ──
    "ilink_operator": {  # internal implementation/admin — full access
        "tabs": {
            "executive_roi": True, "supplier_intelligence": True,
            "live_sales": True, "transfer_intelligence": True, "stock_review": True,
            "smart_ordering": True, "oasis_processor": True, "allocation_engine": True,
            "simulation_validation": True, "analytics": True, "settings": True,
        },
        "can_view_all_stores": True,
        "can_approve_po": True,
        "can_execute_transfers": True,
        "can_view_audit_log": True,
        "can_edit_config": True,
    },
    "executive": {  # oversight: view + approve gates, no engine config
        "tabs": {
            "executive_roi": True, "supplier_intelligence": False,
            "live_sales": True, "transfer_intelligence": True, "stock_review": True,
            "smart_ordering": False, "oasis_processor": False, "allocation_engine": False,
            "simulation_validation": True, "analytics": True, "settings": False,
        },
        "can_view_all_stores": True,
        "can_approve_po": False,
        "can_execute_transfers": False,
        "can_view_audit_log": True,
        "can_edit_config": False,
    },
    "finance": {  # capital recovery + analytics, read-mostly
        "tabs": {
            "executive_roi": True, "supplier_intelligence": False,
            "live_sales": False, "transfer_intelligence": False, "stock_review": False,
            "smart_ordering": False, "oasis_processor": False, "allocation_engine": False,
            "simulation_validation": False, "analytics": True, "settings": False,
        },
        "can_view_all_stores": True,
        "can_approve_po": False,
        "can_execute_transfers": False,
        "can_view_audit_log": False,
        "can_edit_config": False,
    },
    "approval_manager": {  # the buyer → daily PO approval + operations
        "tabs": {
            "executive_roi": True, "supplier_intelligence": True,
            "live_sales": True, "transfer_intelligence": True, "stock_review": True,
            "smart_ordering": True, "oasis_processor": True, "allocation_engine": True,
            "simulation_validation": False, "analytics": True, "settings": False,
        },
        "can_view_all_stores": True,
        "can_approve_po": True,
        "can_execute_transfers": True,
        "can_view_audit_log": False,
        "can_edit_config": False,
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
        conn = get_auth_db_conn(db_path)
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

        row_dict = dict(row)

        # Check rate limits
        lockout_until = row_dict.get("LOCKOUT_UNTIL")
        if lockout_until:
            try:
                lockout_dt = datetime.fromisoformat(lockout_until)
                if datetime.now() < lockout_dt:
                    logger.warning(f"Login failed: user '{username}' is locked out until {lockout_dt}")
                    return None
            except Exception:
                pass

        if not verify_password(password, row_dict["PASSWORD_HASH"], username, db_path):
            record_failed_login(username, row_dict.get("FAILED_ATTEMPTS", 0), db_path)
            logger.warning(f"Login failed: invalid password for '{username}'")
            return None

        # Update last login and clear failures
        record_login(username, db_path)
        
        session_token = create_session(username, db_path)

        user = {
            "user_id": row_dict["USER_ID"],
            "username": row_dict["USERNAME"],
            "display_name": row_dict["DISPLAY_NAME"],
            "role": row_dict["ROLE"],
            "assigned_org": row_dict["ASSIGNED_ORG"],
            "email": row_dict["EMAIL"],
            "permissions": get_user_permissions(row_dict["ROLE"]),
            "session_token": session_token,
            "tenant_id": row_dict.get("TENANT_ID") or "default_tenant",
        }
        logger.info(f"Login successful: {username} ({row_dict['ROLE']})")
        return user

    except Exception as e:
        logger.error(f"Authentication error: {e}")
        return None


def record_login(username: str, db_path: str):
    """Update LAST_LOGIN_DT and clear failed attempts."""
    try:
        conn = get_auth_db_conn(db_path)
        conn.execute(
            """UPDATE OASIS_USERS 
               SET LAST_LOGIN_DT = ?, FAILED_ATTEMPTS = 0, LOCKOUT_UNTIL = NULL 
               WHERE USERNAME = ?""",
            (datetime.now().isoformat(), username)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Failed to record login: {e}")

def record_failed_login(username: str, current_attempts: int, db_path: str):
    """Log failed attempt and apply lockout if >= 5"""
    try:
        new_attempts = (current_attempts or 0) + 1
        lockout_until = None
        if new_attempts >= 5:
            lockout_until = (datetime.now() + timedelta(minutes=5)).isoformat()
            logger.warning(f"User {username} locked out for 5 minutes due to multiple failed attempts.")
            
        conn = get_auth_db_conn(db_path)
        conn.execute(
            "UPDATE OASIS_USERS SET FAILED_ATTEMPTS = ?, LOCKOUT_UNTIL = ? WHERE USERNAME = ?",
            (new_attempts, lockout_until, username)
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"Error recording failed login for {username}: {e}")

def create_session(username: str, db_path: str) -> str:
    """Create a new session token valid for 24 hours."""
    session_id = str(uuid.uuid4())
    created = datetime.now()
    expires = created + timedelta(hours=24)
    
    try:
        conn = get_auth_db_conn(db_path)
        conn.execute(
            """INSERT INTO OASIS_SESSIONS (SESSION_ID, USERNAME, CREATED_DT, EXPIRES_DT, IS_REVOKED)
               VALUES (?, ?, ?, ?, 0)""",
            (session_id, username, created.isoformat(), expires.isoformat())
        )
        conn.commit()
        conn.close()
        return session_id
    except Exception as e:
        logger.error(f"Error creating session for {username}: {e}")
        return ""
        
def validate_session(session_id: str, db_path: str) -> Optional[Dict[str, Any]]:
    """Validate a session token and return the associated user profile."""
    if not session_id:
        return None
        
    try:
        conn = get_auth_db_conn(db_path)
        conn.row_factory = sqlite3.Row
        
        # Join SESSIONS -> USERS to ensure token valid AND user active
        cursor = conn.execute(
            """SELECT u.*, s.EXPIRES_DT 
               FROM OASIS_SESSIONS s
               JOIN OASIS_USERS u ON s.USERNAME = u.USERNAME
               WHERE s.SESSION_ID = ? AND s.IS_REVOKED = 0 AND u.ACTIVE_FLAG = 'Y'""",
            (session_id,)
        )
        row = cursor.fetchone()
        conn.close()
        
        if not row:
            return None

        row_dict = dict(row)

        expires = datetime.fromisoformat(row_dict["EXPIRES_DT"])
        if datetime.now() > expires:
            return None

        return {
            "user_id": row_dict["USER_ID"],
            "username": row_dict["USERNAME"],
            "display_name": row_dict["DISPLAY_NAME"],
            "role": row_dict["ROLE"],
            "assigned_org": row_dict["ASSIGNED_ORG"],
            "email": row_dict["EMAIL"],
            "permissions": get_user_permissions(row_dict["ROLE"]),
            "tenant_id": row_dict.get("TENANT_ID") or "default_tenant",
        }
    except Exception as e:
        logger.error(f"Error validating session {session_id}: {e}")
        return None


def get_all_users(db_path: str) -> list:
    """Get all users (for admin view). Excludes password hashes."""
    try:
        conn = get_auth_db_conn(db_path)
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
# Passwords are NOT stored in source. Seed passwords come from an explicit
# `seed_users(db, password=...)` call (how first-run setup applies the
# operator's own choice), the OASIS_SEED_PASSWORD env var, or per-user
# overrides like OASIS_SEED_PASSWORD_OPS_ADMIN. If none is given, a random
# password is generated per run and printed once to the log — operators must
# then reset passwords through the admin UI or first-run setup.
#
# The one known password in the product is onboarding.DEMO_SEED_PASSWORD, and
# it is passed explicitly for SAMPLE data only. See tests/test_seed_credentials.py.

DEFAULT_USERS = [
    {
        "username": "ops_admin",
        "display_name": "Operations Admin",
        "role": "ops_admin",
        "assigned_org": None,
        "email": "ops@example.com",
    },
    {
        "username": "regional_mgr",
        "display_name": "Regional Manager",
        "role": "regional_manager",
        "assigned_org": None,
        "email": "regional@example.com",
    },
    {
        "username": "branch_mgr",
        "display_name": "Branch 1 Manager",
        "role": "branch_manager",
        "assigned_org": "ORG001",
        "email": "branch1.mgr@example.com",
    },
    {
        "username": "branch_mgr2",
        "display_name": "Branch 2 Manager",
        "role": "branch_manager",
        "assigned_org": "ORG002",
        "email": "branch2.mgr@example.com",
    },
    {
        "username": "demo_user",
        "display_name": "Demo User",
        "role": "branch_manager",
        "assigned_org": "ORG001",
        "email": "demo@example.com",
    },
    # ── Journey-role seed accounts (added; existing accounts unaffected) ──
    {
        "username": "ilink_operator",
        "display_name": "iLink Operator",
        "role": "ilink_operator",
        "assigned_org": None,
        "email": "ops@ilink.co.ke",
    },
    {
        "username": "exec_user",
        "display_name": "Executive",
        "role": "executive",
        "assigned_org": None,
        "email": "exec@example.com",
    },
    {
        "username": "finance_user",
        "display_name": "Finance",
        "role": "finance",
        "assigned_org": None,
        "email": "finance@example.com",
    },
    {
        "username": "approval_mgr",
        "display_name": "Approval Manager",
        "role": "approval_manager",
        "assigned_org": None,
        "email": "approvals@example.com",
    },
]


def _resolve_seed_password(username: str) -> str:
    """Resolve the seed password for a user from the environment.

    NEVER returns a value baked into source. Order: a per-user env override,
    then the shared OASIS_SEED_PASSWORD, then a random one-time password that
    is logged once so the operator can sign in and rotate it.

    A literal ``"oasis2026"`` default was reintroduced here at one point, which
    silently reverted the credential removal in cdcf0b7 and put a
    publicly-known password on every real client install. Callers that need a
    deterministic password must pass one explicitly to ``seed_users`` — it must
    never be the fallback.
    """
    import os
    per_user = os.getenv(f"OASIS_SEED_PASSWORD_{username.upper()}")
    if per_user:
        return per_user
    shared = os.getenv("OASIS_SEED_PASSWORD")
    if shared:
        return shared
    generated = secrets.token_urlsafe(12)
    # Printed once so the operator can log in and rotate it; never persisted.
    logger.warning(
        f"OASIS_SEED_PASSWORD not set — generated one-time password for "
        f"'{username}': {generated}  (set OASIS_SEED_PASSWORD to control seeding)"
    )
    return generated


def has_accounts(db_path: str) -> bool:
    """True when the store has at least one user to authenticate against."""
    try:
        return bool(get_all_users(db_path))
    except Exception:
        return False


def set_password(db_path: str, username: str, new_password: str) -> None:
    """Set one account's password. Raises ValueError if too short/unknown user."""
    if len(new_password or "") < 8:
        raise ValueError("Password must be at least 8 characters")
    conn = get_auth_db_conn(db_path)
    try:
        cur = conn.execute(
            "UPDATE OASIS_USERS SET PASSWORD_HASH = ? WHERE USERNAME = ?",
            (hash_password(new_password), username))
        if not cur.rowcount:
            raise ValueError(f"no such user: {username}")
        conn.commit()
    finally:
        conn.close()


def seed_users(db_path: str, password: Optional[str] = None):
    """Seed default users into OASIS_USERS table.

    ``password`` sets every seeded account explicitly — this is how a first-run
    setup flow hands the operator's chosen admin password to the seeder instead
    of relying on an env var or a hardcoded default.
    """
    conn = get_auth_db_conn(db_path)
    now = datetime.now().isoformat()

    # Which accounts already exist. This MUST be checked before resolving a
    # password, not left to INSERT OR IGNORE: _resolve_seed_password generates
    # a fresh random password and LOGS it as the way in. When the row already
    # existed the insert was ignored, the old hash stayed, and the operator was
    # handed a password that had never been stored — every boot printing a new
    # plausible-looking credential that could not work. Locked people out of
    # their own store, and the "Seeded 9 default users" line said it had worked.
    try:
        existing = {r[0] for r in conn.execute(
            "SELECT USERNAME FROM OASIS_USERS").fetchall()}
    except Exception:
        existing = set()

    seeded = 0
    for user in DEFAULT_USERS:
        if user["username"] in existing:
            continue                    # keep the password this account has
        pw_hash = hash_password(password or _resolve_seed_password(user["username"]))
        try:
            conn.execute(
                """INSERT OR IGNORE INTO OASIS_USERS
                   (USERNAME, PASSWORD_HASH, DISPLAY_NAME, ROLE, ASSIGNED_ORG, EMAIL, CREATED_DT)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (user["username"], pw_hash, user["display_name"],
                 user["role"], user["assigned_org"], user["email"], now)
            )
            seeded += 1
        except Exception as e:
            logger.error(f"Failed to seed user {user['username']}: {e}")

    conn.commit()
    conn.close()
    if seeded:
        logger.info(f"Seeded {seeded} default users")
    else:
        logger.info(f"All {len(DEFAULT_USERS)} default users already exist — "
                    "no passwords generated or changed "
                    "(use --mode set-password to rotate one)")
    return seeded
