"""
Phase 1 Tests: Authentication, Audit, and Configuration
========================================================
Tests for the new production foundation modules.

Run: python -m pytest tests/test_auth_and_audit.py -v --tb=short
"""

import os
import sys
import sqlite3
import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.mock_pos_erp import MockPosErpBuilder, summarize_mock_db
from oasis.logic.auth_manager import (
    hash_password, verify_password, authenticate,
    get_user_permissions, get_all_users, seed_users
)
from oasis.logic.audit_logger import (
    log_action, get_recent_logs, get_action_summary,
    ACTION_LOGIN, ACTION_PO_GENERATED, ENTITY_SESSION, ENTITY_PO
)
from oasis.logic.db_connector import (
    load_system_config, load_system_config_full, save_system_config,
    ensure_oasis_tables
)

# Shared test DB path
TEST_DB_PATH = os.path.join(os.path.dirname(__file__), "test_phase1.db")

# Seed password injected via env — credentials are no longer hardcoded in source.
TEST_SEED_PASSWORD = "test-seed-password"
os.environ.setdefault("OASIS_SEED_PASSWORD", TEST_SEED_PASSWORD)


@pytest.fixture(scope="module")
def phase1_db():
    """Create mock DB with OASIS tables for all Phase 1 tests."""
    builder = MockPosErpBuilder(db_path=TEST_DB_PATH, seed=42, fast_mode=True)
    builder.build(reset=True)
    # Apply the same startup migration production runs (sessions table,
    # lockout columns) — the raw builder schema predates v10.3 auth hardening.
    ensure_oasis_tables(TEST_DB_PATH)
    yield TEST_DB_PATH
    try:
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
    except PermissionError:
        pass


# =====================================================================
# Test 1: Schema Verification — New Tables Exist
# =====================================================================
class TestNewSchema:
    def test_oasis_users_table_exists(self, phase1_db):
        summary = summarize_mock_db(phase1_db)
        assert "OASIS_USERS" in summary, "Missing OASIS_USERS table"

    def test_oasis_audit_log_table_exists(self, phase1_db):
        summary = summarize_mock_db(phase1_db)
        assert "OASIS_AUDIT_LOG" in summary, "Missing OASIS_AUDIT_LOG table"

    def test_oasis_system_config_exists(self, phase1_db):
        summary = summarize_mock_db(phase1_db)
        assert "OASIS_SYSTEM_CONFIG" in summary, "Missing OASIS_SYSTEM_CONFIG table"

    def test_integration_transfer_orders_exists(self, phase1_db):
        summary = summarize_mock_db(phase1_db)
        assert "INTEGRATION_TRANSFER_ORDERS" in summary, "Missing INTEGRATION_TRANSFER_ORDERS table"

    def test_seed_users_count(self, phase1_db):
        summary = summarize_mock_db(phase1_db)
        assert summary["OASIS_USERS"] == 5, f"Expected 5 users, got {summary['OASIS_USERS']}"

    def test_seed_config_count(self, phase1_db):
        summary = summarize_mock_db(phase1_db)
        # 10 seeded by the builder + 2 added by the ensure_oasis_tables migration
        assert summary["OASIS_SYSTEM_CONFIG"] == 12, f"Expected 12 configs, got {summary['OASIS_SYSTEM_CONFIG']}"


# =====================================================================
# Test 2: Password Hashing
# =====================================================================
class TestPasswordHashing:
    def test_hash_and_verify(self):
        pw = "mysecretpassword"
        hashed = hash_password(pw)
        assert verify_password(pw, hashed)

    def test_wrong_password_fails(self):
        hashed = hash_password("correct")
        assert not verify_password("wrong", hashed)

    def test_hash_format(self):
        hashed = hash_password("test")
        assert hashed.startswith("$2b$"), "Hash should be bcrypt format"
        assert len(hashed) == 60  # standard bcrypt hash length

    def test_legacy_sha256_still_verifies(self):
        # Legacy 'salt:hash' entries must keep working (transparent migration path)
        import hashlib
        salt = "a" * 32
        legacy = f"{salt}:{hashlib.sha256(f'{salt}test'.encode()).hexdigest()}"
        assert verify_password("test", legacy)
        assert not verify_password("wrong", legacy)


# =====================================================================
# Test 3: Authentication
# =====================================================================
class TestAuthentication:
    def test_authenticate_valid_admin(self, phase1_db):
        user = authenticate("ops_admin", TEST_SEED_PASSWORD, phase1_db)
        assert user is not None
        assert user["username"] == "ops_admin"
        assert user["role"] == "ops_admin"
        assert user["display_name"] == "Operations Admin"

    def test_authenticate_valid_branch(self, phase1_db):
        user = authenticate("branch_mgr", TEST_SEED_PASSWORD, phase1_db)
        assert user is not None
        assert user["role"] == "branch_manager"
        assert user["assigned_org"] == "ORG001"

    def test_authenticate_invalid_password(self, phase1_db):
        user = authenticate("ops_admin", "wrongpassword", phase1_db)
        assert user is None

    def test_authenticate_nonexistent_user(self, phase1_db):
        user = authenticate("nobody", TEST_SEED_PASSWORD, phase1_db)
        assert user is None

    def test_authenticate_demo_user(self, phase1_db):
        user = authenticate("demo_user", TEST_SEED_PASSWORD, phase1_db)
        assert user is not None
        assert user["role"] == "branch_manager"


# =====================================================================
# Test 4: Role Permissions
# =====================================================================
class TestRolePermissions:
    def test_admin_has_all_tabs(self):
        perms = get_user_permissions("ops_admin")
        assert all(perms["tabs"].values()), "Admin should have all tabs"
        assert perms["can_view_all_stores"]
        assert perms["can_approve_po"]
        assert perms["can_edit_config"]

    def test_branch_no_transfers(self):
        perms = get_user_permissions("branch_manager")
        assert not perms["tabs"]["transfer_intelligence"]
        assert not perms["tabs"]["settings"]
        assert not perms["can_view_all_stores"]
        assert not perms["can_approve_po"]

    def test_regional_can_approve(self):
        perms = get_user_permissions("regional_manager")
        assert perms["tabs"]["transfer_intelligence"]
        assert perms["can_approve_po"]
        assert perms["can_view_all_stores"]
        assert not perms["can_edit_config"]


# =====================================================================
# Test 5: Audit Logging
# =====================================================================
class TestAuditLogging:
    def test_log_and_read(self, phase1_db):
        log_action(phase1_db, "test_user", ACTION_LOGIN, ENTITY_SESSION)
        logs = get_recent_logs(phase1_db, limit=5)
        assert len(logs) >= 1
        last = logs.iloc[0]
        assert last["USERNAME"] == "test_user"
        assert last["ACTION"] == ACTION_LOGIN

    def test_log_with_details(self, phase1_db):
        details = {"items": 5, "total_qty": 120}
        log_action(phase1_db, "test_user", ACTION_PO_GENERATED, ENTITY_PO,
                   "PO001", "ORG001", details)
        logs = get_recent_logs(phase1_db, limit=5, action=ACTION_PO_GENERATED)
        assert len(logs) >= 1

    def test_log_filter_by_org(self, phase1_db):
        log_action(phase1_db, "branch_user", ACTION_LOGIN, ENTITY_SESSION, org_cd="ORG002")
        logs = get_recent_logs(phase1_db, limit=10, org_cd="ORG002")
        assert len(logs) >= 1


# =====================================================================
# Test 6: System Config
# =====================================================================
class TestSystemConfig:
    def test_load_defaults(self, phase1_db):
        config = load_system_config(phase1_db)
        assert "safety_stock_days" in config
        assert config["safety_stock_days"] == "14"
        assert "spike_threshold_pct" in config
        assert config["spike_threshold_pct"] == "200.0"

    def test_load_full_config(self, phase1_db):
        configs = load_system_config_full(phase1_db)
        # 10 seeded by the builder + 2 added by the ensure_oasis_tables migration
        assert len(configs) == 12
        # Check structure
        first = configs[0]
        assert "CONFIG_KEY" in first
        assert "CONFIG_VALUE" in first
        assert "CONFIG_GROUP" in first

    def test_save_config(self, phase1_db):
        result = save_system_config(phase1_db, "safety_stock_days", "21", "test_admin")
        assert result
        # Read back
        config = load_system_config(phase1_db)
        assert config["safety_stock_days"] == "21"


# =====================================================================
# Test 7: ensure_oasis_tables Migration
# =====================================================================
class TestMigration:
    def test_ensure_tables_on_fresh_db(self):
        """Test that ensure_oasis_tables can create tables on a blank DB."""
        fresh_path = os.path.join(os.path.dirname(__file__), "test_fresh_migration.db")
        try:
            # Create a minimal DB with only one table
            conn = sqlite3.connect(fresh_path)
            conn.execute("CREATE TABLE IF NOT EXISTS dummy (id INTEGER)")
            conn.commit()
            conn.close()

            # Run migration
            ensure_oasis_tables(fresh_path)

            # Verify tables were created
            conn = sqlite3.connect(fresh_path)
            tables = [r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()]
            conn.close()

            assert "OASIS_USERS" in tables
            assert "OASIS_AUDIT_LOG" in tables
            assert "OASIS_SYSTEM_CONFIG" in tables
        finally:
            if os.path.exists(fresh_path):
                os.remove(fresh_path)


# =====================================================================
# Test 8: User Management
# =====================================================================
class TestUserManagement:
    def test_get_all_users(self, phase1_db):
        users = get_all_users(phase1_db)
        assert len(users) == 5
        # Should not contain password hashes
        for u in users:
            assert "PASSWORD_HASH" not in u
            assert "USERNAME" in u
            assert "ROLE" in u


# CLI Runner
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
