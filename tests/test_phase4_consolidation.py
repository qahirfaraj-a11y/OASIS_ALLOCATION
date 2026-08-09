"""
Phase 4 Consolidation Tests
============================
Tests for:
- allocation_engine permission in all roles
- OasisScheduler creation, job registry, toggle, run_now
- Mobile CSS (checking style block is present)
"""
import sys
import os
import json
import sqlite3
import tempfile

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest


# ── 4.1: Role Permission Tests ──────────────────────────────────────

class TestAllocationPermissions:
    """Verify allocation_engine tab exists in all roles."""

    def test_branch_manager_has_allocation(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        perms = ROLE_PERMISSIONS["branch_manager"]
        assert "allocation_engine" in perms["tabs"], "branch_manager missing allocation_engine tab"
        assert perms["tabs"]["allocation_engine"] is True

    def test_regional_manager_has_allocation(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        perms = ROLE_PERMISSIONS["regional_manager"]
        assert "allocation_engine" in perms["tabs"]
        assert perms["tabs"]["allocation_engine"] is True

    def test_ops_admin_has_allocation(self):
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        perms = ROLE_PERMISSIONS["ops_admin"]
        assert "allocation_engine" in perms["tabs"]
        assert perms["tabs"]["allocation_engine"] is True

    def test_all_roles_have_9_tabs(self):
        """With allocation_engine, each role should have 9 tab permissions."""
        from oasis.logic.auth_manager import ROLE_PERMISSIONS
        for role, perms in ROLE_PERMISSIONS.items():
            # Every role must answer for EVERY tab — the count follows the table
            # rather than a literal, since executive_roi and
            # supplier_intelligence were missing from it entirely and so
            # returned None for every role.
            expected = set(ROLE_PERMISSIONS["ops_admin"]["tabs"])
            assert set(perms["tabs"]) == expected, (
                f"{role} does not answer for {expected ^ set(perms['tabs'])}")


# ── 4.2: Scheduler Tests ────────────────────────────────────────────

class TestOasisScheduler:
    """Test the OasisScheduler service."""

    @pytest.fixture
    def temp_db(self, tmp_path):
        """Create a temporary SQLite database with required tables."""
        db_path = str(tmp_path / "test_sched.db")
        conn = sqlite3.connect(db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS OASIS_SYSTEM_CONFIG (
                CONFIG_KEY TEXT PRIMARY KEY,
                CONFIG_VALUE TEXT,
                CONFIG_GROUP TEXT DEFAULT 'general',
                DESCRIPTION TEXT DEFAULT '',
                UPDATED_BY TEXT DEFAULT 'system',
                UPDATED_DT TEXT DEFAULT ''
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS OASIS_AUDIT_LOG (
                LOG_ID INTEGER PRIMARY KEY AUTOINCREMENT,
                LOG_DT TEXT, USERNAME TEXT, ACTION TEXT,
                ENTITY_TYPE TEXT, ENTITY_ID TEXT,
                OLD_VALUE TEXT, NEW_VALUE TEXT, DETAILS TEXT
            )
        """)
        conn.commit()
        conn.close()
        return db_path

    def test_scheduler_creation(self, temp_db):
        from oasis.logic.scheduler_service import OasisScheduler
        sched = OasisScheduler(temp_db)
        assert sched is not None
        assert not sched.is_running()

    def test_scheduler_has_three_jobs(self, temp_db):
        from oasis.logic.scheduler_service import OasisScheduler
        sched = OasisScheduler(temp_db)
        assert len(sched.jobs) == 3

    def test_scheduler_job_ids(self, temp_db):
        from oasis.logic.scheduler_service import OasisScheduler
        sched = OasisScheduler(temp_db)
        expected_ids = {"morning_po", "hourly_monitor", "evening_summary"}
        assert set(sched.jobs.keys()) == expected_ids

    def test_toggle_job(self, temp_db):
        from oasis.logic.scheduler_service import OasisScheduler
        sched = OasisScheduler(temp_db)
        sched.toggle_job("morning_po", False)
        assert sched.jobs["morning_po"].enabled is False
        sched.toggle_job("morning_po", True)
        assert sched.jobs["morning_po"].enabled is True

    def test_get_job_status(self, temp_db):
        from oasis.logic.scheduler_service import OasisScheduler
        sched = OasisScheduler(temp_db)
        statuses = sched.get_job_status()
        assert len(statuses) == 3
        for s in statuses:
            assert "job_id" in s
            assert "name" in s
            assert "enabled" in s
            assert "cron" in s

    def test_save_and_reload_config(self, temp_db):
        from oasis.logic.scheduler_service import OasisScheduler
        sched1 = OasisScheduler(temp_db)
        sched1.toggle_job("hourly_monitor", False)

        # Create a new scheduler instance — should load saved config
        sched2 = OasisScheduler(temp_db)
        assert sched2.jobs["hourly_monitor"].enabled is False

    def test_run_now_unknown_job(self, temp_db):
        from oasis.logic.scheduler_service import OasisScheduler
        sched = OasisScheduler(temp_db)
        result = sched.run_now("nonexistent_job")
        assert "Unknown job" in result


# ── 4.3: Docker File Existence ───────────────────────────────────────

class TestDockerFiles:
    """Verify Docker deployment files exist."""

    def test_dockerfile_exists(self):
        project_root = os.path.join(os.path.dirname(__file__), "..")
        assert os.path.exists(os.path.join(project_root, "Dockerfile"))

    def test_docker_compose_exists(self):
        project_root = os.path.join(os.path.dirname(__file__), "..")
        assert os.path.exists(os.path.join(project_root, "docker-compose.yml"))

    def test_dockerignore_exists(self):
        project_root = os.path.join(os.path.dirname(__file__), "..")
        assert os.path.exists(os.path.join(project_root, ".dockerignore"))


# ── 4.5: Documentation Existence ────────────────────────────────────

class TestDocumentation:
    """Verify documentation files exist and have content."""

    def test_user_guide_exists(self):
        project_root = os.path.join(os.path.dirname(__file__), "..")
        path = os.path.join(project_root, "USER_GUIDE.md")
        assert os.path.exists(path)
        with open(path, encoding="utf-8") as f:
            content = f.read()
        assert len(content) > 500, "USER_GUIDE.md appears too short"
        assert "Quick Start" in content
        assert "Allocation Engine" in content

    def test_deployment_guide_exists(self):
        project_root = os.path.join(os.path.dirname(__file__), "..")
        path = os.path.join(project_root, "DEPLOYMENT_GUIDE.md")
        assert os.path.exists(path)
        with open(path, encoding="utf-8") as f:
            content = f.read()
        assert len(content) > 500, "DEPLOYMENT_GUIDE.md appears too short"
        assert "Docker" in content
        assert "Scheduler" in content


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
