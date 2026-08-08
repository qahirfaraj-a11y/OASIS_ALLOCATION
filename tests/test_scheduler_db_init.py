"""
Test that the scheduler correctly formats DB paths for UniversalConnector.
This prevents regression of D-3 (Ship-blocking error where UniversalConnector
received a raw filesystem path instead of a sqlalchemy URL).
"""

import os
import pytest
from oasis.logic.scheduler_service import OasisScheduler

def test_scheduler_db_path_format(tmp_path, monkeypatch):
    """
    Ensure the scheduler passes a valid SQLAlchemy URL to the UniversalConnector
    rather than a raw filesystem path.
    """
    db_file = tmp_path / "test_scheduler.db"
    db_path = str(db_file)
    
    # We don't need to actually run the job completely if the DB is empty,
    # we just need to ensure the UniversalConnector doesn't throw ArgumentError
    # "Could not parse rfc1738 URL from string"
    
    scheduler = OasisScheduler(db_path)
    
    # The actual execution will fail (no tables), but it should NOT fail
    # with a SQLAlchemy ArgumentError due to a bad connection string format.
    result = scheduler._run_morning_po()
    
    assert "Could not parse rfc1738 URL" not in result
    assert "Could not parse SQLAlchemy URL" not in result
    assert "ArgumentError" not in result

