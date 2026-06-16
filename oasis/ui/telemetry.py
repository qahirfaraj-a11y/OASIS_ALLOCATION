"""
OASIS UI usage telemetry (U5).

Lightweight page-view / action events written to the existing audit log
(entity type ``UI``) so adoption and friction are measurable per screen —
the "gradeable" signal the platform otherwise lacks. Logging never raises:
telemetry must never break a render.

``page_view_counts`` reads the events back for the Settings adoption panel.
"""

from __future__ import annotations

import logging
from typing import Dict, Optional

logger = logging.getLogger("OASIS.UI.Telemetry")

ENTITY_UI = "UI"
ACTION_PAGE_VIEW = "PAGE_VIEW"


def _safe_log(db_path: str, username: str, action: str,
              entity_id: Optional[str] = None, details=None) -> None:
    try:
        from ..logic.audit_logger import log_action
        log_action(db_path, username, action, ENTITY_UI, entity_id, None, details)
    except Exception as e:  # telemetry is best-effort, never fatal
        logger.debug("telemetry log skipped: %s", e)


def log_page_view(db_path: str, username: str, page_key: str) -> None:
    """Record that *username* opened *page_key* in the shell."""
    _safe_log(db_path, username, ACTION_PAGE_VIEW, entity_id=page_key)


def log_ui_action(db_path: str, username: str, action: str, details=None) -> None:
    """Record a UI action (e.g. RUN_ALLOCATION, QUEUE_TRANSFERS)."""
    _safe_log(db_path, username, action, details=details)


def page_view_counts(db_path: str, days: int = 7) -> Dict[str, int]:
    """{page_key: view_count} for PAGE_VIEW events in the last *days* days.

    Reads the SAME database that log_page_view wrote to (the passed db_path),
    not the env default — so the two are always consistent. Returns {} on any
    error.
    """
    try:
        from ..logic import db as oasis_db
        url = db_path if "://" in str(db_path) else f"sqlite:///{db_path}"
        conn = oasis_db.get_raw_connection(db_url=url)
        try:
            cur = conn.execute(
                """SELECT ENTITY_ID, COUNT(*) AS n
                   FROM OASIS_AUDIT_LOG
                   WHERE ACTION = ? AND ENTITY_TYPE = ?
                     AND CREATED_DT >= datetime('now', ?)
                   GROUP BY ENTITY_ID ORDER BY n DESC""",
                (ACTION_PAGE_VIEW, ENTITY_UI, f"-{int(days)} days"),
            )
            return {row[0]: int(row[1]) for row in cur.fetchall() if row[0]}
        finally:
            conn.close()
    except Exception as e:
        logger.debug("page_view_counts failed: %s", e)
        return {}
