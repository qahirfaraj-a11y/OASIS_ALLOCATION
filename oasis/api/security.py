"""
OASIS API Security
==================
Shared authentication and CORS policy for all OASIS FastAPI apps.

Auth model (two accepted schemes, checked in order):
  1. ``X-API-Key`` header matching the ``OASIS_API_KEY`` env var — for
     machine-to-machine callers (mobile app, scripts).
  2. ``Authorization: Bearer <session_id>`` validated against OASIS_SESSIONS
     via auth_manager — for logged-in dashboard users.

If ``OASIS_API_KEY`` is not configured, a random key is generated at process
start and printed once to the log, so the API is never unintentionally open.
"""

import os
import secrets
import logging
from typing import Optional, List

from fastapi import Security, HTTPException
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials

logger = logging.getLogger("OASIS-API-Security")

_api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
_bearer = HTTPBearer(auto_error=False)

_API_KEY = os.getenv("OASIS_API_KEY", "")
if not _API_KEY:
    _API_KEY = secrets.token_urlsafe(32)
    logger.warning(
        "OASIS_API_KEY not set — generated ephemeral key for this run: %s "
        "(set OASIS_API_KEY in the environment for a stable key)", _API_KEY
    )


def _auth_db_path() -> str:
    default_db = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "data", "mock_pos_erp.db")
    )
    return os.getenv("OASIS_DB_PATH", default_db)


async def require_auth(
    api_key: Optional[str] = Security(_api_key_header),
    bearer: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
) -> dict:
    """FastAPI dependency: rejects the request unless it carries a valid
    API key or a valid dashboard session token. Returns an identity dict."""
    if api_key and secrets.compare_digest(api_key, _API_KEY):
        return {"auth": "api_key", "role": "service"}

    if bearer and bearer.credentials:
        try:
            from ..logic.auth_manager import validate_session
            user = validate_session(bearer.credentials, _auth_db_path())
        except Exception as e:
            logger.error("Session validation error: %s", e)
            user = None
        if user:
            return {"auth": "session", **user}

    raise HTTPException(
        status_code=401,
        detail="Unauthorized: provide X-API-Key or a valid session bearer token.",
    )


def allowed_origins() -> List[str]:
    """CORS origins from OASIS_ALLOWED_ORIGINS (comma-separated).
    Defaults to localhost dashboard/dev ports only — never '*'."""
    raw = os.getenv("OASIS_ALLOWED_ORIGINS", "")
    if raw.strip():
        return [o.strip() for o in raw.split(",") if o.strip()]
    return [
        "http://localhost:8501",
        "http://localhost:8502",
        "http://localhost:8503",
        "http://localhost:8505",
        "http://localhost:8506",
        "http://localhost:8550",
        "http://127.0.0.1:8501",
    ]
