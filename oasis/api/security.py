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


def _load_or_generate_key(env_var: str, key_path: str) -> str:
    """Env key wins; else reuse a persisted key; else generate + persist so the
    key survives restarts instead of being a fresh one only visible in logs
    (finding F-9)."""
    key = os.getenv(env_var, "")
    if key:
        return key
    if os.path.exists(key_path):
        with open(key_path, encoding="utf-8") as f:
            key = f.read().strip()
    if not key:
        key = secrets.token_urlsafe(32)
        try:
            os.makedirs(os.path.dirname(key_path), exist_ok=True)
            with open(key_path, "w", encoding="utf-8") as f:
                f.write(key)
            logger.warning(
                "%s not set — generated key saved to %s (set %s for an explicit key)",
                env_var, key_path, env_var,
            )
        except OSError as e:
            logger.warning("%s not set — could not persist key to %s: %s", env_var, key_path, e)
    return key


_API_KEY = _load_or_generate_key(
    "OASIS_API_KEY",
    os.getenv(
        "OASIS_API_KEY_FILE",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                     "data", ".oasis_api_key"),
    ),
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
