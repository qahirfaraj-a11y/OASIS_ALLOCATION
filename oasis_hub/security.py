"""
Hub authentication — three schemes, one per caller class.

  * Admin      X-Hub-Admin-Key: <OASIS_HUB_ADMIN_KEY>   provisioning + licensing
  * Store      Authorization: Bearer <ingest_token>      pushing telemetry
  * Supplier   Authorization: Bearer <session_token>     reading the portal

Ingest tokens are high-entropy machine secrets → stored as fast SHA-256 hashes,
looked up directly. Supplier passwords are human-chosen → bcrypt (slow) hashes.
"""

import os
import hashlib
import logging
import secrets
from datetime import datetime
from typing import Optional

import bcrypt
from fastapi import Security, HTTPException, Depends
from fastapi.security import APIKeyHeader, HTTPBearer, HTTPAuthorizationCredentials
from sqlalchemy.orm import Session

from . import tokens
from .db import get_session
from .models import HubIngestToken

logger = logging.getLogger("OASIS.Hub.Security")

_admin_header = APIKeyHeader(name="X-Hub-Admin-Key", auto_error=False)
_bearer = HTTPBearer(auto_error=False)

_ADMIN_KEY = os.getenv("OASIS_HUB_ADMIN_KEY", "")
if not _ADMIN_KEY:
    _ADMIN_KEY = secrets.token_urlsafe(32)
    logger.warning(
        "OASIS_HUB_ADMIN_KEY not set — generated ephemeral admin key for this run: "
        "%s (set OASIS_HUB_ADMIN_KEY for a stable key)", _ADMIN_KEY
    )


# ── password hashing (suppliers) ─────────────────────────────────────────
def hash_password(password: str) -> str:
    return bcrypt.hashpw(password.encode("utf-8"),
                         bcrypt.gensalt(rounds=12)).decode("utf-8")


def verify_password(password: str, stored_hash: Optional[str]) -> bool:
    if not stored_hash:
        return False
    try:
        return bcrypt.checkpw(password.encode("utf-8"), stored_hash.encode("utf-8"))
    except (ValueError, TypeError):
        return False


# ── ingest tokens (stores) ───────────────────────────────────────────────
def new_ingest_token() -> str:
    """Mint a raw ingest token to hand to a store (shown once)."""
    return "oist_" + secrets.token_urlsafe(32)


def hash_ingest_token(raw: str) -> str:
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


# ── FastAPI dependencies ─────────────────────────────────────────────────
def require_admin(admin_key: Optional[str] = Security(_admin_header)) -> dict:
    if admin_key and secrets.compare_digest(admin_key, _ADMIN_KEY):
        return {"auth": "admin"}
    raise HTTPException(status_code=401, detail="Unauthorized: valid X-Hub-Admin-Key required.")


def require_ingest(
    bearer: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
    db: Session = Depends(get_session),
) -> dict:
    """Resolve a store from its ingest bearer token; stamp last_used_at."""
    if not (bearer and bearer.credentials):
        raise HTTPException(status_code=401, detail="Unauthorized: store ingest token required.")
    token_hash = hash_ingest_token(bearer.credentials)
    row = (db.query(HubIngestToken)
             .filter(HubIngestToken.token_hash == token_hash,
                     HubIngestToken.active.is_(True))
             .first())
    if not row:
        raise HTTPException(status_code=401, detail="Unauthorized: invalid or revoked ingest token.")
    row.last_used_at = datetime.utcnow()
    return {"auth": "store", "store_id": row.store_id, "token_id": row.id}


def require_supplier(
    bearer: Optional[HTTPAuthorizationCredentials] = Security(_bearer),
) -> dict:
    """Validate a supplier session token."""
    if not (bearer and bearer.credentials):
        raise HTTPException(status_code=401, detail="Unauthorized: supplier session required.")
    payload = tokens.verify(bearer.credentials)
    if not payload or payload.get("role") != "supplier":
        raise HTTPException(status_code=401, detail="Unauthorized: invalid or expired session.")
    return {"auth": "supplier", "supplier_id": payload["sub"], **payload}
