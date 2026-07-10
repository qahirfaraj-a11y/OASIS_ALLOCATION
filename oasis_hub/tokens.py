"""
Dependency-free signed session tokens (HS256-style), for supplier portal logins.

Format:  base64url(header).base64url(payload).base64url(hmac_sha256)
This mirrors the compact shape of a JWT without pulling in a JWT library — the
codebase already hand-rolls HMAC integrity (see license_manager trial stamps).
The signing secret is OASIS_HUB_TOKEN_SECRET; if unset, a random per-process
secret is generated so tokens are never signed with an empty/guessable key
(they simply won't survive a restart, which is fine for dev).
"""

import os
import json
import hmac
import base64
import hashlib
import logging
import secrets
from datetime import datetime, timezone
from typing import Optional

logger = logging.getLogger("OASIS.Hub.Tokens")

_SECRET = os.getenv("OASIS_HUB_TOKEN_SECRET", "").encode() or None
if _SECRET is None:
    _SECRET = secrets.token_bytes(32)
    logger.warning(
        "OASIS_HUB_TOKEN_SECRET not set — using an ephemeral per-process secret. "
        "Set it in the environment for stable supplier sessions across restarts."
    )

_HEADER = {"alg": "HS256", "typ": "OHT"}   # OASIS Hub Token


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _unb64(s: str) -> bytes:
    pad = "=" * (-len(s) % 4)
    return base64.urlsafe_b64decode(s + pad)


def _now() -> int:
    return int(datetime.now(timezone.utc).timestamp())


def sign(subject: str, role: str = "supplier", ttl_seconds: int = 8 * 3600,
         **claims) -> str:
    """Issue a signed token for ``subject`` (e.g. a supplier id)."""
    payload = {
        "sub": subject,
        "role": role,
        "iat": _now(),
        "exp": _now() + int(ttl_seconds),
        **claims,
    }
    head = _b64(json.dumps(_HEADER, separators=(",", ":")).encode())
    body = _b64(json.dumps(payload, separators=(",", ":")).encode())
    signing_input = f"{head}.{body}".encode()
    sig = hmac.new(_SECRET, signing_input, hashlib.sha256).digest()
    return f"{head}.{body}.{_b64(sig)}"


def verify(token: str) -> Optional[dict]:
    """Return the payload dict if the token is valid and unexpired, else None."""
    try:
        head, body, sig = token.split(".")
    except (ValueError, AttributeError):
        return None
    signing_input = f"{head}.{body}".encode()
    expected = hmac.new(_SECRET, signing_input, hashlib.sha256).digest()
    try:
        if not hmac.compare_digest(expected, _unb64(sig)):
            return None
        payload = json.loads(_unb64(body))
    except Exception:
        return None
    if not isinstance(payload, dict) or payload.get("exp", 0) < _now():
        return None
    return payload
