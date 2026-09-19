"""Who may use the web console, and what the public demo may do.

WHY THIS EXISTS
---------------
The console shipped with no login, and was safe only because it bound to
127.0.0.1. Putting it on a website removes that: a reverse proxy in front of a
loopback-bound server serves the whole internet, so binding is not a control.
Unauthenticated, anyone could read a client's order book, costs and suppliers,
push draft purchase orders into their ERP (under any name they typed), rewrite
the competitive field, and set off population downloads and 40-second site
searches in a loop.

TWO MODES, NEVER MIXED
----------------------
private (default)  Every /api/ call needs a signed-in OASIS user: the same
                   accounts, password rules, 5-strike lockout and 24-hour
                   sessions as the consoles (auth_manager). Actions are
                   checked against the role's permissions, and a user bound to
                   one store sees only that store.
public             OASIS_WEB_PUBLIC=1. The website demo. No accounts, no
                   writes, no downloads, heavy jobs rate-limited per visitor —
                   and it REFUSES TO START unless the active store is the
                   built-in sample data, so a misconfigured demo cannot put a
                   real client's figures on the internet.

Also, in both modes: state-changing requests must carry the console's own
header (a cross-site form cannot set one without a CORS preflight this app
never grants), and connection strings are scrubbed from every JSON response.
"""
from __future__ import annotations

import os
import re
import threading
import time
from collections import defaultdict, deque
from typing import Any, Dict, Optional

PUBLIC_ENV = "OASIS_WEB_PUBLIC"
SESSION_COOKIE = "oasis_sid"
#: Sent by the page on every call; a cross-site form cannot add it.
CLIENT_HEADER = "x-oasis-client"
SESSION_SECONDS = 24 * 3600

#: Endpoints that change data or reach out to the internet. Never in public.
WRITE_ROUTES = (
    re.compile(r"^/api/orders/[^/]+/push$"),
    re.compile(r"^/api/sites/place$"),
    re.compile(r"^/api/sites/region$"),
    re.compile(r"^/api/sites/competitor$"),
)

#: What a role must be allowed before an action runs (auth_manager permissions).
PERMISSIONS = (
    (re.compile(r"^/api/orders/[^/]+/push$"), "can_approve_po"),
    (re.compile(r"^/api/sites/place$"), "can_edit_config"),
    (re.compile(r"^/api/sites/region$"), "can_edit_config"),
    (re.compile(r"^/api/sites/competitor$"), "can_edit_config"),
    (re.compile(r"^/api/jobs/transfers$"), "tab:transfer_intelligence"),
)

#: Paths that name a store; a user bound to one store may only name theirs.
STORE_ROUTES = (
    re.compile(r"^/api/jobs/orders/(?P<org>[^/]+)$"),
    re.compile(r"^/api/orders/(?P<org>[^/]+)/push$"),
)

#: Seconds of work a single request can cost; limited per visitor in public
#: mode and per user in private mode. (calls, per seconds)
HEAVY_ROUTES = (
    (re.compile(r"^/api/jobs/"), (6, 60)),
    (re.compile(r"^/api/sites/(suggest|score|evaluate)$"), (10, 60)),
)
LOGIN_LIMIT = (10, 300)

#: Paths reachable without a session in private mode.
OPEN_PATHS = ("/api/login", "/api/me")


def public_mode() -> bool:
    return (os.getenv(PUBLIC_ENV) or "").strip().lower() in ("1", "true", "yes", "on")


# ── the public demo's precondition ──────────────────────────────────────────
def assert_public_is_sample(root: Optional[str]) -> None:
    """Refuse to serve the public demo from anything but the sample store.

    Checked at startup. Three ways a demo could end up reading real data, all
    closed: the onboarding record says the store is not the sample; a live POS
    or ERP URL is configured (the adapter would read it whatever the store
    file says); an ERP backend is selected.
    """
    problems = []
    for var in ("OASIS_POS_DB_URL", "OASIS_DB_URL", "OASIS_ERP"):
        if (os.getenv(var) or "").strip():
            problems.append(f"{var} is set")
    try:
        # the setup wizard's "Connect a POS" choice is read from the INSTALL,
        # not from OASIS_ROOT, so a demo sharing an install with a live client
        # would read that client's POS whatever the demo folder says
        from oasis.logic.db import has_distinct_pos
        if has_distinct_pos() and not any("OASIS_POS_DB_URL" in p for p in problems):
            problems.append("this install has a live POS connected (setup wizard) — "
                            "run the public demo from a separate install")
    except Exception as e:
        problems.append(f"the POS connection could not be checked ({str(e)[:80]})")
    try:
        from oasis.desktop import data as D
        prov = D.data_provenance(root)
        if not prov.get("is_sample"):
            problems.append(f"the active store is '{prov.get('source')}', not the built-in sample")
    except Exception as e:
        problems.append(f"the data source could not be verified ({str(e)[:80]})")
    if problems:
        raise RuntimeError("Public mode serves the sample store only, and "
                           + "; ".join(problems) + ". Unset OASIS_WEB_PUBLIC to run "
                           "the signed-in console, or point OASIS_ROOT at a sample install.")


# ── sessions ─────────────────────────────────────────────────────────────────
def _db(root: Optional[str]) -> str:
    from oasis.desktop import data as D
    return D.store_db_path(root)


def resolve_user(session_id: Optional[str], root: Optional[str]) -> Optional[Dict[str, Any]]:
    """The signed-in user for a session cookie, or None. (Tests replace this.)"""
    if not session_id:
        return None
    from oasis.logic.auth_manager import validate_session
    return validate_session(session_id, _db(root))


def allowed(user: Dict[str, Any], need: str) -> bool:
    perms = user.get("permissions") or {}
    if need.startswith("tab:"):
        return bool((perms.get("tabs") or {}).get(need[4:]))
    return bool(perms.get(need))


def may_see_store(user: Dict[str, Any], org_cd: str) -> bool:
    if (user.get("permissions") or {}).get("can_view_all_stores"):
        return True
    return bool(user.get("assigned_org")) and str(user["assigned_org"]) == str(org_cd)


def uses_install_password(username: str, root: Optional[str]) -> bool:
    """True if the account still signs in with the seeded installation password.

    On a website that password is a published default; an account still using
    it is open to anyone who read the install notes.
    """
    try:
        import sqlite3
        from oasis.logic.auth_manager import verify_password
        conn = sqlite3.connect(_db(root), timeout=10.0)
        try:
            row = conn.execute("SELECT PASSWORD_HASH FROM OASIS_USERS WHERE USERNAME=?",
                               (username,)).fetchone()
        finally:
            conn.close()
        return bool(row and row[0]) and verify_password(
            os.getenv("OASIS_SEED_PASSWORD", "oasis2026"), row[0])
    except Exception:
        return False


# ── rate limiting ────────────────────────────────────────────────────────────
class RateLimiter:
    """Sliding-window call counter per (bucket, caller). In-process by design:
    the console runs as one server process, and the job queue is in-process
    too."""

    def __init__(self):
        self._hits: Dict[tuple, deque] = defaultdict(deque)
        self._lock = threading.Lock()

    def hit(self, bucket: str, who: str, limit: int, per_s: float) -> bool:
        now = time.monotonic()
        with self._lock:
            q = self._hits[(bucket, who)]
            while q and now - q[0] > per_s:
                q.popleft()
            if len(q) >= limit:
                return False
            q.append(now)
            return True

    def reset(self) -> None:
        with self._lock:
            self._hits.clear()


LIMITER = RateLimiter()


def heavy_limit(path: str):
    for rx, lim in HEAVY_ROUTES:
        if rx.search(path):
            return rx.pattern, lim
    return None


# ── response scrubbing ───────────────────────────────────────────────────────
_URI_CREDS = re.compile(r"\b([a-zA-Z][a-zA-Z0-9+.\-]*://)[^\s\"'/@]+@")
_KV_SECRET = re.compile(r"(?i)\b(pwd|password|passwd|uid|user id)\s*=\s*[^;\"'\s]*")


def scrub(text: str) -> str:
    """Remove credentials from anything about to reach a browser.

    Endpoints return str(exception) so an operator can see WHY a read failed,
    and a database driver's exception can carry its connection string —
    user, password and host. The message stays; the secret does not.
    """
    text = _URI_CREDS.sub(r"\1<redacted>@", text)
    return _KV_SECRET.sub(lambda m: m.group(1) + "=<redacted>", text)


def client_id(request) -> str:
    """The caller, for rate limits.

    X-Forwarded-For is taken only when OASIS_WEB_TRUST_PROXY is set — i.e. the
    operator has put their own reverse proxy in front, which overwrites it.
    Without a proxy any visitor can send the header, and a limit keyed on it
    is a limit each request can dodge by naming a new address.
    """
    if (os.getenv("OASIS_WEB_TRUST_PROXY") or "").strip().lower() in ("1", "true", "yes"):
        fwd = request.headers.get("x-forwarded-for")
        if fwd:
            return fwd.split(",")[0].strip()
    return getattr(request.client, "host", "") or "unknown"


def secure_cookie(request) -> bool:
    if (os.getenv("OASIS_WEB_SECURE_COOKIE") or "").strip().lower() in ("1", "true", "yes"):
        return True
    if request.url.scheme == "https":
        return True
    trust = (os.getenv("OASIS_WEB_TRUST_PROXY") or "").strip().lower() in ("1", "true", "yes")
    return trust and request.headers.get("x-forwarded-proto", "").lower() == "https"
