"""
OASIS unified auth gate (U2).

One ``require_login()`` wrapper every Streamlit surface calls at the top —
closing the hole where only the command center was gated and 7 other apps
(including the PO-approval dashboard, which authorizes spend) were wide open.

Uses the existing hardened bcrypt auth in ``oasis.logic.auth_manager`` (no new
auth logic) and the SYS v2.9 theme/components for a consistent branded login.
Adds an idle-timeout layer on top of the DB session.

Pure, import-safe helpers (``is_session_expired``, ``role_allowed``) are unit
tested without Streamlit; ``require_login`` / ``render_login`` touch ``st``.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional, Sequence

from . import theme

USER_KEY = "user"
LAST_ACTIVE_KEY = "_oasis_last_active"
DEFAULT_TTL_MIN = 480  # 8h idle timeout


# ── pure helpers (testable) ──────────────────────────────────────────────
def is_session_expired(last_active_iso: Optional[str], ttl_minutes: int,
                       now: Optional[datetime] = None) -> bool:
    """True if the last-active timestamp is older than the idle TTL."""
    if not last_active_iso:
        return False  # no stamp yet → treat as fresh (just logged in)
    now = now or datetime.now()
    try:
        last = datetime.fromisoformat(last_active_iso)
    except (TypeError, ValueError):
        return True  # unparseable → force re-login
    return (now - last).total_seconds() > ttl_minutes * 60


def role_allowed(role: Optional[str], allowed_roles: Optional[Sequence[str]]) -> bool:
    """True if no allowlist is set, or the role is in it."""
    if not allowed_roles:
        return True
    return role in set(allowed_roles)


def current_user(st_module) -> Optional[dict]:
    return st_module.session_state.get(USER_KEY)


# ── login screen (branded) ───────────────────────────────────────────────
def render_login(st_module, db_path: str, app_title: str = "OASIS") -> None:
    """Render the branded login form and authenticate on submit."""
    from ..logic.auth_manager import authenticate
    theme.inject_theme(st_module)

    st_module.markdown(
        f'<div class="oasis-card" style="text-align:center;">'
        f'<h2 style="color:var(--oasis-teal);margin:0;">OASIS</h2>'
        f'<div style="color:var(--oasis-text-2);">{app_title}</div></div>',
        unsafe_allow_html=True,
    )
    cols = st_module.columns([1, 2, 1])
    with cols[1]:
        with st_module.form("oasis_login"):
            username = st_module.text_input("Username", placeholder="e.g. ops_admin")
            password = st_module.text_input("Password", type="password")
            submitted = st_module.form_submit_button(
                "Sign In", type="primary", use_container_width=True)
        if submitted:
            if not (username and password):
                st_module.warning("Enter both username and password.")
                return
            user = authenticate(username, password, db_path)
            if user:
                st_module.session_state[USER_KEY] = user
                st_module.session_state[LAST_ACTIVE_KEY] = datetime.now().isoformat()
                _audit(db_path, username, "LOGIN")
                st_module.rerun()
            else:
                st_module.error("Invalid username or password.")
        st_module.caption("Contact your administrator for credentials.")


def logout(st_module, db_path: str = None) -> None:
    user = current_user(st_module)
    if user and db_path:
        _audit(db_path, user.get("username", ""), "LOGOUT")
    st_module.session_state[USER_KEY] = None
    st_module.session_state.pop(LAST_ACTIVE_KEY, None)


# ── the gate ─────────────────────────────────────────────────────────────
def require_login(st_module, db_path: str, app_title: str = "OASIS",
                  allowed_roles: Optional[Sequence[str]] = None,
                  ttl_minutes: int = DEFAULT_TTL_MIN) -> dict:
    """Gate a page. Returns the authenticated user or halts rendering.

    - No user in session → render login, then ``st.stop()``.
    - Idle longer than ttl_minutes → clear session, re-login, ``st.stop()``.
    - Role not in allowed_roles → show 'no access' + logout, ``st.stop()``.
    Refreshes the idle timestamp on every successful pass.
    """
    user = current_user(st_module)

    if not user:
        render_login(st_module, db_path, app_title)
        st_module.stop()

    if is_session_expired(st_module.session_state.get(LAST_ACTIVE_KEY), ttl_minutes):
        logout(st_module, db_path)
        st_module.warning("Session timed out. Please sign in again.")
        render_login(st_module, db_path, app_title)
        st_module.stop()

    if not role_allowed(user.get("role"), allowed_roles):
        theme.inject_theme(st_module)
        st_module.error(
            f"Your role ({user.get('role')}) does not have access to {app_title}."
        )
        if st_module.button("Sign in as a different user"):
            logout(st_module, db_path)
            st_module.rerun()
        st_module.stop()

    # Passed — refresh idle stamp and return the identity.
    st_module.session_state[LAST_ACTIVE_KEY] = datetime.now().isoformat()
    return user


def _audit(db_path: str, username: str, action: str) -> None:
    try:
        from ..logic.audit_logger import log_action
        log_action(db_path, username, action, "SESSION")
    except Exception:
        pass  # auth must never fail because audit logging did
