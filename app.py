"""
O.A.S.I.S. — unified shell (U3).

The single front door. One login, the SYS v2.9 theme, and journey-driven,
role-gated navigation to every function. Replaces the sprawl of standalone
dashboards + 19 launchers.

Run:  streamlit run app.py      (or: python entrypoint.py --mode shell)
"""

import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.ui import theme
from oasis.ui.auth import require_login, logout
from oasis.ui import shell
from oasis.ui.components import safe_render
from oasis.ui.telemetry import log_page_view

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.getenv(
    "OASIS_DB_PATH",
    os.path.join(PROJECT_ROOT, "oasis", "data", "mock_pos_erp.db"),
)

st.set_page_config(page_title="O.A.S.I.S.", page_icon="◎", layout="wide",
                   initial_sidebar_state="expanded")
theme.inject_theme(st)


def _ensure_seeded(db_path: str) -> None:
    """First-run convenience: ensure auth tables exist and seed the default
    accounts ONLY if an operator opted in via OASIS_SEED_PASSWORD and no users
    exist yet. Never invents credentials; a no-op otherwise."""
    try:
        from oasis.logic.db_connector import ensure_oasis_tables
        ensure_oasis_tables(db_path)
        if os.getenv("OASIS_SEED_PASSWORD"):
            from oasis.logic.auth_manager import get_all_users, seed_users
            if not get_all_users(db_path):
                seed_users(db_path)
    except Exception:
        pass  # never block the app on seeding


_ensure_seeded(DB_PATH)

# One gate for the whole platform.
user = require_login(st, DB_PATH, app_title="Retail Intelligence Platform")
role = user.get("role")

# Page registry, filtered to the signed-in role.
pages = shell.visible_pages(shell.build_registry(), role)
labels = {f"{p.icon}  {p.label}": p for p in pages}

with st.sidebar:
    st.markdown(
        f'<div class="oasis-badge">OASIS · '
        f'<span style="color:var(--oasis-teal);">{role}</span></div>',
        unsafe_allow_html=True,
    )
    st.caption(f"Signed in as {user.get('display_name', user['username'])}")
    choice = st.radio("Navigate", list(labels.keys()), label_visibility="collapsed")
    st.divider()
    if st.button("Log out", use_container_width=True):
        logout(st, DB_PATH)
        st.rerun()

ctx = {
    "st": st,
    "user": user,
    "role": role,
    "username": user.get("username"),
    "db_path": DB_PATH,
    "project_root": PROJECT_ROOT,
}

selected = labels[choice]

# U5: record a page view once per navigation (not on every Streamlit rerun).
if st.session_state.get("_last_page") != selected.key:
    log_page_view(DB_PATH, user.get("username", ""), selected.key)
    st.session_state["_last_page"] = selected.key

# U4: every page render is wrapped — a page error shows a calm panel and is
# logged in full, never a traceback on screen.
safe_render(selected.render, ctx)
