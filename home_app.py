"""
O.A.S.I.S. Home — suite launcher entry point.

Run:  streamlit run home_app.py      (or: python entrypoint.py --mode home)

One front door: console cards with live/offline status, license posture,
and the store-system snapshot. The consoles themselves stay separate apps.
"""

import os
import sys

import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.ui.home import render_home_page

PROJECT_ROOT = os.path.dirname(os.path.abspath(__file__))

st.set_page_config(page_title="O.A.S.I.S. — Home", page_icon="🏠",
                   layout="wide", initial_sidebar_state="collapsed")


def _gate() -> None:
    """Sign in to Home when — and only when — there is an account to sign in with.

    Home had no auth at all, yet it can start and stop console processes,
    activate a licence and reset onboarding (deep-analysis finding S5). It is
    also the first-run front door, so the gate has to respect two things a
    blanket require_login would break:

      * a fresh install has no store and no users, and the wizard must stay
        reachable — there is nothing to authenticate against yet;
      * a store with no seeded accounts would otherwise lock the operator out
        of the one surface that can reset onboarding. Gating there would trade
        an open launcher for an unrecoverable one.

    So: gate once accounts exist, stay open before that. Signing in here also
    mints the suite sid, which is what finally lets Home's console links carry
    ?sid= — the front door now takes part in the single sign-on it fronts.
    """
    from oasis.logic.onboarding import is_onboarded, resolved_db_path
    if not is_onboarded(PROJECT_ROOT):
        return                                   # first-run wizard, no store yet

    db_path = resolved_db_path(PROJECT_ROOT)
    from oasis.ui.shell import ensure_seeded
    ensure_seeded(db_path)                       # same first-run seed the consoles do
    try:
        from oasis.logic.auth_manager import get_all_users
        has_accounts = bool(get_all_users(db_path))
    except Exception:
        has_accounts = False
    if not has_accounts:
        return                                   # nothing to authenticate — see above

    from oasis.ui.auth import require_login
    require_login(st, db_path, app_title="OASIS Home")


_gate()
render_home_page(st, PROJECT_ROOT)
