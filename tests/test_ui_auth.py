"""Tests for the unified auth gate (oasis/ui/auth.py).

Pure helpers are tested directly; the gate is driven with a fake Streamlit
that records stop()/markdown/button calls (no server, no real auth DB).
"""

import os
import sys
from datetime import datetime, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.ui import auth


class TestSessionExpiry:
    def test_no_stamp_is_fresh(self):
        assert auth.is_session_expired(None, 480) is False

    def test_recent_is_not_expired(self):
        recent = (datetime.now() - timedelta(minutes=5)).isoformat()
        assert auth.is_session_expired(recent, 480) is False

    def test_old_is_expired(self):
        old = (datetime.now() - timedelta(minutes=600)).isoformat()
        assert auth.is_session_expired(old, 480) is True

    def test_unparseable_forces_relogin(self):
        assert auth.is_session_expired("not-a-date", 480) is True

    def test_boundary(self):
        now = datetime(2026, 6, 13, 12, 0, 0)
        stamp = (now - timedelta(minutes=480, seconds=1)).isoformat()
        assert auth.is_session_expired(stamp, 480, now=now) is True
        stamp_ok = (now - timedelta(minutes=479)).isoformat()
        assert auth.is_session_expired(stamp_ok, 480, now=now) is False


class TestRoleAllowed:
    def test_no_allowlist_permits_all(self):
        assert auth.role_allowed("anything", None) is True
        assert auth.role_allowed("anything", []) is True

    def test_allowlist_enforced(self):
        allowed = ["ops_admin", "regional_manager"]
        assert auth.role_allowed("ops_admin", allowed) is True
        assert auth.role_allowed("branch_manager", allowed) is False

    def test_none_role_blocked_when_allowlist_set(self):
        assert auth.role_allowed(None, ["ops_admin"]) is False


class FakeStop(Exception):
    pass


class FakeSt:
    """Minimal Streamlit stand-in for gate tests."""
    def __init__(self, session=None, button_returns=False):
        self.session_state = session or {}
        self.stopped = False
        self.errors = []
        self.warnings = []
        self._button_returns = button_returns

    def stop(self):
        self.stopped = True
        raise FakeStop()

    def markdown(self, *a, **k):
        pass

    def error(self, msg):
        self.errors.append(msg)

    def warning(self, msg):
        self.warnings.append(msg)

    def caption(self, *a, **k):
        pass

    def button(self, *a, **k):
        return self._button_returns

    def columns(self, spec):
        n = spec if isinstance(spec, int) else len(spec)
        return [self for _ in range(n)]

    # context-manager use (with cols[1]:) and form
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def form(self, *a, **k):
        return self

    def text_input(self, *a, **k):
        return ""

    def form_submit_button(self, *a, **k):
        return False

    def rerun(self):
        pass


class TestRequireLoginGate:
    def test_unauthenticated_renders_login_and_stops(self):
        st = FakeSt(session={})
        with pytest.raises(FakeStop):
            auth.require_login(st, db_path=":memory:", app_title="Test")
        assert st.stopped

    def test_authenticated_passes_and_refreshes_stamp(self):
        user = {"username": "u", "role": "ops_admin"}
        st = FakeSt(session={auth.USER_KEY: user})
        out = auth.require_login(st, db_path=":memory:", app_title="Test")
        assert out is user
        assert auth.LAST_ACTIVE_KEY in st.session_state  # stamp refreshed

    def test_expired_session_forces_relogin(self):
        old = (datetime.now() - timedelta(minutes=999)).isoformat()
        st = FakeSt(session={
            auth.USER_KEY: {"username": "u", "role": "ops_admin"},
            auth.LAST_ACTIVE_KEY: old,
        })
        with pytest.raises(FakeStop):
            auth.require_login(st, db_path=":memory:", ttl_minutes=480)
        assert st.stopped
        assert st.session_state[auth.USER_KEY] is None  # logged out

    def test_role_not_allowed_blocks(self):
        st = FakeSt(session={
            auth.USER_KEY: {"username": "u", "role": "branch_manager"},
        })
        with pytest.raises(FakeStop):
            auth.require_login(st, db_path=":memory:",
                               allowed_roles=["ops_admin", "regional_manager"])
        assert st.stopped
        assert any("does not have access" in e for e in st.errors)

    def test_role_allowed_passes(self):
        user = {"username": "u", "role": "regional_manager"}
        st = FakeSt(session={auth.USER_KEY: user})
        out = auth.require_login(st, db_path=":memory:",
                                 allowed_roles=["ops_admin", "regional_manager"])
        assert out is user


class TestLogout:
    def test_logout_clears_user(self):
        st = FakeSt(session={
            auth.USER_KEY: {"username": "u", "role": "ops_admin"},
            auth.LAST_ACTIVE_KEY: datetime.now().isoformat(),
        })
        auth.logout(st)
        assert st.session_state[auth.USER_KEY] is None
        assert auth.LAST_ACTIVE_KEY not in st.session_state
