"""Suite SSO (audit F1) + multi-store wizard path (audit B3)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.ui import auth


class FakeSt:
    def __init__(self, sid=None):
        self.session_state = {}
        self.query_params = {"sid": sid} if sid else {}


def test_adopt_valid_sid(monkeypatch):
    monkeypatch.setattr("oasis.logic.auth_manager.validate_session",
                        lambda sid, db: {"username": "ops_admin", "role": "admin"}
                        if sid == "good" else None)
    st = FakeSt(sid="good")
    user = auth.try_adopt_sso(st, "x.db")
    assert user and user["username"] == "ops_admin"
    assert st.session_state[auth.USER_KEY] == user
    assert st.session_state[auth.SID_KEY] == "good"
    assert st.session_state[auth.LAST_ACTIVE_KEY]


def test_invalid_sid_is_rejected(monkeypatch):
    monkeypatch.setattr("oasis.logic.auth_manager.validate_session",
                        lambda sid, db: None)
    st = FakeSt(sid="forged")
    assert auth.try_adopt_sso(st, "x.db") is None
    assert auth.USER_KEY not in st.session_state


def test_no_sid_is_a_noop():
    assert auth.try_adopt_sso(FakeSt(), "x.db") is None


def test_sid_list_form_handled(monkeypatch):
    monkeypatch.setattr("oasis.logic.auth_manager.validate_session",
                        lambda sid, db: {"username": "u"} if sid == "s1" else None)
    st = FakeSt()
    st.query_params = {"sid": ["s1"]}
    assert auth.try_adopt_sso(st, "x.db")["username"] == "u"


def test_suite_links_carry_the_sid():
    from oasis.ui.home import suite_links

    class LinkSt(FakeSt):
        class _Side:
            def __init__(self):
                self.md = ""

            def markdown(self, text, **kw):
                self.md = text
        def __init__(self):
            super().__init__()
            self.sidebar = self._Side()

    st = LinkSt()
    st.session_state[auth.SID_KEY] = "tok123"
    suite_links(st, "ops")
    assert "sid=tok123" in st.sidebar.md
    # hub keeps its own supplier auth — no OASIS sid leaks to it
    assert "8700/?sid" not in st.sidebar.md


def test_apply_multi_demo_records_demo_source(tmp_path, monkeypatch):
    """Note: this used to monkeypatch install_profile.init_install, which meant
    it passed on a machine where the real call could not work — the analysis
    flagged it as a test exercising a shape production never runs. apply_multi_demo
    no longer routes through init_install at all (S3); see tests/test_wave_a.py
    for the real code-resident build. Kept here for the recording behaviour."""
    from oasis.logic import onboarding as OB
    root = tmp_path.as_posix()
    os.makedirs(os.path.join(root, "oasis", "data"), exist_ok=True)
    monkeypatch.setattr("oasis.logic.multi_store_pos.seed_multi_store_history",
                        lambda db, **kw: {})
    monkeypatch.setenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
    monkeypatch.setenv("OASIS_DB_PATH", os.path.join(root, "oasis", "data", "m.db"))
    s = OB.apply_multi_demo(root=root)
    assert s["profile"] == "multi"
    ob = OB.load_onboarding(root)
    assert ob["source"] == "demo" and ob["multi"] is True
    assert OB.is_demo(root), "multi demo must carry the SAMPLE banner"
