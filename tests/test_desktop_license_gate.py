"""Desktop Phase 3 / P3.0 — licensing enforcement in the native window.

Before this, ``console_gate`` WAS the decision and it only spoke Streamlit, so
the four browser consoles locked at trial expiry while ``--mode desktop`` — the
front door OASIS.bat calls RECOMMENDED — opened straight into store data
(Phase 3 finding R-2).

These tests hold the two properties that finding turns on:

  * the DECISION is shared. ``gate_status`` is what both front doors read, and
    it blocks exactly when the console lock screen would have fired.
  * the native window RENDERS it. A locked install builds a lock screen that
    still offers all three doors (activate / export / what a license buys), and
    a partial license renders upsell stubs instead of paid content.

Every view here is built as a real Flet control tree, headless — the harness
that caught the Phase-1 assumed-API defects. "It launched" is not evidence.
"""

import json
import os
import sys
from datetime import date, timedelta

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.desktop import data as D
from oasis.desktop.views.license_view import (build_lock_view, build_notice,
                                              build_upsell)
from oasis.desktop.views.ops_view import TAB_MODULES, build_ops_view
from oasis.logic import license_manager as LM


def _count(control) -> int:
    """Walk a Flet control tree; proves every node constructed."""
    n = 0
    stack = [control]
    while stack:
        c = stack.pop()
        n += 1
        for attr in ("controls", "content", "tabs"):
            v = getattr(c, attr, None)
            if isinstance(v, list):
                stack.extend(v)
            elif v is not None and hasattr(v, "_get_control_name"):
                stack.append(v)
    return n


def _text(control) -> str:
    """All text in a control tree, lowercased — for asserting what a user reads."""
    out, stack = [], [control]
    while stack:
        c = stack.pop()
        for attr in ("value", "text", "hint_text", "label"):
            v = getattr(c, attr, None)
            if isinstance(v, str):
                out.append(v)
        for attr in ("controls", "content", "tabs"):
            v = getattr(c, attr, None)
            if isinstance(v, list):
                stack.extend(v)
            elif v is not None and hasattr(v, "_get_control_name"):
                stack.append(v)
    return " ".join(out).lower()


@pytest.fixture
def install(tmp_path, monkeypatch):
    """An install whose license state the test controls completely."""
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DB_PATH",
                       str(tmp_path / "oasis" / "data" / "store.db"))
    monkeypatch.setenv("OASIS_LICENSE_KEY", str(tmp_path / "oasis_license.key"))
    monkeypatch.setattr(LM, "_ROOT", str(tmp_path))
    monkeypatch.setenv("OASIS_LICENSE_SALT", "test-salt")
    D.reset_adapter()
    yield tmp_path
    D.reset_adapter()


def _expire_trial(monkeypatch, root):
    """No key, trial exhausted — the day-15 state."""
    monkeypatch.setenv("OASIS_TRIAL_DAYS", "14")
    monkeypatch.setattr(LM.OfflineLicenseManager, "_first_run",
                        lambda self: date.today() - timedelta(days=90))


def _write_key(root, modules, days=365):
    """A genuine key (signed with the test salt) for exactly `modules`."""
    mgr = LM.OfflineLicenseManager()
    tenant = "TESTCO"
    expiry = (date.today() + timedelta(days=days)).isoformat()
    key = {"tenant_id": tenant, "expiry_date": expiry,
           "authorized_modules": {m: mgr._fingerprint(tenant, m, expiry)
                                  for m in modules}}
    path = os.path.join(str(root), "oasis_license.key")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(key, f)
    return key


# ── the shared decision ──────────────────────────────────────────────────
def test_gate_status_blocks_exactly_when_the_console_lock_fires(install,
                                                                monkeypatch):
    _expire_trial(monkeypatch, install)
    gate = LM.gate_status("core")
    assert gate["mode"] == "locked" and gate["blocked"] is True
    assert gate["notice"][0] == "error"


def test_gate_status_does_not_block_during_evaluation(install, monkeypatch):
    monkeypatch.setenv("OASIS_TRIAL_DAYS", "14")
    monkeypatch.setattr(LM.OfflineLicenseManager, "_first_run",
                        lambda self: date.today())
    gate = LM.gate_status("core")
    assert gate["mode"] == "evaluation" and gate["blocked"] is False
    assert "evaluation" in gate["notice"][1].lower()


def test_gate_status_warns_before_a_licence_lapses(install):
    _write_key(install, ["core"], days=10)
    gate = LM.gate_status("core")
    assert gate["mode"] == "licensed" and gate["blocked"] is False
    assert gate["notice"][0] == "warning" and "renews" in gate["notice"][1]


def test_desktop_gate_fails_closed_when_licensing_cannot_answer(monkeypatch):
    """An unanswerable licence subsystem must lock, not admit — as _needs_auth does."""
    def _boom(module="core", mgr=None):
        raise RuntimeError("no licence subsystem")
    monkeypatch.setattr(LM, "gate_status", _boom)
    gate = D.license_gate("core")
    assert gate["blocked"] is True and gate["mode"] == "locked"


def test_allowed_modules_degrades_to_core_not_to_everything(monkeypatch):
    monkeypatch.setattr(LM, "allowed_modules",
                        lambda *a, **k: (_ for _ in ()).throw(RuntimeError()))
    assert D.allowed_modules() == {"core"}


# ── the native rendering ─────────────────────────────────────────────────
def test_locked_install_builds_a_lock_screen_with_all_three_doors(install,
                                                                  monkeypatch):
    _expire_trial(monkeypatch, install)
    gate = LM.gate_status("core")
    view = build_lock_view(None, str(install), gate)
    assert _count(view) > 30
    body = _text(view)
    for door in ("activate", "export my data", "what a license includes"):
        assert door in body, f"the lock screen must keep the '{door}' door"


def test_the_lock_screen_never_becomes_a_dead_end_without_a_store(install,
                                                                  monkeypatch):
    """No store to export yet — the door stays, it just says so."""
    _expire_trial(monkeypatch, install)
    view = build_lock_view(None, str(install), LM.gate_status("core"))
    assert "not present yet" in _text(view)


def test_export_door_copies_the_store_a_locked_client_still_owns(install,
                                                                 monkeypatch):
    db = install / "oasis" / "data" / "store.db"
    db.write_bytes(b"sqlite-ish")
    monkeypatch.setattr(LM, "lock_screen_exports",
                        lambda root=None: {"db": str(db), "report": None})
    written = LM.copy_exports(str(install / "out"), str(install))
    assert written and os.path.exists(written[0])
    assert open(written[0], "rb").read() == b"sqlite-ish"


def test_activation_installs_a_valid_key_and_rejects_a_bad_one(install):
    key = _write_key(install, ["core", "ordering"])
    os.remove(os.path.join(str(install), "oasis_license.key"))

    ok, detail = LM.activate_key("not json at all")
    assert ok is False and "license key" in detail.lower()
    assert not os.path.exists(LM.default_key_path())

    ok, detail = LM.activate_key(json.dumps(key))
    assert ok is True and "TESTCO" in detail
    assert os.path.exists(LM.default_key_path())
    assert LM.gate_status("core")["mode"] == "licensed"


def test_notice_is_placeable_even_when_there_is_nothing_to_say():
    assert build_notice({}) is not None
    assert build_notice(None) is not None


# ── module gating in the views ───────────────────────────────────────────
@pytest.fixture
def store(tmp_path, monkeypatch):
    """A real sample store with a controllable license."""
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DB_PATH",
                       str(tmp_path / "oasis" / "data" / "store.db"))
    monkeypatch.delenv("OASIS_DB_URL", raising=False)
    monkeypatch.delenv("OASIS_POS_DB_URL", raising=False)
    D.reset_adapter()
    from oasis.logic import onboarding as OB
    OB.apply_demo(root=str(tmp_path))
    D.reset_adapter()
    yield str(tmp_path)
    D.reset_adapter()


def test_core_only_licence_paywalls_the_paid_tabs_but_not_the_core_ones(
        store, monkeypatch):
    """A starter (core-only) client keeps stock, loses ordering and network."""
    monkeypatch.setattr(D, "allowed_modules", lambda: {"core"})
    body = _text(build_ops_view(None, store))
    # the paid tabs became upsell stubs …
    assert "smart ordering module" in body
    assert "network (transfers & allocation) module" in body
    # … while the core stock cards stayed visible.
    assert "stockouts" in body and "stock value" in body


def test_full_licence_shows_the_real_panels_instead_of_upsells(store,
                                                               monkeypatch):
    monkeypatch.setattr(D, "allowed_modules",
                        lambda: set(LM.KNOWN_MODULES))
    body = _text(build_ops_view(None, store))
    assert "largest by sku count" in body      # supplier panel rendered
    assert "engines live" in body              # allocation panel rendered
    assert "contact ilink to activate" not in body   # and no upsell anywhere


def test_every_gated_tab_names_a_real_module_sku():
    """A typo in TAB_MODULES would paywall a tab forever — nothing could grant it."""
    assert set(TAB_MODULES.values()) <= set(LM.KNOWN_MODULES)


def test_upsell_sells_the_module_it_actually_locked():
    body = _text(build_upsell("network"))
    assert "network (transfers & allocation)" in body
    assert "contact ilink" in body


# ── the front door itself ────────────────────────────────────────────────
class _FakePage:
    """Enough ft.Page for main() to boot against, headless.

    The component tests above prove a lock screen can be BUILT. This proves the
    window actually routes to it — which is the whole of finding R-2. A lock
    screen nothing navigates to would have passed every other test in this file.

    Booting through ``main()`` also constructs the NavigationRail, which is how
    the ``icon_content`` removal in flet 0.28 gets caught: the view-level tests
    never touched the rail, so the app could not open on the pinned version
    while every test stayed green.
    """

    def __init__(self):
        self.controls, self.overlay = [], []
        self._session = {}
        self.session = self

    # ft.Page.session
    def contains_key(self, k):
        return k in self._session

    def get(self, k, default=None):
        return self._session.get(k, default)

    def set(self, k, v):
        self._session[k] = v

    # ft.Page
    def clean(self):
        self.controls = []

    def add(self, *controls):
        self.controls.extend(controls)

    def update(self):
        pass

    def __setattr__(self, k, v):
        object.__setattr__(self, k, v)


def test_the_desktop_front_door_locks_instead_of_opening_the_store(install,
                                                                   monkeypatch):
    """--mode desktop on an expired trial must show the lock, not the console."""
    from oasis.desktop import app as APP
    _expire_trial(monkeypatch, install)
    monkeypatch.setattr(APP, "_PROJECT_ROOT", str(install))

    page = _FakePage()
    APP.main(page)

    body = _text(ft_column(page.controls))
    assert "o.a.s.i.s. is locked" in body
    assert "activate" in body and "export my data" in body
    # and none of the console shell got drawn
    assert "signed in as" not in body
    assert "operations" not in body


def test_the_desktop_front_door_opens_when_the_licence_allows(install,
                                                              monkeypatch):
    """The gate must not become a wall — a valid licence boots through it."""
    from oasis.desktop import app as APP
    _write_key(install, ["core"])
    monkeypatch.setattr(APP, "_PROJECT_ROOT", str(install))

    page = _FakePage()
    APP.main(page)

    body = _text(ft_column(page.controls))
    assert "o.a.s.i.s. is locked" not in body


def ft_column(controls):
    """Wrap a page's controls so the text walker can descend them."""
    import flet as ft
    return ft.Column(controls=list(controls))
