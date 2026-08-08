"""Seed credentials: no known password may reach a REAL store.

Commit cdcf0b7 ("sec: remove hardcoded credentials") replaced the literal
"oasis2026" seed password with a random one-time password. A later change
reinstated it as the fallback in _resolve_seed_password, which silently put a
publicly-known credential on every real client install — and no test caught it.

These are that missing test. The split they lock down:

  * SAMPLE data may use a known password (that is the point of a demo store,
    and it sits behind a permanent SAMPLE banner)
  * a REAL store (empty / init / connect) must never receive it — its accounts
    get a random one-time password until first-run setup sets the operator's
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import auth_manager as AM
from oasis.logic import onboarding as OB


@pytest.fixture
def root(tmp_path, monkeypatch):
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.delenv("OASIS_SEED_PASSWORD", raising=False)
    monkeypatch.delenv("OASIS_SEED_PASSWORD_OPS_ADMIN", raising=False)
    monkeypatch.setenv("OASIS_DB_PATH",
                       str(tmp_path / "oasis" / "data" / "store.db"))
    return str(tmp_path)


# ── the fallback itself ──────────────────────────────────────────────────
def test_seed_password_fallback_is_never_a_source_literal(monkeypatch):
    monkeypatch.delenv("OASIS_SEED_PASSWORD", raising=False)
    monkeypatch.delenv("OASIS_SEED_PASSWORD_OPS_ADMIN", raising=False)
    a = AM._resolve_seed_password("ops_admin")
    b = AM._resolve_seed_password("ops_admin")
    assert a != b, "fallback must be random per call, not a fixed literal"
    assert a != "oasis2026", "the removed hardcoded credential is back"
    assert len(a) >= 12


def test_env_still_controls_the_seed_password(monkeypatch):
    monkeypatch.setenv("OASIS_SEED_PASSWORD", "FromTheEnv1")
    assert AM._resolve_seed_password("ops_admin") == "FromTheEnv1"
    monkeypatch.setenv("OASIS_SEED_PASSWORD_OPS_ADMIN", "PerUserPw1")
    assert AM._resolve_seed_password("ops_admin") == "PerUserPw1"


# ── real vs sample stores ────────────────────────────────────────────────
def test_real_store_rejects_the_known_demo_password(root):
    """The regression: a catalogue/empty store must not ship a public password."""
    OB.apply_empty(store_name="Acme Duka", root=root)
    db = OB.resolved_db_path(root)
    assert AM.authenticate("ops_admin", OB.DEMO_SEED_PASSWORD, db) is None, \
        "a REAL store accepted the sample password"


def test_sample_store_stays_frictionless(root):
    """Demo data keeps a known login — touring the consoles is the point."""
    OB.apply_demo(root=root)
    db = OB.resolved_db_path(root)
    assert AM.authenticate("ops_admin", OB.DEMO_SEED_PASSWORD, db), \
        "the sample store must log in out of the box"
    assert OB.is_demo(root), "…and must carry the SAMPLE banner"


def test_multi_store_sample_stays_frictionless_too(root):
    """The same promise for the OTHER demo card — which did not keep it.

    apply_demo passed DEMO_SEED_PASSWORD; apply_multi_demo never did, so every
    account in the 5-store demo got a random one-time password logged once at
    build time. It stayed invisible while OASIS.bat exported
    OASIS_SEED_PASSWORD unconditionally; removing that (right, because it gave
    REAL stores a published credential) left this path with no way in and the
    operator locked out of their own demo network.
    """
    OB.apply_multi_demo(root=root)
    db = OB.resolved_db_path(root)
    assert AM.authenticate("ops_admin", OB.DEMO_SEED_PASSWORD, db), \
        "the multi-store sample must log in out of the box"
    assert OB.is_demo(root), "…and must carry the SAMPLE banner"


def test_reseeding_never_announces_a_password_it_did_not_store(root, caplog):
    """The lockout bug: told a password that was never written.

    seed_users generated a fresh random password per user on EVERY call and
    logged it as the way in — then INSERT OR IGNORE discarded it because the
    row already existed, leaving the old hash in place. Every boot printed a
    new plausible credential that could not work, and "Seeded 9 default users"
    claimed success. The operator could not log into their own store and no
    logged password would ever match.
    """
    import logging
    from oasis.logic.db_connector import ensure_oasis_tables
    db = os.environ["OASIS_DB_PATH"]
    os.makedirs(os.path.dirname(db), exist_ok=True)
    ensure_oasis_tables(db)
    AM.set_password(db, "ops_admin", "OperatorChosen123")

    with caplog.at_level(logging.WARNING):
        caplog.clear()
        AM.seed_users(db)                      # a re-seed over existing rows

    announced = [r.message for r in caplog.records
                 if "one-time password" in r.message]
    assert not announced, \
        f"announced a password for an account it did not write: {announced}"
    # …and the existing password is untouched.
    assert AM.authenticate("ops_admin", "OperatorChosen123", db)


def test_seed_users_reports_what_it_actually_wrote(root):
    from oasis.logic.db_connector import ensure_oasis_tables
    db = os.environ["OASIS_DB_PATH"]
    os.makedirs(os.path.dirname(db), exist_ok=True)
    ensure_oasis_tables(db)
    assert AM.seed_users(db) == 0, "re-seed claimed to write rows it skipped"


def test_the_demo_password_does_not_leak_into_the_environment(root):
    """Setting it for the build must not leave it set for whatever runs next."""
    before = os.environ.get("OASIS_SEED_PASSWORD")
    OB.apply_multi_demo(root=root)
    assert os.environ.get("OASIS_SEED_PASSWORD") == before


# ── the first-run set-password path ──────────────────────────────────────
def test_first_run_password_gives_the_operator_control(root):
    """Exactly what the desktop first-run screen does.

    Note ensure_oasis_tables() *also* seeds the default accounts (with random
    one-time passwords), so first-run must ROTATE ops_admin rather than assume
    it gets to seed it — an earlier version of this flow only called seed_users
    when has_accounts() was False, and therefore never applied the operator's
    chosen password at all.
    """
    from oasis.logic.db_connector import ensure_oasis_tables
    db = os.environ["OASIS_DB_PATH"]
    os.makedirs(os.path.dirname(db), exist_ok=True)
    ensure_oasis_tables(db)

    if AM.has_accounts(db):
        AM.set_password(db, "ops_admin", "ChosenPw123")
    else:
        AM.seed_users(db, password="ChosenPw123")

    assert AM.authenticate("ops_admin", "ChosenPw123", db), \
        "the operator's chosen password must actually work after first-run setup"
    assert AM.authenticate("ops_admin", "oasis2026", db) is None


# ── the lockout this nearly shipped ──────────────────────────────────────
def test_real_store_asks_for_a_password_instead_of_an_impossible_login(root):
    """Seeding ALWAYS runs (ensure_oasis_tables calls seed_users), so a real
    store always has accounts — holding random passwords that were only logged.
    Reading "accounts exist" as "the operator can sign in" is a lockout: the
    desktop would show a login form for a password nobody has ever seen."""
    OB.apply_empty(store_name="Acme Duka", root=root)
    db = OB.resolved_db_path(root)
    assert AM.has_accounts(db), "precondition: seeding always creates accounts"
    assert OB.needs_admin_password(root) is True, \
        "a real store must be asked to SET a password, not to guess one"


def test_password_prompt_stops_once_the_operator_has_set_one(root):
    OB.apply_empty(store_name="Acme Duka", root=root)
    db = OB.resolved_db_path(root)
    AM.set_password(db, "ops_admin", "ChosenPw123")
    OB.mark_admin_password_set(root)
    assert OB.needs_admin_password(root) is False
    assert AM.authenticate("ops_admin", "ChosenPw123", db)


def test_sample_store_is_never_asked_to_set_a_password(root):
    """The demo login is published in the wizard — prompting would be noise."""
    OB.apply_demo(root=root)
    assert OB.needs_admin_password(root) is False


def test_fresh_install_is_not_asked_either(root):
    assert OB.needs_admin_password(root) is False, "no store yet — wizard first"


def test_set_password_rejects_short_and_unknown(root):
    from oasis.logic.db_connector import ensure_oasis_tables
    db = os.environ["OASIS_DB_PATH"]
    os.makedirs(os.path.dirname(db), exist_ok=True)
    ensure_oasis_tables(db)
    AM.seed_users(db, password="ChosenPw123")
    with pytest.raises(ValueError):
        AM.set_password(db, "ops_admin", "short")
    with pytest.raises(ValueError):
        AM.set_password(db, "nobody_here", "LongEnough123")
    AM.set_password(db, "ops_admin", "RotatedPw456")
    assert AM.authenticate("ops_admin", "RotatedPw456", db)
