"""
Test quarantine for known pre-existing failures (GAP-4: green the CI baseline).

These 20 tests fail for reasons that are NOT shipping-path defects — but a
permanently-red suite masks real regressions, so they are quarantined here with
explicit reasons rather than silently rewritten:

* **xfail** = the implementation has *diverged* from the test's encoded
  expectation and which is "correct" is an open spec question. Kept as expected-
  failure so the signal is preserved — if the impl is reconciled the test will
  ``xpass`` and flag here. NOT silently rewritten to match the impl.
* **skip** = the test asserts the wrong thing (brittle literal source-string
  checks that drift with WIP) or needs missing infra (pytest-asyncio).

This is centralized + documented on purpose: one place to see exactly what is
parked and why, and to delete entries as they're properly fixed. New code must
still pass; nothing here suppresses real regressions in shipping logic.
"""

import os

import pytest


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        "real_store: test genuinely needs the installed store, not a sandbox")
    config.addinivalue_line(
        "markers",
        "real_trial_clock: test IS about the trial clock — _trial_is_not_a_clock "
        "must stand aside or it reads straight past the test's own first-run date")
    config.addinivalue_line(
        "markers",
        "network: test genuinely needs an outbound connection — the suite is "
        "offline by default so it cannot depend on a third party being up")

# nodeid substring -> reason. The impl diverged from the encoded formula/spec.
#
# Burn-down (this session): the math_validation (transfer cost 700, excess
# clamp/14-day-safety basis), mobile_api (fail-closed 401 + authed 200), and
# consignment (Dict return + micro-budget essentials scale) tests were
# reconciled to the current spec and un-quarantined — they now pass for real.
# All 13 original xfails are now reconciled and un-quarantined:
# math_validation (impl constants), mobile_api (authed calls), consignment
# (Dict API), and the 7 pos_erp_integration tests (rewritten to invariants +
# the real adapter API — which also exposed and fixed a broken /erp/sync
# bridge endpoint calling a never-shipped OrderEngine.load_from_erp).
_XFAIL = {}

# All 6 original skips are also reconciled: the 5 source-string tests now
# assert wiring/concepts instead of UI copy or superseded constants, and
# test_v10_parity was rewritten from an assertion-free async smoke script into
# a real sync test. The quarantine is EMPTY — keep it that way: new failures
# get fixed, not parked.
_SKIP = {}


def pytest_collection_modifyitems(config, items):
    for item in items:
        nid = item.nodeid.replace("\\", "/")
        for sub, reason in _SKIP.items():
            if sub in nid:
                item.add_marker(pytest.mark.skip(reason=f"[quarantined] {reason}"))
                break
        else:
            for sub, reason in _XFAIL.items():
                if sub in nid:
                    item.add_marker(pytest.mark.xfail(reason=f"[quarantined] {reason}",
                                                      strict=False))
                    break


# ── the real store is not a fixture ──────────────────────────────────────
# test_customer_flow rendered the onboarding wizard with a bare MagicMock, whose
# button() is truthy — so every setup button read as clicked and the render
# actually executed apply_multi_demo() against the real project root. The
# developer's own rhapta_multi_store.db was deleted and rebuilt, and their
# onboarding record rewritten, on every run of the suite. It passed the whole
# time; it only surfaced when Windows refused to delete an open file.
#
# This guard makes that class of escape fail loudly instead of silently
# destroying the working install.
_REAL_DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))), "oasis", "data")


#: Tests do not need 4,000 SKUs to prove a control tree builds or an accessor
#: returns the right shape — and paying for the full catalogue on every store
#: fixture took the suite from five minutes to twenty. Anything that genuinely
#: needs the full range can setenv it back.
DEMO_SKUS_IN_TESTS = "150"
DEMO_HISTORY_DAYS_IN_TESTS = "7"


@pytest.fixture(autouse=True)
def _small_demo_catalogue(monkeypatch):
    monkeypatch.setenv("OASIS_DEMO_MAX_SKUS", DEMO_SKUS_IN_TESTS)
    monkeypatch.setenv("OASIS_DEMO_HISTORY_DAYS", DEMO_HISTORY_DAYS_IN_TESTS)
    yield


@pytest.fixture(autouse=True)
def _never_touch_the_real_store(monkeypatch, tmp_path_factory, request):
    """Point store writes away from the install unless a test opts in.

    The sandbox goes in its OWN temp dir, never inside the test's ``tmp_path``:
    several tests assert their tmp_path is empty, and a stray directory there
    would fail them for the wrong reason.

    Opt out with @pytest.mark.real_store for anything that genuinely must read
    the installed store.
    """
    if request.node.get_closest_marker("real_store"):
        yield
        return

    real_before = _snapshot_real_store()
    if not os.environ.get("OASIS_DB_PATH"):
        sandbox = tmp_path_factory.mktemp("store_sandbox")
        monkeypatch.setenv("OASIS_DB_PATH", str(sandbox / "store.db"))
    yield
    after = _snapshot_real_store()
    changed = {k for k in after if real_before.get(k) != after[k]}
    assert not changed, (
        f"test wrote to the REAL store: {sorted(changed)}. Pass root=/db_path "
        f"under tmp_path, or mark the test @pytest.mark.real_store.")


#: The env vars that repoint OASIS at a data source OTHER than the one a test
#: set up. ``_never_touch_the_real_store`` above pins OASIS_DB_PATH, but these
#: three outrank it: OASIS_POS_DB_URL makes ``has_distinct_pos()`` true, and
#: OASIS_ERP=odoo bypasses the store database entirely.
_DATA_SOURCE_ENV = ("OASIS_POS_DB_URL", "OASIS_DB_URL", "OASIS_ERP")


@pytest.fixture(autouse=True)
def _no_inherited_data_source(monkeypatch):
    """Ignore whatever POS/ERP the DEVELOPER has configured.

    Porting work leaves these set as persistent user environment variables —
    after the RXL port, OASIS_POS_DB_URL pointed at a SQL Server in Docker on
    the dev machine. Any test that built a store under tmp_path then read it
    back through ``desktop.data.get_adapter`` got an adapter aimed at THAT
    server instead, and when the container was not running the read failed and
    was swallowed into an empty list: a five-store network asserted as zero
    stores, with the traceback nowhere in the output.

    Several test files already delenv'd these one by one, which is the same fix
    written five times and forgotten on the sixth. A test that genuinely wants
    one of them sets it itself — this fixture runs first, so that still wins.
    """
    for var in _DATA_SOURCE_ENV:
        monkeypatch.delenv(var, raising=False)
    yield


@pytest.fixture(autouse=True)
def _trial_is_not_a_clock(monkeypatch, request):
    """Pin the evaluation trial open, so the suite does not depend on WHEN it runs.

    The install's trial is anchored to a first-run date on the developer's
    machine. Fourteen days later it lapses, the module gates close, and tests
    that never mentioned licensing start failing on content they do assert —
    ``test_operations_still_shows_the_order_book_read_only`` went red overnight
    because the ops view had swapped its order book for "contact iLink to
    activate Network (Transfers)". Nothing in the code had changed; the date had.

    Same family as ``_no_inherited_data_source``: ambient machine state leaking
    into assertions.

    THE OPT-OUT IS NOT OPTIONAL. This patches ``_first_run`` itself, so a test
    that establishes its trial posture by writing a first-run date — which is
    how the trial is actually anchored — has that date read straight past.
    ``_trial_days_left()`` then returns a flat 14 no matter what the test set
    up, and every assertion about expiry or restart fails while the code is
    perfectly correct. Four tests were red for exactly this reason.

    The docstring here used to claim such tests "set their own posture after
    this fixture runs, so they still win". That is only true of tests that
    patch the same method; one that writes state cannot win, because the state
    is never read. Mark those ``@pytest.mark.real_trial_clock`` and this
    fixture stands aside.
    """
    if request.node.get_closest_marker("real_trial_clock"):
        yield
        return
    from datetime import date
    try:
        from oasis.logic import license_manager as LM
    except Exception:                    # licensing not importable in this env
        yield
        return
    monkeypatch.setenv("OASIS_TRIAL_DAYS", "14")
    monkeypatch.setattr(LM.OfflineLicenseManager, "_first_run",
                        lambda self: date.today(), raising=False)
    yield


# ── the suite does not go online ─────────────────────────────────────────
# A test called data.fetch_region_data(layers=[]) meaning "fetch nothing".
# `layers or (...)` collapsed the empty list into "fetch everything", so the
# test really did hit Overpass and WorldPop, wrote 100 competitor rows, 6,912
# population cells and 5,209 POIs into the developer's oasis/data, and took 62
# seconds. It PASSED the whole time. The same class of escape would, on a
# customer's machine or in CI, make the suite depend on a third party being up.
#
# So the default is offline. Loopback stays open — a test may bind a local
# server — and anything that genuinely needs the internet marks itself.
_LOCAL_HOSTS = {"127.0.0.1", "::1", "localhost", "0.0.0.0", ""}


class NetworkUseInTests(RuntimeError):
    """Raised when a test reaches for a host it did not declare."""


def _is_local(address) -> bool:
    host = address[0] if isinstance(address, (tuple, list)) and address else address
    return str(host) in _LOCAL_HOSTS


@pytest.fixture(autouse=True)
def _no_network(monkeypatch, request):
    """Block outbound connections unless a test opts in.

    Patched at the socket layer on purpose: requests, urllib and httpx all sit
    on top of it, so one guard covers every client a test might reach for.
    DNS is left alone — resolving a name costs nothing and some libraries do it
    at import time.

    Opt out with @pytest.mark.network for a test that genuinely must go out.
    """
    if request.node.get_closest_marker("network"):
        yield
        return

    import socket

    real_connect = socket.socket.connect
    real_connect_ex = socket.socket.connect_ex
    real_create = socket.create_connection

    def _blocked(address):
        return NetworkUseInTests(
            f"test tried to reach {address!r}. The suite runs offline so it "
            f"cannot depend on a third party being up, and so a fetch cannot "
            f"quietly write into the developer's oasis/data. Stub the client, "
            f"or mark the test @pytest.mark.network.")

    def _guarded_connect(self, address, *a, **kw):
        if not _is_local(address):
            raise _blocked(address)
        return real_connect(self, address, *a, **kw)

    def _guarded_connect_ex(self, address, *a, **kw):
        if not _is_local(address):
            raise _blocked(address)
        return real_connect_ex(self, address, *a, **kw)

    def _guarded_create(address, *a, **kw):
        if not _is_local(address):
            raise _blocked(address)
        return real_create(address, *a, **kw)

    monkeypatch.setattr(socket.socket, "connect", _guarded_connect)
    monkeypatch.setattr(socket.socket, "connect_ex", _guarded_connect_ex)
    monkeypatch.setattr(socket, "create_connection", _guarded_create)
    yield


def _snapshot_real_store() -> dict:
    """mtimes of the installed store DBs and onboarding record."""
    out = {}
    if not os.path.isdir(_REAL_DATA_DIR):
        return out
    # The onboarding record is ".oasis_onboarding.json" (leading dot) — it is
    # what decides which store the app opens, so a test rewriting it silently
    # repoints the whole install.
    for name in os.listdir(_REAL_DATA_DIR):
        if name.endswith(".db") or name == ".oasis_onboarding.json":
            p = os.path.join(_REAL_DATA_DIR, name)
            try:
                out[name] = os.path.getmtime(p)
            except OSError:
                pass
    return out
