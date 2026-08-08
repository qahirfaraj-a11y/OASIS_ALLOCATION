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
