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

import pytest

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
