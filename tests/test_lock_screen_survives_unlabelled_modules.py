"""The lock screen is the one surface that must never fail.

It is all a locked operator can see. If it raises, every console dies at entry
with a traceback instead of the sentence naming what is missing -- which is
what happened: store_allocation went into KNOWN_MODULES without a matching
MODULE_LABELS entry, and the Command Center crashed with KeyError before it
could report "OASIS_LICENSE_SALT not configured".

This asserts the invariant rather than the specific module, so adding the
label later does not quietly retire the guard.
"""
import pytest

from oasis.logic import license_manager as lm


class _FakeCol:
    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


class _FakeSt:
    """Enough Streamlit to walk render_lock_screen without a browser."""

    def __init__(self):
        self.markdown_calls = []

    def markdown(self, text, **kw):
        self.markdown_calls.append(text)

    def columns(self, *a, **kw):
        n = a[0] if a else 2
        n = len(n) if isinstance(n, (list, tuple)) else int(n)
        return tuple(_FakeCol() for _ in range(n))

    # tabs unpacks like columns; __getattr__'s None would fail the unpack
    # before the code under test is ever reached.
    tabs = columns

    def __getattr__(self, _name):
        return lambda *a, **kw: None


def test_every_known_module_renders_a_label():
    missing = [m for m in lm.KNOWN_MODULES if m not in lm.MODULE_LABELS]
    st = _FakeSt()
    lm.render_lock_screen(st)          # must not raise, labelled or not
    body = "\n".join(st.markdown_calls)
    for m in lm.KNOWN_MODULES:
        assert lm.MODULE_LABELS.get(m, m.replace("_", " ").title()) in body, (
            f"{m} is in KNOWN_MODULES but never reaches the lock screen")
    # Reported, not asserted away: an unlabelled module renders a derived name,
    # which is a safe fallback and not a substitute for a real product label.
    if missing:
        print(f"unlabelled modules rendering a derived name: {missing}")


def test_an_unlabelled_module_does_not_break_the_screen(monkeypatch):
    monkeypatch.setattr(lm, "KNOWN_MODULES",
                        tuple(lm.KNOWN_MODULES) + ("a_brand_new_module",))
    st = _FakeSt()
    lm.render_lock_screen(st)
    assert "A Brand New Module" in "\n".join(st.markdown_calls)


def test_the_locked_reason_is_what_the_operator_needs_to_see():
    """The crash hid the diagnosis. Keep the diagnosis reachable."""
    s = lm.gate_status("core")
    assert s["mode"] in ("locked", "evaluation", "licensed")
    if s["mode"] == "locked":
        assert s["reason"], "a locked gate must say why"
