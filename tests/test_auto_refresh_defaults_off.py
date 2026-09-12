"""Auto-refresh must be opt-in, because ON it cancels long-running views.

The sidebar checkbox drives st_autorefresh, and the branch behind it calls
st.cache_data.clear() on every tick. At the 10-second default that clears
every cached fetch and reruns the script, so any view whose computation takes
longer than one tick is restarted before it can finish and never renders.

Measured on a 39,728-SKU store: 27 full product re-enrichments in 25 seconds,
and the Smart Ordering pipeline never reached the line that caches its result.
The ordering tab was unreachable on any real catalogue for as long as this
defaulted to True.

A default is invisible to functional testing -- the widget works either way --
so this asserts the default in the source.
"""
import ast
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONSOLE = os.path.join(ROOT, "ops_dashboard.py")


def _checkbox_defaults(path):
    """[(label, value_kwarg_literal)] for every st.*.checkbox call."""
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)
    out = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        if getattr(node.func, "attr", None) != "checkbox":
            continue
        label = None
        if node.args and isinstance(node.args[0], ast.Constant):
            label = node.args[0].value
        value = None
        for kw in node.keywords:
            if kw.arg == "value" and isinstance(kw.value, ast.Constant):
                value = kw.value.value
        out.append((label, value, node.lineno))
    return out


@pytest.mark.skipif(not os.path.exists(CONSOLE), reason="console not present")
def test_the_auto_refresh_checkbox_defaults_off():
    boxes = [b for b in _checkbox_defaults(CONSOLE)
             if b[0] and "Auto-refresh" in b[0]]
    assert boxes, "the Auto-refresh checkbox is gone or was renamed"
    for label, value, line in boxes:
        assert value is False, (
            f"{CONSOLE}:{line} — {label!r} defaults to {value!r}. ON, each "
            f"tick clears the data cache and reruns the page, which cancels "
            f"the ordering pipeline before it can finish on a real catalogue.")


@pytest.mark.skipif(not os.path.exists(CONSOLE), reason="console not present")
def test_the_cache_clear_stays_behind_the_opt_in():
    """cache_data.clear() must not escape the `if auto_refresh:` branch.

    Outside it, the clear would run on every rerun regardless of the checkbox
    and the default would stop protecting anything. The button that clears on
    an explicit "Pull latest POS bills" is a deliberate, user-initiated clear
    and is matched by its own st.rerun() on the same line range.
    """
    with open(CONSOLE, encoding="utf-8") as f:
        src = f.read()
    tree = ast.parse(src, filename=CONSOLE)

    def _is_blanket_clear(call):
        """st.cache_data.clear() — the one that empties EVERY cached fetch.

        Targeted clears like load_all_stocks.clear() are a different thing:
        they invalidate one function after an action that really did change
        its data, and they are not what starved the ordering pipeline.
        """
        f = call.func
        return (getattr(f, "attr", None) == "clear"
                and getattr(getattr(f, "value", None), "attr", None)
                == "cache_data")

    guarded = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.If) and getattr(node.test, "id", None) == "auto_refresh":
            for sub in ast.walk(node):
                if isinstance(sub, ast.Call) and _is_blanket_clear(sub):
                    guarded.add(sub.lineno)

    lines = src.splitlines()
    found = 0
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and _is_blanket_clear(node)):
            continue
        found += 1
        if node.lineno in guarded:
            continue
        window = "\n".join(lines[max(0, node.lineno - 4):node.lineno + 2])
        assert "button" in window, (
            f"{CONSOLE}:{node.lineno} — st.cache_data.clear() outside both "
            f"the auto_refresh branch and a button handler runs on EVERY "
            f"rerun, which defeats the default this module just set.")
    assert found, "st.cache_data.clear() is gone; this guard needs rewriting"
