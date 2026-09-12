"""The .env file has to be read before anything reads an environment variable.

The Command Center parsed .env at line 198 and ran the licence gate at line
71. An operator who put OASIS_LICENSE_SALT in .env -- the documented place for
it -- was told "OASIS_LICENSE_SALT not configured" anyway, with no way to tell
why, because the file was correct and was read, just 127 lines too late.

Import order is invisible to every functional test: the gate's behaviour is
right, the .env parser's behaviour is right, and only their sequence is wrong.
So this asserts the sequence directly, in the source.
"""
import ast
import os

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CONSOLE = os.path.join(ROOT, "ops_dashboard.py")


def _module_level_calls(path):
    """{name: first module-level line} for plain calls and their line numbers."""
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=path)
    out = {}
    for node in tree.body:                       # module level only
        if not isinstance(node, ast.Expr):
            continue
        call = node.value
        if not isinstance(call, ast.Call):
            continue
        fn = call.func
        name = getattr(fn, "id", None) or getattr(fn, "attr", None)
        if name and name not in out:
            out[name] = node.lineno
    return out


@pytest.mark.skipif(not os.path.exists(CONSOLE), reason="console not present")
def test_env_is_loaded_before_the_licence_gate_runs():
    calls = _module_level_calls(CONSOLE)
    env_line = calls.get("load_env_local")
    gate_line = calls.get("_license_gate") or calls.get("console_gate")

    assert env_line, "ops_dashboard.py no longer loads .env at module level"
    assert gate_line, "ops_dashboard.py no longer runs the licence gate"
    assert env_line < gate_line, (
        f".env is parsed at line {env_line} but the licence gate decides at "
        f"line {gate_line}. Every variable the gate reads -- OASIS_LICENSE_SALT "
        f"above all -- is still unset when it decides, so a correct .env "
        f"cannot unlock the console.")
