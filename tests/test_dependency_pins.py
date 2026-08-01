"""The declared environment must be able to run the code (Phase 4 blocker).

requirements pinned flet==0.25.2 while oasis/desktop was written against the
ft.Icons / ft.Colors enum API introduced in 0.26 (it replaced ft.icons /
ft.colors). Anyone building from the declared environment — which is exactly
what `flet pack` and a clean client install do — would get AttributeError on
every view. It went unnoticed because this machine happened to have 0.28.3
installed, so the code ran here and nowhere else.

These tests tie the pin to the API the code actually uses, in both directions.
"""

import io
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

_ROOT = os.path.join(os.path.dirname(__file__), '..')
_REQ = os.path.join(_ROOT, "requirements.txt")
_LOCK = os.path.join(_ROOT, "requirements.lock.txt")

#: first flet release with the ft.Icons / ft.Colors enum API
MIN_FLET = (0, 26)


def _pins(path: str) -> dict:
    """{package: version} for pinned (==) requirements in a file."""
    out = {}
    with io.open(path, encoding="utf-8-sig") as f:
        for line in f:
            m = re.match(r'^([A-Za-z0-9_.\-]+)==([0-9][^\s;#]*)', line.strip())
            if m:
                out[m.group(1).lower()] = m.group(2)
    return out


def _tup(v: str) -> tuple:
    return tuple(int(p) for p in re.findall(r'\d+', v)[:3])


def test_flet_is_pinned_in_both_files():
    for path in (_REQ, _LOCK):
        assert "flet" in _pins(path), f"flet must be pinned in {os.path.basename(path)}"


def test_flet_pin_supports_the_api_the_desktop_code_uses():
    """The regression: a pin below 0.26 cannot run oasis/desktop at all."""
    for path in (_REQ, _LOCK):
        ver = _pins(path)["flet"]
        assert _tup(ver)[:2] >= MIN_FLET, (
            f"{os.path.basename(path)} pins flet=={ver}, but oasis/desktop uses "
            f"the ft.Icons/ft.Colors API added in {MIN_FLET[0]}.{MIN_FLET[1]}"
        )


def test_requirements_and_lock_agree_on_flet():
    req, lock = _pins(_REQ), _pins(_LOCK)
    for pkg in ("flet", "flet-desktop"):
        if pkg in req and pkg in lock:
            assert req[pkg] == lock[pkg], (
                f"{pkg} drifted: requirements.txt={req[pkg]} lock={lock[pkg]}")


def test_desktop_window_backend_is_declared():
    """`--mode desktop` opens a native window, which needs flet-desktop."""
    assert "flet-desktop" in _pins(_REQ), \
        "flet-desktop must be declared — ft.app() needs it for a native window"


def test_installed_flet_actually_provides_the_api():
    """Code and environment must agree here, not just on paper."""
    ft = pytest.importorskip("flet")
    assert hasattr(ft, "Icons"), \
        "installed flet lacks ft.Icons — oasis/desktop cannot run against it"
    assert ft.Icons.HOME is not None


def test_installed_flet_satisfies_the_pin():
    md = pytest.importorskip("importlib.metadata")
    try:
        installed = md.version("flet")
    except Exception:                       # not installed in this env
        pytest.skip("flet not installed")
    assert _tup(installed)[:2] >= MIN_FLET
    assert installed == _pins(_REQ)["flet"], (
        f"installed flet {installed} != pinned {_pins(_REQ)['flet']}; the "
        "declared environment is not the one being tested")
