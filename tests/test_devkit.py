"""The dev toolkit boundary.

devkit/ holds tooling outside the operational scope of a client install. Two
things must stay true, and both fail silently otherwise:

  * nothing in devkit/ ever reaches a client zip
  * every script in devkit/ can still find ``oasis`` from its new home

The second one has no other coverage: these are operator-run scripts with no
callers, so a broken ``sys.path`` bootstrap surfaces as ModuleNotFoundError the
next time a human runs one, possibly months later.
"""

import ast
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.release_packager import should_ship_clean, _OASIS_DEV_ONLY

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEVKIT = os.path.join(ROOT, "devkit")

#: the exact bootstrap every devkit script must use to reach the repo root.
BOOTSTRAP = ("sys.path.insert(0, os.path.dirname(os.path.dirname("
             "os.path.abspath(__file__))))")


def _devkit_scripts():
    if not os.path.isdir(DEVKIT):
        return []
    return sorted(f for f in os.listdir(DEVKIT) if f.endswith(".py"))


def test_devkit_exists_and_is_not_empty():
    assert _devkit_scripts(), "devkit/ has no scripts — was it moved or deleted?"


@pytest.mark.parametrize("script", _devkit_scripts())
def test_no_devkit_script_ships(script):
    """A client zip must never carry dev tooling."""
    ok, _why = should_ship_clean(f"devkit/{script}")
    assert not ok, f"devkit/{script} would ship to a client"


def test_devkit_readme_does_not_ship():
    assert not should_ship_clean("devkit/README.md")[0]
    assert not should_ship_clean("devkit/modes.bat")[0]


@pytest.mark.parametrize("script", _devkit_scripts())
def test_every_devkit_script_parses(script):
    """Cheap guard: these have no import-time coverage anywhere else."""
    with open(os.path.join(DEVKIT, script), encoding="utf-8") as fh:
        ast.parse(fh.read(), filename=script)


@pytest.mark.parametrize("script", _devkit_scripts())
def test_every_devkit_script_can_reach_the_repo_root(script):
    """devkit/ does not contain oasis/, so each script must add the PARENT.

    Scripts that never import oasis are exempt — but if a script does import it,
    the bootstrap has to be there and has to be the two-level one.
    """
    with open(os.path.join(DEVKIT, script), encoding="utf-8") as fh:
        src = fh.read()
    if "oasis" not in src:
        pytest.skip(f"{script} does not import oasis")
    assert BOOTSTRAP in src, (
        f"devkit/{script} imports oasis but does not add the repo root to "
        f"sys.path. Use exactly:\n    {BOOTSTRAP}"
    )


def test_dev_only_modules_are_named_and_still_exist():
    """_OASIS_DEV_ONLY must not rot into a list of deleted paths.

    A stale entry is worse than none: it reads as 'we deliberately withhold
    this' when the file is simply gone.
    """
    assert _OASIS_DEV_ONLY, "the dev-only exclusion list is empty"
    for rel in _OASIS_DEV_ONLY:
        assert os.path.exists(os.path.join(ROOT, rel)), \
            f"{rel} is excluded from the zip but no longer exists — drop it"
        assert not should_ship_clean(rel)[0], f"{rel} leaked into the zip"


def test_dev_only_modules_have_a_devkit_importer():
    """Each excluded module must be reachable from devkit/ — otherwise it is
    not 'dev-only', it is dead, and it should be deleted rather than hidden."""
    blob = ""
    for script in _devkit_scripts():
        with open(os.path.join(DEVKIT, script), encoding="utf-8") as fh:
            blob += fh.read()
    for rel in _OASIS_DEV_ONLY:
        module = os.path.basename(rel)[:-3]          # strip .py
        assert module in blob, (
            f"{rel} is kept out of the zip as 'dev-only' but nothing in "
            f"devkit/ imports it — it is dead code, delete it instead"
        )
