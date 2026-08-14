"""Every entrypoint mode is reachable, and every launcher points at a real one.

Three failures this guards, all of which shipped at least once:

  * ``preflight`` had a dispatch branch AND a docstring entry but was missing
    from the argparse ``choices`` list, so the documented install step died at
    argument parsing. The cold-start proof ran it and swallowed the error.
  * a launcher offering ``--mode <typo>`` fails only when a client picks that
    menu item, which is the worst possible time to find out.
  * a mode in ``choices`` with no dispatch branch falls through to whatever the
    final ``else`` does, silently doing the wrong thing.
"""

import io
import os
import re
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ENTRYPOINT = os.path.join(ROOT, "entrypoint.py")

#: launchers that SHIP. A broken --mode here reaches a client.
SHIPPED_LAUNCHERS = ("OASIS.bat", "install.bat", "serve.bat",
                     "register_service.bat", "unregister_service.bat")


def _source() -> str:
    return io.open(ENTRYPOINT, encoding="utf-8").read()


def _declared_modes() -> set:
    """The argparse choices list for --mode."""
    src = _source()
    m = re.search(r'"--mode"\s*,\s*\n?\s*choices=\[(.*?)\]\s*,', src, re.S)
    assert m, "could not locate the --mode choices list in entrypoint.py"
    return set(re.findall(r'"([a-z0-9-]+)"', m.group(1)))


def _dispatched_modes() -> set:
    """Modes reached by `args.mode == "x"` OR `args.mode in ("x", "y")`."""
    src = _source()
    modes = set(re.findall(r'args\.mode\s*==\s*"([a-z0-9-]+)"', src))
    for group in re.findall(r'args\.mode\s+in\s*\(([^)]*)\)', src):
        modes.update(re.findall(r'"([a-z0-9-]+)"', group))
    return modes


def test_mode_choices_parse():
    assert len(_declared_modes()) > 20, "choices list looks truncated"


def test_every_dispatched_mode_is_selectable():
    """The preflight bug: implemented and documented, but argparse said no."""
    missing = sorted(_dispatched_modes() - _declared_modes())
    assert not missing, (
        "these modes have a dispatch branch but are absent from --mode "
        f"choices, so argparse rejects them before they can run: {missing}"
    )


def test_every_selectable_mode_has_a_dispatch_branch():
    """The inverse: offered on the CLI but falls through to the default path."""
    declared = _declared_modes()
    dispatched = _dispatched_modes()
    # modes handled structurally rather than by an == branch
    handled_elsewhere = {"full", "engine", "dashboard"}
    orphans = sorted(declared - dispatched - handled_elsewhere)
    assert not orphans, (
        f"selectable on the CLI but no dispatch branch: {orphans}"
    )


def _launcher_modes(fname):
    path = os.path.join(ROOT, fname)
    if not os.path.exists(path):
        return set()
    text = io.open(path, encoding="utf-8", errors="ignore").read()
    return set(re.findall(r'--mode\s+([a-z0-9-]+)', text))


@pytest.mark.parametrize("launcher", SHIPPED_LAUNCHERS)
def test_shipped_launcher_only_offers_real_modes(launcher):
    """A menu entry for a nonexistent mode is a client-facing crash."""
    declared = _declared_modes()
    bad = sorted(_launcher_modes(launcher) - declared)
    assert not bad, f"{launcher} offers modes that do not exist: {bad}"


def test_the_recovery_path_is_reachable_from_the_client_menu():
    """backup/restore/upgrade are the operator's worst-day commands.

    They existed as CLI modes for a long time while OASIS.bat offered no route
    to any of them, so a client who lost their database had no supported way
    back without being told a command line.
    """
    menu = _launcher_modes("OASIS.bat")
    for needed in ("backup", "restore", "list-backups", "upgrade"):
        assert needed in menu, f"OASIS.bat has no route to --mode {needed}"


def test_init_install_does_not_crash_on_undefined_names():
    """--mode init raised NameError on EVERY run, for both profiles.

    DEFAULT_SINGLE_DB / DEFAULT_MULTI_DB were referenced in install_profile but
    defined only in onboarding. They sit inside os.getenv() default arguments,
    which Python evaluates eagerly whether or not OASIS_DB_PATH is set — so the
    documented one-command install path was dead on arrival, in a module with
    no import-time coverage. Import the module and touch both names.
    """
    from oasis.logic import install_profile
    assert install_profile.DEFAULT_SINGLE_DB
    assert install_profile.DEFAULT_MULTI_DB


@pytest.mark.parametrize("profile", ["single", "multi"])
def test_init_install_runs_end_to_end_in_a_sandbox(tmp_path, monkeypatch, profile):
    """Both profiles must complete and REPORT problems, never raise."""
    data_dir = tmp_path / "oasis" / "data"
    data_dir.mkdir(parents=True)
    monkeypatch.setenv("OASIS_DATA_DIR", str(data_dir))
    monkeypatch.setenv("OASIS_DB_PATH", str(data_dir / "x.db"))
    monkeypatch.setenv("OASIS_CASH_DIR", str(tmp_path / "no_such_dir"))
    from oasis.logic.install_profile import init_install
    summary = init_install(profile, "TestCo", root=str(tmp_path))
    assert summary["profile"] == profile
    assert summary["db_path"], "init must resolve a database path"
