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


def _imports_oasis(tree):
    """Whether the script really imports oasis, by parsing rather than reading.

    This used to be `if "oasis" not in src`, a SUBSTRING test — so a script
    that merely mentioned oasis/data in a file path, or in its own docstring,
    was treated as importing the package and required to carry a bootstrap it
    had no use for. Two of the four scripts this test failed on do not import
    oasis at all.
    """
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            if any(a.name.split(".")[0] == "oasis" for a in node.names):
                return True
        elif isinstance(node, ast.ImportFrom):
            if (node.module or "").split(".")[0] == "oasis":
                return True
    return False


def _reaches_the_repo_root(tree):
    """Whether the script puts the PARENT of devkit/ on sys.path.

    Behaviour, not spelling. The previous version demanded one exact string, so
    a script computing the identical path through a ROOT variable failed while
    being perfectly correct — the same mistake as the test that asserted the
    literals "60" and "14" appeared in a method's source and therefore never
    noticed an entire second engine.

    What actually matters is that the inserted path is DERIVED from __file__
    and goes up two levels. A hardcoded absolute path is the real defect: one
    developer's machine, working nowhere else, least of all in CI.
    """
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        f = node.func
        if not (isinstance(f, ast.Attribute) and f.attr == "insert"):
            continue
        if not (isinstance(f.value, ast.Attribute) and f.value.attr == "path"):
            continue
        # the inserted expression, wherever it came from
        inserted = ast.dump(node.args[1]) if len(node.args) > 1 else ""
        if "__file__" in inserted and inserted.count("dirname") >= 2:
            return True
        # or a name bound earlier to that same expression, e.g. ROOT
        if isinstance(node.args[1] if len(node.args) > 1 else None, ast.Name):
            target = node.args[1].id
            for assign in ast.walk(tree):
                if isinstance(assign, ast.Assign) and any(
                        isinstance(t, ast.Name) and t.id == target
                        for t in assign.targets):
                    src = ast.dump(assign.value)
                    if "__file__" in src and src.count("dirname") >= 2:
                        return True
    return False


@pytest.mark.parametrize("script", _devkit_scripts())
def test_every_devkit_script_can_reach_the_repo_root(script):
    """devkit/ does not contain oasis/, so a script that imports it must add
    the PARENT — and must DERIVE that path rather than hardcode one."""
    with open(os.path.join(DEVKIT, script), encoding="utf-8") as fh:
        src = fh.read()
    tree = ast.parse(src, filename=script)
    if not _imports_oasis(tree):
        pytest.skip(f"{script} does not import oasis")
    assert _reaches_the_repo_root(tree), (
        f"devkit/{script} imports oasis but never puts the repo root on "
        f"sys.path. Derive it from __file__, conventionally:\n    {BOOTSTRAP}"
    )


@pytest.mark.parametrize("script", _devkit_scripts())
def test_no_devkit_script_hardcodes_a_developers_path(script):
    """The defect the bootstrap test existed to catch, stated directly.

    measure_order_sensitivity.py pinned an absolute Windows path three times
    over, so it ran on exactly one machine.
    """
    with open(os.path.join(DEVKIT, script), encoding="utf-8") as fh:
        src = fh.read()
    for marker in ("C:\\Users", "/home/", "/Users/"):
        assert marker not in src, (
            f"devkit/{script} hardcodes an absolute path containing "
            f"{marker!r} — derive it from __file__ instead")


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


# ── the grid simulation's two real decisions ─────────────────────────────
class TestGridSimulation:
    """The yardstick is only as good as how the pins are drawn.

    Uniform sampling over the bounding box was the first attempt and produced
    a nonsense null: median capture 60-79%, everything above the third
    quartile at 100%. The pins were landing in farmland, where a store faces
    no competitor within 10 km and so takes a full share of almost nobody.
    """

    def _sim(self):
        sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..',
                                        'devkit'))
        import grid_simulation
        return grid_simulation

    class _Grid:
        def __init__(self, cells):
            self.cells = cells

    def test_pins_are_drawn_where_people_are(self):
        sim = self._sim()
        south, west, north, east = sim.BBOX
        crowded = ((south + north) / 2, (west + east) / 2, 1_000_000.0)
        empty = (south + 0.01, west + 0.01, 1.0)
        pins = sim.sample_points(400, seed=1,
                                 grid=self._Grid([crowded, empty]))
        near_crowd = sum(1 for la, lo in pins
                         if abs(la - crowded[0]) < 0.02
                         and abs(lo - crowded[1]) < 0.02)
        assert near_crowd > 380, "population weighting is not being applied"

    def test_sampling_is_reproducible(self):
        """An unseeded simulation cannot be re-run to check a surprising
        result — the exact failure this repo's own GNN review records."""
        sim = self._sim()
        g = self._Grid([(-1.28, 36.80, 5000.0), (-1.30, 36.82, 5000.0)])
        assert sim.sample_points(50, 7, g) == sim.sample_points(50, 7, g)
        assert sim.sample_points(50, 7, g) != sim.sample_points(50, 8, g)

    def test_pins_stay_inside_the_region(self):
        sim = self._sim()
        south, west, north, east = sim.BBOX
        g = self._Grid([(-1.28, 36.80, 5000.0)])
        for la, lo in sim.sample_points(200, 3, g):
            assert south - 0.01 <= la <= north + 0.01
            assert west - 0.01 <= lo <= east + 0.01

    def test_an_empty_grid_falls_back_to_uniform(self):
        sim = self._sim()
        assert len(sim.sample_points(25, 1, grid=None)) == 25
        assert len(sim.sample_points(25, 1, grid=self._Grid([]))) == 25

    def test_percentiles_interpolate_and_bracket(self):
        sim = self._sim()
        p = sim.percentiles([0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100])
        assert p[50] == 50.0
        assert p[5] < p[25] < p[50] < p[75] < p[95]
        assert sim.percentiles([]) == {}
