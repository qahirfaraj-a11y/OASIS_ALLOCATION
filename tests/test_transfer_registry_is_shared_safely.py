"""The one transfer registry, written by many surfaces at once.

Every surface now plans into network_registry.json. The file used to be
rewritten in place with no lock: two runs at once kept only the last plan, a
reader could land on half a file, and one store's run erased every other
store's planned transfers.
"""
import json
import os
import subprocess
import sys
import threading

from oasis.logic.transfer_state import TransferRecord, TransferStateTracker

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def plan(path, org, moves, replace=True):
    """One ordering run's save: `moves` = [(from, to, item)]."""
    t = TransferStateTracker()
    for f, to, itm in moves:
        t.register_transfer(TransferRecord(from_org=f, to_org=to, itm_cd=itm, product_name=itm, qty=1))
    t.save_to_file(str(path), replace_orgs={org} if replace else None)


def read(path):
    return json.load(open(path, encoding="utf-8"))


def test_each_stores_plan_survives_the_others(tmp_path):
    reg = tmp_path / "network_registry.json"
    plan(reg, "A", [("B", "A", "x")])
    plan(reg, "B", [("C", "B", "y")])
    assert sorted((d["to_org"], d["itm_cd"]) for d in read(reg)) == [("A", "x"), ("B", "y")]


def test_a_rerun_replaces_only_its_own_plan_pushes_included(tmp_path):
    reg = tmp_path / "network_registry.json"
    # A's run pulls into A AND pushes from A into B
    plan(reg, "A", [("B", "A", "x"), ("A", "B", "push1")])
    plan(reg, "B", [("A", "B", "pull")])            # B's own pull from A
    plan(reg, "A", [("C", "A", "x2"), ("A", "B", "push2")])
    got = sorted((d["planned_for"], d["itm_cd"]) for d in read(reg))
    # A's old pull and push are gone, B's pull from A is kept
    assert got == [("A", "push2"), ("A", "x2"), ("B", "pull")]


def test_records_from_before_the_tag_fall_back_to_their_recipient(tmp_path):
    reg = tmp_path / "network_registry.json"
    reg.write_text(json.dumps([
        {"transfer_id": "old1", "from_org": "B", "to_org": "A", "itm_cd": "x", "product_name": "x", "qty": 1,
         "status": "PENDING", "created_at": "2026-09-01T00:00:00", "eta_hours": 4, "cost_kes": 0},
        {"transfer_id": "old2", "from_org": "A", "to_org": "C", "itm_cd": "z", "product_name": "z", "qty": 1,
         "status": "PENDING", "created_at": "2026-09-01T00:00:00", "eta_hours": 4, "cost_kes": 0}]),
        encoding="utf-8")
    plan(reg, "A", [("B", "A", "new")])
    assert sorted(d["itm_cd"] for d in read(reg)) == ["new", "z"]


def test_without_a_scope_the_file_is_replaced_whole(tmp_path):
    reg = tmp_path / "network_registry.json"
    plan(reg, "A", [("B", "A", "x")])
    plan(reg, "B", [("C", "B", "y")], replace=False)
    assert [d["itm_cd"] for d in read(reg)] == ["y"]


def test_threads_writing_at_once_lose_nothing(tmp_path):
    reg = tmp_path / "network_registry.json"
    orgs = [f"S{i}" for i in range(8)]

    def run(org):
        for k in range(15):
            plan(reg, org, [("HUB", org, f"{org}-{k}")])
    threads = [threading.Thread(target=run, args=(o,)) for o in orgs]
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    got = {d["to_org"]: d["itm_cd"] for d in read(reg)}
    assert got == {o: f"{o}-14" for o in orgs}          # every store's LAST plan, once


def test_processes_writing_at_once_lose_nothing_and_readers_never_see_half_a_file(tmp_path):
    reg = tmp_path / "network_registry.json"
    script = (
        "import sys; sys.path.insert(0, %r)\n"
        "from oasis.logic.transfer_state import TransferRecord, TransferStateTracker\n"
        "org = sys.argv[1]\n"
        "for k in range(25):\n"
        "    t = TransferStateTracker()\n"
        "    for j in range(40):\n"
        "        t.register_transfer(TransferRecord(from_org='HUB', to_org=org, itm_cd=f'{org}-{k}-{j}',"
        " product_name='x' * 60, qty=1))\n"
        "    t.save_to_file(%r, replace_orgs={org})\n") % (ROOT, str(reg))
    bad, stop = [], threading.Event()

    def reader():
        while not stop.is_set():
            try:
                with open(reg, encoding="utf-8") as f:
                    json.load(f)
            except FileNotFoundError:
                pass
            except PermissionError:
                pass                                   # Windows: the rename is in flight
            except ValueError as e:
                bad.append(str(e))
    rt = threading.Thread(target=reader)
    rt.start()
    procs = [subprocess.Popen([sys.executable, "-c", script, org]) for org in ("P1", "P2", "P3", "P4")]
    codes = [p.wait(timeout=240) for p in procs]
    stop.set()
    rt.join()
    assert codes == [0, 0, 0, 0]
    assert bad == [], bad[:3]
    got = read(reg)
    by_org = {}
    for d in got:
        by_org.setdefault(d["to_org"], set()).add(d["itm_cd"].rsplit("-", 1)[0])
    assert by_org == {o: {f"{o}-24"} for o in ("P1", "P2", "P3", "P4")}
    assert len(got) == 4 * 40
    assert not [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]   # no temp files left behind
