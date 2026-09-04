"""Command line over the methodology graph.

    python devkit/methodology/cli.py build          rebuild + integrity + gate report
    python devkit/methodology/cli.py status         the ledger, by status
    python devkit/methodology/cli.py blast <id>     what falls if this is wrong
    python devkit/methodology/cli.py stale [--apply] propagate staleness
    python devkit/methodology/cli.py gate <claim>   the four promotion checks
    python devkit/methodology/cli.py probe <probe>  run a probe, record verdicts
    python devkit/methodology/cli.py traps --scan   static trap scan over the tree
    python devkit/methodology/cli.py frontier       what to interrogate next, ranked
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from devkit.methodology import traps as T                      # noqa: E402
from devkit.methodology.graph import Graph                      # noqa: E402
from devkit.methodology.harness import gate, history, run_probe  # noqa: E402
from devkit.methodology.vault import DRIVING, VAULT             # noqa: E402

BADGE = {"asserted": "· ", "measured": "◐ ", "validated": "◕ ",
         "trusted": "● ", "stale": "◌ ", "falsified": "✗ "}


def _print_issues(issues) -> int:
    if not issues:
        print("  no issues")
        return 0
    rank = {"GATE": 0, "ERROR": 1, "WARN": 2, "INFO": 3}
    for sev, nid, msg in sorted(issues, key=lambda x: (rank.get(x[0], 3), x[1])):
        print(f"  {sev:<5} {nid:<44} {msg}")
    return sum(1 for s, _, _ in issues if s in ("GATE", "ERROR"))


def cmd_build(a) -> int:
    g = Graph.load()
    out = g.export()
    print(f"graph: {len(g.nodes)} nodes, {sum(len(v) for v in g.out.values())} edges")
    print(f"exported → {out.relative_to(Path.cwd()) if str(out).startswith(str(Path.cwd())) else out}")
    print("\nintegrity + gate:")
    return 1 if _print_issues(g.check()) else 0


def cmd_status(a) -> int:
    g = Graph.load()
    claims = g.of_type("claim")
    by = {}
    for n in claims:
        by.setdefault(n.status, []).append(n)
    order = ["falsified", "stale", "asserted", "measured", "validated", "trusted"]
    for st in order:
        ns = by.get(st, [])
        if not ns:
            continue
        print(f"\n{BADGE.get(st,'  ')}{st.upper()}  ({len(ns)})")
        for n in sorted(ns, key=lambda x: (x.domain, x.id)):
            worth = n.fm.get("worth") or ""
            print(f"    {n.domain:<9} {n.id:<46} {worth}")
    drives = [n for n in claims if n.status in DRIVING]
    print(f"\n{len(claims)} claims · {len(drives)} permitted to drive a decision")
    params = g.of_type("parameter")
    live = [p for p in params if p.links("feeds")]
    print(f"{len(params)} parameters · {len(live)} feeding a live decision")
    return 0


def cmd_blast(a) -> int:
    g = Graph.load()
    if not g.get(a.node):
        print(f"no such node: {a.node}")
        return 2
    radius = g.blast_radius(a.node)
    n = g.get(a.node)
    print(f"{BADGE.get(n.status,'')}{n.id}  [{n.status}]  {n.fm.get('title','')}")
    if not radius:
        print("\n  nothing depends on this node")
        return 0
    print(f"\n  if this is wrong, {len(radius)} node(s) are in question:\n")
    for nid in radius:
        d = g.get(nid)
        mark = "  ← MONEY" if d and d.type == "decision" else ""
        print(f"    {BADGE.get(d.status,'') if d else ''}{nid:<46}"
              f"{(d.type if d else '?'):<10}{mark}")
    return 0


def cmd_stale(a) -> int:
    g = Graph.load()
    changes = g.propagate_stale(apply=a.apply)
    if not changes:
        print("nothing to mark stale")
        return 0
    verb = "marked" if a.apply else "would mark"
    print(f"{verb} {len(changes)} node(s) stale:\n")
    for nid, reason in changes:
        print(f"    {nid:<46} {reason}")
    if not a.apply:
        print("\n  (dry run — pass --apply to write)")
    return 0


def cmd_gate(a) -> int:
    r = gate(a.claim)
    print(json.dumps(r, indent=2))
    return 0 if r["passed"] else 1


def cmd_probe(a) -> int:
    for r in run_probe(a.probe, apply=not a.dry_run, extra_args=a.args):
        arrow = (f"{r['status_before']} → {r['status_after']}"
                 if r["status_before"] != r["status_after"] else r["status_before"])
        print(f"  {r['claim']:<46} {r['verdict']:<13} {arrow}")
        for nid in r["blast_radius"]:
            print(f"      ↳ now in question: {nid}")
    return 0


def cmd_traps(a) -> int:
    if a.scan:
        root = a.path or "."
        print(f"T2 static scan over {root} (unsorted listdir/glob):\n")
        results = T.scan_unsorted_globs(root, include=("*.py",))
        for r in results[:a.limit]:
            print("  " + r.detail)
        print(f"\n  {len(results)} site(s) where filesystem order decides which file wins")
        return 1 if results else 0
    for code, name in T.TRAPS.items():
        print(f"  {code}  {name}")
    return 0


def cmd_frontier(a) -> int:
    """What to interrogate next: money touched x staleness x unexamined-ness."""
    g = Graph.load()
    rows = []
    for n in g.of_type("claim"):
        radius = g.blast_radius(n.id)
        money = sum(1 for i in radius
                    if g.get(i) and g.get(i).type == "decision")
        unexamined = {"asserted": 3.0, "stale": 2.5, "measured": 1.0,
                      "validated": 0.4, "trusted": 0.15,
                      "falsified": 0.2}.get(n.status, 1.0)
        ev = history(n.id)
        staleness = 2.0 if not ev else 1.0
        score = (1 + 3 * money + len(radius)) * unexamined * staleness
        rows.append((score, n, money, len(radius), len(ev)))
    rows.sort(key=lambda r: -r[0])
    print("  score  money  deps  runs  claim")
    for score, n, money, deps, ev in rows[:a.limit]:
        print(f"  {score:6.1f}  {money:5d}  {deps:4d}  {ev:4d}  "
              f"{BADGE.get(n.status,'')}{n.id}")
    print("\n  interrogate the top row: python devkit/methodology/cli.py blast "
          + (rows[0][1].id if rows else "<id>"))
    return 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="methodology", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("build").set_defaults(fn=cmd_build)
    sub.add_parser("status").set_defaults(fn=cmd_status)

    p = sub.add_parser("blast"); p.add_argument("node"); p.set_defaults(fn=cmd_blast)
    p = sub.add_parser("stale"); p.add_argument("--apply", action="store_true")
    p.set_defaults(fn=cmd_stale)
    p = sub.add_parser("gate"); p.add_argument("claim"); p.set_defaults(fn=cmd_gate)
    p = sub.add_parser("probe"); p.add_argument("probe")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("args", nargs="*"); p.set_defaults(fn=cmd_probe)
    p = sub.add_parser("traps"); p.add_argument("--scan", action="store_true")
    p.add_argument("--path"); p.add_argument("--limit", type=int, default=25)
    p.set_defaults(fn=cmd_traps)
    p = sub.add_parser("frontier"); p.add_argument("--limit", type=int, default=10)
    p.set_defaults(fn=cmd_frontier)

    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
