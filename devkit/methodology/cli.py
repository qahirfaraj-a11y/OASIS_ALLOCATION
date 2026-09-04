"""Command line over the methodology graph.

    python devkit/methodology/cli.py build          rebuild + integrity + gate report
    python devkit/methodology/cli.py status         the ledger, by status
    python devkit/methodology/cli.py blast <id>     what falls if this is wrong
    python devkit/methodology/cli.py stale [--apply] propagate staleness
    python devkit/methodology/cli.py gate <claim>   the four promotion checks
    python devkit/methodology/cli.py probe <probe>  run a probe, record verdicts
    python devkit/methodology/cli.py traps --scan   static trap scan over the tree
    python devkit/methodology/cli.py frontier       what to interrogate next, ranked
    python devkit/methodology/cli.py motifs         structural anti-patterns
    python devkit/methodology/cli.py drift          how fast the shape is changing
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from devkit.methodology import traps as T                      # noqa: E402
from devkit.methodology import analysis as A                     # noqa: E402
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


SNAPSHOTS = VAULT / "_graph" / "snapshots"


def cmd_build(a) -> int:
    g = Graph.load()
    out = g.export()
    if getattr(a, "snapshot", False):
        snap = A.snapshot(g, SNAPSHOTS)
        print(f"snapshot   → {snap.name}")
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
    """What to interrogate next.

    money x staleness x unexamined-ness, multiplied by TOPOLOGICAL importance.
    A parameter feeding one surface directly and a parameter the whole
    derivation runs through are not the same size of question, and counting
    dependents alone cannot tell them apart.
    """
    g = Graph.load()
    ids = list(g.nodes)
    # A money surface is worth more than a box: weight Katz by what is at stake.
    weight = {n.id: (4.0 if (n.type == "decision" and n.fm.get("money", True))
                     else 1.0) for n in g.nodes.values()}
    katz = A.katz_downstream(ids, g.out, weight=weight)
    btw = A.betweenness(ids, g.out)
    kmax = max(katz.values()) or 1.0
    bmax = max(btw.values()) or 1.0
    sole = A.articulation_claims(g)

    rows = []
    for n in g.of_type("claim"):
        radius = g.blast_radius(n.id)
        money = sum(1 for i in radius
                    if g.get(i) and g.get(i).type == "decision"
                    and g.get(i).fm.get("money", True))
        unexamined = {"asserted": 3.0, "stale": 2.5, "measured": 1.0,
                      "validated": 0.4, "trusted": 0.15,
                      "falsified": 0.2}.get(n.status, 1.0)
        ev = history(n.id)
        staleness = 2.0 if not ev else 1.0
        k, b = katz.get(n.id, 0.0) / kmax, btw.get(n.id, 0.0) / bmax
        central = 1.0 + 2.0 * k + 1.0 * b
        spof = 1.5 if n.id in sole else 1.0
        score = (1 + 3 * money + len(radius)) * unexamined * staleness * central * spof
        rows.append((score, n, money, len(radius), len(ev), k, b, n.id in sole))
    rows.sort(key=lambda r: -r[0])

    print("   score  money  deps  runs   katz   btwn  claim")
    for score, n, money, deps, ev, k, b, is_sole in rows[:a.limit]:
        mark = " ◆" if is_sole else "  "
        print(f"  {score:7.1f}  {money:5d}  {deps:4d}  {ev:4d}  {k:5.2f}  {b:5.2f}"
              f"{mark}{BADGE.get(n.status,'')}{n.id}")
    print("\n  katz = how much rests on it, discounted by distance (exact on a "
          "DAG;\n         eigenvector centrality is degenerate here — a DAG's "
          "adjacency\n         matrix is nilpotent, so its principal "
          "eigenvector collapses).\n  btwn = how much of the derivation passes "
          "THROUGH it.\n  ◆    = sole support of a money surface.")
    if rows:
        print(f"\n  interrogate the top row: python devkit/methodology/cli.py "
              f"blast {rows[0][1].id}")
    return 0


def cmd_motifs(a) -> int:
    """Failure modes as structure, found by shape rather than by memory."""
    g = Graph.load()
    ms = A.motifs(g)
    if not ms:
        print("  no anti-pattern motifs found")
        return 0
    for m in ms:
        print(f"  {m['severity']:<5} {m['motif']}")
        print(f"        {m['why']}")
        for nid in m["nodes"][:6]:
            n = g.get(nid)
            print(f"          {BADGE.get(n.status,'') if n else ''}{nid}"
                  f"  [{n.type if n else '?'}]")
        if len(m["nodes"]) > 6:
            print(f"          … and {len(m['nodes']) - 6} more")
        print()
    print(f"  {len(ms)} motif(s)")
    return 1 if any(m["severity"] in ("GATE", "ERROR") for m in ms) else 0


def cmd_drift(a) -> int:
    """How fast the methodology itself is changing shape."""
    g = Graph.load()
    if a.snapshot:
        print(f"snapshot → {A.snapshot(g, SNAPSHOTS).name}")
    d = A.drift(SNAPSHOTS, limit=a.limit)
    if "note" in d:
        print(f"  {d['snapshots']} snapshot(s) — {d['note']}")
        return 0
    print("  from        to          structural  status  ged")
    for s in d["steps"]:
        print(f"  {s['from']:<11} {s['to']:<11} {s['structural_edits']:>10}"
              f"  {s['status_moves']:>6}  {s['ged']:>3}")
    print(f"\n  mean structural edits/step: {d['mean_structural_edits']}"
          f" · latest: {d['latest_structural_edits']}")
    print(f"  {d['verdict']}")
    last = d["steps"][-1]["detail"]
    for k in ("nodes_added", "nodes_removed", "edges_added", "edges_removed"):
        if last[k]:
            print(f"    {k}: {last[k][:5]}")
    return 1 if d["unstable"] else 0


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(prog="methodology", description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("build")
    p.add_argument("--snapshot", action="store_true",
                   help="also write a dated snapshot for drift measurement")
    p.set_defaults(fn=cmd_build)
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
    p = sub.add_parser("motifs"); p.set_defaults(fn=cmd_motifs)
    p = sub.add_parser("drift"); p.add_argument("--snapshot", action="store_true")
    p.add_argument("--limit", type=int, default=8); p.set_defaults(fn=cmd_drift)

    a = ap.parse_args(argv)
    return a.fn(a)


if __name__ == "__main__":
    raise SystemExit(main())
