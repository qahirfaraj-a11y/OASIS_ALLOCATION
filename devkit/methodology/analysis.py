"""The graph as a computational surface: centrality, motifs, drift.

Three things reachability alone cannot tell you:

  CENTRALITY  which claim is systemically important, not merely connected
  MOTIFS      failure modes expressed as STRUCTURE, so they are found by shape
              rather than by someone remembering to write a check
  DRIFT       how fast the methodology itself is mutating, week to week

Stdlib only, like the rest of the toolkit.

A NOTE ON EIGENVECTOR CENTRALITY
    The obvious choice is eigenvector centrality, and on this graph it is the
    wrong one. A DAG's adjacency matrix is nilpotent — A^k = 0 once k exceeds
    the longest path — so its only eigenvalue is zero and the principal
    eigenvector is degenerate. Power iteration converges to nothing.

    Katz centrality is the same idea with the defect removed: it sums over ALL
    paths with a decay factor per hop, and the leading term keeps it non-zero.
    On a DAG the path set is finite, so Katz is not merely convergent, it is
    EXACT — one pass in reverse topological order, no iteration.
"""
from __future__ import annotations

import datetime as _dt
import json
import math
from collections import defaultdict, deque
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

KATZ_ALPHA = 0.5           # decay per hop; < 1 so distant dependents count less
DRIFT_WARN_EDITS = 12      # structural edits per snapshot before we call it churn


# ------------------------------------------------------------------ ordering
def topo_order(nodes: Iterable[str], out: Dict[str, Set[str]]) -> List[str]:
    """Kahn's algorithm. Nodes inside a cycle are appended last, in id order —
    a cycle is a finding, not a reason to refuse to rank anything."""
    nodes = list(nodes)
    indeg = {n: 0 for n in nodes}
    for a in nodes:
        for b in out.get(a, ()):
            if b in indeg:
                indeg[b] += 1
    q = deque(sorted(n for n in nodes if indeg[n] == 0))
    order: List[str] = []
    while q:
        n = q.popleft()
        order.append(n)
        for m in sorted(out.get(n, ())):
            if m in indeg:
                indeg[m] -= 1
                if indeg[m] == 0:
                    q.append(m)
    order += sorted(n for n in nodes if n not in set(order))
    return order


# ---------------------------------------------------------------- centrality
def katz_downstream(nodes: Iterable[str], out: Dict[str, Set[str]],
                    alpha: float = KATZ_ALPHA,
                    weight: Optional[Dict[str, float]] = None) -> Dict[str, float]:
    """How much rests on this node, discounted by distance. Exact on a DAG.

        katz[v] = w[v] + alpha * SUM over successors of katz[successor]

    `weight` lets a decision surface that actually spends money count for more
    than a node that does not, so centrality answers "how much money is
    downstream of this", not merely "how many boxes".
    """
    nodes = list(nodes)
    w = weight or {}
    order = topo_order(nodes, out)
    katz: Dict[str, float] = {}
    for n in reversed(order):                    # successors resolved first
        katz[n] = w.get(n, 1.0) + alpha * sum(
            katz.get(m, 0.0) for m in out.get(n, ()) if m in katz)
    return katz


def betweenness(nodes: Iterable[str], out: Dict[str, Set[str]]) -> Dict[str, float]:
    """Brandes, unweighted. A chokepoint through which many derivations run.

    High betweenness with low Katz is the interesting case: a node that is not
    itself load-bearing but sits on the path of everything that is.
    """
    nodes = list(nodes)
    cb = {n: 0.0 for n in nodes}
    for s in nodes:
        stack: List[str] = []
        preds: Dict[str, List[str]] = {n: [] for n in nodes}
        sigma = {n: 0.0 for n in nodes}
        dist = {n: -1 for n in nodes}
        sigma[s], dist[s] = 1.0, 0
        q = deque([s])
        while q:
            v = q.popleft()
            stack.append(v)
            for w_ in sorted(out.get(v, ())):
                if w_ not in dist:
                    continue
                if dist[w_] < 0:
                    dist[w_] = dist[v] + 1
                    q.append(w_)
                if dist[w_] == dist[v] + 1:
                    sigma[w_] += sigma[v]
                    preds[w_].append(v)
        delta = {n: 0.0 for n in nodes}
        while stack:
            w_ = stack.pop()
            for v in preds[w_]:
                if sigma[w_]:
                    delta[v] += (sigma[v] / sigma[w_]) * (1.0 + delta[w_])
            if w_ != s:
                cb[w_] += delta[w_]
    n = len(nodes)
    scale = 1.0 / ((n - 1) * (n - 2)) if n > 2 else 1.0
    return {k: v * scale for k, v in cb.items()}


def articulation_claims(g) -> Dict[str, List[str]]:
    """Claims whose removal disconnects one or more money surfaces.

    Not the undirected articulation point of textbooks — the question here is
    directed and specific: if this claim were the only thing holding a surface
    up, losing it takes the surface with it.
    """
    surfaces = [n.id for n in g.of_type("decision") if n.fm.get("money", True)]
    out: Dict[str, List[str]] = {}
    for claim in g.of_type("claim"):
        reach = set(g.blast_radius(claim.id))
        owned = [s for s in surfaces if s in reach]
        if not owned:
            continue
        sole = []
        for s in owned:
            others = [c.id for c in g.of_type("claim")
                      if c.id != claim.id and s in set(g.blast_radius(c.id))]
            if not others:
                sole.append(s)
        if sole:
            out[claim.id] = sorted(sole)
    return out


# -------------------------------------------------------------------- motifs
def motifs(g) -> List[Dict[str, Any]]:
    """Failure modes expressed as structure, found by shape.

    A procedural check finds what someone thought to look for. A motif finds
    every instance of a SHAPE, including the ones nobody anticipated — which is
    the point of having a graph rather than a checklist.
    """
    found: List[Dict[str, Any]] = []

    def add(motif: str, severity: str, nodes: List[str], why: str) -> None:
        found.append({"motif": motif, "severity": severity,
                      "nodes": nodes, "why": why})

    # M1 · circularity as a literal cycle -----------------------------------
    for cyc in g.cycles():
        add("circularity", "ERROR", cyc,
            "a dependency cycle: each of these rests on the next. T5 is not "
            "only a runtime property of a measure; it has a shape.")

    # M2 · a parameter reaching money with nothing holding it up ------------
    for p in g.of_type("parameter"):
        money = [t for t in p.links("feeds")
                 if g.get(t) is None or g.get(t).fm.get("money", True)]
        if money and not g.supporting_claims(p.id):
            add("unsupported-parameter", "GATE", [p.id] + money,
                "feeds a money surface with no claim beneath it — there is "
                "nothing that could even be falsified.")

    # M3 · two claims that contradict each other, holding up the same parameter
    for p in g.of_type("parameter"):
        sup = g.supporting_claims(p.id)
        ids = {c.id for c in sup}
        for c in sup:
            clash = sorted(ids & set(c.links("contradicts")))
            if clash:
                add("contradiction-diamond", "ERROR",
                    sorted({c.id, *clash, p.id}),
                    "a parameter resting on claims that contradict each other. "
                    "Whichever is true, the parameter is currently justified by "
                    "the other.")

    # M4 · one claim supporting parameters that feed different regimes -------
    for c in g.of_type("claim"):
        regimes: Dict[str, List[str]] = defaultdict(list)
        for p_id in c.links("supports"):
            p = g.get(p_id)
            if not p:
                continue
            for s_id in p.links("feeds"):
                s = g.get(s_id)
                if s:
                    regimes[s.domain].append(s_id)
        if len(regimes) > 1:
            add("cross-regime-support", "WARN",
                [c.id] + sorted(x for v in regimes.values() for x in v),
                "one claim justifies decisions in " + ", ".join(sorted(regimes))
                + ". Initial load is not replenishment; a claim that holds in "
                  "one regime need not hold in the other (T7).")

    # M5 · a path to money with no evidence anywhere on it -------------------
    from .harness import history
    for c in g.of_type("claim"):
        if history(c.id):
            continue
        money = [i for i in g.blast_radius(c.id)
                 if g.get(i) and g.get(i).type == "decision"
                 and g.get(i).fm.get("money", True)]
        if money:
            add("evidence-free-path-to-money", "GATE", [c.id] + sorted(money),
                "money depends on a claim that has never been probed once.")

    # M6 · every claim in a region tested by a single probe -------------------
    by_probe: Dict[str, List[str]] = defaultdict(list)
    for c in g.of_type("claim"):
        for pr in c.links("tested_by"):
            by_probe[pr].append(c.id)
    for pr, claims in by_probe.items():
        solo = [c for c in claims
                if len(g.get(c).links("tested_by")) == 1] if claims else []
        if len(solo) >= 3:
            add("probe-monoculture", "WARN", [pr] + sorted(solo),
                f"{len(solo)} claims are tested by {pr} and nothing else. One "
                "broken probe blinds the whole region, and a probe cannot "
                "falsify its own bug.")

    # M7 · a claim that is the sole support of a money surface ---------------
    for claim, surfaces in articulation_claims(g).items():
        add("single-point-of-failure", "WARN", [claim] + surfaces,
            "the only claim holding up " + ", ".join(surfaces)
            + ". Its TTL is the surface's TTL.")

    order = {"GATE": 0, "ERROR": 1, "WARN": 2}
    found.sort(key=lambda f: (order.get(f["severity"], 3), f["motif"]))
    return found


# --------------------------------------------------------------------- drift
def snapshot(g, out_dir: Path, name: Optional[str] = None) -> Path:
    """A dated snapshot. Named to the minute, because more than one cycle can
    land in a day and a drift series that silently overwrites itself measures
    nothing."""
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = name or _dt.datetime.now().strftime("%Y-%m-%dT%H%M")
    path = out_dir / f"{stamp}.json"
    path.write_text(json.dumps(g.to_dict(), indent=2), encoding="utf-8")
    return path


def _load(path: Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def edit_distance(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
    """Exact graph edit distance — which the general problem is NP-hard for, and
    this one is not.

    General GED is hard because the correspondence between nodes is unknown, so
    every matching has to be searched. Here node ids are stable and meaningful:
    `param.R` in last week's snapshot IS `param.R` in this week's. The matching
    is given, so the distance collapses to a symmetric difference plus a count
    of changed attributes.
    """
    an = {n["id"]: n for n in a.get("nodes", [])}
    bn = {n["id"]: n for n in b.get("nodes", [])}
    ae = {(e["from"], e["to"]) for e in a.get("edges", [])}
    be = {(e["from"], e["to"]) for e in b.get("edges", [])}

    added, removed = sorted(set(bn) - set(an)), sorted(set(an) - set(bn))
    e_added, e_removed = sorted(be - ae), sorted(ae - be)

    status_changes, other_changes = [], []
    for nid in sorted(set(an) & set(bn)):
        if an[nid].get("status") != bn[nid].get("status"):
            status_changes.append(
                {"id": nid, "from": an[nid].get("status"),
                 "to": bn[nid].get("status")})
        for k in ("type", "domain", "ttl_days"):
            if an[nid].get(k) != bn[nid].get(k):
                other_changes.append({"id": nid, "field": k,
                                      "from": an[nid].get(k), "to": bn[nid].get(k)})

    # Status moves are the graph WORKING; structural edits are the graph
    # CHANGING SHAPE. Only the second is drift.
    structural = (len(added) + len(removed) + len(e_added) + len(e_removed)
                  + len(other_changes))
    return {
        "ged": structural + len(status_changes),
        "structural_edits": structural,
        "status_moves": len(status_changes),
        "nodes_added": added, "nodes_removed": removed,
        "edges_added": [list(e) for e in e_added],
        "edges_removed": [list(e) for e in e_removed],
        "status_changes": status_changes, "other_changes": other_changes,
    }


def drift(snapshot_dir: Path, limit: int = 8) -> Dict[str, Any]:
    """Methodological drift: is the structure settling, or churning?

    A graph whose SHAPE keeps changing week to week is a methodology still
    being invented. That is fine early and alarming late — and either way it is
    a fact somebody should have to look at, rather than a feeling.
    """
    snaps = sorted(Path(snapshot_dir).glob("*.json"))
    if len(snaps) < 2:
        return {"snapshots": len(snaps),
                "note": "need at least two snapshots; run `build --snapshot` "
                        "on a schedule so drift has something to measure"}
    steps = []
    for a, b in list(zip(snaps, snaps[1:]))[-limit:]:
        d = edit_distance(_load(a), _load(b))
        steps.append({"from": a.stem, "to": b.stem,
                      "ged": d["ged"], "structural_edits": d["structural_edits"],
                      "status_moves": d["status_moves"],
                      "detail": d})
    rates = [s["structural_edits"] for s in steps]
    mean = sum(rates) / len(rates)
    latest = steps[-1]
    return {
        "snapshots": len(snaps), "steps": steps,
        "mean_structural_edits": round(mean, 2),
        "latest_structural_edits": latest["structural_edits"],
        "unstable": latest["structural_edits"] > max(DRIFT_WARN_EDITS, 2 * mean),
        "verdict": ("structure is churning — the methodology is still being "
                    "invented, and nothing resting on it should be promoted"
                    if latest["structural_edits"] > max(DRIFT_WARN_EDITS, 2 * mean)
                    else "structure is settling; status is doing the moving"),
    }
