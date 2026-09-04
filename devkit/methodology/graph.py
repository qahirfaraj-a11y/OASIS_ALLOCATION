"""The methodology DAG: what rests on what, and what falls when something does.

NOT A NEURAL GRAPH. Deterministic edges, no learned weights, no embeddings.
OASIS already has a GNN whose risk head turned out to be a static attribute
prior trained on synthetic labels; the corrective for that is auditability, not
a second learned structure.

WHAT THE EDGES MEAN FOR STALENESS
    A claim supports a parameter. A parameter feeds a decision. A claim may
    depend on another claim. So when a claim falls, the fall travels:

        claim ──supports──▸ parameter ──feeds──▸ decision
          │
          └──(reverse depends_on)──▸ dependent claims

    ``contradicts`` is deliberately NOT a staleness edge. Two claims in tension
    is a state worth holding and reporting, not resolving by propagation.

THE GATE
    A parameter may only feed a decision if every claim supporting it is
    ``validated`` or ``trusted``. This is exactly what OASIS_GNN_ORDERING_WEIGHT=0
    already does for one model, generalised to every parameter in the system.
"""
from __future__ import annotations

import datetime as _dt
import json
from collections import defaultdict, deque
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

from .vault import (DRIVING, VAULT, Node, iter_nodes)

# Edges that carry staleness downstream, as (source key, direction).
DOWNSTREAM_KEYS = ("supports", "feeds")
UPSTREAM_KEYS = ("depends_on",)   # reversed when propagating


class Graph:
    def __init__(self, nodes: Iterable[Node]):
        self.nodes: Dict[str, Node] = {}
        for n in nodes:
            if n.id in self.nodes:
                # A duplicate id is the vault equivalent of the stale-duplicate
                # loader bug. Refuse it loudly rather than picking one.
                raise ValueError(
                    f"duplicate node id {n.id!r}: "
                    f"{self.nodes[n.id].path.name} and {n.path.name}")
            self.nodes[n.id] = n
        self.out: Dict[str, Set[str]] = defaultdict(set)
        self.inn: Dict[str, Set[str]] = defaultdict(set)
        for n in self.nodes.values():
            for key in DOWNSTREAM_KEYS:
                for tgt in n.links(key):
                    self._edge(n.id, tgt)
            for key in UPSTREAM_KEYS:
                for src in n.links(key):
                    self._edge(src, n.id)   # reversed: dependency -> dependent
            for tgt in n.links("tested_by"):
                self.inn[n.id].add(tgt)     # probes are inputs, not dependents

    def _edge(self, a: str, b: str) -> None:
        self.out[a].add(b)
        self.inn[b].add(a)

    # ------------------------------------------------------------------
    @classmethod
    def load(cls, vault: Optional[Path] = None) -> "Graph":
        return cls(list(iter_nodes(vault or VAULT)))

    def get(self, node_id: str) -> Optional[Node]:
        return self.nodes.get(node_id)

    def of_type(self, t: str) -> List[Node]:
        return [n for n in self.nodes.values() if n.type == t]

    # ------------------------------------------------------------------
    def is_firebreak(self, node_id: str) -> bool:
        """A parameter pinned to a no-op value stops the fall travelling.

        OASIS_GNN_ORDERING_WEIGHT = 0 is the case that matters: the GNN claims
        are falsified, but the parameter is pinned to zero, so no purchase order
        is in question. Propagating past it would cry wolf about the entire
        ordering surface and teach everyone to ignore the alarm.
        """
        n = self.nodes.get(node_id)
        if not n:
            return False
        # `neutralised` — a parameter pinned to a no-op value.
        # `mitigated`   — a defect that is real, known, and handled downstream.
        #                 The receipt history IS incomplete; the ledger refuses
        #                 every span that crosses the hole, so R measured on it
        #                 is sound. Propagating anyway would put the whole
        #                 ordering surface in question for a defect we already
        #                 answered, which is how an alarm gets ignored.
        return bool(n.fm.get("neutralised") or n.fm.get("mitigated"))

    def blast_radius(self, node_id: str, through_firebreaks: bool = False) -> List[str]:
        """Everything that would be in question if this node were wrong."""
        seen, q = set(), deque([node_id])
        while q:
            cur = q.popleft()
            if cur != node_id and not through_firebreaks and self.is_firebreak(cur):
                continue          # mark the firebreak itself, go no further
            for nxt in sorted(self.out.get(cur, ())):
                if nxt not in seen:
                    seen.add(nxt)
                    q.append(nxt)
        seen.discard(node_id)
        return sorted(seen)

    def supporting_claims(self, param_id: str) -> List[Node]:
        return [self.nodes[i] for i in sorted(self.inn.get(param_id, ()))
                if i in self.nodes and self.nodes[i].type == "claim"]

    # ------------------------------------------------------------------
    def propagate_stale(self, apply: bool = False,
                        today: Optional[_dt.date] = None) -> List[Tuple[str, str]]:
        """Mark what a fallen claim puts in question — and clear what recovered.

        A loop that only marks things down is not self-correcting, it is just
        pessimistic. So this runs in both directions: a node marked stale by
        propagation records what it was, and is restored when nothing upstream
        is failing any more.

        Returns (node_id, reason) for every change.
        """
        today = today or _dt.date.today()
        changes: List[Tuple[str, str]] = []

        # Phase A — what is actually failing, on its own account.
        bad: Dict[str, str] = {}
        for n in self.nodes.values():
            if self.is_firebreak(n.id):
                # A firebreak stops the fall travelling THROUGH it; it must also
                # stop it starting FROM it. A defect that is known and handled
                # is not a live alarm, whichever end of the edge it sits on.
                continue
            if n.status == "falsified":
                bad[n.id] = f"downstream of falsified {n.id}"
            elif n.expired(today):
                bad[n.id] = f"downstream of expired {n.id}"
                if n.status != "stale":
                    changes.append((n.id, f"evidence older than ttl_days={n.ttl_days}"))
                    if apply:
                        n.fm.setdefault("stale_from", n.status)
                        n.set_status("stale", "TTL expired")
                        n.write()

        # Phase B — everything those failures reach.
        implicated: Dict[str, str] = {}
        for root, reason in bad.items():
            for dep in self.blast_radius(root):
                implicated.setdefault(dep, reason)

        # Phase C — mark, and clear.
        for n in self.nodes.values():
            reason = implicated.get(n.id)

            if reason:
                if n.status in ("stale", "falsified") or n.fm.get("neutralised"):
                    continue          # already down, or pinned to a safe value
                if n.type == "decision":
                    # A surface is where money happens; its status is not a
                    # meaningful thing to mutate. Report it and leave it alone,
                    # so the alarm stays precise.
                    changes.append((n.id, reason + "  [MONEY IN QUESTION]"))
                    continue
                changes.append((n.id, reason))
                if apply:
                    n.fm.setdefault("stale_from", n.status)
                    n.set_status("stale", reason)
                    n.write()
                continue

            # Nothing upstream is failing. Should this node come back?
            if (n.status == "stale" and n.fm.get("stale_from")
                    and not n.expired(today)):
                back = str(n.fm["stale_from"])
                changes.append((n.id, f"recovered → {back}: nothing upstream is failing"))
                if apply:
                    n.set_status(back, "upstream recovered")
                    n.fm.pop("stale_from", None)
                    n.write()

        seen, out = set(), []
        for nid, reason in changes:
            if nid not in seen:
                seen.add(nid)
                out.append((nid, reason))
        return out

    # ------------------------------------------------------------------
    def check(self, today: Optional[_dt.date] = None) -> List[Tuple[str, str, str]]:
        """Integrity + gate checks. Returns (severity, node_id, message)."""
        today = today or _dt.date.today()
        issues: List[Tuple[str, str, str]] = []

        for n in self.nodes.values():
            for key in ("supports", "feeds", "depends_on", "tested_by",
                        "guarded_by", "guards", "contradicts"):
                for tgt in n.links(key):
                    if tgt not in self.nodes:
                        issues.append(("ERROR", n.id, f"{key} → unknown node {tgt!r}"))

            if n.type == "claim":
                if n.status in ("measured", "validated", "trusted") and not n.links("tested_by"):
                    issues.append(("ERROR", n.id,
                                   f"status={n.status} but no probe tests it — "
                                   "an unprobed claim is an opinion"))
                if n.status in DRIVING and not n.last_evidence:
                    issues.append(("ERROR", n.id,
                                   f"status={n.status} with no last_evidence date"))
                if n.expired(today):
                    issues.append(("WARN", n.id,
                                   f"evidence stale: last {n.last_evidence}, "
                                   f"ttl {n.ttl_days}d"))
                if n.status == "asserted" and self.blast_radius(n.id):
                    issues.append(("WARN", n.id,
                                   "asserted claim with downstream dependents: "
                                   + ", ".join(self.blast_radius(n.id)[:4])))

            if n.type == "parameter" and n.links("feeds"):
                money = [t for t in n.links("feeds")
                         if self.nodes.get(t) is None
                         or self.nodes[t].fm.get("money", True)]
                if not money:
                    continue      # display-only surface: nothing to gate
                if n.fm.get("neutralised") or n.fm.get("mitigated"):
                    issues.append(("INFO", n.id,
                                   "gated to a no-op value ("
                                   + str(n.fm.get("value", "0"))
                                   + ") — the claims below it need not be driving, "
                                     "and the fall stops here"))
                    continue
                bad = [c.id for c in self.supporting_claims(n.id)
                       if c.status not in DRIVING]
                if bad:
                    issues.append(("GATE", n.id,
                                   "feeds a live decision but rests on non-driving "
                                   "claims: " + ", ".join(sorted(bad))))
                if not self.supporting_claims(n.id):
                    issues.append(("GATE", n.id,
                                   "feeds a live decision with NO supporting claim"))

        for cyc in self.cycles():
            issues.append(("ERROR", cyc[0], "dependency cycle: " + " → ".join(cyc)))
        return issues

    def cycles(self) -> List[List[str]]:
        colour: Dict[str, int] = {}
        stack: List[str] = []
        found: List[List[str]] = []

        def walk(u: str) -> None:
            colour[u] = 1
            stack.append(u)
            for v in sorted(self.out.get(u, ())):
                if v not in self.nodes:
                    continue
                if colour.get(v, 0) == 0:
                    walk(v)
                elif colour.get(v) == 1 and v in stack:
                    found.append(stack[stack.index(v):] + [v])
            stack.pop()
            colour[u] = 2

        for nid in sorted(self.nodes):
            if colour.get(nid, 0) == 0:
                walk(nid)
        return found

    # ------------------------------------------------------------------
    def to_dict(self) -> dict:
        return {
            "generated_at": _dt.datetime.now().isoformat(timespec="seconds"),
            "nodes": [
                {"id": n.id, "type": n.type, "status": n.status,
                 "domain": n.domain, "title": n.fm.get("title", ""),
                 "worth": n.fm.get("worth", ""),
                 "last_evidence": str(n.fm.get("last_evidence") or ""),
                 "ttl_days": n.ttl_days, "file": n.path.name}
                for n in sorted(self.nodes.values(), key=lambda x: x.id)
            ],
            "edges": [
                {"from": a, "to": b}
                for a in sorted(self.out) for b in sorted(self.out[a])
            ],
        }

    def export(self, path: Optional[Path] = None) -> Path:
        path = Path(path or (VAULT / "_graph" / "methodology.json"))
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(self.to_dict(), indent=2), encoding="utf-8")
        return path
