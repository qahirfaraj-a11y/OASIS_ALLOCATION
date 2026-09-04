"""Probe harness: one verdict schema, append-only evidence, status that can fall.

WHY A SCHEMA AT ALL
    devkit already holds the right probes — siting_robustness, measure_order_
    sensitivity, compare_transfer_methodologies, backtest_allocation. Each wrote
    a one-off JSON that nobody re-read. A probe that produces a SERIES makes
    degradation visible; a probe that produces a file does not.

THE RULE THAT MATTERS
    Evidence is append-only. Status is derived from evidence, and may go down.
    A claim that stops being re-probed does not stay validated — it expires.
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import subprocess
import sys
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

from .graph import Graph
from .vault import VALIDATING_PROVENANCE, VAULT, Node

SUPPORTS, CONTRADICTS, INCONCLUSIVE = "supports", "contradicts", "inconclusive"
VERDICTS = (SUPPORTS, CONTRADICTS, INCONCLUSIVE)

# How many consecutive validated runs earn TRUSTED.
TRUST_STREAK = 3


def config_hash(payload: Dict[str, Any]) -> str:
    """Stable hash of the parameters in force. Ties evidence to a configuration."""
    blob = json.dumps(payload, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:12]


@dataclass
class Verdict:
    claim: str
    probe: str
    verdict: str
    metric: Dict[str, Any] = field(default_factory=dict)
    notes: str = ""
    held_out: bool = False
    baseline: Optional[str] = None
    beat_baseline: Optional[bool] = None
    traps: List[str] = field(default_factory=list)
    #: Where the data behind this verdict came from. Synthetic and extrapolated
    #: data can exercise a probe end to end; neither can validate a claim.
    provenance: str = "unknown"
    sources: List[str] = field(default_factory=list)
    config_hash: str = ""
    run_at: str = field(default_factory=lambda: _dt.datetime.now().isoformat(timespec="seconds"))

    def __post_init__(self) -> None:
        if self.verdict not in VERDICTS:
            raise ValueError(f"verdict must be one of {VERDICTS}, got {self.verdict!r}")


def evidence_dir(claim_id: str, vault: Optional[Path] = None) -> Path:
    d = Path(vault or VAULT) / "Evidence" / claim_id
    d.mkdir(parents=True, exist_ok=True)
    return d


def history(claim_id: str, vault: Optional[Path] = None) -> List[dict]:
    d = Path(vault or VAULT) / "Evidence" / claim_id
    if not d.is_dir():
        return []
    out = []
    for p in sorted(d.glob("*.json")):
        try:
            out.append(json.loads(p.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            continue
    return out


def _next_status(node: Node, v: Verdict, past: List[dict]) -> Optional[str]:
    """The status lattice, applied. Down is as reachable as up."""
    cur = node.status
    if v.verdict == CONTRADICTS:
        return "falsified"
    if v.verdict == INCONCLUSIVE:
        return None
    # supports
    if not v.held_out:
        # `falsified` is recoverable, and deliberately so: falsification is a
        # statement about the world, and the world changes when the code does.
        # It recovers to `measured` — never straight back to a licence to move
        # money — and both verdicts stay in the evidence trail.
        return "measured" if cur in ("asserted", "stale", "falsified") else None
    if v.provenance not in VALIDATING_PROVENANCE:
        # The probe ran, the machinery works, the number is real — and it was
        # computed on data nobody observed. That is a successful EXERCISE, not
        # a validation, and the difference is the whole point of the gate.
        return "measured"
    if v.beat_baseline is False:
        # Measured honestly, but it did not earn the right to drive anything.
        return "measured"
    streak = 0
    for rec in reversed(past + [asdict(v)]):
        if rec.get("verdict") == SUPPORTS and rec.get("held_out") and rec.get("beat_baseline"):
            streak += 1
        else:
            break
    if streak >= TRUST_STREAK:
        return "trusted"
    return "validated"


def record(v: Verdict, vault: Optional[Path] = None,
           graph: Optional[Graph] = None, apply: bool = True) -> Dict[str, Any]:
    """Write evidence, move status, and report the blast radius of a fall."""
    g = graph or Graph.load(vault)
    node = g.get(v.claim)
    if node is None:
        raise KeyError(f"no such claim: {v.claim}")

    past = history(v.claim, vault)
    stamp = v.run_at.replace(":", "").replace("-", "")
    path = evidence_dir(v.claim, vault) / f"{stamp}_{v.probe.split('.')[-1]}.json"
    if apply:
        path.write_text(json.dumps(asdict(v), indent=2), encoding="utf-8")

    nxt = _next_status(node, v, past)
    result: Dict[str, Any] = {
        "claim": v.claim, "probe": v.probe, "verdict": v.verdict,
        "status_before": node.status, "status_after": nxt or node.status,
        "evidence": str(path), "blast_radius": [],
    }
    if nxt and nxt != node.status:
        if apply:
            node.set_status(nxt, f"{v.probe} → {v.verdict}")
            if v.verdict == SUPPORTS:
                node.fm["last_evidence"] = _dt.date.today().isoformat()
            node.write()
        if nxt == "falsified":
            result["blast_radius"] = g.blast_radius(v.claim)
    elif v.verdict == SUPPORTS and apply:
        node.fm["last_evidence"] = _dt.date.today().isoformat()
        node.write()
    return result


# ------------------------------------------------------------------ running
def run_probe(probe_id: str, vault: Optional[Path] = None,
              graph: Optional[Graph] = None, apply: bool = True,
              extra_args: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Execute a probe node's entrypoint and record whatever verdicts it emits.

    Contract: the entrypoint prints one JSON object per line on stdout for each
    claim it judges, with at least {"claim", "verdict"}. Anything else on stdout
    is ignored, so a probe may log freely.
    """
    g = graph or Graph.load(vault)
    probe = g.get(probe_id)
    if probe is None:
        raise KeyError(f"no such probe: {probe_id}")
    entry = probe.fm.get("entrypoint")
    if not entry:
        raise ValueError(f"{probe_id} has no entrypoint; it is a placeholder")

    cmd = [sys.executable] + str(entry).split() + list(extra_args or [])
    print(f"[probe] {probe_id}: {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True,
                          cwd=str(Path(__file__).resolve().parents[2]))
    if proc.returncode != 0:
        print(proc.stderr[-2000:], file=sys.stderr)
        raise RuntimeError(f"{probe_id} exited {proc.returncode}")

    out: List[Dict[str, Any]] = []
    for line in proc.stdout.splitlines():
        line = line.strip()
        if not (line.startswith("{") and line.endswith("}")):
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if "claim" not in payload or "verdict" not in payload:
            continue
        payload.setdefault("probe", probe_id)
        out.append(record(Verdict(**payload), vault, g, apply))
    if not out:
        print(f"[probe] {probe_id} emitted no verdicts — "
              "a probe that judges nothing is not a probe")
    return out


# ------------------------------------------------------------------ the gate
def gate(claim_id: str, vault: Optional[Path] = None,
         graph: Optional[Graph] = None,
         shadow_cycles_required: int = 2) -> Dict[str, Any]:
    """The four checks from the architecture. All four, or the answer is no.

    1. Held-out    measured on a window the proposal did not touch
    2. Baseline    beats the incumbent, including the trivial incumbent
    3. Rank check  the resulting hierarchy diffs clean against the operating docs
    4. Shadow      ran in shadow mode for N cycles with divergence logged
    """
    g = graph or Graph.load(vault)
    node = g.get(claim_id)
    if node is None:
        raise KeyError(claim_id)
    recs = history(claim_id, vault)
    latest = recs[-1] if recs else {}

    checks = {
        "observed_provenance": latest.get("provenance") in VALIDATING_PROVENANCE,
        "held_out": bool(latest.get("held_out")),
        "beat_baseline": bool(latest.get("beat_baseline")),
        "rank_check": "T6" in (latest.get("traps") or []),
        "shadow": sum(1 for r in recs if (r.get("metric") or {}).get("shadow_cycle")) >= shadow_cycles_required,
    }
    passed = all(checks.values())
    return {
        "claim": claim_id,
        "status": node.status,
        "checks": checks,
        "passed": passed,
        "verdict": ("may drive a live decision" if passed else
                    "MEASURED only — not permitted to move money"),
        "provenance": latest.get("provenance", "unknown"),
        "blocked_on_data": (not checks["observed_provenance"]
                            and all(v for k, v in checks.items()
                                    if k != "observed_provenance")),
        "baseline": latest.get("baseline"),
        "evidence_count": len(recs),
    }
