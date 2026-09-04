#!/usr/bin/env python3
"""PostToolUse hook: when a file that defines a parameter is edited, say what falls.

This is the mechanism that stops a code change silently invalidating a claim.
Edit `site_scoring.py` and you are told, at the moment you do it, that
DISTANCE_DECAY feeds every site recommendation and rests on a claim nobody has
ever measured.

Fails open, always. A hook that breaks the session is worse than a hook that
misses a warning.
"""
import json
import re
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def main() -> int:
    try:
        payload = json.load(sys.stdin)
    except Exception:
        return 0

    path = (payload.get("tool_input") or {}).get("file_path") or ""
    if not path:
        return 0
    try:
        rel = Path(path).resolve().relative_to(REPO).as_posix()
    except Exception:
        rel = Path(path).name

    # Never fire on the vault itself, or the toolkit.
    if rel.startswith(("oasis_vault/", "devkit/methodology/", ".claude/")):
        return 0

    sys.path.insert(0, str(REPO))
    try:
        from devkit.methodology.graph import Graph
        g = Graph.load()
    except Exception:
        return 0

    stem = Path(rel).name
    hits = []
    for n in g.of_type("parameter"):
        where = str(n.fm.get("defined_in") or "")
        if stem and (stem in where or rel in where):
            hits.append(n)
    if not hits:
        return 0

    lines = []
    for n in hits:
        radius = g.blast_radius(n.id)
        money = [i for i in radius
                 if g.get(i) and g.get(i).type == "decision"
                 and g.get(i).fm.get("money", True)]
        claims = [c for c in g.supporting_claims(n.id)]
        weak = [c.id for c in claims if c.status not in ("validated", "trusted")]
        line = f"  {n.id}  ({n.fm.get('value', '?')})"
        if money:
            line += f"  → feeds {', '.join(money)}"
        if weak:
            line += f"\n      rests on non-driving claims: {', '.join(weak)}"
        if n.fm.get("neutralised"):
            line += "\n      (gated to a no-op value — the fall stops here)"
        lines.append(line)

    msg = (f"Methodology graph — {rel} defines {len(hits)} live parameter(s):\n"
           + "\n".join(lines)
           + "\n  Changing these changes what the engine decides. Re-probe the "
             "claims beneath them, or mark them stale:\n"
           + "      python devkit/methodology/cli.py stale --apply")

    print(json.dumps({"hookSpecificOutput": {
        "hookEventName": "PostToolUse", "additionalContext": msg}}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except SystemExit:
        raise
    except Exception:
        raise SystemExit(0)   # fail open, always
