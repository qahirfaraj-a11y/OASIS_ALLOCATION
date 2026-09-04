"""Vault nodes: markdown with YAML-ish frontmatter, parsed without PyYAML.

WHY NOT PyYAML
    A dependency the loop can fail to import is a loop that silently stops
    running. The frontmatter dialect used here is deliberately small enough to
    parse in eighty lines: scalars, inline lists, and block lists. If a node
    ever needs more than that, the node is doing too much.

THE NODE
    One markdown file per node. Frontmatter is the machine-readable skeleton;
    the body is the argument, for humans. Both live in git, so a claim's
    history is a diff and an amendment is a commit.
"""
from __future__ import annotations

import datetime as _dt
import os
import re
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

REPO = Path(__file__).resolve().parents[2]
VAULT = Path(os.environ.get("OASIS_VAULT", REPO / "oasis_vault"))

NODE_DIRS = {
    "claim": "Claims",
    "parameter": "Parameters",
    "probe": "Probes",
    "trap": "Traps",
    # DECISION nodes live in Surfaces/, not Decisions/ — Decisions/ is the
    # existing narrative record and keeps its role untouched.
    "decision": "Surfaces",
}

# The status lattice. Order matters: index is rank for promotion checks.
STATUSES = ["asserted", "measured", "validated", "trusted"]
DEGRADED = ["stale", "falsified"]
ALL_STATUSES = STATUSES + DEGRADED

# Only these may drive a live decision.
DRIVING = {"validated", "trusted"}

LIST_KEYS = {
    "supports", "feeds", "depends_on", "tested_by", "guards",
    "guarded_by", "contradicts", "realised_as", "tags",
}


class VaultError(Exception):
    pass


def _coerce(raw: str) -> Any:
    v = raw.strip()
    if v.startswith("[") and v.endswith("]"):
        inner = v[1:-1].strip()
        if not inner:
            return []
        return [p.strip().strip("'\"") for p in inner.split(",") if p.strip()]
    if v.startswith(("'", '"')) and v.endswith(("'", '"')) and len(v) > 1:
        return v[1:-1]
    low = v.lower()
    if low in ("true", "false"):
        return low == "true"
    if low in ("null", "none", "~", ""):
        return None
    if re.fullmatch(r"-?\d+", v):
        return int(v)
    if re.fullmatch(r"-?\d*\.\d+", v):
        return float(v)
    return v


def parse_frontmatter(text: str) -> tuple[Dict[str, Any], str]:
    """Return (frontmatter, body). Tolerates a missing frontmatter block."""
    if not text.startswith("---"):
        return {}, text
    end = text.find("\n---", 3)
    if end == -1:
        return {}, text
    head = text[3:end].strip("\n")
    body = text[end + 4:].lstrip("\n")

    data: Dict[str, Any] = {}
    pending: Optional[str] = None
    for line in head.splitlines():
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        if line.lstrip().startswith("- ") and pending:
            data.setdefault(pending, [])
            data[pending].append(_coerce(line.lstrip()[2:]))
            continue
        if ":" not in line:
            continue
        key, _, rest = line.partition(":")
        key = key.strip()
        if not rest.strip():
            pending = key
            data.setdefault(key, [])
        else:
            pending = None
            data[key] = _coerce(rest)
    for k in LIST_KEYS:
        if k in data and not isinstance(data[k], list):
            data[k] = [data[k]] if data[k] else []
    return data, body


def dump_frontmatter(fm: Dict[str, Any]) -> str:
    order = ["id", "type", "status", "domain", "title", "worth", "ttl_days",
             "last_evidence", "owner", "permitted_claim"]
    keys = [k for k in order if k in fm] + [k for k in fm if k not in order]
    out = ["---"]
    for k in keys:
        v = fm[k]
        if isinstance(v, list):
            out.append(f"{k}: [{', '.join(str(x) for x in v)}]")
        elif v is None:
            out.append(f"{k}:")
        elif isinstance(v, bool):
            out.append(f"{k}: {'true' if v else 'false'}")
        elif isinstance(v, str) and (":" in v or v.strip() != v):
            out.append(f'{k}: "{v}"')
        else:
            out.append(f"{k}: {v}")
    out.append("---")
    return "\n".join(out)


class Node:
    __slots__ = ("path", "fm", "body")

    def __init__(self, path: Path, fm: Dict[str, Any], body: str):
        self.path, self.fm, self.body = path, fm, body

    # --- identity -----------------------------------------------------
    @property
    def id(self) -> str:
        return str(self.fm.get("id") or self.path.stem)

    @property
    def type(self) -> str:
        return str(self.fm.get("type") or "claim").lower()

    @property
    def status(self) -> str:
        return str(self.fm.get("status") or "asserted").lower()

    @property
    def domain(self) -> str:
        return str(self.fm.get("domain") or "general")

    def links(self, key: str) -> List[str]:
        v = self.fm.get(key) or []
        return list(v) if isinstance(v, list) else [v]

    # --- lifecycle ----------------------------------------------------
    @property
    def ttl_days(self) -> Optional[int]:
        v = self.fm.get("ttl_days")
        return int(v) if isinstance(v, (int, float)) else None

    @property
    def last_evidence(self) -> Optional[_dt.date]:
        v = self.fm.get("last_evidence")
        if not v:
            return None
        try:
            return _dt.date.fromisoformat(str(v)[:10])
        except ValueError:
            return None

    def expired(self, today: Optional[_dt.date] = None) -> bool:
        """A claim with no fresh evidence is not a claim, it is a memory."""
        ttl = self.ttl_days
        if not ttl:
            return False
        last = self.last_evidence
        if last is None:
            return self.status in ("measured", "validated", "trusted")
        return ((today or _dt.date.today()) - last).days > ttl

    def drives_money(self) -> bool:
        return self.status in DRIVING

    # --- io -----------------------------------------------------------
    def write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_text(
            dump_frontmatter(self.fm) + "\n\n" + self.body.strip() + "\n",
            encoding="utf-8",
        )

    def set_status(self, status: str, reason: str = "") -> None:
        status = status.lower()
        if status not in ALL_STATUSES:
            raise VaultError(f"unknown status {status!r}")
        prev = self.status
        if prev == status:
            return
        self.fm["status"] = status
        stamp = _dt.date.today().isoformat()
        line = f"- {stamp} — `{prev}` → `{status}`" + (f" — {reason}" if reason else "")
        if "## Status history" in self.body:
            self.body = self.body.replace(
                "## Status history", "## Status history\n\n" + line, 1)
        else:
            self.body = self.body.rstrip() + "\n\n## Status history\n\n" + line

    def __repr__(self) -> str:
        return f"<{self.type} {self.id} [{self.status}]>"


def load_node(path: Path) -> Node:
    fm, body = parse_frontmatter(Path(path).read_text(encoding="utf-8"))
    return Node(Path(path), fm, body)


def iter_nodes(vault: Optional[Path] = None) -> Iterable[Node]:
    root = Path(vault or VAULT)
    for sub in NODE_DIRS.values():
        d = root / sub
        if not d.is_dir():
            continue
        for p in sorted(d.glob("*.md")):
            if p.name.startswith("_"):
                continue
            try:
                yield load_node(p)
            except Exception as exc:  # a malformed node must not kill the loop
                print(f"[vault] skipping {p.name}: {exc}")


def node_path(node_id: str, node_type: str, vault: Optional[Path] = None) -> Path:
    sub = NODE_DIRS.get(node_type.lower(), "Claims")
    slug = node_id.split(".")[-1] if "." in node_id else node_id
    return Path(vault or VAULT) / sub / f"{slug}.md"
