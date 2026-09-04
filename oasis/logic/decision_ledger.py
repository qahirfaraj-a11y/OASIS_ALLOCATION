"""The decision ledger — every decision, with the inputs and parameters in force.

WHY THIS IS STEP ONE
    Loop B attributes realised outcomes back to specific parameters. That is
    impossible after the fact unless the decision was written down WITH the
    inputs it saw and the configuration it ran under. Reconstructing "what did
    the engine think on 14 March" from code and today's config is guesswork, and
    guesswork is how an unvalidated model quietly keeps its job.

    So: append-only, written at decision time, cheap enough to always be on.

WHAT GOES IN
    decision   one PO line, one transfer, one site recommendation
    inputs     the enriched vector the engine actually saw (ADS, cv, on_order,
               lead time, observed gap, stock position, catchment, …)
    params     R, L, z, base_safety, SIZE_EXPONENT, DISTANCE_DECAY, blend ratio…
    config_hash  stable hash of params, so evidence ties to a configuration

WHAT COMES BACK LATER
    outcomes   realised GRN, realised sales, stockout, spoilage, store revenue
               — linked by decision id, observed afterwards, so the measure
               cannot be circular (see traps.T5).

USAGE
    from oasis.logic.decision_ledger import DecisionLedger
    led = DecisionLedger()
    did = led.log_decision(kind="order", org="ORG001", entity="SKU123",
                           quantity=48, inputs={...}, params={...},
                           source="command_center")
    ...
    led.log_outcome(did, kind="grn_received", value=48, observed_at=...)
"""
from __future__ import annotations

import datetime as _dt
import hashlib
import json
import os
import sqlite3
import uuid
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

REPO = Path(__file__).resolve().parents[2]
DEFAULT_DB = Path(os.environ.get(
    "OASIS_DECISION_LEDGER", REPO / "oasis" / "data" / "oasis_decisions.db"))

KINDS = ("order", "transfer", "site", "allocation")

SCHEMA = """
CREATE TABLE IF NOT EXISTS decisions (
    id            TEXT PRIMARY KEY,
    decided_at    TEXT NOT NULL,
    kind          TEXT NOT NULL,
    org           TEXT,
    entity        TEXT,
    quantity      REAL,
    inputs        TEXT NOT NULL,
    params        TEXT NOT NULL,
    config_hash   TEXT NOT NULL,
    engine_version TEXT,
    source        TEXT,
    run_id        TEXT
);
CREATE INDEX IF NOT EXISTS ix_dec_kind_time ON decisions(kind, decided_at);
CREATE INDEX IF NOT EXISTS ix_dec_entity    ON decisions(org, entity);
CREATE INDEX IF NOT EXISTS ix_dec_config    ON decisions(config_hash);

CREATE TABLE IF NOT EXISTS outcomes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    decision_id   TEXT NOT NULL REFERENCES decisions(id),
    observed_at   TEXT NOT NULL,
    kind          TEXT NOT NULL,
    value         REAL,
    payload       TEXT,
    UNIQUE(decision_id, kind, observed_at)
);
CREATE INDEX IF NOT EXISTS ix_out_decision ON outcomes(decision_id);
CREATE INDEX IF NOT EXISTS ix_out_kind     ON outcomes(kind, observed_at);
"""


def config_hash(params: Dict[str, Any]) -> str:
    blob = json.dumps(params, sort_keys=True, default=str).encode()
    return hashlib.sha256(blob).hexdigest()[:12]


class DecisionLedger:
    def __init__(self, db_path: Optional[os.PathLike | str] = None):
        self.path = Path(db_path or DEFAULT_DB)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(str(self.path))
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript(SCHEMA)
        self._conn.commit()

    # -------------------------------------------------------------- write
    def log_decision(self, *, kind: str, inputs: Dict[str, Any],
                     params: Dict[str, Any], org: str = "", entity: str = "",
                     quantity: Optional[float] = None, source: str = "",
                     engine_version: str = "", run_id: str = "",
                     decided_at: Optional[str] = None) -> str:
        if kind not in KINDS:
            raise ValueError(f"kind must be one of {KINDS}, got {kind!r}")
        did = uuid.uuid4().hex
        self._conn.execute(
            "INSERT INTO decisions (id, decided_at, kind, org, entity, quantity,"
            " inputs, params, config_hash, engine_version, source, run_id)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
            (did, decided_at or _dt.datetime.now().isoformat(timespec="seconds"),
             kind, org, entity, quantity,
             json.dumps(inputs, default=str, sort_keys=True),
             json.dumps(params, default=str, sort_keys=True),
             config_hash(params), engine_version, source, run_id))
        self._conn.commit()
        return did

    def log_many(self, kind: str, rows: Iterable[Dict[str, Any]],
                 params: Dict[str, Any], **common: Any) -> List[str]:
        """Bulk path — an ordering run writes thousands of lines; keep it one commit."""
        ch = config_hash(params)
        pj = json.dumps(params, default=str, sort_keys=True)
        now = _dt.datetime.now().isoformat(timespec="seconds")
        run_id = common.pop("run_id", uuid.uuid4().hex[:12])
        payload, ids = [], []
        for r in rows:
            did = uuid.uuid4().hex
            ids.append(did)
            payload.append((
                did, r.get("decided_at") or now, kind,
                r.get("org", common.get("org", "")),
                r.get("entity", ""), r.get("quantity"),
                json.dumps(r.get("inputs", {}), default=str, sort_keys=True),
                pj, ch, common.get("engine_version", ""),
                common.get("source", ""), run_id))
        self._conn.executemany(
            "INSERT INTO decisions (id, decided_at, kind, org, entity, quantity,"
            " inputs, params, config_hash, engine_version, source, run_id)"
            " VALUES (?,?,?,?,?,?,?,?,?,?,?,?)", payload)
        self._conn.commit()
        return ids

    def merge_from(self, other: os.PathLike | str) -> int:
        """Pull another ledger's rows in, in one transaction.

        A sweep writing 21,000 rows one INSERT at a time takes 109 seconds
        against a network-mounted database and half a second against a local
        one. So the agents write locally and the run is merged here — the
        durable ledger still ends up with every row, and the run stops being
        dominated by filesystem latency.
        """
        other = Path(other)
        if not other.exists():
            return 0
        self._conn.execute("ATTACH DATABASE ? AS src", (str(other),))
        try:
            n = self._conn.execute(
                "SELECT COUNT(*) FROM src.decisions").fetchone()[0]
            self._conn.execute(
                "INSERT OR IGNORE INTO decisions SELECT * FROM src.decisions")
            self._conn.execute(
                "INSERT OR IGNORE INTO outcomes (decision_id, observed_at, kind,"
                " value, payload) SELECT decision_id, observed_at, kind, value,"
                " payload FROM src.outcomes")
            self._conn.commit()
        finally:
            self._conn.execute("DETACH DATABASE src")
        return n

    def log_outcome(self, decision_id: str, *, kind: str,
                    value: Optional[float] = None,
                    payload: Optional[Dict[str, Any]] = None,
                    observed_at: Optional[str] = None) -> None:
        self._conn.execute(
            "INSERT OR REPLACE INTO outcomes (decision_id, observed_at, kind,"
            " value, payload) VALUES (?,?,?,?,?)",
            (decision_id,
             observed_at or _dt.datetime.now().isoformat(timespec="seconds"),
             kind, value, json.dumps(payload or {}, default=str)))
        self._conn.commit()

    # --------------------------------------------------------------- read
    def decisions(self, kind: Optional[str] = None, since: Optional[str] = None,
                  org: Optional[str] = None, limit: int = 10_000) -> List[dict]:
        sql = "SELECT * FROM decisions WHERE 1=1"
        args: List[Any] = []
        if kind:
            sql, _ = sql + " AND kind=?", args.append(kind)
        if since:
            sql, _ = sql + " AND decided_at>=?", args.append(since)
        if org:
            sql, _ = sql + " AND org=?", args.append(org)
        sql += " ORDER BY decided_at DESC LIMIT ?"
        args.append(limit)
        return [self._row(r) for r in self._conn.execute(sql, args)]

    def resolved(self, outcome_kind: str, decision_kind: Optional[str] = None,
                 limit: int = 10_000) -> List[dict]:
        """Decisions that have a realised outcome — Loop B's working set."""
        sql = ("SELECT d.*, o.kind AS outcome_kind, o.value AS outcome_value,"
               " o.observed_at, o.payload AS outcome_payload"
               " FROM decisions d JOIN outcomes o ON o.decision_id = d.id"
               " WHERE o.kind = ?")
        args: List[Any] = [outcome_kind]
        if decision_kind:
            sql, _ = sql + " AND d.kind = ?", args.append(decision_kind)
        sql += " ORDER BY o.observed_at DESC LIMIT ?"
        args.append(limit)
        return [self._row(r) for r in self._conn.execute(sql, args)]

    def unresolved(self, kind: str = "order", older_than_days: int = 30) -> List[dict]:
        """Decisions still waiting on an outcome. A growing number here means
        the loop is open at the observation step, not the analysis step."""
        cutoff = (_dt.date.today() - _dt.timedelta(days=older_than_days)).isoformat()
        return [self._row(r) for r in self._conn.execute(
            "SELECT d.* FROM decisions d LEFT JOIN outcomes o"
            " ON o.decision_id = d.id WHERE o.id IS NULL AND d.kind=?"
            " AND d.decided_at < ? ORDER BY d.decided_at", (kind, cutoff))]

    def summary(self) -> Dict[str, Any]:
        cur = self._conn.execute(
            "SELECT kind, COUNT(*) n, MIN(decided_at) a, MAX(decided_at) b"
            " FROM decisions GROUP BY kind")
        by_kind = {r["kind"]: {"n": r["n"], "first": r["a"], "last": r["b"]}
                   for r in cur}
        outs = self._conn.execute(
            "SELECT kind, COUNT(*) n FROM outcomes GROUP BY kind").fetchall()
        configs = self._conn.execute(
            "SELECT COUNT(DISTINCT config_hash) n FROM decisions").fetchone()["n"]
        return {"db": str(self.path), "decisions": by_kind,
                "outcomes": {r["kind"]: r["n"] for r in outs},
                "distinct_configs": configs}

    @staticmethod
    def _row(r: sqlite3.Row) -> dict:
        d = dict(r)
        for k in ("inputs", "params", "outcome_payload"):
            if isinstance(d.get(k), str):
                try:
                    d[k] = json.loads(d[k])
                except json.JSONDecodeError:
                    pass
        return d

    def close(self) -> None:
        self._conn.close()

    def __enter__(self) -> "DecisionLedger":
        return self

    def __exit__(self, *exc: Any) -> None:
        self.close()


if __name__ == "__main__":
    import argparse
    ap = argparse.ArgumentParser(description="Decision ledger status")
    ap.add_argument("--db", default=None)
    ap.add_argument("--unresolved", action="store_true")
    a = ap.parse_args()
    led = DecisionLedger(a.db)
    print(json.dumps(led.summary(), indent=2))
    if a.unresolved:
        rows = led.unresolved()
        print(f"\n{len(rows)} order decisions with no outcome after 30 days")
