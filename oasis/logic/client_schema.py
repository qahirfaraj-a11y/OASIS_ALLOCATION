"""
Per-client schema adaptation (client onboarding).

OASIS's adapter SQL uses canonical table/column names (ITEM_MST, ITM_CD, …). A
client whose POS/ERP uses different names is bridged with **read-only views**
named as the canonical contract. This module turns a small **schema profile**
(canonical name → the client's actual table/column) into the `CREATE VIEW` DDL a
DBA runs once, and validates the profile against the required contract.

The canonical contract is imported from :mod:`oasis.logic.preflight`, so this
adapter layer, the preflight, and the onboarding doc all share one source of
truth.

Usage:
    python entrypoint.py --mode build-views          # identity (RXL schema)
    OASIS_SCHEMA_PROFILE=client.json python entrypoint.py --mode build-views

Profile JSON shape:
    {
      "ITEM_MST": {"source": "dbo.Products",
                   "columns": {"ITM_CD": "ProductCode", "ITM_LONG_NAME": "Name", ...}},
      ...
    }
"""

from __future__ import annotations

import json
import re
from typing import Dict, List

from .preflight import RECOMMENDED_POS_TABLES, REQUIRED_POS_TABLES

# Canonical contract = required + recommended (preflight is the source of truth).
CANONICAL: Dict[str, List[str]] = {**REQUIRED_POS_TABLES, **RECOMMENDED_POS_TABLES}

_CREATE_CLAUSE = {
    "mssql": "CREATE OR ALTER VIEW",
    "postgres": "CREATE OR REPLACE VIEW",
    "postgresql": "CREATE OR REPLACE VIEW",
    "mysql": "CREATE OR REPLACE VIEW",
    "sqlite": "CREATE VIEW",
    "ansi": "CREATE VIEW",
}


#: Dialects where each CREATE VIEW must be its own batch. SQL Server rejects
#: "Incorrect syntax near the keyword 'CREATE'" on the SECOND statement
#: onwards, because CREATE OR ALTER VIEW has to be first in its batch — so a
#: script without separators silently creates only view #1.
_BATCH_SEPARATOR = {"mssql": "GO"}


def create_clause_for(dialect: str) -> str:
    """The CREATE-VIEW prefix for a SQL dialect (pure)."""
    return _CREATE_CLAUSE.get(str(dialect or "").lower(), "CREATE VIEW")


def batch_separator_for(dialect: str) -> str:
    """Statement batch separator for a dialect, '' when none is needed (pure)."""
    return _BATCH_SEPARATOR.get(str(dialect or "").lower(), "")


def identity_profile() -> dict:
    """A no-op profile: canonical names map to themselves (the RXL schema)."""
    return {t: {"source": t, "columns": {c: c for c in cols}}
            for t, cols in CANONICAL.items()}


def load_profile(path: str) -> dict:
    """Load a client schema profile from JSON."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def _mapped_columns(spec: dict) -> set:
    """Canonical columns a spec provides — via real-column map OR literal.

    A malformed spec (a string, a list) provides nothing; it must not raise.
    validate_profile is what a DBA runs to find out whether their hand-written
    profile is usable, so it has to survive bad input and report, not explode.
    """
    if not isinstance(spec, dict):
        return set()
    if spec.get("sql"):
        # A raw-SQL view cannot be parsed reliably, so it must declare what it
        # provides. Without this, validate_profile would call every hand-written
        # view empty and report the whole contract as missing.
        return set(spec.get("provides") or [])
    return (set(spec.get("columns", {}) or {})
            | set(spec.get("literals", {}) or {}))


def validate_profile(profile: dict) -> dict:
    """Check a profile covers the REQUIRED contract (pure).

    Returns {ok, missing_tables, missing_columns}. A required column counts as
    satisfied if mapped to a real column OR provided as a literal/default.
    Recommended tables are not required.
    """
    profile = profile or {}
    missing_tables: List[str] = []
    missing_columns: Dict[str, List[str]] = {}
    for table, req_cols in REQUIRED_POS_TABLES.items():
        if table not in profile:
            missing_tables.append(table)
            continue
        mapped = _mapped_columns(profile[table])
        miss = [c for c in req_cols if c not in mapped]
        if miss:
            missing_columns[table] = miss
    return {
        "ok": not missing_tables and not missing_columns,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
        "self_referencing": self_referencing_tables(profile),
    }


def self_referencing_tables(profile: dict) -> List[str]:
    """Canonical tables whose source has the SAME name (pure).

    Each one is a DDL collision unless the views are emitted into their own
    schema: a view cannot share a name with the table it selects from. RXL hits
    this on nearly every table, because it already uses the canonical names and
    only the COLUMNS differ. Silently emitting that DDL hands a DBA a script
    that cannot run.
    """
    out = []
    for canon_table, spec in (profile or {}).items():
        if canon_table.startswith("_") or not isinstance(spec, dict):
            continue
        source = str(spec.get("source") or "")
        # compare on the bare object name, so dbo.ITEM_MST still counts
        if source.rsplit(".", 1)[-1].upper() == canon_table.upper():
            out.append(canon_table)
    return sorted(out)


_PARAM_RE = re.compile(r":([A-Za-z_][A-Za-z0-9_]*)")


def substitute_params(statements: List[str], params: Dict[str, str]) -> List[str]:
    """Replace ``:name`` placeholders with literal values (pure).

    Profiles use bind-style placeholders in ``where`` clauses (``:store_level``
    pins a multi-level master to the store tier). A bind parameter is NOT valid
    inside CREATE VIEW — the server has nothing to bind it to — so the DDL has
    to carry a literal before a DBA can run it.
    """
    out = []
    for sql in statements:
        for name, value in (params or {}).items():
            sql = re.sub(rf":{re.escape(name)}\b", str(value), sql)
        out.append(sql)
    return out


def unresolved_params(statements: List[str]) -> List[str]:
    """Placeholder names still present after substitution (pure, sorted)."""
    found = set()
    for sql in statements:
        found.update(_PARAM_RE.findall(sql))
    return sorted(found)


def generate_view_ddl(profile: dict, create_clause: str = "CREATE VIEW",
                      schema: str = "", source_schema: str = "") -> List[str]:
    """Emit one CREATE VIEW statement per mapped table (pure).

    Each view exposes the client's table under the canonical name. Three spec
    keys per table:
      * ``columns``  : {canonical: client_column}  → ``client_column AS canonical``
      * ``literals`` : {canonical: sql_expr}        → ``sql_expr AS canonical``
                       (e.g. ``"NULL"`` for an attribute the ERP doesn't carry —
                       the adapter's defaults then apply)
      * ``where``    : optional filter (e.g. pin a multi-level master to the
                       store tier: ``"SM_LEVEL_NUMBER = 1"``)

    ``schema`` qualifies the VIEW name only, never the source. This matters:
    RXL already calls its tables ITEM_MST, STOCK_MASTER, POS_SALES_DTL and so
    on, so an unqualified view would collide with the very table it selects
    from ("There is already an object named 'ITEM_MST'"). Emit into a dedicated
    schema — ``OASIS_VIEW_SCHEMA=OASIS`` — and set the OASIS service account's
    DEFAULT schema to it. The adapter queries unqualified names, so they then
    resolve to the views first and fall back to the client's own objects for
    anything not overridden, with no adapter change and no writes to dbo.

    ``source_schema`` then becomes MANDATORY, not cosmetic. Inside a view body,
    an unqualified name resolves against the VIEW's schema first — so
    ``CREATE VIEW OASIS.ITEM_MST AS SELECT ... FROM ITEM_MST`` binds to itself
    and SQL Server refuses it: "contains a self-reference". The source must be
    qualified (``dbo.ITEM_MST``) so the view reads the real table.
    """
    stmts: List[str] = []
    for canon_table, spec in (profile or {}).items():
        # Profiles carry documentation alongside table specs — the shipped RXL
        # profile leads with a "_README" string. `spec or {}` keeps a non-empty
        # string, so .get() raised AttributeError and build-views died on the
        # only real profile we have. Metadata keys are not tables: skip anything
        # that is not a mapping, and any key the author marked private with "_".
        if canon_table.startswith("_") or not isinstance(spec, dict):
            continue
        # Escape hatch: a canonical table whose data cannot be produced by
        # renaming columns on ONE source. Real RXL needs this — SUPPLIER_CD is
        # not on ITEM_MST (it lives on BASIC_CP_MST.BCP_VEND_CD) and the
        # department NAME is in DIVISION_MST — so a 1:1 mapping cannot express
        # it at all. `sql` is emitted verbatim as the view body.
        raw = spec.get("sql")
        if raw:
            view_name = f"{schema}.{canon_table}" if schema else canon_table
            stmts.append(f"{create_clause} {view_name} AS {raw.strip().rstrip(';')};")
            continue

        source = spec.get("source")
        cols = spec.get("columns", {}) or {}
        literals = spec.get("literals", {}) or {}
        where = spec.get("where")
        if not source or (not cols and not literals):
            continue
        parts = [f"{src} AS {canon}" for canon, src in cols.items()]
        parts += [f"{expr} AS {canon}" for canon, expr in literals.items()]
        view_name = f"{schema}.{canon_table}" if schema else canon_table
        # Qualify the SOURCE unless the profile already did. Without this, a
        # view in a non-default schema binds its own name and self-references.
        src = source if "." in source else (
            f"{source_schema}.{source}" if source_schema else source)
        sql = f"{create_clause} {view_name} AS SELECT {', '.join(parts)} FROM {src}"
        if where:
            sql += f" WHERE {where}"
        stmts.append(sql + ";")
    return stmts
