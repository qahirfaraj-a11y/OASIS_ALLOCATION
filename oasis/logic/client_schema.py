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


def create_clause_for(dialect: str) -> str:
    """The CREATE-VIEW prefix for a SQL dialect (pure)."""
    return _CREATE_CLAUSE.get(str(dialect or "").lower(), "CREATE VIEW")


def identity_profile() -> dict:
    """A no-op profile: canonical names map to themselves (the RXL schema)."""
    return {t: {"source": t, "columns": {c: c for c in cols}}
            for t, cols in CANONICAL.items()}


def load_profile(path: str) -> dict:
    """Load a client schema profile from JSON."""
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def validate_profile(profile: dict) -> dict:
    """Check a profile covers the REQUIRED contract (pure).

    Returns {ok, missing_tables, missing_columns}. Recommended tables are not
    required, so their absence does not fail validation.
    """
    profile = profile or {}
    missing_tables: List[str] = []
    missing_columns: Dict[str, List[str]] = {}
    for table, req_cols in REQUIRED_POS_TABLES.items():
        if table not in profile:
            missing_tables.append(table)
            continue
        mapped = (profile[table] or {}).get("columns", {}) or {}
        miss = [c for c in req_cols if c not in mapped]
        if miss:
            missing_columns[table] = miss
    return {
        "ok": not missing_tables and not missing_columns,
        "missing_tables": missing_tables,
        "missing_columns": missing_columns,
    }


def generate_view_ddl(profile: dict, create_clause: str = "CREATE VIEW") -> List[str]:
    """Emit one CREATE VIEW statement per mapped table (pure).

    Each view exposes the client's table under the canonical name with the
    client's columns aliased to canonical columns, e.g.:
        CREATE VIEW ITEM_MST AS SELECT ProductCode AS ITM_CD, ... FROM dbo.Products;
    """
    stmts: List[str] = []
    for canon_table, spec in (profile or {}).items():
        spec = spec or {}
        source = spec.get("source")
        cols = spec.get("columns", {}) or {}
        if not source or not cols:
            continue
        select = ", ".join(f"{src} AS {canon}" for canon, src in cols.items())
        stmts.append(f"{create_clause} {canon_table} AS SELECT {select} FROM {source};")
    return stmts
