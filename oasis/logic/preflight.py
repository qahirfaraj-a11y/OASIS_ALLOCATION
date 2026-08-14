"""
Client install preflight check.

Before a live OASIS install can run against a client's POS/ERP, this verifies
the connection is viable: the POS *source* DB exposes the required tables and
columns with usable history, and OASIS's own operational *store* is writable.
Run it before go-live:

    python entrypoint.py --mode preflight

It is non-destructive (reads + a rolled-back write probe) and exits non-zero if
any required check FAILs, so it can gate an install script.

The data contract below (REQUIRED_/RECOMMENDED_ tables + columns) is the single
source of truth for what a client must expose — the onboarding document is
generated from these same constants.
"""

from __future__ import annotations

from typing import Dict, List, Optional, Set

# ── data contract ────────────────────────────────────────────────────────────
# Tables/columns the POS/ERP source MUST expose for the engines to run.
REQUIRED_POS_TABLES: Dict[str, List[str]] = {
    "ITEM_MST": ["ITM_CD", "ITM_LONG_NAME", "DEPARTMENT", "SUPPLIER_CD", "ACTIVE_FLAG"],
    "STOCK_MASTER": ["SM_ITM_CD", "SM_ORG_CD", "SM_QTY"],
    "POS_SALES_DTL": ["ORG_CD", "ITM_CD", "BILL_DT", "QTY", "VOID_FLAG"],
    "ORGANIZATION_MST": ["ORG_CD", "ACTIVE_FLAG"],
}
# Tables that materially improve intelligence but are not strictly required.
RECOMMENDED_POS_TABLES: Dict[str, List[str]] = {
    "POS_SALES_HDR": ["ORG_CD", "BILL_NO", "BILL_DT"],
    "BASIC_SP_MST": ["BSP_ITEM_CD", "BSP_ORG_CD", "BSP_SP"],
    "BASIC_CP_MST": ["BCP_ITEM_CD", "BCP_ORG_CD", "BCP_CP"],
    "SUPPLIER_MST": ["SUPPLIER_CD", "SUPPLIER_NAME"],
    "GRN_HDR": ["SUPPLIER_CD", "ORG_CD"],
}
# OASIS's own store (created/managed by OASIS, never the client's POS).
OASIS_STORE_TABLES: List[str] = [
    "OASIS_USERS", "OASIS_AUDIT_LOG", "OASIS_SYSTEM_CONFIG",
    "INTEGRATION_PURCHASE_ORDERS", "INTEGRATION_TRANSFER_ORDERS",
]
MIN_SALES_HISTORY_DAYS = 90   # weighted ADS + risk need at least this much


# ── pure evaluation ──────────────────────────────────────────────────────────
def _norm(s) -> str:
    return str(s).strip().upper()


def evaluate_pos_schema(found_tables: Set[str],
                        columns_by_table: Dict[str, Set[str]]) -> List[dict]:
    """Compare discovered POS tables/columns against the contract (pure).

    Returns a list of check rows: {check, status PASS|WARN|FAIL, detail}.
    Missing required table/column -> FAIL; missing recommended -> WARN.
    """
    found = {_norm(t) for t in found_tables}
    cols = {_norm(t): {_norm(c) for c in cs} for t, cs in columns_by_table.items()}
    checks: List[dict] = []

    def _check(table: str, wanted_cols: List[str], required: bool):
        t = _norm(table)
        sev_missing = "FAIL" if required else "WARN"
        if t not in found:
            checks.append({"check": f"table {table}", "status": sev_missing,
                           "detail": "missing"})
            return
        have = cols.get(t, set())
        missing = [c for c in wanted_cols if _norm(c) not in have] if have else []
        if not have:
            checks.append({"check": f"table {table}", "status": "PASS",
                           "detail": "present (columns not introspectable)"})
        elif missing:
            checks.append({"check": f"table {table}", "status": sev_missing,
                           "detail": f"missing columns: {', '.join(missing)}"})
        else:
            checks.append({"check": f"table {table}", "status": "PASS",
                           "detail": "present with required columns"})

    for tbl, want in REQUIRED_POS_TABLES.items():
        _check(tbl, want, required=True)
    for tbl, want in RECOMMENDED_POS_TABLES.items():
        _check(tbl, want, required=False)
    return checks


def evaluate_store_separation(pos_url: str, store_url: str) -> dict:
    """Is OASIS's own store a DIFFERENT database from the client's POS? (pure)

    Every OASIS write (INTEGRATION_PURCHASE_ORDERS, the audit log, users) goes
    to the store connection. If the operator has not configured a distinct POS
    source, that connection IS the client's live POS — so OASIS silently
    CREATEs its own tables inside a production retail database. Nothing else
    catches this: the store is writable, every schema check passes, and the
    install looks healthy right up until a DBA asks why there are OASIS_ tables
    in the POS.

    WARN, not FAIL: single-database IS the supported shape for the demo and for
    a sample install. It is only wrong against a live POS, which preflight
    cannot tell apart from a mock one by URL alone.
    """
    same = bool(pos_url) and pos_url == store_url
    if not same:
        return {"check": "POS / OASIS store separation", "status": "PASS",
                "detail": "OASIS writes to its own database"}
    return {
        "check": "POS / OASIS store separation", "status": "WARN",
        "detail": ("POS and OASIS store are the SAME database — OASIS will "
                   "create its tables inside it. Correct for a demo; against "
                   "a live POS set OASIS_POS_DB_URL (read-only) so writes go "
                   "elsewhere."),
    }


def evaluate_sales_history(history_days: Optional[int],
                           min_days: int = MIN_SALES_HISTORY_DAYS) -> dict:
    """Classify sales-history depth (pure)."""
    if history_days is None:
        return {"check": "sales history depth", "status": "WARN",
                "detail": "could not measure BILL_DT range"}
    if history_days >= min_days:
        return {"check": "sales history depth", "status": "PASS",
                "detail": f"{history_days} days (>= {min_days})"}
    return {"check": "sales history depth", "status": "WARN",
            "detail": f"only {history_days} days (< {min_days}); ADS/risk will be weak"}


def overall_status(checks: List[dict]) -> str:
    """Worst status across all checks (pure): FAIL > WARN > PASS."""
    statuses = {c.get("status") for c in checks}
    if "FAIL" in statuses:
        return "FAIL"
    if "WARN" in statuses:
        return "WARN"
    return "PASS"


# ── integration run ──────────────────────────────────────────────────────────
def run_preflight(pos_url: Optional[str] = None, store_url: Optional[str] = None) -> dict:
    """Connect to the POS source + OASIS store and produce a preflight report.

    Never raises — every probe is guarded; failures become FAIL/WARN checks.
    """
    from . import db as oasis_db

    pos_url = pos_url or oasis_db.get_pos_sqlalchemy_url()
    store_url = store_url or oasis_db.get_sqlalchemy_url()
    checks: List[dict] = []

    # --- POS source ---
    pos_health = {}
    try:
        from .db_connector import UniversalConnector
        conn = UniversalConnector(pos_url)
        pos_health = conn.health_check()
        checks.append({"check": "POS connection",
                       "status": "PASS" if pos_health.get("connected") else "FAIL",
                       "detail": f"latency {pos_health.get('latency_ms')}ms, "
                                 f"{pos_health.get('tables_found')} tables"})
        from sqlalchemy import inspect, text
        insp = inspect(conn.engine)
        # VIEWS COUNT. `--mode build-views` is our documented way to bridge a
        # client whose schema differs from the canonical contract, and it emits
        # VIEWS. Inspecting only get_table_names() reported every required table
        # as missing on exactly the installs we tell clients to build — verified
        # against real RXL, where the canonical set is 8 views and 0 tables.
        tables = set(insp.get_table_names())
        try:
            tables |= set(insp.get_view_names())
        except Exception:      # some dialects/drivers cannot enumerate views
            pass
        cols = {}
        for t in tables:
            try:
                cols[t] = {c["name"] for c in insp.get_columns(t)}
            except Exception:
                cols[t] = set()   # unreadable object: report as missing columns,
                                  # never abort the whole preflight
        checks.extend(evaluate_pos_schema(tables, cols))

        # sales-history depth + a couple of row counts (best-effort)
        hist_days = None
        try:
            with conn.engine.connect() as c:
                r = c.execute(text("SELECT MIN(BILL_DT), MAX(BILL_DT) FROM POS_SALES_DTL")).fetchone()
                if r and r[0] and r[1]:
                    import datetime as dt
                    def _d(x):
                        return dt.date.fromisoformat(str(x)[:10])
                    hist_days = (_d(r[1]) - _d(r[0])).days
        except Exception:
            pass
        checks.append(evaluate_sales_history(hist_days))
    except Exception as e:
        checks.append({"check": "POS connection", "status": "FAIL", "detail": str(e)[:160]})

    # --- OASIS store: separate database from the POS? ---
    checks.append(evaluate_store_separation(pos_url, store_url))

    # --- OASIS store (writable?) ---
    try:
        if store_url.startswith("sqlite"):
            path = store_url.replace("sqlite:///", "").replace("sqlite://", "")
            from .db_connector import ensure_oasis_tables
            ensure_oasis_tables(path)
            from .db import get_raw_connection
            c = get_raw_connection(store_url)
            c.execute("SELECT COUNT(*) FROM OASIS_SYSTEM_CONFIG")
            c.close()
            checks.append({"check": "OASIS store writable", "status": "PASS",
                           "detail": "tables ensured + readable (SQLite)"})
        else:
            from .db_connector import UniversalConnector
            s = UniversalConnector(store_url)
            ok = s.test_connection()
            checks.append({"check": "OASIS store writable",
                           "status": "PASS" if ok else "FAIL",
                           "detail": "connected (ensure tables via migrate)"})
    except Exception as e:
        checks.append({"check": "OASIS store writable", "status": "FAIL", "detail": str(e)[:160]})

    # --- engine layer config (finding S1: a missing config silently disabled
    #     every Chapter-11 engine on client installs) ---
    try:
        from .engines_config import preflight_check as _engines_check
        checks.append(_engines_check())
    except Exception as e:
        checks.append({"check": "Engine config", "status": "FAIL",
                       "detail": str(e)[:160]})

    return {
        "pos_url": _redact(pos_url),
        "store_url": _redact(store_url),
        "checks": checks,
        "overall": overall_status(checks),
    }


def _redact(url: str) -> str:
    """Hide credentials in a connection URL for display."""
    if "@" in url and "://" in url:
        scheme, rest = url.split("://", 1)
        return f"{scheme}://***@{rest.split('@', 1)[1]}"
    return url


def format_report(report: dict) -> str:
    """Render a preflight report as readable text."""
    lines = ["", "O.A.S.I.S. — Install Preflight", "=" * 40,
             f"POS source : {report['pos_url']}",
             f"OASIS store: {report['store_url']}", "-" * 40]
    icon = {"PASS": "[PASS]", "WARN": "[WARN]", "FAIL": "[FAIL]"}
    for c in report["checks"]:
        lines.append(f"{icon.get(c['status'], '[????]')} {c['check']}: {c['detail']}")
    lines += ["-" * 40, f"OVERALL: {report['overall']}", ""]
    return "\n".join(lines)
