"""
First-run onboarding — how a fresh OASIS install chooses its data source.

Before this existed, ``--mode init`` silently built the Rhapta demo catalogue,
which (a) masqueraded as real data and (b) broke on a real client install
because the demo spreadsheets aren't shipped. Onboarding replaces that with an
explicit first-run choice the operator makes in the Home launcher:

  * ``demo``  — build a self-contained SAMPLE store (oasis.logic.demo_seed) so
                the operator can explore the product immediately. Clearly badged.
  * ``empty`` — build the full OASIS schema with ZERO items: a genuine fresh
                console to begin from (import/connect data next).
  * ``connect``— point OASIS at an existing POS database (records the URL and
                verifies it is reachable + has the canonical schema).

The choice is recorded in ``oasis/data/.oasis_onboarding.json``. Until a choice
is made, ``is_onboarded()`` is False and the Home page shows the wizard instead
of pretending an empty/mock store is real.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional

ONBOARD_FILE = ".oasis_onboarding.json"
SOURCES = ("demo", "empty", "connect")


def _root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def onboard_path(root: Optional[str] = None) -> str:
    return os.path.join(root or _root(), "oasis", "data", ONBOARD_FILE)


def load_onboarding(root: Optional[str] = None) -> dict:
    p = onboard_path(root)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, ValueError):
        return {}


def save_onboarding(payload: dict, root: Optional[str] = None) -> str:
    p = onboard_path(root)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return p


def is_onboarded(root: Optional[str] = None) -> bool:
    return bool(load_onboarding(root).get("source"))


def is_demo(root: Optional[str] = None) -> bool:
    """True when the active store is the built-in SAMPLE data (drives the badge)."""
    return load_onboarding(root).get("source") == "demo"


def reset_onboarding(root: Optional[str] = None) -> None:
    p = onboard_path(root)
    if os.path.exists(p):
        os.remove(p)


def default_db_path(root: Optional[str] = None) -> str:
    """Where a demo/empty store is built. Honors OASIS_DB_PATH so the consoles
    (which read the same env) pick it up automatically."""
    return os.getenv("OASIS_DB_PATH",
                     os.path.join(root or _root(), "oasis", "data", "rhapta_pos.db"))


def resolved_db_path(root: Optional[str] = None) -> str:
    """Single source of truth for the active store DB.

    Priority chain:
      1. ``OASIS_DB_PATH`` env var (explicit override — always wins)
      2. Onboarding state ``db_path`` (set by the first-run wizard)
      3. Install profile ``db_path`` (set by ``--mode init``)
      4. Default fallback (``oasis/data/rhapta_pos.db``)

    Every console and the Home app should call this instead of hardcoding
    a default path. This closes W-7 (DB path fragmentation).
    """
    env = os.getenv("OASIS_DB_PATH")
    if env:
        return env
    ob = load_onboarding(root)
    if ob.get("db_path") and os.path.exists(ob["db_path"]):
        return ob["db_path"]
    from .install_profile import load_profile
    ip = load_profile(root)
    if ip.get("db_path") and os.path.exists(ip["db_path"]):
        return ip["db_path"]
    return os.path.join(root or _root(), "oasis", "data", "rhapta_pos.db")


# ── applying a choice ────────────────────────────────────────────────────
def _maybe_restart_trial(root: Optional[str]) -> dict:
    """One-time trial restart when REAL data first onboards (audit D1).

    The 14-day evaluation should measure OASIS against the user's store, not
    against the week they spent poking the sample. Fires only on the
    transition (none|demo) → real, and only once per install — the flag lives
    in the onboarding record, so a plain source-switch can't farm resets.
    """
    prev = load_onboarding(root)
    if prev.get("trial_restarted"):
        return {"trial_restarted": True,
                "trial_restarted_at": prev.get("trial_restarted_at")}
    if prev.get("source") not in (None, "", "demo"):
        return {}                      # was already on real data — no reset
    from .license_manager import restart_trial
    day = restart_trial(root)
    return {"trial_restarted": True, "trial_restarted_at": day.isoformat()}

def apply_demo(store_name: str = "OASIS Sample Store",
               root: Optional[str] = None) -> dict:
    """Build the self-contained sample store and record the choice."""
    from .demo_seed import demo_catalog_rows
    from .mock_pos_build import build_pos_db_from_catalog
    db = default_db_path(root)
    summary = build_pos_db_from_catalog(demo_catalog_rows(), db,
                                        org_name=store_name)
    _record("demo", root, db_path=db, store_name=store_name,
            skus=summary.get("items", 0))
    return summary


def apply_empty(store_name: str = "My Store",
                root: Optional[str] = None) -> dict:
    """Build the full OASIS schema with no items — a fresh console to begin from."""
    from .mock_pos_build import build_pos_db_from_catalog
    db = default_db_path(root)
    extra = _maybe_restart_trial(root)
    summary = build_pos_db_from_catalog([], db, org_name=store_name)
    _record("empty", root, db_path=db, store_name=store_name, skus=0, **extra)
    return summary


def apply_connect(db_url: str, root: Optional[str] = None) -> dict:
    """Record an external POS DB and verify it is reachable with a POS schema.

    Does NOT copy any data — OASIS reads the client system in place (read-only).
    Returns {ok, detail}. Only records the choice when the check passes.
    """
    check = verify_pos_connection(db_url)
    if check["ok"]:
        extra = _maybe_restart_trial(root)
        _record("connect", root, db_url=db_url,
                items=check.get("items"), **extra)
    return check


def verify_pos_connection(db_url: str) -> dict:
    """Connect and confirm the canonical POS catalogue table (ITEM_MST) exists."""
    from . import db as oasis_db
    try:
        with oasis_db.connection(db_url) as conn:
            cur = conn.cursor()
            try:
                cur.execute("SELECT COUNT(*) FROM ITEM_MST")
                n = cur.fetchone()[0]
            except Exception:
                return {"ok": False,
                        "detail": "Connected, but no ITEM_MST table found — this "
                                  "POS uses a different schema and needs a mapping "
                                  "profile. Contact OASIS to set one up."}
        return {"ok": True, "items": n,
                "detail": f"Connected — {n:,} items found in ITEM_MST."}
    except Exception as e:
        return {"ok": False, "detail": f"Could not connect: {e}"}


def _record(source: str, root: Optional[str], **extra) -> None:
    payload = {"source": source, "configured_at": datetime.now().isoformat(timespec="seconds")}
    payload.update({k: v for k, v in extra.items() if v is not None})
    save_onboarding(payload, root)


def apply_init(profile: str, root: Optional[str] = None) -> dict:
    """Build the DB from client catalog using install_profile, and record the choice."""
    from .install_profile import init_install
    summary = init_install(profile=profile, root=root)
    _record("init", root, db_path=summary.get("db_path"), profile=profile)
    return summary
