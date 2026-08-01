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
#: every value _record() can write. "init" is a real catalogue-built store —
#: it was missing here, so the badge fell through to "not onboarded" (S4).
SOURCES = ("demo", "empty", "connect", "init")
#: sources that represent the operator's OWN data (not the built-in sample).
REAL_SOURCES = ("empty", "connect", "init")


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
      3. A connected POS recorded as a local ``sqlite:///`` URL
      4. Install profile ``db_path`` (set by ``--mode init``)
      5. Default fallback (``oasis/data/rhapta_pos.db``)

    Every console and the Home app should call this instead of hardcoding
    a default path. This closes W-7 (DB path fragmentation).

    Tier 3 exists because "Connect a POS" records a ``db_url``, not a
    ``db_path``: without it a connect-only install resolved to the default
    file, which does not exist there, while the badge claimed a live
    connection (finding S2). Non-SQLite POS URLs cannot be represented as a
    path at all — those flow through ``db.get_pos_db_url()`` instead, and
    ``connected_pos_url()`` below is how a caller detects that case.
    """
    env = os.getenv("OASIS_DB_PATH")
    if env:
        return env
    ob = load_onboarding(root)
    if ob.get("db_path") and os.path.exists(ob["db_path"]):
        return ob["db_path"]
    if ob.get("source") == "connect" and ob.get("db_url"):
        from .db import sqlite_path_from_url
        p = sqlite_path_from_url(ob["db_url"])
        if p and os.path.exists(p):
            return p
    from .install_profile import load_profile
    ip = load_profile(root)
    if ip.get("db_path") and os.path.exists(ip["db_path"]):
        return ip["db_path"]
    return os.path.join(root or _root(), "oasis", "data", "rhapta_pos.db")


ADMIN_PW_FLAG = "admin_password_set"


def needs_admin_password(root: Optional[str] = None) -> bool:
    """True when this install still has no password the operator actually knows.

    Seeding always runs — ``ensure_oasis_tables()`` calls ``seed_users()`` — so
    a real store always HAS accounts, holding random one-time passwords that
    were only ever written to a log. A desktop client with no console never
    sees them, so "accounts exist" must not be read as "the operator can sign
    in": that combination is a lockout, not a login.

    True only for a REAL store (a sample store's password is known and
    published in the wizard) that has not yet recorded a set password.
    """
    rec = load_onboarding(root)
    if rec.get("source") not in REAL_SOURCES:
        return False                      # not onboarded, or SAMPLE data
    return not rec.get(ADMIN_PW_FLAG)


def mark_admin_password_set(root: Optional[str] = None) -> None:
    """Record that the operator has chosen the administrator password."""
    rec = load_onboarding(root)
    if rec:
        rec[ADMIN_PW_FLAG] = True
        save_onboarding(rec, root)


def connected_pos_url(root: Optional[str] = None) -> Optional[str]:
    """The POS URL this install connected to, or None. See ``db.get_pos_db_url``."""
    ob = load_onboarding(root)
    if ob.get("source") == "connect" and ob.get("db_url"):
        return str(ob["db_url"])
    return None


def catalog_available(root: Optional[str] = None) -> dict:
    """Are the client's catalogue spreadsheets present for a ``--mode init`` build?

    ``{ok, files, detail}``. The release zip ships no ``.xlsx`` (they are client
    data), so on a clean install this is False and the two catalogue-backed
    onboarding paths cannot run. The wizard asks first and explains, instead of
    offering a button that returns a red error (finding S3).
    """
    import glob
    data_dir = os.getenv("OASIS_DATA_DIR",
                         os.path.join(root or _root(), "oasis", "data"))
    files = sorted(glob.glob(os.path.join(data_dir, "dept_*.xlsx")))
    if files:
        return {"ok": True, "files": len(files),
                "detail": f"{len(files)} catalogue file(s) found in {data_dir}"}
    return {"ok": False, "files": 0,
            "detail": f"no dept_*.xlsx catalogue files in {data_dir}"}


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

#: Password for the SAMPLE store's seeded accounts. This is demo data behind a
#: permanent SAMPLE banner, so a known password is the point — you can tour the
#: consoles immediately. It is passed explicitly and ONLY here: a real store
#: (empty / init / connect) never receives it, and its accounts get a random
#: one-time password until first-run setup sets the operator's own.
DEMO_SEED_PASSWORD = "oasis2026"


def apply_demo(store_name: str = "OASIS Sample Store",
               root: Optional[str] = None) -> dict:
    """Build the self-contained sample store and record the choice."""
    from .demo_seed import demo_catalog_rows
    from .mock_pos_build import build_pos_db_from_catalog
    db = default_db_path(root)
    summary = build_pos_db_from_catalog(demo_catalog_rows(), db,
                                        org_name=store_name,
                                        seed_password=DEMO_SEED_PASSWORD)
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


#: the demo network seeds a shallower history than a real multi-store install.
#: Full profile depth is 62,400 bills across the five stores — minutes of work
#: behind a first-run button. 14 days at a third of the density still gives the
#: velocity, cover and imbalance signal the transfer/allocation tour needs.
DEMO_HISTORY_DAYS = 14
DEMO_HISTORY_DENSITY = 0.33


def _demo_history_profiles():
    """STORE_PROFILES with the history depth dialled down for the demo build."""
    from dataclasses import replace
    from .multi_store_profiles import STORE_PROFILES
    return [replace(p, history_days=DEMO_HISTORY_DAYS,
                    history_bills_per_day=max(
                        20, int(p.history_bills_per_day * DEMO_HISTORY_DENSITY)))
            for p in STORE_PROFILES]


def apply_multi_demo(root: Optional[str] = None) -> dict:
    """Build the multi-store DEMO network and record the choice (audit B3).

    This is explicitly a demo topology (source='demo' → the SAMPLE banner shows
    everywhere, and the trial clock is untouched). Real multi-store rollouts are
    an assisted onboarding — this card exists so a multi install still goes
    through an explicit first-run choice instead of a silent eager build.

    Built from the code-resident demo catalogue, NOT from the client catalogue
    spreadsheets. It used to route through ``init_install(profile="multi")``,
    which loads ``dept_*.xlsx`` — files the release deliberately never ships —
    so on every clean install this card returned a catalog error (finding S3).
    The store topology itself (multi_store_profiles) was already code-resident;
    only the catalogue source needed replacing, and build_multi_store_db()
    already takes rows rather than a directory.
    """
    from .demo_seed import demo_catalog_rows
    from .install_profile import save_profile
    from .multi_store_build import build_multi_store_db
    from .multi_store_pos import seed_multi_store_history

    data_dir = os.getenv("OASIS_DATA_DIR",
                         os.path.join(root or _root(), "oasis", "data"))
    db = os.getenv("OASIS_DB_PATH", os.path.join(data_dir, "rhapta_multi_store.db"))

    r = build_multi_store_db(demo_catalog_rows(), db)
    summary = {"profile": "multi", "db_path": db,
               "stores": r.get("stores", 0),
               "catalog": f"{r.get('stores', 0)} stores, "
                          f"{r.get('catalog_skus', 0):,} SKUs",
               "per_store": r.get("per_store", {})}
    try:
        h = seed_multi_store_history(db, profiles=_demo_history_profiles())
        summary["history"] = (f"{sum(v.get('total_bills', 0) for v in h.values()):,} "
                              "bills seeded")
    except Exception as e:                      # history is a nicety, not the store
        summary["history_error"] = str(e)

    # launchers autodetect topology from the install profile (is_multi_store)
    save_profile({k: v for k, v in summary.items() if k != "per_store"}, root=root)
    _record("demo", root, db_path=db, store_name="Multi-store demo network",
            multi=True, detail=summary["catalog"])
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
    """Build the DB from the client catalogue via install_profile; record the choice.

    Restarts the trial like the other real-data paths do (S4). This is the one
    choice that is unambiguously the operator's own data, and it was the only
    one NOT getting the fresh 14-day clock that rule exists to give.
    """
    from .install_profile import init_install
    summary = init_install(profile=profile, root=root)
    if summary.get("catalog_error"):
        return summary                          # nothing built — record nothing
    extra = _maybe_restart_trial(root)
    tenant = summary.get("tenant")
    if not tenant or tenant == "(unnamed)":     # init_install's placeholder
        tenant = "Your store"
    _record("init", root, db_path=summary.get("db_path"), profile=profile,
            store_name=tenant, detail=summary.get("catalog"), **extra)
    return summary
