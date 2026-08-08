"""
Install profiles — one-command onboarding for single-store vs multi-store.

The old flow made the client run 3–4 modes in order and know which one applies.
`--mode init --profile single` (or `--profile multi`) does the whole sequence,
picks the right DB path, and records the choice in
``oasis/data/.oasis_install_profile.json`` so downstream launchers (Home,
consoles, POS stream) can autodetect the topology.

Both profiles are idempotent — safe to re-run.
"""

from __future__ import annotations

import json
import os
from datetime import datetime
from typing import Optional

PROFILE_FILE = ".oasis_install_profile.json"


def _root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def profile_path(root: Optional[str] = None) -> str:
    return os.path.join(root or _root(), "oasis", "data", PROFILE_FILE)


def load_profile(root: Optional[str] = None) -> dict:
    """Return the recorded install profile, or {} if none."""
    p = profile_path(root)
    if not os.path.exists(p):
        return {}
    try:
        with open(p, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, ValueError):
        return {}


def save_profile(payload: dict, root: Optional[str] = None) -> str:
    p = profile_path(root)
    os.makedirs(os.path.dirname(p), exist_ok=True)
    payload = {**payload, "written_at": datetime.now().isoformat()}
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)
    return p


def init_install(profile: str = "", tenant: str = "",
                 root: Optional[str] = None) -> dict:
    """One-shot onboarding for the chosen profile.

    single: build-pos-db + seed-real-demand (falls back gracefully if the
            cash-file dir is absent) — the small-shop path.
    multi : build-multi-store-db + seed-multi-history — the network path.

    Returns a summary dict of what was done + the DB path. Records the choice
    in the install profile file so launchers autodetect the topology.
    """
    root = root or _root()
    # Default is the NETWORK shape — see onboarding.DEFAULT_PROFILE. A
    # single-store default leaves every cross-store surface with nothing to say.
    from .onboarding import DEFAULT_PROFILE
    profile = (profile or DEFAULT_PROFILE).lower()
    if profile not in ("single", "multi"):
        raise SystemExit(f"unknown profile '{profile}' — pick 'single' or 'multi'")

    data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
    summary: dict = {"profile": profile, "tenant": tenant or "(unnamed)"}

    if profile == "single":
        from .mock_pos_build import build_from_xlsx
        # NOT resolved_db_path(): this branch CREATES the store, so it must not
        # follow a resolved path into an existing onboarded DB and overwrite it
        # (deep-analysis finding S7).
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(data_dir, DEFAULT_SINGLE_DB))
        summary["db_path"] = db_path
        try:
            r = build_from_xlsx(data_dir, db_path)
            summary["catalog"] = f"{r.get('items', 0):,} SKUs / {r.get('suppliers', 0)} suppliers"
        except Exception as e:
            summary["catalog_error"] = str(e)
        # Optional real-demand seed if the client has already placed sales files
        cash_dir = os.getenv("OASIS_CASH_DIR",
                             os.path.join(os.path.expanduser("~"), "Desktop", "Projects"))
        if os.path.isdir(cash_dir) and any(f.endswith("_cash.xlsx")
                                           for f in os.listdir(cash_dir)):
            try:
                from .real_demand import seed_real_demand_from_files
                r = seed_real_demand_from_files(db_path, cash_dir)
                summary["demand"] = (f"{r.get('matched_skus', 0):,} SKUs / "
                                     f"{r.get('coverage_pct', 0)}% coverage")
            except Exception as e:
                summary["demand_error"] = str(e)
        else:
            summary["demand"] = "(no sales files found — run seed-real-demand later)"
    else:
        from .multi_store_build import build_multi_store_from_xlsx
        from .multi_store_pos import seed_multi_store_history
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(data_dir, DEFAULT_MULTI_DB))
        summary["db_path"] = db_path
        try:
            r = build_multi_store_from_xlsx(data_dir, db_path)
            summary["catalog"] = (f"{r.get('stores', 0)} stores, "
                                  f"{r.get('catalog_skus', 0):,} SKUs")
        except Exception as e:
            summary["catalog_error"] = str(e)
        try:
            r = seed_multi_store_history(db_path)
            summary["history"] = f"{sum(v.get('total_bills', 0) for v in r.values()):,} bills seeded"
        except Exception as e:
            summary["history_error"] = str(e)

    summary["profile_file"] = save_profile(summary, root=root)
    return summary


def is_multi_store(root: Optional[str] = None) -> bool:
    """Convenience for launchers: True if this install was initialised multi."""
    return load_profile(root).get("profile") == "multi"


def detected_db_path(root: Optional[str] = None) -> Optional[str]:
    """The DB path the install profile recorded (or None if uninitialised)."""
    return load_profile(root).get("db_path")
