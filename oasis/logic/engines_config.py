"""
Engine feature-flag / parameter config — one resolver for the whole engine layer.

Chapter-11 engines (MOP-UP, AMIT, LATA, DHARAM, MANDE, dead-stock) read their
flags and thresholds from ``oasis_engines_config.json``. That file is *tuned per
install*, so it is machine state and does not ship in a client release.

Before this module existed, each engine opened the file itself and fell back to
an empty dict when it was missing — which on every client install meant
``is_engine_enabled()`` returned False for every engine and the whole layer sat
dormant and silent (deep-analysis finding S1).

The fix is a two-tier lookup:

  1. ``oasis_engines_config.json``          — this install's tuned config, if present
  2. ``oasis_engines_config.default.json``  — the shipped methodology defaults

Tier 2 IS shipped (see release_packager._OASIS_DATA_WHITELIST), so a fresh
install runs the engines with the reference parameters instead of running
nothing. ``resolve_source()`` reports which tier answered, so consoles and
preflight can say so out loud rather than implying a tuned config exists.
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("OASIS.EnginesConfig")

LIVE_FILE = "oasis_engines_config.json"
DEFAULT_FILE = "oasis_engines_config.default.json"

#: engines gated by an ``enabled`` flag (used by preflight's active list).
#: ``dead_stock`` is deliberately absent — it is a parameter block read by
#: amit_governance, not a flag-gated engine, so reporting it as "inactive"
#: would be misleading.
KNOWN_ENGINES = ("mop_up", "amit", "lata", "dharam", "mande")
#: config sections that carry parameters but no on/off flag
PARAM_ONLY_SECTIONS = ("dead_stock",)


def _package_data_dir() -> str:
    return os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        "data")


def candidate_paths(data_dir: Optional[str] = None) -> List[Tuple[str, str]]:
    """[(tier, path)] in priority order. Pure — does not touch the filesystem.

    ``tier`` is "live" for the tuned per-install config and "default" for the
    shipped methodology defaults. The package data dir is searched after the
    caller's data_dir so an install with a relocated OASIS_DATA_DIR still finds
    the shipped defaults.
    """
    out: List[Tuple[str, str]] = []
    dirs = []
    if data_dir:
        dirs.append(data_dir)
    pkg = _package_data_dir()
    if pkg not in dirs:
        dirs.append(pkg)
    for name, tier in ((LIVE_FILE, "live"), (DEFAULT_FILE, "default")):
        for d in dirs:
            out.append((tier, os.path.join(d, name)))
    return out


def resolve_source(data_dir: Optional[str] = None,
                   exists=os.path.exists) -> Tuple[Optional[str], Optional[str]]:
    """(tier, path) of the config that will be used, or (None, None). Pure-ish.

    ``exists`` is injectable so the priority logic is unit-testable without a
    filesystem.
    """
    for tier, path in candidate_paths(data_dir):
        if exists(path):
            return tier, path
    return None, None


def load_engines_config(data_dir: Optional[str] = None) -> Dict[str, Any]:
    """The engine config dict, from the tuned file or the shipped defaults.

    Never raises: an unreadable file logs a warning and falls through to the
    next candidate, so a corrupt tuned config still leaves the engines running
    on defaults rather than switching the layer off.
    """
    for tier, path in candidate_paths(data_dir):
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                cfg = json.load(f) or {}
            if tier == "default":
                logger.info("Engine config: using shipped defaults (%s) — "
                            "no tuned oasis_engines_config.json on this install",
                            os.path.basename(path))
            return cfg
        except Exception as e:
            logger.warning("Engine config unreadable (%s): %s — trying next", path, e)
    logger.warning("No engine config found (looked for %s / %s) — the engine "
                   "layer will run with library defaults", LIVE_FILE, DEFAULT_FILE)
    return {}


def engine_params(engine: str, data_dir: Optional[str] = None) -> Dict[str, Any]:
    """Parameters for one engine (``{}`` when absent)."""
    cfg = load_engines_config(data_dir)
    return (cfg.get("engines", {}) or {}).get(engine, {}) or {}


def is_engine_enabled(engine: str, data_dir: Optional[str] = None) -> bool:
    """Feature-flag check for one engine, resolved through both tiers."""
    return bool(engine_params(engine, data_dir).get("enabled", False))


# ── preflight support ────────────────────────────────────────────────────
def evaluate_engines_config(tier: Optional[str],
                            enabled: Optional[List[str]] = None) -> dict:
    """Preflight check row for the engine layer. Pure.

    A missing config is a FAIL, not a WARN: it is the exact condition that
    silently disabled every engine on client installs. Running on shipped
    defaults is a PASS that says so.
    """
    if tier is None:
        return {"check": "Engine config", "status": "FAIL",
                "detail": f"neither {LIVE_FILE} nor {DEFAULT_FILE} found — "
                          "every Chapter-11 engine would be disabled"}
    names = ", ".join(enabled) if enabled else "NONE"
    if not enabled:
        return {"check": "Engine config", "status": "WARN",
                "detail": f"{tier} config found but no engine is enabled"}
    if tier == "default":
        return {"check": "Engine config", "status": "PASS",
                "detail": f"shipped defaults (untuned) — active: {names}"}
    return {"check": "Engine config", "status": "PASS",
            "detail": f"tuned config — active: {names}"}


def preflight_check(data_dir: Optional[str] = None) -> dict:
    """Run evaluate_engines_config against this install."""
    tier, _path = resolve_source(data_dir)
    cfg = load_engines_config(data_dir) if tier else {}
    enabled = [k for k, v in (cfg.get("engines", {}) or {}).items()
               if isinstance(v, dict) and v.get("enabled")]
    return evaluate_engines_config(tier, enabled)
