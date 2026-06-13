"""
OASIS journey state (U3 / U0 support).

A tiny persistent store for *where a client is on the implementation journey*:
the current phase (0–6, see the Customer Journey doc), the derived operating
mode (SETUP / SHADOW / ACTIVE / AUTONOMOUS), and cumulative capital recovered.

This is the data behind the shell's persistent Mode+Phase+value badge and the
Home/Journey screen. Advancement is **human-confirmed** (journey decision #4) —
``advance_phase`` is only ever called from an explicit operator/exec action and
records who/when, never automatically.

Pure helpers (phase_to_mode, next_phase) are import-safe and unit tested;
load/save use an atomic JSON write so a concurrent reader never sees a partial
file.
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from datetime import datetime
from typing import Dict, Optional

logger = logging.getLogger("OASIS.JourneyState")

# Stage names by phase index (mirrors oasis.ui.components.JOURNEY_STAGES).
PHASES = ("Diagnose", "Prove", "Stabilise", "Fund", "Shield", "Automate", "Sustain")
MAX_PHASE = len(PHASES) - 1

# Phase → operating mode (Customer Journey §4).
_MODE_BY_PHASE = {
    0: "SETUP",       # Diagnose (pre-contract / operator)
    1: "SHADOW",      # Prove (observe, humans order)
    2: "ACTIVE",      # Stabilise
    3: "ACTIVE",      # Fund
    4: "ACTIVE",      # Shield
    5: "AUTONOMOUS",  # Automate
    6: "AUTONOMOUS",  # Sustain
}

_DEFAULT_FILENAME = "journey_state.json"


def phase_to_mode(phase: int) -> str:
    """Operating mode for a phase index (clamped to range)."""
    phase = max(0, min(int(phase), MAX_PHASE))
    return _MODE_BY_PHASE[phase]


def phase_name(phase: int) -> str:
    phase = max(0, min(int(phase), MAX_PHASE))
    return PHASES[phase]


def next_phase(phase: int) -> Optional[int]:
    """The next phase index, or None if already at the final phase."""
    phase = int(phase)
    return phase + 1 if phase < MAX_PHASE else None


def default_state() -> Dict:
    return {
        "phase": 0,
        "phase_name": PHASES[0],
        "mode": phase_to_mode(0),
        "value_recovered": 0.0,
        "value_target": 0.0,
        "updated_by": None,
        "updated_dt": None,
    }


def _state_path(path: Optional[str] = None) -> str:
    if path:
        return path
    data_dir = os.getenv(
        "OASIS_DATA_DIR",
        os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"),
    )
    return os.path.join(data_dir, _DEFAULT_FILENAME)


def _normalize(raw: Dict) -> Dict:
    """Fill any missing keys and re-derive mode/name from phase."""
    state = default_state()
    if isinstance(raw, dict):
        state.update({k: raw[k] for k in state if k in raw})
    state["phase"] = max(0, min(int(state.get("phase", 0) or 0), MAX_PHASE))
    state["phase_name"] = phase_name(state["phase"])
    state["mode"] = phase_to_mode(state["phase"])
    return state


def load_state(path: Optional[str] = None) -> Dict:
    """Load journey state, returning a normalized default if absent/corrupt."""
    p = _state_path(path)
    if not os.path.exists(p):
        return default_state()
    try:
        with open(p, "r", encoding="utf-8") as f:
            return _normalize(json.load(f))
    except Exception as e:
        logger.warning("Could not read journey state %s: %s", p, e)
        return default_state()


def _write_atomic(path: str, state: Dict) -> None:
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(state, f, indent=2)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def save_state(state: Dict, path: Optional[str] = None) -> Dict:
    state = _normalize(state)
    _write_atomic(_state_path(path), state)
    return state


def set_value_recovered(recovered: float, target: Optional[float] = None,
                        path: Optional[str] = None) -> Dict:
    state = load_state(path)
    state["value_recovered"] = round(float(recovered or 0), 2)
    if target is not None:
        state["value_target"] = round(float(target or 0), 2)
    return save_state(state, path)


def advance_phase(by_user: str, path: Optional[str] = None) -> Dict:
    """Advance to the next phase (human-confirmed). Records who/when.

    No-op (returns current state) if already at the final phase.
    """
    state = load_state(path)
    nxt = next_phase(state["phase"])
    if nxt is None:
        return state
    state["phase"] = nxt
    state["updated_by"] = by_user
    state["updated_dt"] = datetime.now().isoformat()
    saved = save_state(state, path)
    logger.info("Journey advanced to phase %s (%s) by %s",
                saved["phase"], saved["phase_name"], by_user)
    return saved
