"""Operator-tunable transfer windows, surfaced in Settings -> System Configuration.

WHY THESE ARE OVERRIDES, NOT VALUES
-----------------------------------
The engine derives its horizons: relief comes from LATA's measured GRN history,
category thresholds from AMIT's perishability tiers. That is the point — a
number nobody derived is a number nobody can defend.

But a derived default is not always the right answer for a given chain. A
retailer running a tighter service level, a depot with a weekly van instead of
a daily one, a category the tiers do not describe well — these are real, and an
operator who cannot express them will simply stop trusting the output.

So every entry here is an **override with a derived default**. Left blank (or
absent) the engine derives; set, it uses the number given and SAYS SO in the
scan log, because an overridden horizon should never look like a measured one.

Each setting carries its bounds. A value outside them is refused and logged
rather than clamped silently: a store told to hold 900 days of cover has a
typo, not a policy.

Reads `OASIS_SYSTEM_CONFIG`, the same table the Settings panel renders, so
adding a row here makes it appear in the UI with no UI change at all.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

logger = logging.getLogger("TransferSettings")

CONFIG_GROUP = "transfers"


class Setting:
    """One tunable, with the bounds that make it meaningful."""

    __slots__ = ("key", "label", "kind", "low", "high", "derived", "note")

    def __init__(self, key, label, kind, low, high, derived, note):
        self.key = key
        self.label = label
        self.kind = kind          # float | int
        self.low = low
        self.high = high
        self.derived = derived    # what the engine uses when unset
        self.note = note

    def parse(self, raw) -> Optional[float]:
        """Value if usable, else None — never a silent substitution."""
        if raw is None or str(raw).strip() == "":
            return None
        try:
            v = float(str(raw).strip())
        except (TypeError, ValueError):
            logger.warning("transfer setting %s: %r is not a number — "
                           "deriving instead", self.key, raw)
            return None
        if not (self.low <= v <= self.high):
            logger.warning("transfer setting %s: %s is outside [%s, %s] — "
                           "REFUSED, deriving instead. A value this far out is "
                           "a typo, not a policy.", self.key, v, self.low, self.high)
            return None
        return int(v) if self.kind == "int" else v


#: The levers. Each maps to a symbol in OASIS_Master_Transfer_Formulae.md.
#:
#: Keys are UNPREFIXED, matching the `transfers` group that db_connector
#: already seeds. Two of these keys ALREADY EXISTED in that seed and were shown
#: in the Settings panel — `max_transfer_cost_kes` and `min_excess_ratio` — but
#: nothing ever read them: the names appear in fulfillment_decider only as
#: parameter defaults. An operator could change either and get no effect. They
#: are wired here rather than duplicated under a new name, which is the same
#: mistake as two constants for one idea, one level up.
SETTINGS = (
    Setting("release_fraction",
            "Donor release fraction (rho) — share of a donor's excess that may "
            "leave in one scan. Dead stock is exempt and always releases in full.",
            "float", 0.05, 1.0, 0.5,
            "Raise to move more per scan; lower to protect donors harder."),

    Setting("dead_stock_days",
            "Dead-stock window (days) — a line with zero demand is DEAD once it "
            "has been silent this long.",
            "int", 14, 365, 90,
            "Lower to clear frozen capital sooner; raise to be surer it is "
            "genuinely dead before moving it."),

    Setting("max_relief_days",
            "Maximum relief horizon (days) — ceiling on how far ahead a transfer "
            "may cover, before the category's own shelf life is applied.",
            "int", 3, 120, 45,
            "A guard against one bad supplier record, not a target."),

    Setting("max_transfer_cost_kes",
            "Transfer cost (KES) — logistics cost of one movement. Also the "
            "viability floor: dead stock worth less than this is a markdown, "
            "not a lorry.",
            "float", 0.0, 100000.0, 500.0,
            "Set to your real per-trip cost; it decides which moves pay."),

    Setting("min_excess_ratio",
            "Donor eligibility ratio — a donor must hold at least this multiple "
            "of its safety stock. Velocity still adjusts it (1.5x fast / 2.5x slow).",
            "float", 1.0, 5.0, 2.0,
            "Raise to protect donors harder; lower to widen the donor pool."),

    Setting("fallback_deficit_days",
            "Fallback deficit trigger (days) — used ONLY for suppliers neither "
            "LATA nor the calendar knows.",
            "int", 1, 90, 7,
            "With LATA present the network median is used instead; this is the "
            "last resort."),

    Setting("fallback_target_days",
            "Fallback fill target (days) — how much cover to restore when the "
            "relief horizon cannot be derived.",
            "int", 1, 120, 14,
            "Only reached when a supplier is unknown to every source."),

    Setting("default_safety_days",
            "Default safety floor (sigma, days) — cover a store keeps for "
            "itself before any of its stock may be donated. Used for stores "
            "whose own safety_days is missing.",
            "int", 1, 120, 14,
            "A store's own safety_days wins where it has one. Note LATA "
            "measures a median relief of 23 days, so 14 already sits below "
            "what the supplier book requires."),
)

BY_KEY = {s.key: s for s in SETTINGS}


def load(db_path: Optional[str]) -> Dict[str, Any]:
    """Overrides an operator has actually set. Absent keys are simply absent.

    Never raises: a settings table that cannot be read must degrade to derived
    behaviour, not stop a transfer scan.
    """
    if not db_path:
        return {}
    try:
        from .db_connector import load_system_config_full
        rows = load_system_config_full(db_path) or []
    except Exception as e:
        logger.warning("transfer settings unreadable (%s) — deriving all",
                       str(e)[:120])
        return {}

    out: Dict[str, Any] = {}
    for r in rows:
        key = r.get("CONFIG_KEY")
        s = BY_KEY.get(key)
        if not s:
            continue
        v = s.parse(r.get("CONFIG_VALUE"))
        if v is not None and v != s.derived:
            out[key] = v
    if out:
        logger.info("transfer settings OVERRIDDEN by operator: %s",
                    ", ".join(f"{k.split('.')[-1]}={v}" for k, v in sorted(out.items())))
    return out


def seed(db_path: str, updated_by: str = "system") -> int:
    """Write any missing rows so the Settings panel can render them.

    Only INSERTs what is absent — an operator's saved value is never
    overwritten by a later seed.
    """
    from .db_connector import get_sqlite_conn
    from datetime import datetime
    written = 0
    try:
        conn = get_sqlite_conn(db_path)
        existing = {r[0] for r in conn.execute(
            "SELECT CONFIG_KEY FROM OASIS_SYSTEM_CONFIG").fetchall()}
        for s in SETTINGS:
            if s.key in existing:
                continue
            conn.execute(
                "INSERT INTO OASIS_SYSTEM_CONFIG "
                "(CONFIG_KEY, CONFIG_VALUE, CONFIG_GROUP, DESCRIPTION, "
                " UPDATED_BY, UPDATED_DT) VALUES (?,?,?,?,?,?)",
                (s.key, str(s.derived), CONFIG_GROUP,
                 f"{s.label} [{s.low}-{s.high}] {s.note}",
                 updated_by, datetime.now().isoformat()))
            written += 1
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning("could not seed transfer settings (%s)", str(e)[:140])
    return written
