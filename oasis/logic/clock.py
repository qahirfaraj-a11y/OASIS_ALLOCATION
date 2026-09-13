"""The date the engine MEASURES against, which is not always today.

WHY THIS EXISTS
    The ordering path read `datetime.now()` in five places that decide
    something: how stale a line's last delivery is, where the 30/60/90-day
    demand windows fall, and which weekday the supplier calendar is checked
    against. That is correct against a live POS and wrong against an extract.

    The anchor store's receipt history runs 2025-01-01 to 2025-12-09 --
    106,526 receipts, per-SKU, with real spread. Against a 2026 wall clock
    every line reads as 278 to 619 days since delivery, so the stale-fresh and
    dead-stock gates fire on everything or, once the mock builder writes one
    flat receipt date, on nothing. Neither is a fact about the shelf.

    Measured against the data's own horizon the same field spreads properly:
    median 39 days, p75 102, max 341, with 20.3% over 120 days and 15.0% over
    200. The data was always credible. The clock was wrong.

WHAT IT DOES NOT COVER
    Write timestamps. A purchase order created now IS created now, whatever
    period is being analysed, so `created_at`-style stamps keep the wall clock.
    This is only for the questions "how long ago" and "since when".

USAGE
    OASIS_AS_OF=2025-12-09   evaluate as if standing on that date
    unset                    today, which is the live-POS default
"""
from __future__ import annotations

import logging
import os
from datetime import datetime

logger = logging.getLogger("OASIS.Clock")

#: Environment variable holding an ISO date (YYYY-MM-DD) or datetime.
AS_OF_ENV = "OASIS_AS_OF"

_WARNED = False


def as_of() -> datetime:
    """The moment to measure from. Wall clock unless OASIS_AS_OF is set.

    Read at call time, never cached: a harness sweeping several dates in one
    process must get each one, and a stale module-level constant is exactly
    the bug this module exists to correct.
    """
    global _WARNED
    raw = (os.getenv(AS_OF_ENV) or "").strip()
    if not raw:
        return datetime.now()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            parsed = datetime.strptime(raw, fmt)
        except ValueError:
            continue
        if not _WARNED:
            logger.info("Measuring as of %s, not today (%s=%s)",
                        parsed.date(), AS_OF_ENV, raw)
            _WARNED = True
        return parsed
    # FAIL LOUD, NOT OPEN. Silently falling back to today would make a
    # mistyped date look like a working run whose numbers mean something else.
    raise ValueError(
        f"{AS_OF_ENV}={raw!r} is not a date. Use YYYY-MM-DD, "
        f"or unset it to measure against today.")


def is_pinned() -> bool:
    """True when an as-of date is in force, for surfaces that should say so."""
    return bool((os.getenv(AS_OF_ENV) or "").strip())
