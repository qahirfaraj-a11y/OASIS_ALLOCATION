"""
Take-rate accounting for brokered supplier offers.

OASIS brokers the agreement between a store and its supplier, so it books a
commission when an offer is ACCEPTED. This module only computes and RECORDS
what is owed — no money moves through the hub, and no payment credentials are
held anywhere in this system.

The rate is configuration, never a hardcoded business decision:
    OASIS_HUB_COMMISSION_RATE   e.g. 0.02 for 2%  (default 0 = disabled)

Deliberately conservative about the amount: many offers are expressed as a
PERCENTAGE (a 5% rebate on future volume), where the cash value is not knowable
at acceptance time. In those cases we record the rate and the basis and leave
the amount NULL for settlement against actuals, rather than inventing a figure.
"""

import os
from typing import Any, Dict, Optional, Tuple

#: Terms that carry an unambiguous monetary sum we can take a rate on now.
_MONETARY_TERMS = ("fee_amount", "amount", "value_kes", "lump_sum")

#: Terms that are rate-based — real value depends on volume actually shipped.
_RATE_TERMS = ("rebate_pct", "support_pct", "discount_pct")


def commission_rate() -> float:
    """Configured take-rate (0 disables commission entirely)."""
    try:
        return float(os.getenv("OASIS_HUB_COMMISSION_RATE", "0") or 0)
    except ValueError:
        return 0.0


def compute_commission(terms: Dict[str, Any],
                       rate: Optional[float] = None
                       ) -> Tuple[float, Optional[float], Optional[str]]:
    """(rate, amount, basis) for an accepted offer.

    ``amount`` is None when the offer is percentage-based — the value depends on
    volume that has not shipped yet, so it settles later against actuals.
    """
    rate = commission_rate() if rate is None else rate
    if rate <= 0:
        return 0.0, None, None

    for key in _MONETARY_TERMS:
        val = terms.get(key)
        if isinstance(val, (int, float)) and val > 0:
            return rate, round(val * rate, 2), key

    for key in _RATE_TERMS:
        if terms.get(key) is not None:
            return rate, None, f"{key} (settles on actual volume)"

    return rate, None, "no monetary term — settles on invoice"
