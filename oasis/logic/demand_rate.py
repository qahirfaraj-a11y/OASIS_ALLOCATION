"""The one daily demand rate every ERP adapter hands the engine.

WHY THIS IS ITS OWN MODULE
    The order-up-to level starts from d, the average daily sales. Two adapters
    computed d two different ways from the same kind of sales history:

      PosErpAdapter  recency-weighted -- 60% last 30 days, 30% days 30-60,
                     10% days 60-90 -- on the as-of clock, with an
                     observed-window guard for a store younger than 90 days
      OdooAdapter    units / 90, on the wall clock

    Same shop, same till, two different demand rates, and every S, reorder
    point and transfer horizon downstream inherited the difference. The engine
    was one methodology; its input was not. This function is the single
    definition both adapters call, so a store that moves from its POS database
    to Odoo keeps the same d.

THE GUARD
    A bucket is divided by the days it actually observed, and the weights are
    renormalised over the buckets that exist. A store with 40 days of history
    has a full last-30 bucket, 10 days in the next, and none in the last: its
    rate is (0.6 * q30/30 + 0.3 * q30_60/10) / 0.9, not a figure that treats 50
    days of silence as zero sales.

    It assumes sales are recorded DAILY. History loaded as one bill per month
    lands a month of units on a single date and inflates whichever bucket holds
    it (measured on a seeded demo store: 20,843/day against a real 8,624).
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

#: (bucket length in days, weight), most recent first
BUCKETS: Tuple[Tuple[int, float], ...] = ((30, 0.60), (30, 0.30), (30, 0.10))
WINDOW_DAYS = sum(n for n, _ in BUCKETS)


def observed_days(first_sale: Optional[datetime], as_of_dt: datetime) -> float:
    """Days of history the store has, capped at the window."""
    if first_sale is None:
        return float(WINDOW_DAYS)
    return float(min(WINDOW_DAYS, max(1, (as_of_dt - first_sale).days + 1)))


def line_observed_days(line_first: Optional[datetime], store_first: Optional[datetime],
                       as_of_dt: datetime) -> float:
    """Days of history a LINE has: from the later of its first sale and the store's.

    The observed-window guard was per store. A line launched 20 days ago in an
    established store was divided over 90, and the 70 days before it existed
    counted as zero sales -- a line selling a unit a day from launch read as
    about 0.4. Replayed on the bread shelf's mid-period launches
    (devkit/bread_backtest.py, arm launch_window), measuring each line over its
    own days took Supa brown sliced from 64% to 83% fill. A line with sales in
    the oldest bucket existed all window, so callers look up only the rest.
    """
    firsts = [f for f in (line_first, store_first) if f is not None]
    return observed_days(max(firsts) if firsts else None, as_of_dt)


def bucket_days(days_obs: float) -> Tuple[float, float, float]:
    """How many observed days fall in each bucket."""
    out, left = [], float(days_obs)
    for n, _ in BUCKETS:
        take = min(float(n), max(0.0, left))
        out.append(take)
        left -= n
    return tuple(out)  # type: ignore[return-value]


def bucket_units(daily: Dict[date, float], as_of_dt: datetime) -> Tuple[float, float, float]:
    """(q30, q30_60, q60_90) from dated sales, on PosErpAdapter's date edges.

    A day belongs to the last-30 bucket when it is on or after as-of minus 30
    days (the SQL ``BILL_DT >= :c30``), to the next when on or after minus 60,
    to the last when on or after minus 90; older days are outside the window.
    Adapters that read dated sales (Zoho, Tally) bucket through here, so the
    same till history gives the same rate whichever backend delivers it.
    """
    edges = [(as_of_dt - timedelta(days=n)).date() for n in (30, 60, 90)]
    q = [0.0, 0.0, 0.0]
    for d, units in (daily or {}).items():
        d = d.date() if isinstance(d, datetime) else d
        for i, edge in enumerate(edges):
            if d >= edge:
                q[i] += float(units or 0)
                break
    return q[0], q[1], q[2]


def weighted_daily_rate(q30: float, q30_60: float, q60_90: float,
                        days_obs: float = WINDOW_DAYS) -> float:
    """Recency-weighted units a day from the three 30-day buckets."""
    d = bucket_days(days_obs)
    q = (float(q30 or 0), float(q30_60 or 0), float(q60_90 or 0))
    wsum = sum(w for (_, w), dd in zip(BUCKETS, d) if dd > 0) or 1.0
    total = sum(w * (qq / dd) for (_, w), qq, dd in zip(BUCKETS, q, d) if dd > 0)
    return total / wsum
