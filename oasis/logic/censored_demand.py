"""Demand from sales the shelf censored.

The engine measures demand from SALES, and a day the shelf ran out records less
than was wanted. On a one-unit shelf with true demand 1.1 a day, raw sales read
0.669 a day; keeping only the days the shelf never ran out reads 0.0, because
those are exactly the days nobody came. This module reads each day for what it
is:

    opened with stock, did not sell out   demand observed exactly
    opened with stock, sold out           demand was AT LEAST the sales
    opened empty                          says nothing about demand

and estimates the rate by censored-Poisson maximum likelihood per 30-day bucket,
weighted 60/30/10 like demand_rate. Replayed over Apr-Sep 2026 on the bread
shelf (devkit/bread_backtest.py, arm censor_mle), it took lines under one a day
from 87.8% to 89.4% fill, above the bakeries' 88.4%.

WHICH DAYS OPENED WITH STOCK, WHICH SOLD OUT, is not in the sales table. It is
rebuilt here from daily receipts and daily sales by walking the window forward
from an empty shelf: receipts land in the morning, sales leave oldest-first,
units past their sellable life leave at the start of the day. The shelf life is
what makes that walk converge without a stock count to anchor it -- a short-life
line empties every few days whatever it held before the window.

Receipts come from a ReceiptsSource: the store's GRN export for now
(GrnExportReceipts), the POS itself in production.
"""
from __future__ import annotations

import math
import os
from datetime import date, datetime, timedelta
from typing import Dict, Iterable, List, Optional, Protocol, Tuple

from . import demand_rate as dr


# -- the estimator -------------------------------------------------------------
def censored_poisson_rate(sales: Iterable[float], opened: Iterable[bool],
                          soldout: Iterable[bool]) -> Optional[float]:
    """Poisson rate from daily sales, right-censored on sell-out days.

    Maximum likelihood by golden-section search (one parameter, concave).
    Exact days enter as sum(k) log(lam) - n lam, which holds for real-valued
    k. None when no day opened with stock.
    """
    exact, cens = [], []
    for s, o, c in zip(sales, opened, soldout):
        if not o:
            continue
        if c:
            cens.append(int(round(s)))
        else:
            exact.append(float(s))
    if not exact and not cens:
        return None
    if not cens:
        return sum(exact) / len(exact)

    def sf(k: int, lam: float) -> float:                  # P(D >= k)
        if k <= 0:
            return 1.0
        p, cdf = math.exp(-lam), 0.0
        for i in range(k):
            cdf += p
            p *= lam / (i + 1)
        return max(1e-300, 1.0 - cdf)

    s_exact, n_exact = sum(exact), len(exact)

    def ll(lam: float) -> float:
        lam = max(lam, 1e-9)
        return s_exact * math.log(lam) - n_exact * lam + sum(math.log(sf(k, lam)) for k in cens)

    lo, hi = 1e-6, max(1.0, 3.0 * (max(exact + cens) + 1))
    g = (math.sqrt(5) - 1) / 2
    for _ in range(60):
        a, b = hi - g * (hi - lo), lo + g * (hi - lo)
        if ll(a) < ll(b):
            lo = a
        else:
            hi = b
    return (lo + hi) / 2


def censored_weighted_rate(sales: List[float], opened: List[bool],
                           soldout: List[bool]) -> Optional[float]:
    """60/30/10 over the last three 30-day buckets (oldest day first).

    A bucket in which the line never opened with stock carries no information
    and drops out of the weighting -- which also covers the days before a line
    was launched. None when no bucket has an opened day.
    """
    n = len(sales)
    got = []
    for i, (days, w) in enumerate(dr.BUCKETS):
        hi = n - 30 * i
        lo = max(0, hi - days)
        if hi <= 0:
            break
        lam = censored_poisson_rate(sales[lo:hi], opened[lo:hi], soldout[lo:hi])
        if lam is not None:
            got.append((lam, w))
    if not got:
        return None
    return sum(l * w for l, w in got) / sum(w for _, w in got)


# -- the shelf, rebuilt ----------------------------------------------------------
def shelf_signals(sales_by_day: Dict[date, float], receipts_by_day: Dict[date, float],
                  start: date, end: date, life: Optional[float] = None
                  ) -> Tuple[List[float], List[bool], List[bool]]:
    """Units sold, opened-with-stock and sold-out, per day from `start` to `end`.

    Walks forward from an empty shelf. A sale with no stock behind it (stock
    from before the window, a receipt the source missed) is evidence the line
    was open that day; stock is floored at zero and the day reads as a
    sell-out. For a short-life line that can only happen in the first few days
    of the window, before the walk has seen a full shelf cycle; a receipts
    source that misses deliveries throughout will overstate sell-outs, which is
    why callers take this only where it raises the measured rate.
    """
    batches: List[List[float]] = []                        # [arrival day index, qty]
    sales, opened, soldout = [], [], []
    ndays = (end - start).days + 1
    for i in range(ndays):
        day = start + timedelta(days=i)
        if life and life > 0:
            batches = [b for b in batches if i - b[0] < life]
        q_in = float(receipts_by_day.get(day, 0.0) or 0.0)
        if q_in > 0:
            batches.append([i, q_in])
        stock = sum(b[1] for b in batches)
        s = float(sales_by_day.get(day, 0.0) or 0.0)
        opened.append(stock > 1e-9 or s > 0)
        left = s
        for b in batches:                                   # oldest first
            take = min(b[1], left)
            b[1] -= take
            left -= take
            if left <= 1e-9:
                break
        batches = [b for b in batches if b[1] > 1e-9]
        sales.append(s)
        soldout.append(opened[-1] and sum(b[1] for b in batches) <= 1e-9)
    return sales, opened, soldout


#: Fewest covered days the correction will work from.
MIN_COVERED_DAYS = 30


def correct_line(daily_sales: Dict[date, float], daily_receipts: Dict[date, float],
                 measured: float, start: date, end: date,
                 coverage: Optional[Tuple[date, date]],
                 life: Optional[float]) -> Optional[Tuple[float, int]]:
    """(corrected rate, sell-out days) for one line, or None to keep `measured`.

    Walks only the days the receipts cover, and needs MIN_COVERED_DAYS of
    them: a GRN export that stops before today would otherwise turn every
    later sale into a sell-out. Censoring can only have HIDDEN demand, so the
    correction is taken only when the line sold out at least once and the
    censored rate is above the measured one -- noise on a line that never ran
    out cannot lower it.
    """
    if coverage is None:
        return None
    lo, hi = max(start, coverage[0]), min(end, coverage[1])
    if (hi - lo).days + 1 < MIN_COVERED_DAYS:
        return None
    s, o, c = shelf_signals(daily_sales, daily_receipts, lo, hi, life)
    if not any(c):
        return None
    lam = censored_weighted_rate(s, o, c)
    if lam is None or lam <= measured:
        return None
    return lam, int(sum(c))


# -- where receipts come from ------------------------------------------------------
class ReceiptsSource(Protocol):
    """Daily goods received per item. The GRN export now; the POS in production."""

    def daily_receipts(self, item_keys: Iterable[str], since: date,
                       until: date) -> Dict[str, Dict[date, float]]:
        ...

    def items(self) -> List[str]:
        ...

    def coverage(self) -> Optional[Tuple[date, date]]:
        """First and last day the source covers. Outside it, a sale with no
        receipt would read as a sell-out, so callers walk only these days."""
        ...


class GrnExportReceipts:
    """Receipts from the store's GRN report export, until the POS feed carries them.

    Columns used: GRN Date, Bar Code, GRN Qty, FOC Qty (free units reach the
    shelf too), PO No. With the matching PO report, the Sunday-posting
    correction is applied: this store dates no GRN on a Sunday, so a Monday
    GRN whose PO was raised on Saturday or earlier is a Sunday delivery.
    """

    def __init__(self, grn_path: str, po_path: Optional[str] = None):
        import pandas as pd
        g = pd.read_excel(grn_path)
        g = g[g["GRN No"].astype(str) != "Total"].copy()
        g["date"] = pd.to_datetime(g["GRN Date"], format="%d-%b-%Y").dt.date
        g["qty"] = g["GRN Qty"].fillna(0).astype(float) + g.get("FOC Qty", 0).fillna(0).astype(float)
        po_date = {}
        if po_path and os.path.exists(po_path):
            p = pd.read_excel(po_path)
            p = p[p["PO NO"].astype(str) != "Total"]
            po_date = dict(zip(p["PO NO"].astype(str), pd.to_datetime(p["PO DATE"], format="%d-%b-%Y").dt.date))
        self._by_item: Dict[str, Dict[date, float]] = {}
        for bar, d, q, po in zip(g["Bar Code"].astype(str), g["date"], g["qty"], g["PO No"].astype(str)):
            if d.weekday() == 0 and po in po_date and (d - po_date[po]).days >= 2:
                d = d - timedelta(days=1)
            day = self._by_item.setdefault(bar.strip(), {})
            day[d] = day.get(d, 0.0) + float(q)

    def items(self) -> List[str]:
        return list(self._by_item)

    def coverage(self) -> Optional[Tuple[date, date]]:
        days = [d for v in self._by_item.values() for d in v]
        return (min(days), max(days)) if days else None

    def daily_receipts(self, item_keys, since, until):
        keys = set(map(str, item_keys))
        return {k: {d: q for d, q in v.items() if since <= d <= until}
                for k, v in self._by_item.items() if k in keys}
