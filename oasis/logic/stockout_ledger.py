"""
Stockout ledger & labels (Risk-Scoring Redesign, P1).

The foundation for an *outcome-grounded* risk model: reconstruct a SKU's
on-hand position over time from dated flows and derive **realized** stockout /
lost-sales labels — the ground truth the redesigned risk score is trained
against (see OASIS_Risk_Scoring_Methodology_Redesign.md §2).

Everything here is **pure** (no I/O, no pandas) and unit tested. A separate
loader (P1b) parses the real GRN/transfer/sales/returns files into the
``DayFlow`` stream this engine consumes; keeping the logic pure means it is
CI-testable without the data and unit-agnostic — the same engine works on
monthly periods now and daily POS periods once the live feed is connected.

Model of a period (the order of operations is a deliberate, documented choice):
  1. receipts (GRN + transfers IN) add to on-hand,
  2. committed outflow (transfers OUT + vendor returns) ships first, capped at
     availability (you can't ship what you don't have),
  3. demand (attempted POS sales) is met up to remaining stock; any unmet demand
     is **lost sales**; on-hand never goes negative.

A period is a *stockout period* when demand existed but could not be fully met
(``lost > 0``). Supply-side failures (under-filled POs, "Short Supply" returns)
are **direct** labels needing no reconstruction.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Hashable, List, Sequence

EPS = 1e-9


@dataclass(frozen=True)
class DayFlow:
    """Net flows for one period (pure value object)."""
    receipts: float = 0.0   # GRN receipts + transfers IN  (stock added)
    demand: float = 0.0     # attempted POS sales          (may exceed availability)
    outflow: float = 0.0    # transfers OUT + vendor returns (committed removals)


@dataclass(frozen=True)
class DayState:
    """Reconstructed end-of-period state for one SKU."""
    period: Hashable
    receipts: float
    demand: float
    sold: float
    lost: float            # unmet demand = lost sales
    shipped_out: float     # committed outflow actually shipped
    soh_end: float         # on-hand at end of period (>= 0)
    stockout: bool         # demand existed but wasn't fully met


def simulate_ledger(opening_soh: float, periods: Sequence[Hashable],
                    flows: Dict[Hashable, DayFlow]) -> List[DayState]:
    """Forward inventory simulation with lost-sales accounting (pure).

    ``periods`` is the ordered sequence of period keys (dates or day indices);
    ``flows`` maps a period to its :class:`DayFlow` (missing = no activity).
    Returns one :class:`DayState` per period, in order.
    """
    soh = max(0.0, float(opening_soh or 0))
    states: List[DayState] = []
    for p in periods:
        f = flows.get(p, DayFlow())
        avail = soh + max(0.0, f.receipts)
        shipped = min(avail, max(0.0, f.outflow))
        avail -= shipped
        demand = max(0.0, f.demand)
        sold = min(avail, demand)
        lost = demand - sold
        soh = avail - sold
        states.append(DayState(
            period=p, receipts=max(0.0, f.receipts), demand=demand,
            sold=sold, lost=lost, shipped_out=shipped, soh_end=soh,
            stockout=lost > EPS))
    return states


def stockout_summary(states: Sequence[DayState]) -> dict:
    """Aggregate realized-outcome metrics over a reconstructed series (pure)."""
    n = len(states)
    so = sum(1 for s in states if s.stockout)
    lost = sum(s.lost for s in states)
    demand = sum(s.demand for s in states)
    sold = sum(s.sold for s in states)
    return {
        "periods": n,
        "stockout_periods": so,
        "stockout_rate": round(so / n, 4) if n else 0.0,
        "lost_units": round(lost, 4),
        "demand_units": round(demand, 4),
        "fill_rate": round(sold / demand, 4) if demand > EPS else 1.0,  # realized service level
    }


def forward_stockout_labels(states: Sequence[DayState], horizon: int = 1) -> Dict[Hashable, int]:
    """``y[period] = 1`` if a stockout occurs within the next ``horizon`` periods
    starting at that period (inclusive) — the supervised target (pure).

    horizon=1 labels the period itself; horizon=7 labels "stockout in the next
    week". Operates on the ordered sequence, so it is unit-agnostic.
    """
    h = max(1, int(horizon))
    so = [1 if s.stockout else 0 for s in states]
    return {s.period: (1 if any(so[i:i + h]) else 0) for i, s in enumerate(states)}


def days_of_cover(soh: float, ads: float) -> float:
    """Forward days of cover = on-hand / avg daily sales (pure).

    Infinite cover (no demand) is reported as a large sentinel so it sorts as
    "safe" without special-casing downstream.
    """
    ads = float(ads or 0)
    soh = max(0.0, float(soh or 0))
    if ads <= EPS:
        return float("inf") if soh > 0 else 0.0
    return soh / ads


# --- supply-side direct signals (no reconstruction needed) -----------------

def fill_rate(po_qty: float, grn_qty: float) -> float:
    """Realized fill rate for a received PO line = GRN qty / PO qty (pure)."""
    po = float(po_qty or 0)
    if po <= EPS:
        return 1.0
    return max(0.0, min(1.0, float(grn_qty or 0) / po))


def is_underfill(po_qty: float, grn_qty: float, tol: float = 0.0) -> bool:
    """True when a supplier delivered materially less than ordered (pure)."""
    return fill_rate(po_qty, grn_qty) < (1.0 - max(0.0, tol))


# --- demand spreading (interim until live daily POS) ------------------------

def spread_monthly_to_daily(monthly_qty: float, n_days: int) -> List[float]:
    """Uniformly spread a monthly demand total across days (pure).

    Interim bridge for the current monthly sales granularity; a live POS feed
    replaces this with the real daily quantities (the ledger engine is unchanged).
    """
    if n_days <= 0:
        return []
    per = float(monthly_qty or 0) / n_days
    return [per] * n_days
