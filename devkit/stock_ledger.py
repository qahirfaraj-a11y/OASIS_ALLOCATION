"""Roll a position forward and backward through receipts and demand.

THE METHOD
    A stock snapshot is a position at one instant. Receipts and sales are the
    flows either side of it, so the position at any other date follows:

        stock(t)  =  stock(T)  -  receipts(t, T]  +  demand(t, T]     (backward)
        stock(t)  =  stock(T)  +  receipts(T, t]  -  demand(T, t]     (forward)

    Which turns one snapshot into a series, and a series into something a probe
    can measure cover against.

WHAT IT REFUSES TO DO
    Two things in this book's real export make naive reconstruction produce
    confident nonsense, and both are handled here rather than discovered later:

    * SUBTOTAL ROWS. The fulfilment export carries 639 rows whose vendor and
      item are both "Total" — it was rendered as a report before it was saved as
      data, the same family of defect as the supplier calendar. Summed naively
      they double-count every quantity they subtotal.

    * CALENDAR HOLES. April, May and June 2025 are absent from the receipt
      history entirely. A SKU received in March and again in July shows a
      ~120-day gap that never happened. Any measure built on inter-receipt gaps
      MUST refuse those spans rather than average them in — a hole in the data
      is not a long lead time, and the difference is the whole finding.

PROVENANCE
    Receipts and demand here are OBSERVED — the client's own fulfilment export
    and ADS derived from six months of POS. That is what lets a probe built on
    this ledger validate rather than merely exercise.
"""
from __future__ import annotations

import datetime as _dt
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
FULFILMENT = ROOT / "All_Suppliers_Fulfillment_Detail.xlsx"
ADS_FILE = ROOT / "oasis" / "data" / "corrected_ads_from_pos.json"

#: A row whose vendor or item is a subtotal marker is not a receipt.
SUBTOTAL = re.compile(r"^\s*(total|grand\s*total|sub\s*total)\s*$", re.I)


def _as_float(v) -> Optional[float]:
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _as_date(v) -> Optional[_dt.date]:
    if isinstance(v, _dt.datetime):
        return v.date()
    if isinstance(v, _dt.date):
        return v
    try:
        return _dt.date.fromisoformat(str(v)[:10])
    except (TypeError, ValueError):
        return None


@dataclass
class Receipt:
    item: str
    vendor: str
    date: _dt.date
    qty: float
    grn_no: str = ""
    lead_days: Optional[float] = None   # PO date -> GRN date, observed


@dataclass
class Ledger:
    receipts: Dict[str, List[Receipt]] = field(default_factory=dict)
    ads: Dict[str, float] = field(default_factory=dict)
    months_active: Dict[str, float] = field(default_factory=dict)
    covered_months: List[str] = field(default_factory=list)
    holes: List[str] = field(default_factory=list)
    rejected_subtotals: int = 0
    rejected_undated: int = 0
    subtotal_qty: float = 0.0

    # ---------------------------------------------------------------- spans
    def hole_dates(self) -> List[Tuple[_dt.date, _dt.date]]:
        """First and last day of each missing calendar month."""
        out = []
        for m in self.holes:
            y, mo = (int(x) for x in m.split("-"))
            first = _dt.date(y, mo, 1)
            last = (_dt.date(y + (mo == 12), (mo % 12) + 1, 1) - _dt.timedelta(days=1))
            out.append((first, last))
        return out

    def spans_hole(self, a: _dt.date, b: _dt.date) -> bool:
        return any(a <= h1 and h0 <= b for h0, h1 in self.hole_dates())

    # ---------------------------------------------------------------- gaps
    def gaps(self, item: str) -> List[Dict[str, object]]:
        """Consecutive receipts of one item, with the gap each had to span.

        Gaps straddling a data hole are returned flagged, never silently
        dropped and never silently counted. A caller that wants a mean must
        say which it wants.
        """
        rs = sorted(self.receipts.get(item, []), key=lambda r: r.date)
        out = []
        for a, b in zip(rs, rs[1:]):
            days = (b.date - a.date).days
            if days <= 0:
                continue
            out.append({"item": item, "from": a.date, "to": b.date,
                        "days": days, "qty": a.qty,
                        "spans_hole": self.spans_hole(a.date, b.date)})
        return out

    # ------------------------------------------------------------ position
    def position(self, item: str, snapshot_qty: float, snapshot_date: _dt.date,
                 at: _dt.date) -> Optional[float]:
        """Stock at `at`, rolled from a snapshot. None if the roll crosses a hole.

        Refusing is the point. A number produced across a three-month gap in the
        receipt history looks exactly like a number produced across real trading,
        and only one of them means anything.
        """
        lo, hi = min(snapshot_date, at), max(snapshot_date, at)
        if self.spans_hole(lo, hi):
            return None
        rate = self.ads.get(item)
        if rate is None:
            return None
        recv = sum(r.qty for r in self.receipts.get(item, [])
                   if lo < r.date <= hi)
        demand = rate * (hi - lo).days
        if at >= snapshot_date:
            return snapshot_qty + recv - demand
        return snapshot_qty - recv + demand


def load(fulfilment: Path = FULFILMENT, ads_file: Path = ADS_FILE,
         local_copy: Optional[Path] = None) -> Ledger:
    import openpyxl

    led = Ledger()
    raw = json.loads(Path(ads_file).read_text(encoding="utf-8"))
    for k, v in raw.items():
        if isinstance(v, dict):
            a = v.get("new_ads", v.get("old_ads"))
            if a:
                led.ads[str(k).strip().upper()] = float(a)
                led.months_active[str(k).strip().upper()] = float(
                    v.get("months_active") or 0)

    src = str(local_copy or fulfilment)
    wb = openpyxl.load_workbook(src, read_only=True, data_only=True)
    rows = wb["Sheet1"].iter_rows(values_only=True)
    next(rows, None)
    months = set()
    for r in rows:
        vendor, item = str(r[0] or ""), str(r[6] or "")
        if SUBTOTAL.match(vendor) or SUBTOTAL.match(item):
            led.rejected_subtotals += 1
            try:
                led.subtotal_qty += float(r[7] or 0)
            except (TypeError, ValueError):
                pass
            continue
        d = _as_date(r[4])
        if d is None:
            led.rejected_undated += 1
            continue
        try:
            qty = float(r[7] or 0)
        except (TypeError, ValueError):
            continue
        if qty <= 0:
            continue
        key = item.strip().upper()
        led.receipts.setdefault(key, []).append(
            Receipt(key, vendor.strip(), d, qty, str(r[3] or ""),
                    _as_float(r[5])))
        months.add(d.strftime("%Y-%m"))
    wb.close()

    led.covered_months = sorted(months)
    if led.covered_months:
        y0, m0 = (int(x) for x in led.covered_months[0].split("-"))
        y1, m1 = (int(x) for x in led.covered_months[-1].split("-"))
        cur = _dt.date(y0, m0, 1)
        end = _dt.date(y1, m1, 1)
        while cur <= end:
            tag = cur.strftime("%Y-%m")
            if tag not in months:
                led.holes.append(tag)
            cur = _dt.date(cur.year + (cur.month == 12), (cur.month % 12) + 1, 1)
    return led
