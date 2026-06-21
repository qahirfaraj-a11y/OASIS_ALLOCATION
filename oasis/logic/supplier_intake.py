"""
Supplier & ordering-calendar intake (Universe Init v1.1).

Parses the client-provided ``suppliers.csv`` (see
OASIS_Universe_Initialization_Supplier_Calendar.md) into the supply *policy* the
ERP doesn't carry:
  * an ordering **calendar** -> ``supplier_schedule.json`` ({supplier: "DAILY" or
    [day-of-year]}), which SupplierCalendar prefers over the legacy Excel calendar
    (so the engine's is_ordering_day gating is client-driven);
  * **lead time / reliability / order frequency** overrides merged into
    ``supplier_patterns`` (filling the RXL-absent attributes);
  * the **fresh** supplier list.

Blank fields mean "derive from GRN" — those suppliers simply don't get a schedule
entry, so SupplierCalendar returns None and the engine's phase-stagger cadence
applies. Pure parsing functions are unit tested; ``apply_supplier_intake`` is the
file-writing integration entry, invoked by the intelligence bootstrap.
"""

from __future__ import annotations

import datetime
import re
from typing import Dict, List, Optional

_WEEKDAY = {"MON": 0, "TUE": 1, "WED": 2, "THU": 3, "FRI": 4, "SAT": 5, "SUN": 6}


def _norm_days(order_days) -> Optional[object]:
    """'DAILY' | sorted weekday ints | None (blank → derive) (pure)."""
    if order_days is None:
        return None
    s = order_days if isinstance(order_days, str) else ",".join(map(str, order_days))
    s = s.strip()
    if not s:
        return None
    if s.upper() == "DAILY":
        return "DAILY"
    days = []
    for tok in re.split(r"[,/;\s]+", s):
        k = tok.strip().upper()[:3]
        if k in _WEEKDAY:
            days.append(_WEEKDAY[k])
    return sorted(set(days)) or None


def weekdays_to_doy(weekday_ints, year: int) -> List[int]:
    """Expand weekday indices (Mon=0) to all matching day-of-year values (pure)."""
    wset = set(weekday_ints)
    out = []
    d = datetime.date(year, 1, 1)
    while d.year == year:
        if d.weekday() in wset:
            out.append(d.timetuple().tm_yday)
        d += datetime.timedelta(days=1)
    return out


def supplier_schedule(rows: List[dict], year: int) -> Dict[str, object]:
    """{Supplier_Name: 'DAILY' | [day-of-year]} from intake rows (pure).

    Blank Order_Days (and non-DAILY type) → omitted → GRN-derived later.
    """
    out: Dict[str, object] = {}
    for r in rows:
        name = str(r.get("Supplier_Name", "") or "").strip()
        if not name:
            continue
        typ = str(r.get("Type", "") or "").strip().upper()
        od = _norm_days(r.get("Order_Days"))
        if typ == "DAILY" or od == "DAILY":
            out[name] = "DAILY"
        elif isinstance(od, list) and od:
            out[name] = weekdays_to_doy(od, year)
    return out


def patterns_overrides(rows: List[dict]) -> Dict[str, dict]:
    """{SUPPLIER_NAME_UPPER: {estimated_delivery_days, reliability_score,
    order_frequency}} from the intake (pure) — fills RXL-absent supply attrs."""
    out: Dict[str, dict] = {}
    for r in rows:
        name = str(r.get("Supplier_Name", "") or "").strip()
        if not name:
            continue
        ov: dict = {}
        lt = r.get("Lead_Time_Days")
        if lt not in (None, ""):
            try:
                ov["estimated_delivery_days"] = int(float(lt))
            except (ValueError, TypeError):
                pass
        rel = r.get("Reliability")
        if rel not in (None, ""):
            try:
                ov["reliability_score"] = float(rel)
            except (ValueError, TypeError):
                pass
        typ = str(r.get("Type", "") or "").strip().upper()
        if typ == "DAILY":
            ov["order_frequency"] = "daily"
        elif typ == "FRESH":
            ov["order_frequency"] = "daily"
        if ov:
            out[name.upper()] = ov
    return out


def fresh_suppliers(rows: List[dict]) -> List[str]:
    """Suppliers flagged FRESH (perishable; no auto-transfer) (pure)."""
    return sorted({str(r.get("Supplier_Name", "") or "").strip()
                   for r in rows
                   if str(r.get("Type", "") or "").strip().upper() == "FRESH"
                   and str(r.get("Supplier_Name", "") or "").strip()})


def apply_supplier_intake(data_dir: str, year: Optional[int] = None) -> dict:
    """Read suppliers.csv → write supplier_schedule.json + merge supplier_patterns
    overrides + fresh list (integration). No-op if suppliers.csv is absent."""
    import csv
    import json
    import os

    path = os.path.join(data_dir, "suppliers.csv")
    if not os.path.exists(path):
        return {"applied": False, "reason": "no suppliers.csv"}
    year = year or datetime.date.today().year
    with open(path, encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    sched = supplier_schedule(rows, year)
    sched_path = os.path.join(data_dir, "supplier_schedule.json")
    with open(sched_path, "w", encoding="utf-8") as f:
        json.dump(sched, f)

    ov = patterns_overrides(rows)
    sp_path = os.path.join(data_dir, "supplier_patterns_2025_updated.json")
    sp = {}
    if os.path.exists(sp_path):
        try:
            sp = json.load(open(sp_path, encoding="utf-8"))
        except Exception:
            sp = {}
    for name, o in ov.items():
        sp.setdefault(name, {}).update(o)
    with open(sp_path, "w", encoding="utf-8") as f:
        json.dump(sp, f)

    fresh = fresh_suppliers(rows)
    fresh_path = os.path.join(data_dir, "fresh_suppliers.json")
    with open(fresh_path, "w", encoding="utf-8") as f:
        json.dump(fresh, f)

    return {"applied": True, "scheduled_suppliers": len(sched),
            "patterns_overridden": len(ov), "fresh_suppliers": len(fresh),
            "schedule_file": sched_path}
