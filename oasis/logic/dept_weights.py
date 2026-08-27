"""Department capital weights, rebuilt from priced product data.

WHAT WAS WRONG
--------------
`department_scaling_ratios.csv` drives how a greenfield budget is split across
departments. 171 of its 233 departments carried ``Avg_Price = 0`` and
``Total_Value = 0`` — a gap in the extract it was built from, not a modelling
choice. A department with no value gets no weight, falls into the orphan pool,
and splits a 5% reserve 171 ways.

Measured on a KES 27.5M store: SWEETS held a wallet of **103 shillings**, and
the 73% of departments with missing prices competed for 5% of the capital while
the 62 priced ones held 95%. The starved categories were systematically the
high-unit-cost ones — electricals, heaters, microwaves, diapers, olive oil —
which are exactly the departments that need the most capital to fill a shelf.

The old recalculate_dept_weights.py could not fix this: it recomputed weights
from the same empty Total_Value column and gave zero-value departments a 0.0001
floor, which is what created the orphan pool in the first place.

WHAT THIS DOES
--------------
Rebuilds the file from the allocation scorecard, which carries a unit price for
every one of its rows. For departments that already had prices the relative
weights are preserved — scorecard revenue tracks the old Total_Value at a
consistent ratio — so this ADDS the missing departments rather than
redistributing the existing ones.

WHAT IT DELIBERATELY DOES NOT DO
--------------------------------
Change the DEFINITION of the weight. It is still turnover share: value moved
per department. Whether opening capital should be weighted by turnover at all
is a separate and better question — filling a shelf once costs price x facings
x cover, and a fast-moving perishable needs replenishment rather than capital —
but that is a modelling decision, and this is a data fix.
"""

from __future__ import annotations

import csv
import io
import logging
import os
import shutil
from collections import defaultdict
from typing import Any, Dict, Iterable, List

logger = logging.getLogger("OASIS.DeptWeights")

RATIOS_FILE = "department_scaling_ratios.csv"
COLUMNS = ["Department", "SKU_Count", "Avg_Price", "Avg_Daily_Sales",
           "Total_Value", "Capital_Weight", "SKU_per_Million"]

#: A department with no priced product still needs a wallet — it may be a new
#: range, or one the extract missed. Small, explicit, and applied AFTER the
#: real weights so it can never dilute a measured one.
MIN_WEIGHT = 0.0001


def _f(row: Dict[str, Any], key: str) -> float:
    try:
        return float(row.get(key) or 0)
    except (TypeError, ValueError):
        return 0.0


def aggregate_departments(rows: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, float]]:
    """Per-department totals from priced product rows. Pure."""
    agg: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"sku_count": 0, "price_sum": 0.0, "ads_sum": 0.0, "value": 0.0})
    for r in rows:
        dept = " ".join(str(r.get("Department") or "").upper().split())
        if not dept:
            continue
        price = _f(r, "Unit_Price")
        ads = _f(r, "Avg_Daily_Sales")
        rev = _f(r, "Total_Revenue")
        if rev <= 0:
            # Fall back to the identity revenue is built from, so a scorecard
            # without the column still yields a usable weight.
            rev = price * ads
        a = agg[dept]
        a["sku_count"] += 1
        a["price_sum"] += price
        a["ads_sum"] += ads
        a["value"] += rev
    return dict(agg)


def capital_weights(agg: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    """Share of total value per department, with a floor for the valueless.

    The floor is applied and THEN renormalised, so the weights still sum to 1
    and a department with no priced product cannot silently take capital from
    one that has.
    """
    total = sum(a["value"] for a in agg.values())
    if total <= 0:
        n = max(1, len(agg))
        return {d: 1.0 / n for d in agg}
    weights = {d: (a["value"] / total if a["value"] > 0 else MIN_WEIGHT)
               for d, a in agg.items()}
    s = sum(weights.values())
    return {d: w / s for d, w in weights.items()}


def build_rows(agg: Dict[str, Dict[str, float]]) -> List[Dict[str, Any]]:
    """The CSV rows, in the shape the file has always had."""
    weights = capital_weights(agg)
    total_skus = sum(a["sku_count"] for a in agg.values()) or 1
    out = []
    for dept in sorted(agg):
        a = agg[dept]
        n = a["sku_count"] or 1
        out.append({
            "Department": dept,
            "SKU_Count": int(a["sku_count"]),
            "Avg_Price": round(a["price_sum"] / n, 4),
            "Avg_Daily_Sales": round(a["ads_sum"] / n, 6),
            "Total_Value": round(a["value"], 4),
            "Capital_Weight": weights[dept],
            "SKU_per_Million": round(a["sku_count"] / (total_skus / 1_000_000.0)
                                     / 1_000.0, 6),
        })
    return out


def read_scorecard(path: str) -> List[Dict[str, Any]]:
    with io.open(path, encoding="utf-8", errors="replace", newline="") as f:
        return list(csv.DictReader(f))


def _priced_count(path: str) -> int:
    """How many departments in an existing file carry a real value."""
    try:
        with io.open(path, encoding="utf-8", errors="replace", newline="") as f:
            return sum(1 for r in csv.DictReader(f) if _f(r, "Total_Value") > 0)
    except OSError:
        return 0


def regenerate(scorecard_path: str, data_dir: str, write: bool = True,
               force: bool = False) -> Dict[str, Any]:
    """Rebuild the ratios file from a scorecard. Returns what changed."""
    rows = read_scorecard(scorecard_path)
    agg = aggregate_departments(rows)
    new_rows = build_rows(agg)

    out_path = os.path.join(data_dir, RATIOS_FILE)
    before_priced = _priced_count(out_path)
    after_priced = sum(1 for r in new_rows if r["Total_Value"] > 0)

    result = {
        "scorecard": os.path.basename(scorecard_path),
        "scorecard_rows": len(rows),
        "departments": len(new_rows),
        "priced_before": before_priced,
        "priced_after": after_priced,
        "written": False,
        "backup": None,
        "refused": None,
    }

    # The same discipline the rhythm derivation keeps: never replace richer
    # data with thinner. A scorecard that prices FEWER departments than the
    # file already holds is a worse input, however fresh it is.
    if after_priced < before_priced and not force:
        result["refused"] = (
            f"the existing file prices {before_priced} departments and this "
            f"scorecard prices only {after_priced}; refusing to replace richer "
            f"data with thinner. Pass force=True if that is intended.")
        logger.warning("REFUSED to write: %s", result["refused"])
        return result

    if not write:
        return result

    if os.path.exists(out_path):
        backup = out_path.replace(".csv", ".backup.csv")
        shutil.copy2(out_path, backup)
        result["backup"] = os.path.basename(backup)

    tmp = out_path + ".tmp"
    with io.open(tmp, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(new_rows)
    os.replace(tmp, out_path)
    result["written"] = True
    logger.info("department weights rebuilt: %d departments, %d priced "
                "(was %d)", len(new_rows), after_priced, before_priced)
    return result


def format_report(r: Dict[str, Any]) -> str:
    """ASCII only — this prints to a customer's Windows console."""
    w = ["", "O.A.S.I.S. - department capital weights", "=" * 62]
    w.append(f"  scorecard            {r.get('scorecard', '?')}")
    w.append(f"  products read        {r.get('scorecard_rows', 0):>8,}")
    w.append(f"  departments          {r.get('departments', 0):>8,}")
    w.append(f"  priced before        {r.get('priced_before', 0):>8,}")
    w.append(f"  priced after         {r.get('priced_after', 0):>8,}")
    if r.get("refused"):
        w.append("")
        w.append("  REFUSED TO WRITE")
        w.append(f"     {r['refused']}")
        return "\n".join(w)
    gained = r.get("priced_after", 0) - r.get("priced_before", 0)
    if gained > 0:
        w.append(f"  departments that gained a real weight: {gained:,}")
        w.append("     they were sharing an orphan reserve; they now carry")
        w.append("     their measured share of turnover.")
    if r.get("backup"):
        w.append(f"  previous file kept as {r['backup']}")
    w.append("")
    return "\n".join(w)
