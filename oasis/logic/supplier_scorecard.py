"""
Supplier Scorecard — quarterly negotiation ammunition for procurement.

Turns the GRN-derived supplier intelligence (supplier_patterns_*.json: order
cadence, gaps, lead time, spend) plus the LATA lead-time-variance multiplier —
the platform's one outcome-validated risk signal — into a client-facing report:
who is reliable, who is quietly costing you safety stock, and where the spend
concentration risk sits.

    python entrypoint.py --mode supplier-scorecard --tenant "Client Name"

Pure scoring (scorecard_rows / classify) is unit-tested; write_scorecard writes
the Markdown + CSV artifacts.
"""

from __future__ import annotations

import csv
import glob
import json
import os
from datetime import datetime
from typing import List


def classify(multiplier: float) -> str:
    """LATA multiplier → Playbook reliability class (mirrors the console)."""
    m = float(multiplier or 1.0)
    if m >= 1.5:
        return "HOSTILE"
    if m > 1.0:
        return "WATCH"
    return "RELIABLE"


def load_patterns(data_dir: str) -> dict:
    matches = sorted(glob.glob(os.path.join(data_dir, "supplier_patterns*.json")),
                     reverse=True)
    if not matches:
        return {}
    try:
        with open(matches[0], "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, ValueError):
        return {}


def scorecard_rows(patterns: dict, top: int = 0) -> List[dict]:
    """One row per supplier, biggest spend first. Pure."""
    rows: List[dict] = []
    for supplier, d in patterns.items():
        if not isinstance(d, dict):
            continue
        mult = float(d.get("lata_variance_multiplier", 1.0) or 1.0)
        orders = int(d.get("total_orders_2025", 0) or 0)
        aov = float(d.get("avg_order_value_kes", 0) or 0)
        rows.append({
            "Supplier": str(supplier)[:42],
            "Class": classify(mult),
            "Orders/Yr": orders,
            "Spend (KES)": round(orders * aov, 0),
            "Avg Order (KES)": round(aov, 0),
            "Cadence": str(d.get("order_frequency", "") or ""),
            "Lead Time (d)": float(d.get("estimated_delivery_days", 0) or 0),
            "Gap (d)": float(d.get("avg_gap_days", 0) or 0),
            "LATA x": round(mult, 2),
        })
    rows.sort(key=lambda r: r["Spend (KES)"], reverse=True)
    return rows[:top] if top else rows


def scorecard_summary(rows: List[dict]) -> dict:
    total_spend = sum(r["Spend (KES)"] for r in rows)
    unreliable = [r for r in rows if r["Class"] != "RELIABLE"]
    at_risk_spend = sum(r["Spend (KES)"] for r in unreliable)
    top5 = sum(r["Spend (KES)"] for r in rows[:5])
    return {
        "suppliers": len(rows),
        "unreliable": len(unreliable),
        "total_spend": round(total_spend, 0),
        "at_risk_spend": round(at_risk_spend, 0),
        "at_risk_pct": round(100.0 * at_risk_spend / total_spend, 1) if total_spend else 0.0,
        "top5_concentration_pct": (round(100.0 * top5 / total_spend, 1)
                                   if total_spend else 0.0),
    }


def write_scorecard(data_dir: str, out_dir: str, tenant: str = "",
                    top: int = 40) -> dict:
    rows = scorecard_rows(load_patterns(data_dir))
    s = scorecard_summary(rows)
    os.makedirs(out_dir, exist_ok=True)
    stamp = datetime.now().strftime("%Y-%m-%d")
    md_path = os.path.join(out_dir, f"OASIS_Supplier_Scorecard_{stamp}.md")
    csv_path = os.path.join(out_dir, f"OASIS_Supplier_Scorecard_{stamp}.csv")

    cols = ["Supplier", "Class", "Orders/Yr", "Spend (KES)", "Avg Order (KES)",
            "Cadence", "Lead Time (d)", "LATA x"]
    head = "| " + " | ".join(cols) + " |\n|" + "|".join("---" for _ in cols) + "|\n"
    body = "".join("| " + " | ".join(str(r[c]) for c in cols) + " |\n"
                   for r in rows[:top])

    md = f"""# O.A.S.I.S. Supplier Scorecard — {tenant or 'your store'}

*Generated {stamp} from delivery history. LATA x is the lead-time-variance
multiplier: 1.0 = as promised; ≥1.5 = HOSTILE (their variance forces you to hold
extra safety stock at your cost).*

## Summary
| Metric | Value |
|---|---|
| Suppliers scored | {s['suppliers']:,} |
| Not fully reliable (WATCH/HOSTILE) | {s['unreliable']:,} |
| Annual spend scored | KES {s['total_spend']:,.0f} |
| **Spend with unreliable suppliers** | **KES {s['at_risk_spend']:,.0f} ({s['at_risk_pct']}%)** |
| Top-5 supplier concentration | {s['top5_concentration_pct']}% of spend |

## Scorecard (by spend)
{head}{body}

---
*Negotiation levers: a HOSTILE class on this sheet is quantified leverage — the
variance premium you carry is real safety stock financed by you. Ask for lead-time
SLAs or shift volume to RELIABLE alternatives in the same categories.*
"""
    with open(md_path, "w", encoding="utf-8") as f:
        f.write(md)
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["Supplier"])
        w.writeheader()
        w.writerows(rows)
    return {"markdown": md_path, "csv": csv_path, **s}
