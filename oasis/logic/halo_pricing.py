"""
Halo pricing — turn basket affinity into revenue recommendations (Bible Ch. 8.3).

The basket layer (basket_affinity.csv: co-purchase pairs with confidence + lift)
already knows which Anchors pull which Attachments. This module converts that
into the operator's pricing matrix:

  * the **Anchor** is the price-sensitive destination — never mark it up;
  * the **Attachment** rides the anchor's trips and is price-inelastic — it has
    margin headroom;
  * expected daily halo revenue = anchor_ADS × confidence(anchor→attach) × attach price,
    i.e. the attachment revenue the anchor drags in per day.

Direction follows the same convention as basket_affinity.link_edges: the
higher-velocity SKU anchors; the association antecedent breaks ties.

Pure (fully unit-testable); the Intelligence Console renders the output.
"""

from __future__ import annotations

import csv
import os
from typing import Dict, List


def load_affinity(nn_dir: str) -> List[dict]:
    """Read basket_affinity.csv (written by --mode build-baskets)."""
    path = os.path.join(nn_dir, "basket_affinity.csv")
    if not os.path.exists(path):
        return []
    out: List[dict] = []
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            try:
                out.append({
                    "a": str(row["a"]), "b": str(row["b"]),
                    "co_count": int(float(row["co_count"])),
                    "conf_a_to_b": float(row["conf_a_to_b"]),
                    "conf_b_to_a": float(row["conf_b_to_a"]),
                    "lift": float(row["lift"]),
                })
            except (KeyError, TypeError, ValueError):
                continue
    return out


def halo_pricing_rows(metrics: List[dict], meta: Dict[str, dict],
                      min_lift: float = 1.5, min_conf: float = 0.05,
                      top: int = 50) -> List[dict]:
    """Build the Halo pricing matrix rows, strongest daily-revenue halo first.

    meta: {itm_cd: {"name", "price", "cost", "ads"}} — price/cost may be 0.
    Only pairs above the lift/confidence gates and with a known anchor make it.
    """
    rows: List[dict] = []
    for m in metrics:
        if m["lift"] < min_lift:
            continue
        a, b = m["a"], m["b"]
        if a not in meta or b not in meta:
            continue
        va = float(meta[a].get("ads", 0) or 0)
        vb = float(meta[b].get("ads", 0) or 0)
        if va != vb:
            anchor, attach = (a, b) if va > vb else (b, a)
            conf = m["conf_a_to_b"] if anchor == a else m["conf_b_to_a"]
        else:
            anchor, attach = (a, b) if m["conf_a_to_b"] >= m["conf_b_to_a"] else (b, a)
            conf = max(m["conf_a_to_b"], m["conf_b_to_a"])
        if conf < min_conf:
            continue
        am, bm = meta[anchor], meta[attach]
        anchor_ads = float(am.get("ads", 0) or 0)
        attach_price = float(bm.get("price", 0) or 0)
        attach_cost = float(bm.get("cost", 0) or 0)
        attach_margin_pct = (100.0 * (attach_price - attach_cost) / attach_price
                             if attach_price > 0 else 0.0)
        halo_daily = anchor_ads * conf * attach_price
        rows.append({
            "Anchor": str(am.get("name") or anchor)[:40],
            "Attachment": str(bm.get("name") or attach)[:40],
            "Lift": round(m["lift"], 2),
            "Confidence": round(conf, 3),
            "Anchor ADS": round(anchor_ads, 2),
            "Attach Price": round(attach_price, 2),
            "Attach Margin %": round(attach_margin_pct, 1),
            "Halo Rev/Day": round(halo_daily, 0),
            "Play": ("Protect anchor price; attachment has margin headroom"
                     if attach_margin_pct < 25.0 else
                     "Protect anchor price; keep attachment margin"),
            "anchor_cd": anchor, "attach_cd": attach,
        })
    rows.sort(key=lambda r: r["Halo Rev/Day"], reverse=True)
    return rows[:top]


def halo_summary(rows: List[dict]) -> dict:
    return {
        "pairs": len(rows),
        "anchors": len({r["anchor_cd"] for r in rows}),
        "est_daily_halo_revenue": round(sum(r["Halo Rev/Day"] for r in rows), 0),
        "headroom_pairs": sum(1 for r in rows if "headroom" in r["Play"]),
    }


def product_meta_from_adapter(products: List[dict]) -> Dict[str, dict]:
    """Adapter's enriched products → the meta map halo_pricing_rows expects."""
    meta: Dict[str, dict] = {}
    for p in products or []:
        code = str(p.get("item_code", "") or "")
        if not code:
            continue
        meta[code] = {
            "name": p.get("product_name") or p.get("item_name") or code,
            "price": float(p.get("sell_price", 0) or 0),
            "cost": float(p.get("cost_price", 0) or 0),
            "ads": float(p.get("avg_daily_sales", 0) or 0),
        }
    return meta
