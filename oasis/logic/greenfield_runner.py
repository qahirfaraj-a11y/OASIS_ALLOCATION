"""
Greenfield Allocation Runner
============================
Single shared implementation of the scorecard → engine → basket flow used
by both the standalone Allocation app (allocation_app.py) and the Command
Center's Allocation Engine tab.

Before this module the two UIs each reimplemented the flow and had drifted:
the Command Center tab ran apply_greenfield_allocation() directly, skipping
the enrichment pass and the safety guards that allocation_app applied — so
the same scorecard + budget produced different baskets in the two places.

This runner is the correct path:
    1. enrich_product_data(is_greenfield=True)  — global intelligence
    2. apply_greenfield_allocation()             — multi-pass allocation
    3. apply_safety_guards(initial_load)         — caps, aging, rounding
    4. cost consolidation                        — stratified cost prices
"""

import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

logger = logging.getLogger("GreenfieldRunner")

SCORECARD_GLOB = "Full_Product_Allocation_Scorecard_v*.csv"
SCORECARD_DEFAULT = "Full_Product_Allocation_Scorecard_v3.csv"


@dataclass
class GreenfieldResult:
    """Outcome of a greenfield allocation run."""
    basket: pd.DataFrame
    cash_spend: float
    consignment_value: float
    summary: Dict[str, Any]
    recommendations: List[dict] = field(default_factory=list)

    @property
    def is_empty(self) -> bool:
        return self.basket.empty


def find_latest_scorecard(search_dir: str) -> Optional[str]:
    """Return the highest-versioned scorecard CSV in *search_dir*, or None.

    Versions are parsed from the ``_v<N>`` suffix; unparseable names sort
    to 0 so an explicit version always wins.
    """
    candidates = list(Path(search_dir).glob(SCORECARD_GLOB))
    if not candidates:
        fallback = os.path.join(search_dir, SCORECARD_DEFAULT)
        return fallback if os.path.exists(fallback) else None

    def _version(p: Path) -> int:
        try:
            return int(p.stem.split("_v")[-1])
        except (ValueError, IndexError):
            return 0

    return str(max(candidates, key=_version))


def load_scorecard_recommendations(csv_path: str) -> List[dict]:
    """Read a scorecard CSV into engine-ready recommendation dicts.

    Column mapping (scorecard → engine):
        Product       → product_name
        Unit_Price    → selling_price
        Avg_Daily_Sales → avg_daily_sales
        Department    → product_category
        Pack_Size     → pack_size       (default 1)
        Is_Staple     → is_staple_override
        Margin_Pct    → margin_pct       (None if blank)
        Supplier      → supplier_name
    """
    df = pd.read_csv(csv_path)
    recs: List[dict] = []
    for _, row in df.iterrows():
        margin = row.get("Margin_Pct")
        recs.append({
            "product_name": row.get("Product"),
            "selling_price": float(row.get("Unit_Price", 0) or 0)
                if pd.notnull(row.get("Unit_Price")) else 0.0,
            "avg_daily_sales": float(row.get("Avg_Daily_Sales", 0) or 0)
                if pd.notnull(row.get("Avg_Daily_Sales")) else 0.0,
            "product_category": row.get("Department", "GENERAL"),
            "pack_size": int(row.get("Pack_Size", 1) or 1)
                if pd.notnull(row.get("Pack_Size")) else 1,
            "moq_floor": 0,
            "historical_order_count": 0,  # reset for greenfield
            "is_staple_override": str(row.get("Is_Staple", "False")).upper() == "TRUE",
            "margin_pct": float(margin) if pd.notnull(margin) else None,
            "supplier_name": row.get("Supplier"),
            "recommended_quantity": 0,
            "reasoning": "",
        })
    return recs


def _stratified_cost_price(rec: dict, engine) -> float:
    """Cost price for a rec: cached cost_price first, else engine estimate."""
    cached = rec.get("cost_price")
    if cached is not None and float(cached) > 0:
        return float(cached)
    return float(engine._get_actual_cost_price(rec, float(rec.get("selling_price", 0) or 0)))


def run_greenfield_allocation(
    engine,
    recommendations: List[dict],
    budget: float,
    seasonal_demand_map: Optional[Dict[str, float]] = None,
) -> GreenfieldResult:
    """Run the full greenfield pipeline and return a basket + metrics.

    Args:
        engine: an OrderEngine (its local databases should already be loaded)
        recommendations: scorecard recs from load_scorecard_recommendations()
        budget: capital budget in KES
        seasonal_demand_map: optional {PRODUCT_NAME_UPPER: monthly_units}

    The input list is enriched and mutated in place by the engine, matching
    the existing callers' behavior.
    """
    from oasis.logic.order_logic_guards import apply_safety_guards

    # 1. Enrich (greenfield mode keeps global intelligence, skips store history)
    engine.enrich_product_data(recommendations, is_greenfield=True)

    products_map = {r["product_name"]: r for r in recommendations}

    # 2. Multi-pass allocation
    result = engine.apply_greenfield_allocation(
        recommendations, budget, seasonal_demand_map=seasonal_demand_map
    )
    raw_recs = result["recommendations"]
    summary = result["summary"]

    # 3. Safety guards (initial-load mode)
    final_recs = apply_safety_guards(raw_recs, products_map, allocation_mode="initial_load")

    # 4. Cost consolidation + basket build
    rows: List[dict] = []
    cash_spend = 0.0
    consignment_value = 0.0
    for r in final_recs:
        qty = float(r.get("recommended_quantity", 0) or 0)
        if qty <= 0:
            continue
        cost_price = _stratified_cost_price(r, engine)
        r["cost_price"] = cost_price
        is_consignment = bool(r.get("is_consignment", False))
        cost = round(qty * cost_price, 2)
        price = float(r.get("selling_price", 0) or 0)

        if is_consignment:
            consignment_value = round(consignment_value + cost, 2)
        else:
            cash_spend = round(cash_spend + cost, 2)

        rows.append({
            "Product": r["product_name"],
            "Department": r.get("product_category", "GENERAL"),
            "Qty": qty,
            "Allocated_Cost": cost,
            "Expected_Revenue": qty * price,
            "Reasoning": r.get("reasoning", ""),
            "Type": "CONSIGNMENT" if is_consignment else "CASH",
            "Avg_Daily_Sales": r.get("avg_daily_sales", 0),
        })

    # Keep summary metrics consistent with the realized basket
    summary["total_cash_used"] = cash_spend
    summary["total_consignment_value"] = consignment_value
    summary["utilization_pct"] = round((cash_spend / budget) * 100, 2) if budget > 0 else 0.0

    return GreenfieldResult(
        basket=pd.DataFrame(rows),
        cash_spend=cash_spend,
        consignment_value=consignment_value,
        summary=summary,
        recommendations=final_recs,
    )
