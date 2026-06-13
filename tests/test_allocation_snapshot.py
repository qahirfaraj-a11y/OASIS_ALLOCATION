"""
Allocation snapshot (golden-master) test.

Locks in the exact behavior of OrderEngine.apply_greenfield_allocation()
before it is decomposed into strategy classes. Any refactor that changes
per-product quantities or budget utilization will fail this test, making
behavioral drift visible immediately.

To intentionally update the baseline after a deliberate logic change:
    OASIS_UPDATE_SNAPSHOTS=1 python -m pytest tests/test_allocation_snapshot.py
then commit the regenerated tests/snapshots/allocation_baseline.json.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.order_engine import OrderEngine

SNAPSHOT_PATH = os.path.join(
    os.path.dirname(__file__), "snapshots", "allocation_baseline.json"
)

# Deterministic product mix: staples, fresh, discretionary, consignment,
# a price-ceiling skip candidate, and a zero-sales item.
FIXTURE_PRODUCTS = [
    {"product_name": "SUGAR 2KG", "product_category": "SUGAR",
     "avg_daily_sales": 12.0, "selling_price": 250, "pack_size": 12,
     "margin_pct": 12, "sales_rank": 5, "ABC_Class": "A",
     "is_fresh": False, "shelf_life_days": 365,
     "is_consignment": False, "reliability_score": 95, "demand_cv": 0.2,
     "supplier": "SUPPLIER_ALPHA"},
    {"product_name": "MAIZE FLOUR 2KG", "product_category": "FLOUR",
     "avg_daily_sales": 15.0, "selling_price": 180, "pack_size": 12,
     "margin_pct": 10, "sales_rank": 2, "ABC_Class": "A",
     "is_fresh": False, "shelf_life_days": 180,
     "is_consignment": False, "reliability_score": 92, "demand_cv": 0.25,
     "supplier": "SUPPLIER_ALPHA"},
    {"product_name": "FRESH MILK 500ML", "product_category": "FRESH MILK",
     "avg_daily_sales": 20.0, "selling_price": 60, "pack_size": 24,
     "margin_pct": 8, "sales_rank": 1, "ABC_Class": "A",
     "is_fresh": True, "shelf_life_days": 7,
     "is_consignment": False, "reliability_score": 90, "demand_cv": 0.3,
     "supplier": "SUPPLIER_DAIRY"},
    {"product_name": "YOGURT 150ML", "product_category": "DAIRY",
     "avg_daily_sales": 4.0, "selling_price": 90, "pack_size": 12,
     "margin_pct": 18, "sales_rank": 40, "ABC_Class": "B",
     "is_fresh": True, "shelf_life_days": 5,
     "is_consignment": False, "reliability_score": 88, "demand_cv": 0.4,
     "supplier": "SUPPLIER_DAIRY"},
    {"product_name": "COOKING OIL 1L", "product_category": "COOKING OIL",
     "avg_daily_sales": 8.0, "selling_price": 350, "pack_size": 6,
     "margin_pct": 14, "sales_rank": 8, "ABC_Class": "A",
     "is_fresh": False, "shelf_life_days": 365,
     "is_consignment": False, "reliability_score": 94, "demand_cv": 0.2,
     "supplier": "SUPPLIER_BETA"},
    {"product_name": "BREAD 400G", "product_category": "BREAD",
     "avg_daily_sales": 25.0, "selling_price": 65, "pack_size": 10,
     "margin_pct": 10, "sales_rank": 3, "ABC_Class": "A",
     "is_fresh": True, "shelf_life_days": 3,
     "is_consignment": True, "reliability_score": 96, "demand_cv": 0.15,
     "supplier": "SUPPLIER_BAKERY"},
    {"product_name": "PREMIUM WHISKY 750ML", "product_category": "LIQUOR",
     "avg_daily_sales": 0.3, "selling_price": 8500, "pack_size": 6,
     "margin_pct": 35, "sales_rank": 800, "ABC_Class": "C",
     "is_fresh": False, "shelf_life_days": 3650,
     "is_consignment": False, "reliability_score": 85, "demand_cv": 0.8,
     "supplier": "SUPPLIER_GAMMA"},
    {"product_name": "TOY CAR", "product_category": "TOYS",
     "avg_daily_sales": 0.4, "selling_price": 450, "pack_size": 6,
     "margin_pct": 30, "sales_rank": 900, "ABC_Class": "C",
     "is_fresh": False, "shelf_life_days": 3650,
     "is_consignment": False, "reliability_score": 80, "demand_cv": 0.9,
     "supplier": "SUPPLIER_GAMMA"},
    {"product_name": "DETERGENT 1KG", "product_category": "DETERGENTS",
     "avg_daily_sales": 6.0, "selling_price": 220, "pack_size": 12,
     "margin_pct": 16, "sales_rank": 25, "ABC_Class": "B",
     "is_fresh": False, "shelf_life_days": 730,
     "is_consignment": False, "reliability_score": 91, "demand_cv": 0.3,
     "supplier": "SUPPLIER_BETA"},
    {"product_name": "DEAD STOCK ITEM", "product_category": "GENERAL",
     "avg_daily_sales": 0.0, "selling_price": 300, "pack_size": 6,
     "margin_pct": 20, "sales_rank": 999, "ABC_Class": "C",
     "is_fresh": False, "shelf_life_days": 365,
     "is_consignment": False, "reliability_score": 75, "demand_cv": 1.0,
     "supplier": "SUPPLIER_GAMMA"},
]

BUDGET_TIERS = {
    "micro_150k": 150_000.0,
    "mid_1m": 1_000_000.0,
}


def _fresh_engine(tmp_path) -> OrderEngine:
    """Engine with an empty data dir: no caches, no rhythm DB, no engine flags."""
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    return OrderEngine(str(data_dir))


def _run_allocation(engine: OrderEngine, budget: float) -> dict:
    products = json.loads(json.dumps(FIXTURE_PRODUCTS))  # deep copy
    result = engine.apply_greenfield_allocation(products, total_budget=budget)
    summary = result["summary"]
    return {
        "quantities": {
            r["product_name"]: int(r.get("recommended_quantity", 0))
            for r in result["recommendations"]
        },
        "summary": {
            "total_cash_used": round(float(summary.get("total_cash_used", 0)), 2),
            "total_consignment": round(float(summary.get("total_consignment", 0)), 2),
            "total_skipped": int(summary.get("total_skipped", 0)),
            "skip_reasons": {
                k: int(v) for k, v in sorted(
                    summary.get("skip_reasons", {}).items()
                )
            },
        },
    }


def _capture_snapshot(tmp_path) -> dict:
    return {
        tier: _run_allocation(_fresh_engine(tmp_path), budget)
        for tier, budget in BUDGET_TIERS.items()
    }


def test_allocation_matches_snapshot(tmp_path):
    current = _capture_snapshot(tmp_path)

    if os.getenv("OASIS_UPDATE_SNAPSHOTS") == "1" or not os.path.exists(SNAPSHOT_PATH):
        os.makedirs(os.path.dirname(SNAPSHOT_PATH), exist_ok=True)
        with open(SNAPSHOT_PATH, "w", encoding="utf-8") as f:
            json.dump(current, f, indent=2, sort_keys=True)
        if os.getenv("OASIS_UPDATE_SNAPSHOTS") != "1":
            pytest.skip(
                "Baseline snapshot created at tests/snapshots/"
                "allocation_baseline.json — commit it and re-run."
            )
        return

    with open(SNAPSHOT_PATH, encoding="utf-8") as f:
        baseline = json.load(f)

    for tier in BUDGET_TIERS:
        assert current[tier]["quantities"] == baseline[tier]["quantities"], (
            f"[{tier}] Per-product allocation quantities changed. If this "
            f"was a deliberate logic change, regenerate the baseline with "
            f"OASIS_UPDATE_SNAPSHOTS=1."
        )
        assert current[tier]["summary"] == baseline[tier]["summary"], (
            f"[{tier}] Allocation summary changed (budget usage / skips)."
        )


def test_allocation_is_deterministic(tmp_path):
    """Two runs with identical input must produce identical output."""
    first = _capture_snapshot(tmp_path)
    second = _capture_snapshot(tmp_path)
    assert first == second


def test_skip_count_is_authoritative(tmp_path):
    """A1: summary.total_skipped must equal the number of SKUs that actually
    ended at quantity 0 — counting every stage that zeroed an item, and not
    counting Pass-1 rejections that were later rescued by mop-up re-entry."""
    for budget in BUDGET_TIERS.values():
        engine = _fresh_engine(tmp_path)
        products = json.loads(json.dumps(FIXTURE_PRODUCTS))
        result = engine.apply_greenfield_allocation(products, total_budget=budget)
        recs = result["recommendations"]
        summary = result["summary"]

        actual_zero = sum(1 for r in recs
                          if float(r.get("recommended_quantity", 0)) == 0)
        assert summary["total_skipped"] == actual_zero, (
            f"total_skipped={summary['total_skipped']} but {actual_zero} "
            f"SKUs ended at qty 0 (budget {budget})"
        )
        # Stage breakdown present and consistent with the headline count
        assert "skipped_by_stage" in summary
        assert sum(summary["skipped_by_stage"].values()) == actual_zero
        assert sum(summary["skip_reasons"].values()) == actual_zero


def test_skip_count_excludes_rescued_items(tmp_path):
    """An item Pass 1 rejected but mop-up re-entered must NOT be counted as
    skipped — and must carry a positive quantity."""
    engine = _fresh_engine(tmp_path)
    products = json.loads(json.dumps(FIXTURE_PRODUCTS))
    result = engine.apply_greenfield_allocation(products, total_budget=1_000_000.0)
    for r in result["recommendations"]:
        if "RE-ENTRY" in str(r.get("reasoning", "")):
            assert float(r["recommended_quantity"]) > 0


def test_a2_zero_sales_not_overallocated(tmp_path):
    """A2: a zero-ADS item must never receive mop-up depth. Its quantity may
    be at most its Pass-1 shelf-fill MDQ — not a budget-dumping pile-on."""
    for budget in (1_000_000.0, 5_000_000.0):
        engine = _fresh_engine(tmp_path)
        products = json.loads(json.dumps(FIXTURE_PRODUCTS))
        result = engine.apply_greenfield_allocation(products, total_budget=budget)
        dead = next(r for r in result["recommendations"]
                    if r["product_name"] == "DEAD STOCK ITEM")
        assert float(dead["avg_daily_sales"]) == 0.0  # fixture precondition
        # Comfortably below the old 110-unit pile-on; a few units of shelf
        # presence from Pass 1 is acceptable, mop-up depth is not.
        assert float(dead["recommended_quantity"]) <= 10, (
            f"zero-ADS item got {dead['recommended_quantity']} units at "
            f"budget {budget} — mop-up pile-on regression"
        )


def test_a2_utilization_capped(tmp_path):
    """A2: realized utilization must never exceed max_utilization_pct."""
    from oasis.logic.allocation_strategies import AllocationConfig
    cap = AllocationConfig().max_utilization_pct * 100
    for budget in BUDGET_TIERS.values():
        engine = _fresh_engine(tmp_path)
        products = json.loads(json.dumps(FIXTURE_PRODUCTS))
        result = engine.apply_greenfield_allocation(products, total_budget=budget)
        util = float(result["summary"]["utilization_pct"])
        # Small tolerance for Pass-1/2 spend that lands before the cap gate
        assert util <= cap + 1.0, f"utilization {util}% exceeds cap {cap}%"
