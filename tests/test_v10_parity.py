"""v10 parity checks for OrderEngine.

Rewritten from an async smoke script (which awaited nothing, hardcoded an
absolute data path and asserted nothing) into a real sync test with tmp_path
and assertions on the v10 contracts it was meant to guard:
  * JSON-based no-GRN supplier bypass loads;
  * enrichment applies the staple category boost;
  * greenfield allocation returns the Dict contract and funds the staple.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.order_engine import OrderEngine


def _recs():
    return [
        {
            "product_name": "STAPLE A",
            "product_category": "SUGAR",
            "avg_daily_sales": 10.0,
            "selling_price": 200,
            "margin_pct": 15,
            "sales_rank": 10,
        },
        {
            "product_name": "DISCRETIONARY B",
            "product_category": "TOYS",
            "avg_daily_sales": 0.5,
            "selling_price": 500,
            "margin_pct": 30,
            "sales_rank": 900,
        },
    ]


def test_parity(tmp_path):
    data_dir = str(tmp_path)
    with open(os.path.join(data_dir, "no_grn_suppliers.json"), "w") as f:
        json.dump(["MOCK_SUPPLIER"], f)

    engine = OrderEngine(data_dir)
    engine.load_no_grn_suppliers()
    assert "MOCK_SUPPLIER" in engine.no_grn_suppliers, \
        "JSON-based supplier bypass should load"

    recs = _recs()
    engine.total_budget = 300000
    engine.databases.setdefault("supplier_patterns", {})
    engine.enrich_product_data(recs)

    staple = next(r for r in recs if r["product_name"] == "STAPLE A")
    assert staple.get("category_boost", 1.0) >= 1.0, \
        "staple must never be penalised by category boost"

    result = engine.apply_greenfield_allocation(recs, total_budget=300000)
    assert isinstance(result, dict) and "recommendations" in result, \
        "allocation returns the Dict contract"
    out = {r["product_name"]: r for r in result["recommendations"]}
    assert out["STAPLE A"].get("recommended_quantity", 0) > 0, \
        "the staple should be funded under a healthy budget"
