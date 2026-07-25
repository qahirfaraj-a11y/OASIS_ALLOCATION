"""
Self-contained demo catalogue for first-run onboarding.

The client release deliberately ships NO catalogue spreadsheets (the .xlsx demo
data is excluded from the release zip), so "Explore with sample data" cannot
depend on them. This module generates a small, deterministic catalogue entirely
in code — the same row shape ``build_pos_db_from_catalog`` consumes — so a fresh
install can spin up a believable sample store with zero external files.

Deterministic: no randomness, so the demo store is identical on every machine.
"""

from typing import List, Dict

# (department, [(name, price_KES, stock, vendor)]) — a compact but plausible
# convenience-store spread across a few departments.
_DEMO: Dict[str, list] = {
    "Beverages": [
        ("Coca-Cola 500ml", 55, 240, "Coca-Cola Beverages"),
        ("Coca-Cola 1L", 95, 120, "Coca-Cola Beverages"),
        ("Sprite 500ml", 55, 180, "Coca-Cola Beverages"),
        ("Fanta Orange 500ml", 55, 160, "Coca-Cola Beverages"),
        ("Dasani Water 1L", 50, 300, "Coca-Cola Beverages"),
        ("Minute Maid Mango 1L", 130, 64, "Coca-Cola Beverages"),
        ("Del Monte Juice 1L", 210, 48, "Del Monte Kenya"),
    ],
    "Dairy": [
        ("Brookside Milk 500ml", 60, 200, "Brookside Dairy"),
        ("Brookside Yoghurt 250ml", 75, 90, "Brookside Dairy"),
        ("KCC Butter 250g", 240, 40, "New KCC"),
        ("Gouda Cheese 250g", 420, 24, "Brown's Cheese"),
        ("Fresh Cream 250ml", 180, 36, "Brookside Dairy"),
    ],
    "Bakery": [
        ("White Bread 400g", 65, 120, "Broadways Bakery"),
        ("Brown Bread 400g", 70, 110, "Broadways Bakery"),
        ("Bread Rolls 6pk", 90, 60, "Broadways Bakery"),
        ("Cake Slice", 150, 30, "Broadways Bakery"),
    ],
    "Staples": [
        ("Pembe Maize Flour 2kg", 175, 150, "Pembe Flour Mills"),
        ("Ndovu Wheat Flour 2kg", 195, 130, "Unga Group"),
        ("Basmati Rice 2kg", 460, 70, "Capwell Industries"),
        ("Sugar 2kg", 320, 100, "Mumias Sugar"),
        ("Cooking Oil 2L", 640, 55, "Bidco Africa"),
        ("Salt 1kg", 45, 200, "Kensalt"),
    ],
    "Snacks": [
        ("Tropical Heat Crisps 100g", 120, 80, "Tropical Heat"),
        ("Digestive Biscuits 200g", 140, 65, "Manji Foods"),
        ("Peanuts 100g", 85, 90, "Tropical Heat"),
        ("Chocolate Bar 50g", 110, 120, "Cadbury Kenya"),
    ],
    "Household": [
        ("Omo Detergent 1kg", 350, 60, "Unilever Kenya"),
        ("Dish Soap 500ml", 130, 75, "Unilever Kenya"),
        ("Toilet Paper 4pk", 180, 140, "Chandaria Industries"),
        ("Bar Soap 800g", 160, 100, "Bidco Africa"),
    ],
    "Personal Care": [
        ("Colgate Toothpaste 100ml", 190, 70, "Colgate-Palmolive"),
        ("Nivea Lotion 400ml", 520, 30, "Beiersdorf"),
        ("Shampoo 200ml", 280, 45, "Unilever Kenya"),
        ("Toothbrush", 95, 110, "Colgate-Palmolive"),
    ],
}


def demo_catalog_rows() -> List[dict]:
    """Return catalogue rows in the shape ``build_pos_db_from_catalog`` expects:
    {itm_cd, name, dept, vendor, price, stock}. Deterministic item codes.
    """
    rows: List[dict] = []
    n = 0
    for dept, items in _DEMO.items():
        for name, price, stock, vendor in items:
            n += 1
            rows.append({
                "itm_cd": f"DEMO{n:05d}",
                "name": name,
                "dept": dept,
                "vendor": vendor,
                "price": float(price),
                "stock": float(stock),
            })
    return rows


def demo_summary() -> dict:
    rows = demo_catalog_rows()
    return {
        "skus": len(rows),
        "departments": len({r["dept"] for r in rows}),
        "suppliers": len({r["vendor"] for r in rows}),
    }
