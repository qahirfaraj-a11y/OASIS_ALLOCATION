"""
Multi-Store Profile Definitions for O.A.S.I.S.
================================================

Five distinct Chandarana Foodplus stores, each with a differentiated stock
profile, department emphasis, traffic cadence, and assortment strategy. These
profiles drive:

    1. **Stock seeding** — which SKUs each store carries and at what depth.
    2. **POS traffic** — how many bills/hour and basket sizes differ.
    3. **Department weighting** — stores emphasise different categories.

Store Archetypes
~~~~~~~~~~~~~~~~
    STORE 1 — Rhapta Road (Flagship)   Full-depth premium supermarket
    STORE 2 — Lavington (Upscale)      Curated premium, heavy deli/wine
    STORE 3 — Karen (Family)           Bulk staples, large baskets
    STORE 4 — Westgate (Mall Express)  Convenience, fast turnover, lean stock
    STORE 5 — Yaya Centre (Urban)      Downtown impulse, high traffic, narrow range
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, List, Optional

# ── Department emphasis weights ──────────────────────────────────────────────
# Each store's weight per department group controls:
#   • assortment probability (whether a SKU from that dept is stocked at all)
#   • stock depth multiplier (how many units on hand relative to catalog)
#
# Weights are relative (normalised internally).  1.0 = catalog baseline.
# Groups are coarse labels matched against the dept_*.xlsx DEPARTMENT column.

# The groups are matched by substring — "DAIRY" matches "DAIRY PRODUCTS",
# "FRESH DAIRY", etc.

DEPARTMENT_GROUPS = {
    "STAPLE":       ["RICE", "FLOUR", "SUGAR", "SALT", "MAIZE", "COOKING OIL",
                     "COOKING FAT", "GHEE", "PULSES", "BEANS", "LENTILS"],
    "DAIRY":        ["MILK", "DAIRY", "CHEESE", "YOGHURT", "BUTTER", "CREAM",
                     "EGGS"],
    "FRESH":        ["FRESH", "VEGETABLE", "FRUIT", "SALAD", "HERBS"],
    "MEAT":         ["MEAT", "CHICKEN", "FISH", "SEAFOOD", "BUTCH", "SAUSAGE",
                     "BACON"],
    "BAKERY":       ["BAKERY", "BREAD", "CAKE", "PASTRY", "BISCUIT"],
    "BEVERAGES":    ["BEVERAGE", "JUICE", "WATER", "SODA", "SOFT DRINK",
                     "ENERGY DRINK", "TEA", "COFFEE"],
    "ALCOHOL":      ["WINE", "BEER", "SPIRIT", "LIQUOR", "ALCOHOL", "WHISKY",
                     "VODKA", "GIN", "RUM", "BRANDY", "CHAMPAGNE"],
    "SNACKS":       ["SNACK", "CRISP", "CHIP", "NUT", "POPCORN", "CHOCOLATE",
                     "CANDY", "SWEET", "CONFECTION"],
    "HOUSEHOLD":    ["HOUSEHOLD", "CLEANING", "DETERGENT", "TISSUE", "PAPER",
                     "SOAP", "BLEACH", "DISINFECT"],
    "PERSONAL":     ["PERSONAL", "TOILETRIES", "SHAMPOO", "BODY", "DEODORANT",
                     "COSMETIC", "SKIN", "LOTION", "TOOTHPASTE", "ORAL"],
    "BABY":         ["BABY", "DIAPER", "NAPPY", "INFANT", "FORMULA"],
    "FROZEN":       ["FROZEN", "ICE CREAM"],
    "CONDIMENTS":   ["SAUCE", "KETCHUP", "MAYONNAISE", "MUSTARD", "VINEGAR",
                     "SPICE", "SEASONING", "HONEY", "JAM", "SPREAD",
                     "PICKLE", "CHUTNEY"],
    "PET":          ["PET", "DOG", "CAT", "ANIMAL FEED"],
    "ELECTRONICS":  ["ELECTRONICS", "APPLIANCE", "ELECTRICAL", "BATTERY",
                     "CHARGER", "CABLE"],
    "STATIONERY":   ["STATIONERY", "OFFICE", "SCHOOL"],
}


@dataclass
class StoreProfile:
    """One store's identity, stock personality, and traffic parameters."""

    org_cd: str                       # ORG001 .. ORG005
    name: str                         # Human-readable store name
    short_name: str                   # 6-char code for ORG table
    address: str
    city: str = "Nairobi"

    # ── Stock personality ──────────────────────────────
    assortment_pct: float = 1.0       # fraction of total catalog to carry (0–1)
    stock_depth: float = 1.0          # global multiplier on catalog stock qty
    dept_weights: Dict[str, float] = field(default_factory=dict)
    # Departments NOT in dept_weights default to 1.0 × assortment/depth.

    # ── Traffic / POS parameters ───────────────────────
    bills_per_hour: float = 25.0      # average customer throughput
    avg_basket_size: int = 4          # items per receipt
    max_attach: int = 4               # max attachment SKUs per basket
    pop_exponent: float = 1.2         # Zipfian hero-SKU concentration
    max_qty: int = 4                  # max units per line

    # ── Demand history ─────────────────────────────────
    history_days: int = 30            # how many prior days of synthetic demand
    history_bills_per_day: int = 400  # density of historical demand seeding

    def interval_seconds(self) -> float:
        """Seconds between receipts for real-time streaming."""
        # Accelerated 20x for demo purposes
        return round(3600.0 / max(1.0, self.bills_per_hour * 20.0), 2)


# ── The Five Stores ──────────────────────────────────────────────────────────

STORE_PROFILES: List[StoreProfile] = [

    # ┌─────────────────────────────────────────────────────────────────────┐
    # │  STORE 1 — Rhapta Road (Flagship)                                  │
    # │  The reference store. Full catalog, deep stock, balanced demand.    │
    # │  Serves Westlands professionals — premium with broad range.        │
    # └─────────────────────────────────────────────────────────────────────┘
    StoreProfile(
        org_cd="ORG001",
        name="Chandarana Foodplus – Rhapta Road",
        short_name="RHAPTA",
        address="Rhapta Road, Westlands",
        assortment_pct=1.00,          # carries EVERYTHING
        stock_depth=1.0,              # catalog baseline
        dept_weights={
            "STAPLE": 1.0, "DAIRY": 1.1, "FRESH": 1.0, "MEAT": 1.0,
            "BAKERY": 1.0, "BEVERAGES": 1.0, "ALCOHOL": 1.1, "SNACKS": 1.0,
            "HOUSEHOLD": 1.0, "PERSONAL": 1.0, "BABY": 0.9, "FROZEN": 1.0,
            "CONDIMENTS": 1.0, "PET": 0.8, "ELECTRONICS": 0.7, "STATIONERY": 0.6,
        },
        bills_per_hour=30.0,
        avg_basket_size=5,
        max_attach=4,
        history_bills_per_day=450,
    ),

    # ┌─────────────────────────────────────────────────────────────────────┐
    # │  STORE 2 — Lavington (Premium Curated)                             │
    # │  Upscale neighbourhood: heavy deli, wine, imported goods.          │
    # │  Smaller range but deeper on premium departments.                  │
    # └─────────────────────────────────────────────────────────────────────┘
    StoreProfile(
        org_cd="ORG002",
        name="Chandarana Foodplus – Lavington",
        short_name="LAVING",
        address="James Gichuru Road, Lavington",
        assortment_pct=0.72,          # curated — doesn't carry 28% of catalog
        stock_depth=0.85,             # leaner overall (premium, lower volume)
        dept_weights={
            "STAPLE": 0.6, "DAIRY": 1.4, "FRESH": 1.3, "MEAT": 1.3,
            "BAKERY": 1.2, "BEVERAGES": 1.0, "ALCOHOL": 1.8, "SNACKS": 0.8,
            "HOUSEHOLD": 0.5, "PERSONAL": 1.1, "BABY": 0.6, "FROZEN": 1.2,
            "CONDIMENTS": 1.3, "PET": 1.0, "ELECTRONICS": 0.3, "STATIONERY": 0.2,
        },
        bills_per_hour=18.0,
        avg_basket_size=4,
        max_attach=3,
        pop_exponent=1.4,             # more concentrated hero SKUs
        history_bills_per_day=280,
    ),

    # ┌─────────────────────────────────────────────────────────────────────┐
    # │  STORE 3 — Karen (Family Bulk)                                     │
    # │  Residential suburb: families buying staples in bulk.              │
    # │  Widest assortment on staples + baby; deeper stock.                │
    # └─────────────────────────────────────────────────────────────────────┘
    StoreProfile(
        org_cd="ORG003",
        name="Chandarana Foodplus – Karen",
        short_name="KAREN",
        address="Karen Road, Hardy",
        assortment_pct=0.85,
        stock_depth=1.25,             # deeper stock — bulk buying
        dept_weights={
            "STAPLE": 1.5, "DAIRY": 1.3, "FRESH": 1.1, "MEAT": 1.2,
            "BAKERY": 1.0, "BEVERAGES": 1.1, "ALCOHOL": 0.7, "SNACKS": 1.0,
            "HOUSEHOLD": 1.4, "PERSONAL": 1.0, "BABY": 1.6, "FROZEN": 0.9,
            "CONDIMENTS": 1.0, "PET": 1.3, "ELECTRONICS": 0.5, "STATIONERY": 0.8,
        },
        bills_per_hour=22.0,
        avg_basket_size=7,            # big family baskets
        max_attach=5,
        max_qty=6,                    # buy more per item
        history_bills_per_day=350,
    ),

    # ┌─────────────────────────────────────────────────────────────────────┐
    # │  STORE 4 — Westgate Mall (Express Convenience)                     │
    # │  Mall traffic: small baskets, fast turnover, convenience focus.    │
    # │  Narrow assortment, low depth, high churn.                         │
    # └─────────────────────────────────────────────────────────────────────┘
    StoreProfile(
        org_cd="ORG004",
        name="Chandarana Foodplus – Westgate",
        short_name="WESTGT",
        address="Westgate Shopping Mall, Westlands",
        assortment_pct=0.55,          # convenience — only top-movers
        stock_depth=0.60,             # lean — replenishes fast
        dept_weights={
            "STAPLE": 0.4, "DAIRY": 1.0, "FRESH": 0.8, "MEAT": 0.6,
            "BAKERY": 1.3, "BEVERAGES": 1.5, "ALCOHOL": 0.5, "SNACKS": 1.8,
            "HOUSEHOLD": 0.3, "PERSONAL": 1.2, "BABY": 0.4, "FROZEN": 0.7,
            "CONDIMENTS": 0.6, "PET": 0.2, "ELECTRONICS": 0.8, "STATIONERY": 0.4,
        },
        bills_per_hour=40.0,          # highest traffic
        avg_basket_size=3,            # grab-and-go
        max_attach=2,
        pop_exponent=1.6,             # very concentrated hero items
        max_qty=2,                    # small qty per line
        history_bills_per_day=520,
    ),

    # ┌─────────────────────────────────────────────────────────────────────┐
    # │  STORE 5 — Yaya Centre (Urban Impulse)                             │
    # │  CBD-adjacent: lunch crowds, impulse purchases, office workers.    │
    # │  Moderate assortment, emphasis on ready-to-eat / beverages.        │
    # └─────────────────────────────────────────────────────────────────────┘
    StoreProfile(
        org_cd="ORG005",
        name="Chandarana Foodplus – Yaya Centre",
        short_name="YAYA",
        address="Argwings Kodhek Road, Kilimani",
        assortment_pct=0.65,
        stock_depth=0.75,
        dept_weights={
            "STAPLE": 0.5, "DAIRY": 1.1, "FRESH": 1.2, "MEAT": 0.7,
            "BAKERY": 1.4, "BEVERAGES": 1.6, "ALCOHOL": 1.0, "SNACKS": 1.5,
            "HOUSEHOLD": 0.4, "PERSONAL": 1.3, "BABY": 0.3, "FROZEN": 1.1,
            "CONDIMENTS": 0.9, "PET": 0.2, "ELECTRONICS": 0.5, "STATIONERY": 0.3,
        },
        bills_per_hour=35.0,
        avg_basket_size=3,
        max_attach=3,
        pop_exponent=1.3,
        max_qty=3,
        history_bills_per_day=480,
    ),
]


def get_profile(org_cd: str) -> Optional[StoreProfile]:
    """Look up a store profile by org code."""
    for p in STORE_PROFILES:
        if p.org_cd == org_cd:
            return p
    return None


def classify_department(dept_name: str) -> str:
    """Map a raw DEPARTMENT column value to its coarse group label.

    Returns the group key (e.g. 'STAPLE', 'DAIRY') or 'OTHER' if no match.
    """
    up = str(dept_name).upper()
    for group, keywords in DEPARTMENT_GROUPS.items():
        for kw in keywords:
            if kw in up:
                return group
    return "OTHER"


def dept_weight_for(profile: StoreProfile, dept_name: str) -> float:
    """Effective weight for a specific department at this store."""
    group = classify_department(dept_name)
    return profile.dept_weights.get(group, 1.0)
