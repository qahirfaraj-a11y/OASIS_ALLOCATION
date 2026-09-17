# === ESSENTIAL DEPARTMENT MASTER LIST ===
#
# Single source of truth for essential/staple department categorization
# Used across all allocation logic (Pass 1 price filtering, demand scaling, etc.)
#
# Based on: generate_allocation_scorecard.py (lines 83-86)
# Last updated: 2026-01-28 (GAP-2 fix)

ESSENTIAL_DEPARTMENTS = [
    # Dairy & Fresh
    "FRESH MILK", "BREAD", "EGGS", "YOGHURT", "BUTTER",
    
    # Pantry Staples  
    "FLOUR", "COOKING OIL", "SUGAR", "RICE", "SALT",
    
    # Beverages
    "MINERAL WATER", "SODA",
    
    # Household Basics
    "TOILET ROLL", "TISSUE PAPER",
    
    # Other Staples
    "BREAKFAST CEREALS",
    
    # Gap Analysis Fixes (2026-01-30)
    # FIX M4: Added combined name 'BEANS & LENTILS' to match scorecard department naming
    "GHEE", "BEANS", "LENTILS", "DAIRY", "PULSES", "BEANS & LENTILS"
]

# Fast Five (Duka-specific priority departments)
# Subset of ESSENTIAL_DEPARTMENTS for 60% budget allocation in small stores
FAST_FIVE_DEPARTMENTS = [
    "FRESH MILK", "BREAD", "COOKING OIL", "FLOUR", "SUGAR"
]

# Fresh departments (spoilage risk - 2 day max stock)
FRESH_DEPARTMENTS = [
    "FRESH MILK", "BREAD", "POULTRY", "MEAT", "VEGETABLES", "FRUITS",
    "BAKERY FOODPLUS", "DELICATESSEN", "PASTRY", "EGGS",
    "YOGHURT", "CHEESE", "BUTTER"
]

#: Department names ERP adapters use for the top level of a fresh hierarchy.
#: The POS adapter carried these four; the Odoo and Zoho adapters carried them
#: plus "FRESH" as a SUBSTRING test.
ADAPTER_FRESH_DEPARTMENTS = ["DAIRY", "FRESH PRODUCE", "BUTCHERY", "BAKERY"]


def is_fresh_department(department) -> bool:
    """The one fresh-department rule every ERP adapter applies.

    EXACT match on the normalised name, never a substring. On a real store
    catalogue the substring rule the Odoo adapter used marked AIR FRESHNERS and
    MUKHUWAS (MOUTH FRESHNER) as perishable -- and it disagreed with the POS
    adapter on twelve departments, so the same shop read through two ERPs got
    two different fresh sets and two different shelf-life caps.

    The set is exactly what the POS adapter already applied, so the offline
    path is unchanged; the Odoo path moves onto it. It is NOT widened to
    FRESH_DEPARTMENTS here: the transfer scan reads this raw flag before
    enrichment, and flagging FRESH MILK at this layer would sweep ESL and UHT
    pouches into the fresh horizon. Enrichment (intelligence_mixin) ORs this
    with FRESH_DEPARTMENTS and its keyword test, and applies the long-life
    exclusion, for ordering.
    """
    d = " ".join(str(department or "").upper().split())
    return bool(d) and (d in _FRESH_EXACT)


_FRESH_EXACT = frozenset(" ".join(x.upper().split()) for x in ADAPTER_FRESH_DEPARTMENTS)
