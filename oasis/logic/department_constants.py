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
    "GHEE", "BEANS", "LENTILS", "DAIRY", "PULSES"
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
