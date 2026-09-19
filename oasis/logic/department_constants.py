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

# Fresh departments (spoilage risk - 2 day max stock).
# FALLBACK ONLY: the live list is departments.fresh in the engine config (see
# fresh_departments below). Department names are each store's own taxonomy, so
# they are configuration; these generic names apply only when a config carries
# no departments block.
FRESH_DEPARTMENTS = [
    "FRESH MILK", "BREAD", "POULTRY", "MEAT", "VEGETABLES", "FRUITS",
    "DELICATESSEN", "PASTRY", "EGGS",
    "YOGHURT", "CHEESE", "BUTTER"
]

#: Department names ERP adapters use for the top level of a fresh hierarchy.
#: The POS adapter carried these four; the Odoo and Zoho adapters carried them
#: plus "FRESH" as a SUBSTRING test.
ADAPTER_FRESH_DEPARTMENTS = ["DAIRY", "FRESH PRODUCE", "BUTCHERY", "BAKERY"]

#: Departments the transfer planner never auto-moves (fulfillment_decider),
#: matched as SUBSTRINGS there. Fallback only; the live list is
#: departments.no_auto_transfer.
NO_AUTO_TRANSFER_DEPARTMENTS = ["MILK", "DAIRY", "FRESH", "MEAT", "BREAD", "BAKERY",
                                "SEAFOOD", "FISH", "POULTRY", "PRODUCE", "FRUITS", "VEGETABLES"]

# -- the department ROLES, from the engine config ---------------------------------
# Three lists did three different jobs and lived in three modules, in one
# store's department names. They are one config block now, `departments`, with
# the jobs kept apart -- merging them would change behaviour:
#   fresh             ordering freshness (enrichment, procurement): exact names
#   fresh_raw         the adapter-level flag the transfer scan reads BEFORE
#                     enrichment -- deliberately narrow (see is_fresh_department)
#   no_auto_transfer  never auto-transferred (fulfillment_decider): substrings
_ROLES = None
_ROLE_FALLBACK = {"fresh": FRESH_DEPARTMENTS, "fresh_raw": ADAPTER_FRESH_DEPARTMENTS,
                  "no_auto_transfer": NO_AUTO_TRANSFER_DEPARTMENTS}


def _norm(d) -> str:
    return " ".join(str(d or "").upper().split())


def department_role(role: str) -> list:
    """The configured department list for one role, normalised (fallback: the constants)."""
    global _ROLES
    if _ROLES is None:
        cfg = {}
        try:
            from .engines_config import load_engines_config
            cfg = (load_engines_config(None) or {}).get("departments") or {}
        except Exception:
            cfg = {}
        _ROLES = {r: [_norm(x) for x in (cfg.get(r) if isinstance(cfg.get(r), list) else fb)]
                  for r, fb in _ROLE_FALLBACK.items()}
    return list(_ROLES.get(role, []))


def reset_department_roles() -> None:
    """Drop the cached roles (tests, and after a config edit)."""
    global _ROLES
    _ROLES = None


def fresh_departments() -> list:
    return department_role("fresh")


def no_auto_transfer_departments() -> list:
    return department_role("no_auto_transfer")


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
    d = _norm(department)
    return bool(d) and (d in set(department_role("fresh_raw")))
