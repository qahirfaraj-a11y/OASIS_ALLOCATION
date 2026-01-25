
import math
from typing import Dict, Any, List

def apply_pack_rounding(
    base_qty: float,
    pack_size: int,
    *,
    is_key_sku: bool = False,
    stockout_risk: str = "medium",   # "low" | "medium" | "high"
    max_overage_ratio: float = 0.25  # how much extra vs base we tolerate
) -> Dict[str, Any]:
    """
    Convert a base (non-rounded) order quantity into a pack-size-aligned quantity.

    Returns a dict with:
      - rounded_qty: int
      - rounding_direction: "up" | "down" | "none"
      - rounding_reason: str
      - overage_units: int  (rounded_qty - base_qty if positive else 0)
      - shortage_units: int (base_qty - rounded_qty if positive else 0)
    """
    if pack_size <= 0:
        # Fallback: no pack constraint
        rounded = int(round(base_qty))
        return {
            "rounded_qty": rounded,
            "rounding_direction": "none",
            "rounding_reason": "No valid pack size provided.",
            "overage_units": max(0, rounded - base_qty),
            "shortage_units": max(0, base_qty - rounded),
        }

    # Calculate natural floor/ceil to nearest pack
    packs_exact = base_qty / float(pack_size)
    packs_floor = math.floor(packs_exact)
    packs_ceil = math.ceil(packs_exact)

    qty_down = packs_floor * pack_size
    qty_up = packs_ceil * pack_size

    # Edge case: if base is zero or tiny but we want at least one pack for key SKUs / high risk
    if base_qty <= 0:
        # Check stockout_risk == "high" OR is_key_sku
        if is_key_sku or stockout_risk == "high":
            rounded = pack_size
            return {
                "rounded_qty": rounded,
                "rounding_direction": "up",
                "rounding_reason": "Base qty <= 0 but SKU is critical/high risk; order minimum one pack.",
                "overage_units": max(0, rounded - base_qty),
                "shortage_units": 0,
            }
        else:
            return {
                "rounded_qty": 0,
                "rounding_direction": "down",
                "rounding_reason": "Base qty <= 0 and risk is not high; no order.",
                "overage_units": 0,
                "shortage_units": 0,
            }

    # If already aligned to a pack, no rounding change
    if abs(packs_exact - round(packs_exact)) < 1e-9:
        rounded = int(base_qty)
        return {
            "rounded_qty": rounded,
            "rounding_direction": "none",
            "rounding_reason": "Base quantity already aligned to pack size.",
            "overage_units": 0,
            "shortage_units": 0,
        }

    # Compute overage/shortage if we go up or down
    overage_if_up = max(0.0, qty_up - base_qty)
    shortage_if_down = max(0.0, base_qty - qty_down)

    # Express overage as a ratio of base qty (e.g. +20% if we go up)
    overage_ratio_if_up = overage_if_up / base_qty if base_qty > 0 else float("inf")
    shortage_ratio_if_down = shortage_if_down / base_qty if base_qty > 0 else float("inf")

    # Decision heuristic:
    # - For high risk or key SKUs: bias to rounding UP as long as overage_ratio <= max_overage_ratio
    # - For low risk: bias to rounding DOWN if shortage_ratio is small
    # - Medium: choose direction with smaller percentage deviation from base
    decision_reason = ""
    direction = "none" # Default init
    rounded = int(round(base_qty)) # Default fallthrough

    if is_key_sku or stockout_risk == "high":
        if overage_ratio_if_up <= max_overage_ratio:
            rounded = qty_up
            decision_reason = "Key/high-risk SKU; prefer rounding up within allowed overage."
            direction = "up"
        else:
            # Over-up would be too much; accept slight shortage
            rounded = qty_down
            decision_reason = "Key/high-risk SKU but rounding up would exceed overage tolerance; rounding down."
            direction = "down"

    elif stockout_risk == "low":
        # Prefer to avoid overstock; only round up if rounding down is a large shortage
        if shortage_ratio_if_down <= 0.10:
            # <=10% shortage tolerated
            rounded = qty_down
            decision_reason = "Low-risk SKU; small shortage from rounding down is acceptable."
            direction = "down"
        else:
            rounded = qty_up
            decision_reason = "Low-risk SKU but rounding down would create large shortage; rounding up."
            direction = "up"
    else:
        # Medium risk: choose direction that is closer in percentage terms to base
        if overage_ratio_if_up <= shortage_ratio_if_down:
            rounded = qty_up
            decision_reason = "Medium risk; rounding up is closer to base quantity."
            direction = "up"
        else:
            rounded = qty_down
            decision_reason = "Medium risk; rounding down is closer to base quantity."
            direction = "down"

    return {
        "rounded_qty": int(rounded),
        "rounding_direction": direction,
        "rounding_reason": decision_reason,
        "overage_units": int(max(0, rounded - base_qty)),
        "shortage_units": int(max(0, base_qty - rounded)),
    }
