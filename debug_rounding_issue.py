
import sys
import os

# Set up paths
sys.path.append(os.getcwd())

from oasis.logic.rounding import apply_pack_rounding

def test_rounding():
    # Scenario: Small store item
    # ADS = 0.03 (1 unit every 33 days)
    # Target Coverage = 12 days
    # Base Qty = 12 * 0.03 = 0.36
    # Pack Size = 6 (small pack)
    
    print("--- Low Volume Rounding Test ---")
    base_qty = 0.36
    pack_size = 6
    risk = "high" # Item is stocked out
    is_key = False
    
    result = apply_pack_rounding(
        base_qty=base_qty,
        pack_size=pack_size,
        stockout_risk=risk,
        is_key_sku=is_key,
        max_overage_ratio=0.25
    )
    
    print(f"Base Qty: {base_qty}")
    print(f"Pack Size: {pack_size}")
    print(f"Risk: {risk}")
    print(f"Rounded Qty: {result['rounded_qty']}")
    print(f"Reason: {result['rounding_reason']}")
    print(f"Direction: {result['rounding_direction']}")

    # Scenario: Even smaller pack size 1
    print("\n--- Pack size 1 Test ---")
    result_p1 = apply_pack_rounding(
        base_qty=0.36,
        pack_size=1,
        stockout_risk=risk,
        is_key_sku=is_key,
        max_overage_ratio=0.25
    )
    print(f"Base Qty: 0.36")
    print(f"Pack Size: 1")
    print(f"Rounded Qty: {result_p1['rounded_qty']}")
    print(f"Reason: {result_p1['rounding_reason']}")

if __name__ == "__main__":
    test_rounding()
