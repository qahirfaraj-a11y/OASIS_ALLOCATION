
import sys
import os

# Add the project root to the path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), ".")))

from app.logic.rounding import apply_pack_rounding

def run_test_case(name, inputs, expected_direction=None, expected_qty=None):
    print(f"--- Running Test: {name} ---")
    print(f"Inputs: {inputs}")
    result = apply_pack_rounding(**inputs)
    print(f"Result: {result}")
    
    if expected_direction:
        assert result['rounding_direction'] == expected_direction, f"Expected direction {expected_direction}, got {result['rounding_direction']}"
    if expected_qty is not None:
        assert result['rounded_qty'] == expected_qty, f"Expected qty {expected_qty}, got {result['rounded_qty']}"
    print("PASS\n")

def main():
    print("Verifying Pack Rounding Logic...\n")

    # Case 1: Simple Round Up (Medium Risk)
    # Base 14, Pack 12. Up is 24 (+10/14 = +71%), Down is 12 (-2/14 = -14%). 
    # Wait, 14. 
    # Down: 12. Shortage 2. Ratio 2/14 = 14%. 
    # Up: 24. Overage 10. Ratio 10/14 = 71%.
    # Medium risk prefers smaller deviation. 14% < 71%. Should round DOWN.
    run_test_case(
        "Medium Risk - Closer Option",
        {"base_qty": 14, "pack_size": 12, "stockout_risk": "medium"},
        expected_direction="down",
        expected_qty=12
    )

    # Case 2: Key SKU (Round Up Aggression)
    # Base 14, Pack 12. 
    # High Risk. Overage 71%. Max overage default 25%.
    # 71% > 25%. Rounding up is too aggressive even for high risk. Should round DOWN.
    run_test_case(
        "High Risk - Overage Exceeded",
        {"base_qty": 14, "pack_size": 12, "stockout_risk": "high"},
        expected_direction="down",
        expected_qty=12
    )

    # Case 3: High Risk - Within Tolerance
    # Base 11, Pack 12. Base 11.
    # Up: 12 (+1/11 = +9%). Down: 0.
    # High risk. +9% <= 25%. Should round UP.
    run_test_case(
        "High Risk - Within Tolerance",
        {"base_qty": 11, "pack_size": 12, "stockout_risk": "high"},
        expected_direction="up",
        expected_qty=12
    )

    # Case 4: Low Stockout Risk
    # Base 11, Pack 12.
    # Low Risk. Preference for Down unless shortage > 10%.
    # Down is 0. Shortage 11/11 = 100%. > 10%.
    # Should round UP.
    run_test_case(
        "Low Risk - Stockout Avoidance",
        {"base_qty": 11, "pack_size": 12, "stockout_risk": "low"},
        expected_direction="up",
        expected_qty=12
    )
    
    # Case 5: Low Risk - Acceptable Shortage
    # Base 12.5, Pack 12.
    # Up 24. Down 12.
    # Shortage 0.5. Ratio 0.5/12.5 = 4%.
    # 4% <= 10%. Accept shortage. Round DOWN.
    run_test_case(
        "Low Risk - Acceptable Shortage",
        {"base_qty": 12.5, "pack_size": 12, "stockout_risk": "low"},
        expected_direction="down",
        expected_qty=12
    )

    # Case 6: Key SKU, Base 0
    # Must order 1 pack.
    run_test_case(
        "Key SKU Base 0",
        {"base_qty": 0, "pack_size": 6, "is_key_sku": True, "stockout_risk": "low"}, # Risk overridden by Key SKU
        expected_direction="up",
        expected_qty=6
    )

    # Case 7: Normal SKU, Base 0
    # Should stay 0.
    run_test_case(
        "Normal SKU Base 0",
        {"base_qty": 0, "pack_size": 6, "is_key_sku": False, "stockout_risk": "medium"},
        expected_direction="down",
        expected_qty=0
    )
    
    # Case 8: Exact Match
    run_test_case(
        "Exact Match",
        {"base_qty": 24, "pack_size": 12},
        expected_direction="none",
        expected_qty=24
    )

    print("All Test Cases Passed!")

if __name__ == "__main__":
    main()
