
import os
import sys
import logging

# Setup Path
sys.path.append(os.path.abspath("C:/Users/iLink/.gemini/antigravity/scratch"))

from oasis.logic.budget_manager import BudgetManager

# Mock Setup
class MockBudgetManager(BudgetManager):
    def __init__(self):
        self.staples = {"KABRAS SUGAR 1KG", "AJAB FLOUR 2KG"} # Small mock list
        self.logger = logging.getLogger("MockBM")

def test_staple_logic():
    bm = MockBudgetManager()
    
    print("--- Testing Staple Logic (APS-2) ---")
    
    # CASE 1: In Golden File
    assert bm.is_staple("Kabras Sugar 1kg") == True, "Failed: Exact match should be True"
    assert bm.is_staple("  Ajab Flour 2kg  ") == True, "Failed: Case/Trim match should be True"
    print("Golden File Check: PASS")

    # CASE 2: Fallback (High Vel + Critical Dept)
    # "RICE" is critical. Vel 2.0 is > 1.0.
    is_staple = bm.is_staple("Daawat Rice 5kg", category="RICE", velocity=2.5)
    assert is_staple == True, f"Failed: High Vel Rice should be Staple. Got {is_staple}"
    print("Fallback Heuristic (High Vel Rice): PASS")

    # CASE 3: Fallback (Low Vel + Critical Dept)
    # "RICE" is critical. Vel 0.5 is < 1.0.
    is_staple = bm.is_staple("Unknown Slow Rice", category="RICE", velocity=0.5)
    assert is_staple == False, f"Failed: Low Vel Rice should NOT be Staple. Got {is_staple}"
    print("Fallback Heuristic (Low Vel Rice): PASS")
    
    # CASE 4: Fallback (High Vel + Non-Critical Dept)
    # "BISCUITS" is not critical. Vel 5.0.
    is_staple = bm.is_staple("Manji Biscuits", category="BISCUITS", velocity=5.0)
    assert is_staple == False, f"Failed: High Vel Biscuits should NOT be Staple. Got {is_staple}"
    print("Fallback Heuristic (Non-Critical Dept): PASS")

    print("\nALL TESTS PASSED: Staple Logic is Robust!")

if __name__ == "__main__":
    test_staple_logic()
