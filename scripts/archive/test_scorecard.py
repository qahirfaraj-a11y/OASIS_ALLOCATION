import os
from oasis.logic.shadow_mode import ShadowModeEngine

def run_test():
    scorecard_path = r"C:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"
    if not os.path.exists(scorecard_path):
        print("Scorecard not found")
        return
        
    engine = ShadowModeEngine(r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data")
    engine.run_shadow_cycle(scorecard_path)

if __name__ == "__main__":
    run_test()
