import logging
import os
from .exchange_registry import ExchangeRegistry

# Setup logging to print only the report message to console
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("KUBER.ShadowBank")

class LiquidityAnalyzer:
    """
    Forensic Liquidity Ledger for KUBER Retail Exchange.
    Monitors TVL, Risk Concentration, and GPP Coverage.
    """
    def __init__(self, data_dir: str):
        self.registry = ExchangeRegistry(data_dir)
        self.data = self.registry.registry

    def generate_report(self):
        ledger = self.data["global_ledger"]
        active_pos = self.data["active_positions"]
        
        # 1. TVL & Exposure Metrics
        total_tvl = sum(p["total_cost"] for p in active_pos.values())
        unfunded_vol = sum(p["total_cost"] for p in active_pos.values() if p["status"] == "LISTED")
        funded_vol = sum(p["total_cost"] for p in active_pos.values() if p["status"] == "FUNDED")
        
        # 2. Departmental Concentration
        dept_exposure = {}
        for pid, pos in active_pos.items():
            dept = pos.get("risk_tranche", "Other") # Using risk tranche as proxy for dept weighting
            dept_exposure[dept] = dept_exposure.get(dept, 0) + pos["total_cost"]
            
        # 3. GPP Health (Coverage Ratio)
        # Ratio = GPP Balance / Total Wp (Waste Probability) Exposure
        total_wp_exposure = sum(p["total_cost"] * p["wp_score"] for p in active_pos.values())
        gpp_balance = ledger["gpp_balance"]
        coverage_ratio = (gpp_balance / total_wp_exposure) if total_wp_exposure > 0 else 0
        
        # 4. REPORT PRINTING (Technical Log)
        print("\n" + "="*60)
        print("          KUBER RETAIL EXCHANGE: SHADOW BANK LEDGER")
        print("="*60)
        print(f"REPORT DATE: {os.popen('date /t').read().strip()} {os.popen('time /t').read().strip()}")
        print("-" * 60)
        
        print(f"TOTAL VALUE LOCKED (TVL):   KES {total_tvl:,.2f}")
        print(f"  > FUNDED (ACTIVE):       KES {funded_vol:,.2f}")
        print(f"  > LISTED (PENDING):     KES {unfunded_vol:,.2f}")
        print("-" * 60)
        
        print("DEPARTMENTAL EXPOSURE (CONCENTRATION):")
        for dept, vol in dept_exposure.items():
            share = (vol / total_tvl * 100) if total_tvl > 0 else 0
            print(f"  - {dept:<25}: KES {vol:>12,.2f} ({share:>5.1f}%)")
        print("-" * 60)
        
        print("PRINCIPAL PROTECTION STATUS (GPP / SPG):")
        print(f"  GPP BALANCE:             KES {gpp_balance:,.2f}")
        print(f"  SPG BALANCE:             KES {ledger.get('spg_balance', 0):,.2f}")
        print(f"  Wp EXPOSURE (RISK):      KES {total_wp_exposure:,.2f}")
        print(f"  CAPITAL COVERAGE RATIO:  {coverage_ratio:.2f}")
        
        if coverage_ratio < 1.0:
            print("  ALERT: GPP Under-capitalized relative to systemic risk!")
        elif coverage_ratio > 3.0:
            print("  STATUS: GPP Resilience Optimal (>3x cover).")
        else:
            print("  STATUS: GPP Health Stable.")
            
        print("="*60 + "\n")

if __name__ == "__main__":
    # For standalone diagnostic run
    data_dir = "./oasis/data_validation/" # Default for local validatiaon
    if not os.path.exists(data_dir):
        data_dir = "./oasis/data/"
        
    analyzer = LiquidityAnalyzer(data_dir)
    analyzer.generate_report()
