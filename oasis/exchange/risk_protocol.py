import math
import logging
from typing import Dict, Any, List, Optional
from ..logic.department_constants import FRESH_DEPARTMENTS, FAST_FIVE_DEPARTMENTS

logger = logging.getLogger("KUBER.RiskProtocol")

class RiskAssessor:
    """
    KUBER v2.0 Risk Assessor.
    Assigns Risk Tranches and Waste Probability (Wp) to SKUs.
    """
    
    def __init__(self, default_threshold: float = 0.02):
        self.default_threshold = default_threshold
        # Dynamic Threshold Mapping (Internal Architectural Alignment)
        self.department_thresholds = {
            "STAPLE": 0.005,      # 0.5% for grains/sugar
            "GENERAL": 0.015,     # 1.5% for detergents/household
            "FRESH": 0.08,        # 8.0% for Milk/Bread (High Volatility)
            "PRODUCE": 0.12       # 12.0% for Fruits/Veg (Biological Decay)
        }
        
    def get_threshold_for_dept(self, dept: str) -> float:
        dept = dept.upper()
        if any(f in dept for f in FRESH_DEPARTMENTS): return self.department_thresholds["FRESH"]
        if any(f in dept for f in FAST_FIVE_DEPARTMENTS): return self.department_thresholds["STAPLE"]
        return self.department_thresholds.get(dept, self.default_threshold)

    def calculate_wp(self, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Calculates Waste Probability (Wp) for a specific SKU.
        Formula: Wp = (BaseRisk * LT_Multiplier) / (Ads_Factor)
        """
        name = product_data.get('product_name', '').upper()
        dept = product_data.get('department', '').upper()
        ads = float(product_data.get('avg_daily_sales', 0.1))
        lt_var = float(product_data.get('lata_variance_multiplier', 1.0))
        
        # Determine Threshold BEFORE calculation for context
        dynamic_threshold = self.get_threshold_for_dept(dept)
        
        # 1. Base Risk Tiering
        if any(f in dept or f in name for f in FRESH_DEPARTMENTS):
            base_risk = 0.15 # 15% Base Perishability
            tranche = "Tier 1: High Yield/High Risk"
            yield_premium = 0.08 # +8% Premium
        elif any(f in dept or f in name for f in FAST_FIVE_DEPARTMENTS):
            base_risk = 0.01 # 1% Base Staples
            tranche = "Tier 3: Low Yield/Staple"
            yield_premium = 0.0 # Standard yield
        else:
            base_risk = 0.05 # 5% General
            tranche = "Tier 2: Medium Risk/Traffic"
            yield_premium = 0.03 # +3% Premium
            
        # 2. Velocity Discount: High turnover reduces relative expiry risk
        ads_factor = math.sqrt(max(ads, 0.1))
        
        # 3. Supply Chain Penalty: LT Variance increases risk of stock sitting too long
        calculated_wp = (base_risk * lt_var) / ads_factor
        
        # Cap logic: Fresh items can reach 50% risk, others capped lower
        cap = 0.50 if tranche == "Tier 1: High Yield/High Risk" else 0.15
        final_wp = min(cap, calculated_wp)
        
        return {
            "wp_score": round(final_wp, 4),
            "risk_tranche": tranche,
            "yield_premium": yield_premium,
            "dynamic_gpp_threshold": dynamic_threshold,
            "is_gpp_protected": final_wp > dynamic_threshold,
            "wp_reason": f"Base {base_risk:.1%} | LT Var {lt_var:.2f}x | ADS Adj {1/ads_factor:.2f}x"
        }

    def assign_yield_target(self, base_yield: float, wp_data: Dict[str, Any]) -> float:
        """
        Calculates the final yield target for the investor.
        Yield = Base + Premium + (Wp Adjustment)
        """
        premium = wp_data.get('yield_premium', 0.0)
        wp = wp_data.get('wp_score', 0.0)
        
        # We add 20% of the Wp as an additional Risk Premium
        risk_adj = wp * 0.20
        
        return round(base_yield + premium + risk_adj, 4)
