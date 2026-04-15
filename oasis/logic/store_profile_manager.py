import logging

logger = logging.getLogger("StoreProfileManager")

class StoreProfileManager:
    def __init__(self):
        # Define Keyframes for interpolation
        # Budget -> {params}
        self.keyframes = [
            {
                "budget": 0,
                "tier_name": "Micro (Duka)",
                "depth_days": 7,
                "price_ceiling": 300.0,
                "max_packs": 12,
                "min_display_qty": 3,
                "allow_c_class": False,
                "stale_stock_allowed": False,
                "wallet_buffer_pct": 0.10,
                "supplier_cap": 3
            },
            {
                "budget": 200_000, 
                "tier_name": "Micro (Variety)",
                "depth_days": 10,
                "price_ceiling": 600.0,
                "max_packs": 15,
                "min_display_qty": 3,
                "allow_c_class": False,
                "stale_stock_allowed": False,
                "wallet_buffer_pct": 0.12,
                "supplier_cap": 3
            },
            {
                "budget": 1_000_000, 
                "tier_name": "Mini-Mart (Small)",
                "depth_days": 14,
                "price_ceiling": 2500.0,
                "max_packs": 20,
                "min_display_qty": 4,
                "allow_c_class": True,
                "stale_stock_allowed": False,
                "wallet_buffer_pct": 0.18,
                "supplier_cap": 5
            },
            {
                "budget": 10_000_000, 
                "tier_name": "Standard (Supermarket)",
                "depth_days": 21,
                "price_ceiling": 20000.0,
                "max_packs": 36,
                "min_display_qty": 6,
                "allow_c_class": True,
                "stale_stock_allowed": False,
                "wallet_buffer_pct": 0.25,
                "supplier_cap": 999
            },
            {
                "budget": 50_000_000, 
                "tier_name": "Mega",
                "depth_days": 45,
                "price_ceiling": 1000000.0, # Unlimited proxy
                "max_packs": 999,
                "min_display_qty": 12,
                "allow_c_class": True,
                "stale_stock_allowed": True,
                "wallet_buffer_pct": 0.50,
                "supplier_cap": 999
            }
        ]
        
    def get_profile(self, budget: float) -> dict:
        """
        Returns a configuration dict for the OrderEngine based on the budget.
        Interpolates values between keyframes for smooth scaling.
        """
        # Find bracketing keyframes
        lower = self.keyframes[0]
        upper = self.keyframes[-1]
        
        f_budget = float(budget)
        
        for i in range(len(self.keyframes) - 1):
            l_b = float(self.keyframes[i]["budget"])
            u_b = float(self.keyframes[i+1]["budget"])
            if l_b <= f_budget <= u_b:
                lower = self.keyframes[i]
                upper = self.keyframes[i+1]
                break
        
        if f_budget >= float(upper["budget"]):
            lower = upper
            
        # Calculate ratio
        l_b_final = float(lower["budget"])
        u_b_final = float(upper["budget"])
        
        if u_b_final == l_b_final:
            ratio = 0.0
        else:
            ratio = (f_budget - l_b_final) / (u_b_final - l_b_final)
            
        # Interpolate numeric fields explicitly for type safety
        profile = {}
        numeric_keys = ["depth_days", "price_ceiling", "max_packs", "wallet_buffer_pct", "supplier_cap"]
        for key in numeric_keys:
            l_val = float(lower[key]) # type: ignore
            u_val = float(upper[key]) # type: ignore
            
            val = l_val + (u_val - l_val) * ratio
            if isinstance(lower[key], int):
                profile[key] = int(val)
            else:
                profile[key] = float(round(val, 2))
        
        # v10.0: Discrete MDQ Scaling (Physical Case Alignment)
        mdq_l = float(lower.get("min_display_qty", 3))
        mdq_u = float(upper.get("min_display_qty", 3))
        mdq_raw = mdq_l + (mdq_u - mdq_l) * ratio
        
        # Snap to discrete steps: [2, 3, 4, 6, 12, 24]
        steps = [2, 3, 4, 6, 12, 24]
        profile["min_display_qty"] = min(steps, key=lambda x: abs(x - mdq_raw))
                
        # Non-numeric fields: Take lower bound (conservative) until threshold hit
        profile["allow_c_class"] = f_budget >= 1_000_000.0  # Allow variety above 1M (Mini-Mart)
        profile["stale_stock_allowed"] = bool(lower["stale_stock_allowed"])
        profile["tier_name"] = f"{str(lower['tier_name'])} (Scaled)"
        profile["is_small"] = f_budget < 10_000_000.0  # Small (Mini-Mart) is < 10M KES
        
        logger.info(f"Generated Profile for budget ${budget:,.0f}: {profile}")
        return profile
