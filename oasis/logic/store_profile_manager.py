import logging

logger = logging.getLogger("StoreProfileManager")

class StoreProfileManager:
    def __init__(self):
        # Define Keyframes for interpolation
        # Budget -> {params}
        self.keyframes = [
            {
                "budget": 0,
                "tier_name": "Micro",
                "depth_days": 7,
                "price_ceiling": 300.0,
                "max_packs": 12,
                "min_display_qty": 3,
                "allow_c_class": False,
                "stale_stock_allowed": False,
                "wallet_buffer_pct": 0.10
            },
            {
                "budget": 200_000, # Old Micro Limit
                "tier_name": "Micro+",
                "depth_days": 10,
                "price_ceiling": 500.0,
                "max_packs": 18,
                "min_display_qty": 3,
                "allow_c_class": False,
                "stale_stock_allowed": False,
                "wallet_buffer_pct": 0.15
            },
            {
                "budget": 1_000_000, # Mini Mart
                "tier_name": "Mini-Mart",
                "depth_days": 14,
                "price_ceiling": 2500.0,
                "max_packs": 24,
                "min_display_qty": 6,
                "allow_c_class": True,
                "stale_stock_allowed": False,
                "wallet_buffer_pct": 0.25
            },
            {
                "budget": 10_000_000, # Supermarket
                "tier_name": "Supermarket",
                "depth_days": 21,
                "price_ceiling": 20000.0,
                "max_packs": 48,
                "min_display_qty": 12,
                "allow_c_class": True,
                "stale_stock_allowed": True,
                "wallet_buffer_pct": 0.50
            },
            {
                "budget": 50_000_000, # Mega
                "tier_name": "Mega",
                "depth_days": 30,
                "price_ceiling": 100000.0,
                "max_packs": 999,
                "min_display_qty": 24,
                "allow_c_class": True,
                "stale_stock_allowed": True,
                "wallet_buffer_pct": 1.00
            },
            {
                "budget": 200_000_000, # Ultra
                "tier_name": "Ultra",
                "depth_days": 60,
                "price_ceiling": 999999.0,
                "max_packs": 9999,
                "min_display_qty": 48,
                "allow_c_class": True,
                "stale_stock_allowed": True,
                "wallet_buffer_pct": 2.00
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
        
        for i in range(len(self.keyframes) - 1):
            if self.keyframes[i]["budget"] <= budget <= self.keyframes[i+1]["budget"]:
                lower = self.keyframes[i]
                upper = self.keyframes[i+1]
                break
        
        if budget >= upper["budget"]:
            lower = upper
            
        # Calculate ratio
        if upper["budget"] == lower["budget"]:
            ratio = 0.0
        else:
            ratio = (budget - lower["budget"]) / (upper["budget"] - lower["budget"])
            
        # Interpolate numeric fields
        profile = {}
        for key in ["depth_days", "price_ceiling", "max_packs", "min_display_qty", "wallet_buffer_pct"]:
            val = lower[key] + (upper[key] - lower[key]) * ratio
            # Check type in keyframe to maintain int/float
            if isinstance(lower[key], int):
                profile[key] = int(val)
            else:
                profile[key] = round(val, 2)
                
        # Boolean/String fields take lower bound (conservative) until threshold hit? 
        # Actually better to take closest or just lower. Let's take lower for safety.
        profile["allow_c_class"] = lower["allow_c_class"]
        profile["stale_stock_allowed"] = lower["stale_stock_allowed"]
        
        # Tier Name
        profile["tier_name"] = f"{lower['tier_name']} (Scaled)"
        
        # Derived Logic
        profile["is_small"] = budget < 1_000_000
        
        logger.info(f"Generated Profile for budget ${budget:,.0f}: {profile}")
        return profile
