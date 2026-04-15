import json
import os
import logging
import uuid
from datetime import datetime
from typing import Dict, Any, List, Optional

logger = logging.getLogger("KUBER.Registry")

class ExchangeRegistry:
    """
    KUBER v2.0 Exchange Registry.
    Manages Investor Ledgers, Position Tracking, and Global Performance Pool (GPP).
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.registry_path = os.path.join(data_dir, "kuber_registry.json")
        self.registry = self._load_registry()

    def _load_registry(self) -> Dict[str, Any]:
        """Loads the registry from disk or initializes a new one."""
        if os.path.exists(self.registry_path):
            try:
                with open(self.registry_path, 'r', encoding='utf-8') as f:
                    reg = json.load(f)
                    
                # MIGRATION: Ensure GPP sub-funds exist
                ledger = reg.setdefault("global_ledger", {})
                defaults = {
                    "gpp_balance": 0.0,
                    "gpp_liquidity_fund": 0.0,
                    "gpp_insurance_fund": 0.0,
                    "spg_balance": 0.0,
                    "platform_fees": 0.0,
                    "levy_pct": 0.005,
                    "exchange_fee_pct": 0.001
                }
                for k, v in defaults.items():
                    ledger.setdefault(k, v)
                    
                return reg
            except Exception as e:
                logger.error(f"Failed to load registry: {e}")
        
        return {
            "investors": {},
            "active_positions": {},
            "historical_positions": [],
            "global_ledger": {
                "gpp_balance": 0.0,       # Total insurance/protection fund
                "gpp_liquidity_fund": 0.0, # 30% of fees for buyouts
                "gpp_insurance_fund": 0.0, # 50% of fees for waste
                "spg_balance": 0.0,        # Store Performance Group
                "platform_fees": 0.0,      # 20% of exchange fees + oasis cut
                "levy_pct": 0.005,         # 0.5% GPP Levy on Trades
                "exchange_fee_pct": 0.001, # 0.1% P2P Transfer Fee
                "total_trades": 0,
                "total_volume": 0.0
            }
        }

    def save(self):
        """Persists the registry to disk."""
        try:
            with open(self.registry_path, 'w', encoding='utf-8') as f:
                json.dump(self.registry, f, indent=4)
            logger.info("KUBER Registry saved successfully.")
        except Exception as e:
            logger.error(f"Failed to save registry: {e}")

    # Investor Management
    def add_investor(self, name: str, initial_capital: float) -> str:
        """Registers a new investor."""
        investor_id = f"INV_{uuid.uuid4().hex[:8].upper()}"
        self.registry["investors"][investor_id] = {
            "name": name,
            "total_capital": initial_capital,
            "available_capital": initial_capital,
            "locked_capital": 0.0,
            "yield_generated": 0.0,
            "active_positions": [],
            "kyc_verified": True,   # Mock KYC for simulation
            "total_cash_in": 0.0,
            "cash_flow_log": []      # [{timestamp, amount, type: 'CASH_IN'|'CASH_OUT'}]
        }
        self.save()
        return investor_id

    # Position Management
    def create_position(self, sku: str, qty: int, cost_price: float, wp_data: Dict[str, Any]) -> str:
        """Creates a new unfunded position based on OrderEngine recommendations."""
        pos_id = f"POS_{uuid.uuid4().hex[:8].upper()}"
        self.registry["active_positions"][pos_id] = {
            "sku": sku,
            "total_qty": qty,
            "qty_on_hand": qty,
            "unit_cost": cost_price,
            "total_cost": qty * cost_price,
            "status": "LISTED",
            "risk_tranche": wp_data.get("risk_tranche"),
            "wp_score": wp_data.get("wp_score"),
            "yield_target": 0.0, # Assigned during funding
            "gpp_protected": wp_data.get("is_gpp_protected", False),
            "dynamic_gpp_threshold": wp_data.get("dynamic_gpp_threshold", 0.02),
            "created_at": datetime.now().isoformat(),
            "investor_id": None,      # LEGACY: Keeping for back-compat
            "shareholders": {},       # NEW: {investor_id: share_fraction}
            "shares_funded": 0.0,     # Sum of share_fractions [0.0 - 1.0]
            "pending_settlement_qty": 0,
            "last_settlement_at": None,
            "liquidity_requested_at": None,
            "velocity_history": [0] * 10 # NEW: for sparklines
        }
        self.save()
        return pos_id

    def fund_position(self, investor_id: str, pos_id: str, amount_kes: float, yield_pct: float) -> bool:
        """
        Supports fractional funding (0.1% / 10 KES Ticks).
        Calculates Net Yield by deducting GPP Levy and Platform Fees.
        """
        if investor_id not in self.registry["investors"]: return False
        if pos_id not in self.registry["active_positions"]: return False
        
        investor = self.registry["investors"][investor_id]
        pos = self.registry["active_positions"][pos_id]
        ledger = self.registry["global_ledger"]
        
        # 1. Validation
        if amount_kes < 10.0:
            logger.warning(f"Funding too small: {amount_kes} KES. Min tick: 10 KES")
            return False
            
        remaining_cost = pos["total_cost"] * (1.0 - pos["shares_funded"])
        if amount_kes > (remaining_cost + 0.01): # Adding small epsilon for floats
            amount_kes = remaining_cost # Cap at 100%
            
        if investor["available_capital"] < amount_kes:
            logger.warning(f"Investor {investor_id} has insufficient funds for {pos_id}")
            return False
            
        # 2. Drawdown Capital & Taxes
        # Systemic Hardening: Deduct platform fee (0.5%) and GPP levy (0.5%) from trade exposure logic
        levy_amt = amount_kes * ledger.get("levy_pct", 0.005)
        
        investor["available_capital"] -= amount_kes
        investor["locked_capital"] += amount_kes
        if pos_id not in investor["active_positions"]:
            investor["active_positions"].append(pos_id)
        
        # 3. Fractional Allocation
        share_fraction = amount_kes / pos["total_cost"]
        pos["shareholders"][investor_id] = pos["shareholders"].get(investor_id, 0.0) + share_fraction
        pos["shares_funded"] += share_fraction
            
        if pos["shares_funded"] >= 0.999: # Allowing for float precision
            pos["status"] = "FUNDED"
            pos["funded_at"] = datetime.now().isoformat()
            
        # 4. Net Yield Logic (Forensic Integrity)
        # Net Yield = Gross Yield - Systemic Overhead
        net_yield = yield_pct - ledger.get("levy_pct", 0.005) - ledger.get("exchange_fee_pct", 0.001)
        pos["yield_target"] = max(pos["yield_target"], net_yield)
        
        # Route to GPP
        ledger["gpp_balance"] += levy_amt
        ledger["total_volume"] += amount_kes
        
        self.save()
        return True

    def get_summary(self) -> Dict[str, Any]:
        """Returns a high-level summary of the exchange state."""
        ledger = self.registry["global_ledger"]
        return {
            "active_count": len(self.registry["active_positions"]),
            "investor_count": len(self.registry["investors"]),
            "total_locked": sum(i["locked_capital"] for i in self.registry["investors"].values()),
            "gpp_balance": ledger["gpp_balance"],
            "gpp_coverage": ledger["gpp_balance"] / 100000.0 if ledger["gpp_balance"] > 0 else 0.0, # Mock normalization
            "total_volume": ledger["total_volume"]
        }

    def process_cash_in(self, investor_id: str, amount: float):
        """Simulates an M-Pesa Cash-In event."""
        if investor_id not in self.registry["investors"]: return False
        inv = self.registry["investors"][investor_id]
        inv["available_capital"] += amount
        inv["total_cash_in"] += amount
        inv["cash_flow_log"].append({
            "timestamp": datetime.now().isoformat(),
            "amount": amount,
            "type": "CASH_IN"
        })
        self.save()
        return True
