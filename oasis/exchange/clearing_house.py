import logging
from datetime import datetime
from typing import Dict, Any
from .exchange_registry import ExchangeRegistry

logger = logging.getLogger("KUBER.ClearingHouse")

class ClearingHouse:
    """
    KUBER v2.0 Clearing House.
    Orchestrates the "Retail-Closed-Loop" settlement.
    Handles Revenue Splits, Escrow management, and GPP Levies.
    """
    
    def __init__(self, registry: ExchangeRegistry):
        self.registry = registry
        # Configuration for Revenue Split (from Implementation Plan)
        self.config = {
            "investor_share_gp": 0.60,      # 60% of Gross Profit
            "store_opex_share_gp": 0.345,   # 34.5% of GP (Residual after GPP/Oasis)
            "oasis_platform_fee_gp": 0.05,  # 5% of GP
            "gpp_levy_trade_vol": 0.005     # 0.5% of Total Trade Volume (Risk Levy)
        }

    def process_sale_event(self, pos_id: str, qty_sold: int, sale_price: float, is_final: bool = False) -> Dict[str, Any]:
        """
        Processes a POS scan event.
        Logic: Accumulates sales in 'pending_settlement_qty'.
        Only triggers financial movement (Batch Settlement) if:
        - qty_sold >= 5% of total_qty
        - OR is_final = True
        """
        if pos_id not in self.registry.registry["active_positions"]:
            logger.warning(f"Sale event for unknown position: {pos_id}")
            return {"status": "ERROR", "reason": "UNKNOWN_POSITION"}
            
        pos = self.registry.registry["active_positions"][pos_id]
        if pos["status"] != "FUNDED":
            return {"status": "ERROR", "reason": "POSITION_NOT_FUNDED"}

        # 1. Accumulate
        pos["pending_settlement_qty"] += qty_sold
        threshold_qty = pos["total_qty"] * 0.05 # 5% Batch Threshold
        
        if pos["pending_settlement_qty"] >= threshold_qty or is_final:
            logger.info(f"SETTLEMENT: Threshold reached for {pos_id}. Processing Batch...")
            results = self.process_batch_settlement(pos_id, sale_price, is_final)
            return results
        
        self.registry.save()
        return {"status": "SUCCESS", "message": "Sale queued for batch settlement."}

    def process_batch_settlement(self, pos_id: str, sale_price: float, is_final: bool) -> Dict[str, Any]:
        """
        Distributes funds to all shareholders pro-rata.
        """
        pos = self.registry.registry["active_positions"][pos_id]
        qty_to_settle = pos["pending_settlement_qty"]
        if qty_to_settle <= 0 and not is_final:
            return {"status": "ERROR", "reason": "NO_UNITS_TO_SETTLE"}

        # 1. Calculation Base
        unit_cost = pos["unit_cost"]
        total_trade_vol = qty_to_settle * sale_price
        
        if is_final:
            # If final, we settle for EVERYTHING funded that hasn't been settled yet
            total_target_cost = pos["total_cost"] * (pos["shares_funded"])
            # (In a real system, we'd track 'total_cost_already_settled' but for Phase 3 this works)
        else:
            total_target_cost = qty_to_settle * unit_cost
            
        gross_profit = total_trade_vol - total_target_cost
        
        # 2. Threshold-based Principal Protection
        threshold = pos.get("dynamic_gpp_threshold", 0.02)
        gpp_intervention = 0.0
        if gross_profit < 0:
            actual_loss_pct = abs(gross_profit) / total_target_cost
            if actual_loss_pct > threshold:
                gpp_intervention = total_target_cost * (actual_loss_pct - threshold)
            gross_profit = 0
            
        # 3. Revenue Splits (GPP/Oasis/Yield)
        gpp_cut = total_trade_vol * self.config["gpp_levy_trade_vol"]
        oasis_cut = gross_profit * self.config["oasis_platform_fee_gp"]
        total_investor_yield = gross_profit * self.config["investor_share_gp"]
        store_cut = gross_profit - (total_investor_yield + oasis_cut + gpp_cut)
        if store_cut < 0: store_cut = 0
        
        # 4. Pro-Rata Distribution
        global_ledger = self.registry.registry["global_ledger"]
        principal_recovery = total_trade_vol + gpp_intervention
        
        for investor_id, share in pos["shareholders"].items():
            if investor_id == "KUBER_DAO": continue # Internal buyout fund
            investor = self.registry.registry["investors"].get(investor_id)
            if not investor: continue
            
            # Investor gets their share of recovered principal + yield
            yield_slice = total_investor_yield * (share / pos["shares_funded"])
            principal_slice = principal_recovery * (share / pos["shares_funded"])
            locked_slice = total_target_cost * (share / pos["shares_funded"])
            
            investor["available_capital"] += (principal_slice + yield_slice)
            investor["locked_capital"] -= locked_slice
            investor["yield_generated"] += yield_slice

        # 5. Ledger Updates
        global_ledger["gpp_insurance_fund"] += (gpp_cut - gpp_intervention)
        global_ledger["gpp_balance"] = global_ledger["gpp_insurance_fund"] + global_ledger["gpp_liquidity_fund"]
        global_ledger["spg_balance"] += store_cut
        global_ledger["platform_fees"] += oasis_cut
        global_ledger["total_trades"] += 1
        global_ledger["total_volume"] += total_trade_vol

        # 6. Lifecycle Management
        pos["pending_settlement_qty"] = 0
        pos["last_settlement_at"] = datetime.now().isoformat()
        
        if is_final:
            self.terminate_position(pos_id, "COMPLETED")
        else:
            self.registry.save()
            
        return {
            "status": "SUCCESS",
            "pos_id": pos_id,
            "recovered_principal": round(principal_recovery, 2),
            "total_yield": round(total_investor_yield, 2),
            "gpp_intervention": round(gpp_intervention, 2)
        }

    def terminate_position(self, pos_id: str, reason: str = "COMPLETED"):
        """Moves a fully sold or expired position to history."""
        if pos_id in self.registry.registry["active_positions"]:
            pos = self.registry.registry["active_positions"].pop(pos_id)
            pos["status"] = reason
            pos["terminated_at"] = datetime.now().isoformat()
            self.registry.registry["historical_positions"].append(pos)
            self.registry.save()
            logger.info(f"Position {pos_id} terminated: {reason}")
