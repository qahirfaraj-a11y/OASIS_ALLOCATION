import logging
import os
from datetime import datetime, timedelta
from .exchange_registry import ExchangeRegistry

logger = logging.getLogger("KUBER.SecondaryMarket")

class SecondaryMarket:
    """
    Tiered Liquidity Engine for KUBER.
    Handles P2P matches and GPP 'Last Resort' Buyouts.
    """
    def __init__(self, registry: ExchangeRegistry):
        self.registry = registry
        self.data = self.registry.registry
        self.order_book = {
            "asks": [], # [{investor_id, pos_id, fraction, price_kes, created_at}]
            "bids": []
        }
        # Decision from implementation plan: 20% cap on GPP liquidity usage
        self.gpp_liquidity_cap_pct = 0.20

    def list_shares_for_sale(self, investor_id: str, pos_id: str, fraction: float, price_kes: float):
        """
        Investor broadcasts a PO_LIQUIDITY_REQUESTED event.
        """
        if pos_id not in self.data["active_positions"]: return False
        pos = self.data["active_positions"][pos_id]
        
        # Verify ownership
        current_share = pos["shareholders"].get(investor_id, 0.0)
        if current_share < fraction:
            logger.warning(f"Investor {investor_id} has insufficient shares in {pos_id}")
            return False
            
        order = {
            "order_id": f"ORD_{os.urandom(4).hex().upper()}",
            "investor_id": investor_id,
            "pos_id": pos_id,
            "fraction": fraction,
            "price_kes": price_kes,
            "created_at": datetime.now().isoformat()
        }
        self.order_book["asks"].append(order)
        
        # Mark position as requesting liquidity (for GPP bot timer)
        pos["liquidity_requested_at"] = order["created_at"]
        
        logger.info(f"EVENT: PO_LIQUIDITY_REQUESTED | {pos_id} | Share: {fraction:.1%}")
        return order["order_id"]

    def execute_p2p_matching(self, buyer_id: str, order_id: str) -> bool:
        """
        Another investor 'buys' the position at the market price.
        Enforces 0.1% Growth Engine Fee.
        """
        # Find order
        order = next((o for o in self.order_book["asks"] if o["order_id"] == order_id), None)
        if not order: return False
        
        buyer = self.data["investors"].get(buyer_id)
        seller = self.data["investors"].get(order["investor_id"])
        pos = self.data["active_positions"].get(order["pos_id"])
        
        if not buyer or not seller or not pos: return False
        
        total_price = order["price_kes"]
        exchange_fee = total_price * self.data["global_ledger"]["exchange_fee_pct"]
        total_buyer_cost = total_price + exchange_fee # Buyer pays fee for liquidity access
        
        if buyer["available_capital"] < total_buyer_cost:
            logger.warning(f"Buyer {buyer_id} has insufficient funds.")
            return False
            
        # 1. Financial Movement
        buyer["available_capital"] -= total_buyer_cost
        buyer["locked_capital"] += total_price # Re-locked into the asset
        seller["available_capital"] += total_price
        seller["locked_capital"] -= (pos["total_cost"] * order["fraction"]) # Original cost basis
        
        # 2. 0.1% Growth Engine Split (50/30/20)
        ledger = self.data["global_ledger"]
        ledger["gpp_insurance_fund"] += exchange_fee * 0.50
        ledger["gpp_liquidity_fund"] += exchange_fee * 0.30
        ledger["platform_fees"] += exchange_fee * 0.20
        # Sync GPP total
        ledger["gpp_balance"] = ledger["gpp_insurance_fund"] + ledger["gpp_liquidity_fund"]
        
        # 3. Ownership Transfer
        pos["shareholders"][order["investor_id"]] -= order["fraction"]
        pos["shareholders"][buyer_id] = pos["shareholders"].get(buyer_id, 0.0) + order["fraction"]
        
        # Cleanup
        self.order_book["asks"].remove(order)
        self.registry.save()
        
        logger.info(f"MATCH: P2P Exit for {order['pos_id']}. Fee: {exchange_fee:.2f} split (50/30/20)")
        return True

    def run_gpp_liquidity_bot(self):
        """
        The 'Last Resort' Safety Net.
        Checks for orders older than 48h.
        """
        now = datetime.now()
        ledger = self.data["global_ledger"]
        
        # Cap check
        max_liquidity_spend = ledger["gpp_liquidity_fund"] * self.gpp_liquidity_cap_pct
        
        for order in list(self.order_book["asks"]):
            created_at = datetime.fromisoformat(order["created_at"])
            if now - created_at > timedelta(hours=48):
                # EMERGENCY EXIT: GPP Buys out the position with 7% Penalty Haircut
                pos = self.data["active_positions"][order["pos_id"]]
                cost_basis = pos["total_cost"] * order["fraction"]
                buyout_price = cost_basis * 0.93 # 7% Haircut
                
                if buyout_price > max_liquidity_spend:
                    logger.warning(f"GPP Liquidity Cap Reached. Cannot buyout {order['pos_id']}")
                    continue
                    
                # 1. GPP Movement
                ledger["gpp_liquidity_fund"] -= buyout_price
                seller = self.data["investors"][order["investor_id"]]
                seller["available_capital"] += buyout_price
                seller["locked_capital"] -= cost_basis
                
                # 2. Ownership -> GPP (The exchange now owns a stake in the inventory!)
                pos["shareholders"][order["investor_id"]] -= order["fraction"]
                pos["shareholders"]["KUBER_DAO"] = pos["shareholders"].get("KUBER_DAO", 0.0) + order["fraction"]
                
                self.order_book["asks"].remove(order)
                logger.warning(f"GPP SAFETY NET: Buyout {order['pos_id']} at 7% Penalty Haircut!")
                
        self.registry.save()
