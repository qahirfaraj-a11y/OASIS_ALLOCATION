import logging
from typing import List, Dict, Any
from .exchange_registry import ExchangeRegistry
from .risk_protocol import RiskAssessor

logger = logging.getLogger("KUBER.Hook")

class KuberIntegrationHook:
    """
    Standard Hook for O.A.S.I.S. to Push signals into KUBER.
    Ensures O.A.S.I.S. remains standalone while feeding the exchange.
    """
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.registry = ExchangeRegistry(data_dir)
        self.risk = RiskAssessor()

    def process_oasis_results(self, recommendations: List[Dict[str, Any]]):
        """
        Inbound from retail_order_automation.py
        Format: [{'product_name', 'recommended_quantity', 'cost_price', 'department', ...}]
        """
        logger.info(f"KUBER-HOOK: Received {len(recommendations)} recommendations from O.A.S.I.S.")
        
        for rec in recommendations:
            product_name = rec.get('product_name')
            qty = rec.get('recommended_quantity', 0)
            unit_cost = rec.get('cost_price', 0)
            dept = rec.get('department', 'GENERAL')
            
            if qty <= 0:
                continue # Skip zero-order items
                
            # 1. Parallel Risk Sniffing
            wp_data = self.risk.calculate_wp({
                "product_name": product_name,
                "department": dept,
                "avg_daily_sales": rec.get('avg_daily_sales', 1.0),
                "lata_variance_multiplier": rec.get('lata_var', 1.0)
            })
            
            # 2. List in Registry (Status stays at LISTED for manual review)
            pos_id = self.registry.create_position(
                sku=product_name,
                qty=qty,
                cost_price=unit_cost,
                wp_data=wp_data
            )
            
            # Note: We do NOT call fund_position here. 
            # Positions wait in LISTED status for manual/investor review.
            
        self.registry.save()
        logger.info("KUBER-HOOK: Batch listing complete. Registry updated.")

def push_to_kuber(data_dir: str, recommendations: List[Dict[str, Any]]):
    """Entry point for standalone OASIS scripts."""
    try:
        hook = KuberIntegrationHook(data_dir)
        hook.process_oasis_results(recommendations)
    except Exception as e:
        logger.error(f"KUBER-HOOK FAILED: {e}")
