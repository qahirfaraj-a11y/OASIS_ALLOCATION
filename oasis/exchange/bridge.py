import os
import json
import logging
from datetime import datetime
from .exchange_registry import ExchangeRegistry
from .risk_protocol import RiskAssessor

logger = logging.getLogger("KUBER.Bridge")

class OasisKuberBridge:
    """
    KUBER v2.0 Parallel Bridge.
    Listens to O.A.S.I.S. 'Standalone' procurement signals and 
    maps them into the Exchange Registry for funding.
    """
    
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.registry = ExchangeRegistry(data_dir)
        self.risk = RiskAssessor()
        
    def ingest_oasis_projection(self, po_file_path: str):
        """
        Ingests a JSON dump from O.A.S.I.S. Standalone.
        Expects: List of {'product_name', 'qty_ordered', 'unit_cost', 'department', 'avg_daily_sales'}
        """
        if not os.path.exists(po_file_path):
            logger.error(f"Bridge target not found: {po_file_path}")
            return
            
        with open(po_file_path, 'r', encoding='utf-8') as f:
            po_data = json.load(f)
            
        logger.info(f"BRIDGE: Sniffing {len(po_data)} procurement signals from O.A.S.I.S.")
        
        for item in po_data:
            # 1. Parallel Risk calculation (Standalone from O.A.S.I.S. core)
            wp_data = self.risk.calculate_wp({
                "product_name": item["product_name"],
                "department": item["department"],
                "avg_daily_sales": item["avg_daily_sales"],
                "lata_variance_multiplier": item.get("lata_var", 1.0)
            })
            
            # 2. Create Position in Exchange
            pos_id = self.registry.create_position(
                sku=item["product_name"],
                qty=item["qty_ordered"],
                cost_price=item["unit_cost"],
                wp_data=wp_data
            )
            
            logger.info(f"  -> LIFTED: {item['product_name']} | ID: {pos_id} | Risk: {wp_data['wp_score']} | GPP Threshold: {wp_data['dynamic_gpp_threshold']:.1%}")
            
        return len(po_data)

if __name__ == "__main__":
    # Example standalone usage
    logging.basicConfig(level=logging.INFO)
    bridge = OasisKuberBridge(data_dir="./oasis/data/")
    # This would be triggered by a cron or file-watcher on the O.A.S.I.S. output dir
    # bridge.ingest_oasis_projection("./oasis/data/standalone_orders.json")
