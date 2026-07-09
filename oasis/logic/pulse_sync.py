import os
import json
import asyncio
import httpx
import logging
import socket
from datetime import datetime
from typing import Dict, Any

# Internal imports
# Ensure we can find the parent package
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from oasis.logic.order_engine import OrderEngine

logger = logging.getLogger("OasisPulse")
logging.basicConfig(level=logging.INFO)

class PulseEngine:
    """
    Distributed Pulse Service.
    Aggregates local Store Node metrics and syncs them to the Master Control Hub.
    """
    
    def __init__(self, data_dir: str, hub_url: str = "http://localhost:8000"):
        self.data_dir = data_dir
        self.hub_url = hub_url
        self.engine = OrderEngine(data_dir)
        self.node_id = self._get_node_id()
        self.last_sync = None
        
    def _get_node_id(self) -> str:
        """Returns a unique identifier for this store node."""
        try:
            return f"NODE_{socket.gethostname().upper()}"
        except:
            return "NODE_UNKNOWN"

    async def calculate_kpis(self) -> Dict[str, Any]:
        """Calculates high-level KPIs from local data stores."""
        logger.info("Pulse: Calculating local KPIs...")
        
        # 1. Load local inventory for a snapshot
        inv_file = self.engine.get_latest_inventory_file()
        if not inv_file:
            return {"status": "error", "message": "No inventory file found"}
            
        products = self.engine.parse_inventory_file(inv_file)
        # Just grab a sample or use pre-calculated summaries if available
        total_value = sum(float(p.get('current_stocks', 0)) * float(p.get('selling_price', 0)) for p in products[:2000])
        total_skus = len(products)
        stockouts = sum(1 for p in products if float(p.get('current_stocks', 0)) <= 0)
        
        # 2. Get Exchange Stats if active
        ex_summary = {}
        try:
            from oasis.exchange.exchange_registry import ExchangeRegistry
            registry = ExchangeRegistry(self.data_dir)
            ex_summary = registry.get_summary()
        except Exception as e:
            logger.warning(f"Pulse: KUBER integration inactive: {e}")
        
        return {
            "node_id": self.node_id,
            "timestamp": datetime.now().isoformat(),
            "metrics": {
                "total_inventory_value": total_value,
                "sku_count": total_skus,
                "stockout_rate": stockouts / total_skus if total_skus > 0 else 0,
                "tvl_locked": ex_summary.get("total_locked", 0),
                "active_orders": ex_summary.get("active_count", 0)
            },
            "system_health": {
                "cpu_load": 12, # Mocked for now
                "db_integrity": "OK",
                "last_audit_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }

    async def transmit_pulse(self):
        """Transmits the KPI package to the Master Control Hub."""
        pulse_data = await self.calculate_kpis()
        
        async with httpx.AsyncClient() as client:
            try:
                logger.info(f"Pulse: Transmitting to {self.hub_url}/api/v1/pulse...")
                response = await client.post(
                    f"{self.hub_url}/api/v1/pulse", 
                    json=pulse_data,
                    timeout=10.0
                )
                if response.status_code == 200:
                    logger.info("Pulse: Sync Successful.")
                    self.last_sync = datetime.now()
                    # Apply remote config overrides if provided
                    remote_config = response.json().get("config_overrides")
                    if remote_config:
                        self._apply_remote_config(remote_config)
                else:
                    logger.error(f"Pulse: Transmission failed ({response.status_code})")
            except Exception as e:
                logger.error(f"Pulse: Connection Error: {e}")

    def _apply_remote_config(self, config: Dict[str, Any]):
        """Saves Hub-provided configuration overrides to local disk."""
        config_path = os.path.join(self.data_dir, "oasis_engines_config.json")
        try:
            with open(config_path, 'w', encoding='utf-8') as f:
                json.dump(config, f, indent=4)
            logger.info("Pulse: Applied remote configuration update.")
        except Exception as e:
            logger.error(f"Pulse: Failed to save remote config: {e}")

async def main():
    # Simulation Runner
    data_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data")
    pulse = PulseEngine(data_dir)
    while True:
        await pulse.transmit_pulse()
        await asyncio.sleep(60) # Sync every 60 seconds for demo

if __name__ == "__main__":
    asyncio.run(main())
