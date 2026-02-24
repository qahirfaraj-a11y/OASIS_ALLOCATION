from typing import List, Dict, Any
import logging
from datetime import datetime, timedelta

logger = logging.getLogger("OasisAlertMonitor")

class AlertMonitor:
    """
    Monitors sales streams for anomalies and generates Alerts.
    Focus: Velocity Spikes (Sudden demand surge).
    """
    def __init__(self, spike_threshold_pct: float = 300.0):
        self.spike_threshold_pct = spike_threshold_pct # e.g. 300% of normal

    def check_velocity_spikes(self, realtime_sales: List[Dict[str, Any]], historical_stats: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Compares real-time sales velocity (e.g., last 3 hours) vs. historical average.
        """
        alerts = []
        
        # Group realtime sales by product
        current_rates = {}
        for txn in realtime_sales:
            pid = txn.get('product_id') or txn.get('sku')
            qty = txn.get('qty', 0)
            if pid:
                current_rates[pid] = current_rates.get(pid, 0) + qty

        # Compare against history
        for pid, current_qty in current_rates.items():
            stats = historical_stats.get(pid)
            if not stats:
                continue

            avg_daily = stats.get('avg_daily_sales', 0)
            if avg_daily <= 0:
                continue

            # Normalize: "Hourly" rate approximation (Assuming 12h trading day)
            avg_hourly = avg_daily / 12.0
            
            # Simple Heuristic: If we sold > 3x daily avg in a short window? 
            # Or just check if current_qty (which might be a batch) is huge.
            # Let's assume input 'realtime_sales' is a batch from the last Hour.
            
            deviation = (current_qty / avg_hourly) * 100
            
            if deviation > self.spike_threshold_pct:
                alert = {
                    "type": "VELOCITY_SPIKE",
                    "severity": "high",
                    "product_id": pid,
                    "product_name": stats.get('product_name', 'Unknown'),
                    "message": f"Velocity Spike! Sold {current_qty} units in last hour (Avg: {avg_hourly:.1f}/hr). Deviation: {deviation:.0f}%",
                    "timestamp": datetime.now().isoformat(),
                    "recommended_action": "Check Shelf & Depot Stock immediately."
                }
                alerts.append(alert)
                logger.warning(f"Generated Alert: {alert['message']}")
                
        return alerts
