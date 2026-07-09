from typing import List, Dict, Any
import logging
from datetime import datetime

logger = logging.getLogger("OasisAlertMonitor")

class AlertMonitor:
    """
    Monitors sales streams for anomalies and generates Alerts.
    Focus: Velocity Spikes (Sudden demand surge).
    """
    def __init__(self, spike_threshold_pct: float = 300.0):
        self.spike_threshold_pct = spike_threshold_pct # e.g. 300% of normal

    def check_velocity_spikes(self, realtime_sales: List[Dict[str, Any]], historical_stats: Dict[str, Any], elapsed_hours: float = 1.0) -> List[Dict[str, Any]]:
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

            # Calculate expected cumulative sales for the elapsed time
            expected_cumulative = (avg_daily / 14.0) * elapsed_hours
            if expected_cumulative <= 0.1:
                expected_cumulative = 0.1 # avoid div zero
            
            deviation = (current_qty / expected_cumulative) * 100
            
            if deviation > self.spike_threshold_pct:
                alert = {
                    "type": "VELOCITY_SPIKE",
                    "severity": "high",
                    "product_id": pid,
                    "product_name": stats.get('product_name', 'Unknown'),
                    "message": f"Velocity Spike! Sold {current_qty} units in {elapsed_hours:.1f}h (Expected: {expected_cumulative:.1f}). Deviation: {deviation:.0f}%",
                    "timestamp": datetime.now().isoformat(),
                    "recommended_action": "Check Shelf & Depot Stock immediately."
                }
                alerts.append(alert)
                logger.warning(f"Generated Alert: {alert['message']}")
                
        return alerts
