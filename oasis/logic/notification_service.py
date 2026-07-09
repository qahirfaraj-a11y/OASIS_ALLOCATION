import logging
from typing import List, Dict, Any, Optional
import time

logger = logging.getLogger(__name__)

class NotificationService:
    """
    Evaluates alerts based on system configuration thresholds.
    In a full production environment, this would push to WebSockets, Emails, or SMS.
    Here, it provides data for the in-app notification bell.
    """
    
    def __init__(self, db_connector, alert_monitor):
        self.db_connector = db_connector
        self.alert_monitor = alert_monitor
        
        # In-memory "read" state tracking. In production, use DB table.
        # Format: {username: set(alert_ids)}
        self.read_alerts: Dict[str, set] = {}

    def get_active_alerts(self, org_cd: Optional[str] = None, user_role: str = "branch_manager", 
                         username: str = "unknown") -> List[Dict[str, Any]]:
        """
        Fetch active alerts for the given org or role.
        """
        alerts = []
        
        # Ensure user state exists
        if username not in self.read_alerts:
            self.read_alerts[username] = set()
            
        # 1. System Config Thresholds
        config = self._load_thresholds()
        spike_pct = float(config.get('velocity_spike_threshold', 200.0))
        critical_so = float(config.get('critical_stockout_hours', 4.0))
        
        # We would dynamically check the database for these depending on how data is loaded.
        # But we can also generate synthetic alerts for demonstration purposes if actual data 
        # is complex to pull synchronously. For this demo step, let's create dynamic alerts
        # from unapproved POs and transfer requests that require attention, plus random velocity alerts.
        
        # Fetch pending POs needing approval
        if user_role in ["regional_manager", "ops_admin"]:
            pending_pos = self._get_pending_pos_count(org_cd)
            if pending_pos > 0:
                alerts.append({
                    "id": f"po_pending_{int(time.time())}",
                    "type": "PO_APPROVAL_REQUIRED",
                    "title": "Pending Purchase Orders",
                    "message": f"There are {pending_pos} purchase orders awaiting approval.",
                    "urgency": "HIGH",
                    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
                })
                
        # Fetch incoming transfers that haven't been received
        pending_transfers = self._get_pending_transfers_count(org_cd)
        if pending_transfers > 0:
            alerts.append({
                "id": f"tx_pending_{int(time.time())}",
                "type": "TRANSFER_INCOMING",
                "title": "Incoming Transfers",
                "message": f"You have {pending_transfers} unreceived incoming transfer(s).",
                "urgency": "MEDIUM",
                "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
            })
            
        # Mark whether they have been read
        for a in alerts:
            a["is_read"] = a["id"] in self.read_alerts[username]

        return alerts
        
    def mark_as_read(self, username: str, alert_id: str):
        if username not in self.read_alerts:
            self.read_alerts[username] = set()
        self.read_alerts[username].add(alert_id)
        
    def _load_thresholds(self) -> dict:
        try:
            from oasis.logic.db_connector import load_system_config
            return load_system_config(self.db_connector.engine.url)
        except Exception:
            return {}
            
    def _get_pending_pos_count(self, org_cd: Optional[str]) -> int:
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        adapter = PosErpAdapter(self.db_connector)
        df = adapter.fetch_pending_pos(org_cd)
        return len(df)
        
    def _get_pending_transfers_count(self, org_cd: Optional[str]) -> int:
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        adapter = PosErpAdapter(self.db_connector)
        df = adapter.fetch_transfers(None) # get all
        # Count transfers INCOMING to this org that are IN_TRANSIT or REQUESTED
        if df.empty or org_cd is None: return 0
        incoming = df[(df['TO_ORG_CD'] == org_cd) & (df['STATUS'].isin(['REQUESTED', 'IN_TRANSIT']))]
        return len(incoming)
