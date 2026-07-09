import logging
from typing import List, Dict, Any

# Assume we can import our ERP adapters
from .erp_adapters import ZohoAdapter, TallyAdapter, PurchaseOrderRequest

logger = logging.getLogger("OASIS.AIEmployee")

class AIEmployeeCron:
    """
    The background AI Employee Agent.
    Executes the 98:2 offloading rule for purchase orders and transfers.
    """
    def __init__(self, tenant_id: str, erp_type: str = "zoho", erp_credentials: dict = None):
        self.tenant_id = tenant_id
        
        # In an enterprise setting, this comes from a database config
        self.auto_approve_po_limit = 50000.0  # KES
        self.auto_approve_transfer_limit = 10000.0 # KES
        
        if erp_type == "zoho":
            self.erp_adapter = ZohoAdapter(tenant_id, erp_credentials or {})
        elif erp_type == "tally":
            self.erp_adapter = TallyAdapter(tenant_id, erp_credentials or {})
        else:
            raise ValueError("Unsupported ERP Type")

    def analyze_order_risk(self, order_total: float, items: List[Dict[str, Any]]) -> bool:
        """
        The 2% Flagging Logic (Anomaly Detection).
        Returns True if the order is HIGH RISK and requires human review.
        """
        # Rule 1: Exceeds absolute financial threshold
        if order_total > self.auto_approve_po_limit:
            logger.warning(f"Order total {order_total} exceeds auto-approve limit of {self.auto_approve_po_limit}")
            return True
            
        # Rule 2: Volume Anomaly (e.g., single item qty > 1000)
        for item in items:
            if item.get("qty", 0) > 1000:
                logger.warning(f"Anomaly detected: extremely high volume for item {item.get('item_code')}")
                return True
                
        return False

    def process_purchase_orders(self, pending_orders: List[Dict[str, Any]]):
        """
        Main loop to process pending internal orders.
        pending_orders is a list of dicts: {"supplier_cd": "...", "org_cd": "...", "items": [...]}
        """
        for order_data in pending_orders:
            items = order_data.get("items", [])
            total_value = sum(item.get("qty", 0) * item.get("unit_cost", 0) for item in items)
            
            po_req = PurchaseOrderRequest(
                org_cd=order_data.get("org_cd", "ORG001"),
                supplier_cd=order_data.get("supplier_cd", "SUPP001"),
                items=items,
                requested_by="OASIS_AI_AGENT"
            )

            is_high_risk = self.analyze_order_risk(total_value, items)
            
            if is_high_risk:
                # The 2% - Send to Approval Dashboard
                logger.info(f"Order {po_req.po_id} flagged for HUMAN REVIEW (Total: {total_value} KES). Pushing to Approval Dashboard.")
                self._save_to_approval_queue(po_req, "PENDING_APPROVAL", total_value)
            else:
                # The 98% - Auto Execute
                logger.info(f"Order {po_req.po_id} approved by AI. Pushing directly to ERP.")
                success = self.erp_adapter.push_purchase_order(po_req)
                if success:
                    self._save_to_approval_queue(po_req, "AUTO_COMPLETED", total_value)

    def _save_to_approval_queue(self, po: PurchaseOrderRequest, status: str, total_value: float):
        """
        Mocks saving to the INTEGRATION_PURCHASE_ORDERS table so the Streamlit UI can render it.
        """
        # In reality, this executes an INSERT INTO INTEGRATION_PURCHASE_ORDERS ...
        logger.debug(f"[DB Write] Inserted PO {po.po_id} with status {status} for tenant {self.tenant_id}")

    def run_cycle(self, mock_orders=None):
        logger.info(f"Starting AI Employee cron cycle for tenant: {self.tenant_id}")
        if not mock_orders:
            mock_orders = [
                # 98% Standard Order
                {"org_cd": "ORG001", "supplier_cd": "SUPP_KAPA", "items": [{"item_code": "ITM001", "qty": 50, "unit_cost": 200}]},
                # 2% High Value Anomaly
                {"org_cd": "ORG002", "supplier_cd": "SUPP_UNILEVER", "items": [{"item_code": "ITM002", "qty": 500, "unit_cost": 500}]},
            ]
        self.process_purchase_orders(mock_orders)
        logger.info("Cycle complete.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    agent = AIEmployeeCron(tenant_id="client_chandarana", erp_type="zoho")
    agent.run_cycle()
