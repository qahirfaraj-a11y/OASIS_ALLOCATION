import sys

file_path = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\logic\fulfillment_decider.py'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

# 1. Update risk threshold
content = content.replace('risk_threshold: float = 0.6', 'risk_threshold: float = 0.40')

# 2. Update gap_qty check
content = content.replace('if gap_qty < 1.0 and gap_days > 0:', 'if gap_qty < 0.1 and gap_days > 0:')

# 3. Append ProactiveRebalancer if not present
if 'class ProactiveRebalancer:' not in content:
    proactive_code = """

class ProactiveRebalancer:
    \"\"\"Proactively pushes dead stock from cold nodes to hot nodes.\"\"\"
    def __init__(self, cold_node_days=60, hot_node_days=14):
        self.cold_node_days = cold_node_days
        self.hot_node_days = hot_node_days

    def find_proactive_transfers(self, network_map: NetworkAvailabilityMap) -> List[FulfillmentDecision]:
        transfers = []
        for itm_cd, states in network_map._index.items():
            cold_nodes = []
            hot_nodes = []
            for s in states:
                ads = s.avg_daily_sales
                coverage = s.current_stock / ads if ads > 0 else 999.0
                if coverage > self.cold_node_days and s.current_stock > s.safety_stock * 2:
                    cold_nodes.append(s)
                elif ads > 0.1 and coverage < self.hot_node_days:
                    hot_nodes.append(s)
            
            cold_nodes.sort(key=lambda x: -(x.current_stock - x.safety_stock))
            hot_nodes.sort(key=lambda x: (x.current_stock / x.avg_daily_sales if x.avg_daily_sales > 0 else 0))
            
            for hot in hot_nodes:
                target_stock = hot.avg_daily_sales * 30 # Target 30 days coverage
                shortfall = max(0, target_stock - hot.current_stock)
                if shortfall <= 0: continue
                
                for cold in cold_nodes:
                    avail = cold.current_stock - (cold.safety_stock * 2)
                    if avail <= 0: continue
                    
                    xfer_qty = min(shortfall, avail)
                    
                    d = FulfillmentDecision(
                        itm_cd=itm_cd,
                        product_name=hot.product_name,
                        recipient_org=hot.org_cd,
                        shortfall_qty=shortfall,
                        decision="TRANSFER",
                        transfer_qty=xfer_qty,
                        donor_org=cold.org_cd,
                        donor_name=cold.org_cd,
                        reasoning=f"PROACTIVE: Rebalancing {xfer_qty:.0f} units from cold node ({cold.org_cd}) to hot node ({hot.org_cd})."
                    )
                    transfers.append(d)
                    
                    # Update local states to prevent double-spending
                    cold.current_stock -= xfer_qty
                    hot.current_stock += xfer_qty
                    shortfall -= xfer_qty
                    if shortfall <= 0: break
                    
        return transfers
"""
    content += proactive_code

with open(file_path, 'w', encoding='utf-8') as f:
    f.write(content)

print("Patch applied to fulfillment_decider.py successfully.")
