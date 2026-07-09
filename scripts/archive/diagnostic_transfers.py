import logging
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService

logging.basicConfig(level=logging.INFO)

def main():
    org_names = {"ORG001": "Store 1", "ORG002": "Store 2"}
    
    # Store 1 has 0 stock of ITEM_A (needs 100), Store 2 has 500 stock of ITEM_A (ADS is 10)
    stock_data = {
        "ORG001": [
            {
                "item_code": "ITEM_A",
                "product_name": "Test Item A",
                "current_stocks": 0,
                "avg_daily_sales": 5,
                "selling_price": 1000
            }
        ],
        "ORG002": [
            {
                "item_code": "ITEM_A",
                "product_name": "Test Item A",
                "current_stocks": 500,
                "avg_daily_sales": 10,
                "selling_price": 1000
            }
        ]
    }
    
    distance_map = {
        "ORG001": {"lat": 1.0, "lon": 1.0},
        "ORG002": {"lat": 1.1, "lon": 1.1}
    }
    
    # Let's say ORG001 has a shortfall of 50.
    store_orders = {
        "ORG001": [
            {
                "itm_cd": "ITEM_A",
                "product_name": "Test Item A",
                "recommended_quantity": 50,
                "avg_daily_sales": 5,
                "current_stocks": 0,
                "selling_price": 1000,
                "cost_price": 750,
                "estimated_delivery_days": 3,
                "supplier_name": "Test Supplier"
            }
        ]
    }
    
    risk_scores = {"ORG001": 0.45, "ORG002": 0.14}
    
    cts = ConsolidatedTransferService(
        org_names=org_names,
        stock_data=stock_data,
        distance_map=distance_map,
        cold_node_days=60,
        hot_node_days=14,
        transfer_cost_kes=500.0
    )
    
    print("Testing gap-plug transfers for a stockout item...")
    plan = cts.optimize_network(store_orders, risk_scores=risk_scores)
    
    print("\nDecisions made:")
    for d in plan.decisions:
        print(f"Decision: {d.decision}, Donor: {d.donor_org}, Qty: {d.transfer_qty}")
        print(f"Reasoning: {d.reasoning}")
        
    print("\nTesting proactive transfers for dead stock...")
    stock_data_proactive = {
        "ORG001": [
            {
                "item_code": "ITEM_B",
                "product_name": "Dead Item",
                "current_stocks": 500,
                "avg_daily_sales": 0,
                "last_days_since_last_delivery": 100, # Cold node > 60 days
                "selling_price": 1000
            }
        ],
        "ORG002": [
            {
                "item_code": "ITEM_B",
                "product_name": "Dead Item",
                "current_stocks": 5,
                "avg_daily_sales": 2,
                "last_days_since_last_delivery": 5, # Hot node < 14 days
                "selling_price": 1000
            }
        ]
    }
    
    cts_proactive = ConsolidatedTransferService(
        org_names=org_names,
        stock_data=stock_data_proactive,
        distance_map=distance_map,
        cold_node_days=60,
        hot_node_days=14
    )
    
    plan_proactive = cts_proactive.optimize_network({}, risk_scores=risk_scores)
    print("\nProactive Transfers:")
    for t in plan_proactive.transfers:
        print(f"From: {t.from_org} To: {t.to_org}, Item: {t.itm_cd}, Qty: {t.qty}")

if __name__ == '__main__':
    main()
