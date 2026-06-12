
import pandas as pd
from oasis.logic.order_engine import apply_safety_guards
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
from oasis.logic.transfer_state import TransferStateTracker

def test_bulk_ordering():
    # Dummy product data
    product = {
        'product_name': 'FB 400G BREAD PACKAGING BAGS',
        'department': 'BAKERY FOODPLUS',
        'avg_daily_sales': 1.0,
        'current_stocks': 5.0,
        'pack_size': 1,
        'is_key_sku': False
    }
    products_map = {product['product_name']: product}
    recommendations = [{'itm_cd': '123', 'product_name': product['product_name'], 'recommended_quantity': 2.0, 'reasoning': 'Initial'}]
    
    # Run safety guards (where bulk logic lives)
    final_recs = apply_safety_guards(recommendations, products_map)
    
    rec = final_recs[0]
    print(f"Product: {product['product_name']}")
    print(f"Recommended Quantity: {rec['recommended_quantity']}")
    print(f"Reasoning: {rec['reasoning']}")
    
    # Should be at least 60 (60 * ADS)
    assert rec['recommended_quantity'] >= 60.0

def test_proactive_transfers():
    # Setup tracker
    tracker = TransferStateTracker()
    
    # Setup stores and stock
    # Store A: High stock, zero velocity, aged (Dead Stock)
    # Store B: Low stock, high velocity (Demand)
    stock_data = [
        {
            'org_cd': 'STORE_A', 'org_name': 'Store A', 'itm_cd': 'ITEM1', 
            'product_name': 'Widget', 'current_stocks': 50, 'avg_daily_sales': 0.0,
            'last_days_since_last_delivery': 100, 'department': 'GENERAL'
        },
        {
            'org_cd': 'STORE_B', 'org_name': 'Store B', 'itm_cd': 'ITEM1', 
            'product_name': 'Widget', 'current_stocks': 2, 'avg_daily_sales': 5.0,
            'last_days_since_last_delivery': 1, 'department': 'GENERAL'
        }
    ]
    
    org_names = {'STORE_A': 'Store A', 'STORE_B': 'Store B'}
    stock_dict = {'STORE_A': stock_data[:1], 'STORE_B': stock_data[1:]}
    
    service = ConsolidatedTransferService(org_names, stock_dict)
    risk_scores = {'STORE_B': 0.9} # High risk at Store B
    
    plan = service.optimize_network(store_orders={}, risk_scores=risk_scores)
    
    print(f"\nProactive Transfers Identified: {len(plan.transfers)}")
    for t in plan.transfers:
        print(f"Move {t.qty} of {t.product_name} from {t.from_org} to {t.to_org} (Urgency: {t.urgency})")
    
    assert len(plan.transfers) > 0
    assert plan.transfers[0].from_org == 'STORE_A'
    assert plan.transfers[0].to_org == 'STORE_B'

if __name__ == "__main__":
    try:
        print("Testing Bulk Ordering...")
        test_bulk_ordering()
        print("\nTesting Proactive Transfers...")
        test_proactive_transfers()
        print("\nAll tests passed!")
    except Exception as e:
        print(f"\nTests failed: {e}")
        import traceback
        traceback.print_exc()
