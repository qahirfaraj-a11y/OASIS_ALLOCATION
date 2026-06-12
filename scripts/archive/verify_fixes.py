import sys
import logging
from pprint import pprint
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
from ops_dashboard import load_all_stocks, get_distance_map, get_all_store_risks, get_order_engine
from oasis.logic.simulation_bridge import SimulationOrderUtil

logging.basicConfig(level=logging.INFO)

def main():
    print("Loading data...")
    all_stocks = load_all_stocks()
    org_name_map = {k: k for k in all_stocks.keys()}
    distance_map = get_distance_map()
    
    # Calculate risks with new active assortment logic
    risk_scores_map = get_all_store_risks(9)
    print("\nSample Store Risk Scores (Dynamic Baseline):")
    for k in list(risk_scores_map.keys())[:5]:
        print(f"  {k}: {risk_scores_map[k]:.3f}")
        
    engine = get_order_engine()
    
    print("\nRunning order engine for ORG001...")
    
    cts = ConsolidatedTransferService(
        org_names=org_name_map,
        stock_data=all_stocks,
        distance_map=distance_map,
        cold_node_days=60,
        hot_node_days=14
    )
    
    # Let's just generate raw recommendations for ORG001
    stocks = all_stocks.get("ORG001", [])
    sim_util = SimulationOrderUtil("oasis_database.sqlite", thresholds=None, engine=engine) # thresholds don't matter much here
    enriched = sim_util.prepare_sku_data(stocks)
    raw_recs = sim_util.calculate_order_quantity(enriched, gnn_risk_score=risk_scores_map.get("ORG001", 0.0), use_real_date=True)
    final_recs = sim_util.finalize_orders(raw_recs)
    
    all_store_orders = {"ORG001": final_recs}
        
    print(f"Total PO Recommendations for ORG001: {len(final_recs)}")
    print("Running Network Optimization...")
    network_plan = cts.optimize_network(all_store_orders, risk_scores=risk_scores_map)
    
    print(f"Total transfers registered: {len(network_plan.transfers)}")
    print(f"Total decisions made: {len(network_plan.decisions)}")
    
    print("\nSample Transfers:")
    for t in network_plan.transfers[:10]:
        print(f"From {t.from_org} to {t.to_org}: {t.qty}x {t.itm_cd}")
        
    print("\nSample Donor Additions:")
    for d in list(network_plan.donor_additions.get("ORG001", []))[:5]:
        print(f"Donor Push: {d}")

if __name__ == '__main__':
    main()
