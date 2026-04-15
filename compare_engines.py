
import os
import json
import pandas as pd
import asyncio
from pathlib import Path
from oasis.logic.order_engine import OrderEngine
from oasis.logic.order_logic_guards import apply_safety_guards
from oasis.simulation.data_loader import HistoricalDataLoader

DATA_DIR = Path("C:/Users/iLink/.gemini/antigravity/scratch").resolve()
CONFIG_PATH = DATA_DIR / "oasis/data/oasis_engines_config.json"
BACKUP_PATH = DATA_DIR / "oasis/data/oasis_engines_config.json.bak"

BUDGETS = [500_000, 5_000_000, 10_000_000, 57_000_000]

def find_latest_scorecard():
    candidates = list(DATA_DIR.glob("Full_Product_Allocation_Scorecard_v*.csv"))
    if not candidates:
        return str(DATA_DIR / "Full_Product_Allocation_Scorecard_v3.csv")
    def get_version(p):
        try:
            return int(p.stem.split('_v')[-1])
        except:
            return 0
    latest = max(candidates, key=get_version)
    return str(latest)

SCORECARD_FILE = find_latest_scorecard()

def toggle_engines(enabled: bool):
    with open(CONFIG_PATH, 'r') as f:
        config = json.load(f)
    
    for name in ["amit", "lata", "dharam", "mande"]:
        if name in config["engines"]:
            if name == "mande":
                config["engines"][name]["enabled"] = False
            else:
                config["engines"][name]["enabled"] = enabled
    
    with open(CONFIG_PATH, 'w') as f:
        json.dump(config, f, indent=4)

async def run_allocation(budget, engines_enabled):
    toggle_engines(engines_enabled)
    
    # Reload engine to pick up config changes
    engine = OrderEngine(str(DATA_DIR))
    await engine.load_databases_async()
    
    loader = HistoricalDataLoader(str(DATA_DIR))
    seasonal_map = loader.load_monthly_demand("JAN")
    
    df = pd.read_csv(SCORECARD_FILE)
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size', None)) else 1),
            'moq_floor': 0,
            'historical_order_count': 0,
            'is_staple_override': str(row.get('Is_Staple', 'False')).upper() == 'TRUE',
            'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else None,
            'recommended_quantity': 0,
            'reasoning': ''
        }
        recommendations.append(rec)
    
    engine.enrich_product_data(recommendations, is_greenfield=True)
    products_map = {r['product_name']: r for r in recommendations}
    
    result = engine.apply_greenfield_allocation(recommendations, budget, seasonal_demand_map=seasonal_map)
    raw_recs = result['recommendations']
    alloc_summary = result['summary']
    
    final_recs = apply_safety_guards(raw_recs, products_map, allocation_mode="initial_load")
    
    # Calculate Results
    results = []
    for r in final_recs:
        qty = float(r.get('recommended_quantity', 0))
        if qty > 0:
            price = float(r.get('selling_price', 0))
            cost_price = float(engine._get_actual_cost_price(r, price))
            is_consignment = r.get('is_consignment', False)
            
            results.append({
                "Product": r['product_name'],
                "Cost": qty * cost_price,
                "Revenue": qty * price,
                "Type": "CONSIGNMENT" if is_consignment else "CASH",
                "ADS": r.get('avg_daily_sales', 0)
            })
    
    res_df = pd.DataFrame(results)
    if res_df.empty:
        return {
            "cash_spend": 0, "consignment_val": 0, "est_revenue": 0, 
            "sku_count": 0, "roi": 0, "days_to_roi": 0
        }
    
    cash_spend = res_df[res_df["Type"] == "CASH"]["Cost"].sum()
    consignment_val = res_df[res_df["Type"] == "CONSIGNMENT"]["Cost"].sum()
    est_revenue = res_df["Revenue"].sum()
    total_val = cash_spend + consignment_val
    roi = ((est_revenue - total_val) / total_val * 100) if total_val > 0 else 0
    
    total_qty = sum(r['recommended_quantity'] for r in final_recs)
    total_ads = res_df["ADS"].sum()
    days_to_roi = (total_qty / total_ads) if total_ads > 0 else 0
    
    return {
        "cash_spend": cash_spend,
        "consignment_val": consignment_val,
        "est_revenue": est_revenue,
        "sku_count": len(res_df),
        "roi": roi,
        "days_to_roi": days_to_roi,
        "summary": alloc_summary
    }

async def main():
    # 1. Backup Config
    if os.path.exists(CONFIG_PATH):
        import shutil
        shutil.copy2(CONFIG_PATH, BACKUP_PATH)
        print(f"Backed up config to {BACKUP_PATH}")

    all_results = []

    for budget in BUDGETS:
        print(f"Processing Budget: {budget:,}...")
        
        # OFF
        print("Running Baseline (OFF)...")
        res_off = await run_allocation(budget, False)
        res_off['budget'] = budget
        res_off['mode'] = "Baseline"
        
        # ON
        print("Running OASIS Intelligence (ON)...")
        res_on = await run_allocation(budget, True)
        res_on['budget'] = budget
        res_on['mode'] = "OASIS Intelligence"
        
        all_results.append(res_off)
        all_results.append(res_on)

    # 2. Results output
    report_df = pd.DataFrame(all_results)
    report_df.to_csv("engine_comparison_results.csv", index=False)
    print("\nResults saved to engine_comparison_results.csv")
    
    # 3. Restore Config
    if os.path.exists(BACKUP_PATH):
        import shutil
        shutil.copy2(BACKUP_PATH, CONFIG_PATH)
        print(f"Restored config from {BACKUP_PATH}")

if __name__ == "__main__":
    asyncio.run(main())
