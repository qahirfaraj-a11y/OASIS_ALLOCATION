import sys
import os
import pandas as pd
import json

sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

SCORECARD_FILE = "Full_Product_Allocation_Scorecard_v3.csv"

def calculate_roi_for_budget(engine, recommendations, budget, product_data_map):
    """Run allocation and calculate ROI metrics."""
    
    # Deep copy recommendations to avoid mutation
    recs_copy = [dict(r) for r in recommendations]
    
    # Run allocation
    result = engine.apply_greenfield_allocation(recs_copy, budget)
    final_recs = result['recommendations']
    summary = result['summary']
    
    # Calculate metrics
    total_revenue = 0
    total_cost = 0
    a_class_revenue = 0
    total_allocated_revenue = 0
    total_velocity = 0
    item_count = 0
    
    for r in final_recs:
        qty = r['recommended_quantity']
        if qty > 0:
            price = r['selling_price']
            
            # Calculate cost using same priority as UI
            cost_price = None
            
            # 1. Try GRN
            if hasattr(engine, 'grn_db'):
                p_name = r['product_name']
                p_barcode = str(r.get('barcode', '')).strip()
                grn_key = p_barcode if p_barcode else engine.normalize_product_name(p_name)
                grn_stat = engine.grn_db.get(grn_key)
                if grn_stat and grn_stat.get('avg_cost'):
                    cost_price = grn_stat['avg_cost']
            
            # 2. Try Margin
            if cost_price is None:
                product_info = product_data_map.get(r['product_name'])
                if product_info:
                    margin_pct = product_info['margin_pct']
                    if margin_pct is not None and margin_pct >= 0 and margin_pct < 100:
                        cost_price = price * (1 - margin_pct / 100.0)
            
            # 3. Fallback
            if cost_price is None or cost_price <= 0:
                cost_price = price * 0.75
            
            cost = qty * cost_price
            revenue = qty * price
            
            total_cost += cost
            total_revenue += revenue
            
            # Track A-class items
            if r.get('ABC_Class') == 'A':
                a_class_revenue += revenue
                total_allocated_revenue += revenue
            else:
                total_allocated_revenue += revenue
            
            # Track velocity
            velocity = r.get('avg_daily_sales', 0)
            total_velocity += velocity
            item_count += 1
    
    # Calculate metrics
    roi_pct = ((total_revenue - total_cost) / total_cost * 100) if total_cost > 0 else 0
    avg_margin_pct = ((total_revenue - total_cost) / total_revenue * 100) if total_revenue > 0 else 0
    a_class_pct = (a_class_revenue / total_allocated_revenue * 100) if total_allocated_revenue > 0 else 0
    avg_velocity = total_velocity / item_count if item_count > 0 else 0
    
    return {
        'budget': budget,
        'total_cost': total_cost,
        'total_revenue': total_revenue,
        'roi_pct': round(roi_pct, 2),
        'avg_margin_pct': round(avg_margin_pct, 2),
        'a_class_pct': round(a_class_pct, 2),
        'avg_velocity': round(avg_velocity, 2),
        'item_count': item_count,
        'utilization_pct': summary.get('utilization_pct', 0)
    }

def main():
    # Load data
    df = pd.read_csv(SCORECARD_FILE)
    
    # Build product data map
    product_data_map = {}
    for _, row in df.iterrows():
        product_name = row.get('Product')
        if product_name:
            product_data_map[product_name] = {
                'margin_pct': row.get('Margin_Pct') if pd.notnull(row.get('Margin_Pct')) else None
            }
    
    # Convert to recommendations
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': 1,
            'moq_floor': 0,
            'historical_order_count': 0,
            'is_staple_override': str(row.get('Is_Staple', 'False')).upper() == 'TRUE',
            'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else None,
            'ABC_Class': row.get('ABC_Class', 'B'),
            'recommended_quantity': 0,
            'reasoning': ''
        }
        recommendations.append(rec)
    
    # Initialize engine
    engine = OrderEngine(os.getcwd())
    
    # Define budget levels
    budgets = []
    
    # 100K increments from 100K to 1M
    for i in range(1, 11):
        budgets.append(100_000 * i)
    
    # 10M increments from 10M to 100M
    for i in range(1, 11):
        budgets.append(10_000_000 * i)
    
    # Run simulations
    results = []
    print("Running ROI Simulations...")
    print("=" * 80)
    
    for budget in budgets:
        print(f"Simulating budget: {budget:,} KES...")
        result = calculate_roi_for_budget(engine, recommendations, budget, product_data_map)
        results.append(result)
    
    # Create DataFrame
    results_df = pd.DataFrame(results)
    
    # Save to CSV
    results_df.to_csv('roi_simulation_results.csv', index=False)
    
    # Print summary
    print("\n" + "=" * 80)
    print("ROI SIMULATION RESULTS")
    print("=" * 80)
    print(results_df.to_string(index=False))
    
    # Print insights
    print("\n" + "=" * 80)
    print("KEY INSIGHTS")
    print("=" * 80)
    
    peak_roi = results_df.loc[results_df['roi_pct'].idxmax()]
    lowest_roi = results_df.loc[results_df['roi_pct'].idxmax()]
    
    print(f"\n🏆 PEAK ROI: {peak_roi['roi_pct']:.2f}% at {peak_roi['budget']:,} KES")
    print(f"   - Avg Margin: {peak_roi['avg_margin_pct']:.1f}%")
    print(f"   - A-Class: {peak_roi['a_class_pct']:.1f}%")
    print(f"   - Velocity: {peak_roi['avg_velocity']:.2f} units/day")
    
    roi_drop = results_df.iloc[0]['roi_pct'] - results_df.iloc[-1]['roi_pct']
    print(f"\n📉 ROI DEGRADATION: {roi_drop:.2f}% points (from 100K to 100M)")
    print(f"   - Diminishing returns kick in around {results_df[results_df['roi_pct'] < 30].iloc[0]['budget']:,} KES")
    
    print("\n✅ Results saved to: roi_simulation_results.csv")

if __name__ == "__main__":
    main()
