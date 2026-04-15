import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
import asyncio

# Ensure app path is in sys.path
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

# Configuration
from pathlib import Path

# Use directory of this script for data
DATA_DIR = Path(__file__).parent.resolve()

def find_latest_scorecard():
    # Search for latest scorecard in base dir
    candidates = list(Path(DATA_DIR).glob("Full_Product_Allocation_Scorecard_v*.csv"))
    if not candidates:
        return os.path.join(DATA_DIR, "Full_Product_Allocation_Scorecard_v3.csv")
    
    # Sort by version number
    def get_version(p):
        try:
            return int(p.stem.split('_v')[-1])
        except:
            return 0
            
    latest = max(candidates, key=get_version)
    return str(latest)

SCORECARD_FILE = find_latest_scorecard()

# --- Helper Logic ---
@st.cache_resource
def get_engine():
    return OrderEngine(DATA_DIR)

@st.cache_data
def load_and_run_allocation(budget, target_month="JAN"):
    if not os.path.exists(SCORECARD_FILE):
        return pd.DataFrame(), 0.0, 0.0, {}, {}
    
    # v2.9: Load Seasonal Data (Hybrid Guide)
    # FIX M1: Cash files live in oasis/data/, not the root DATA_DIR (scratch/).
    from oasis.simulation.data_loader import HistoricalDataLoader
    oasis_data_dir = os.path.join(DATA_DIR, 'oasis', 'data')
    loader = HistoricalDataLoader(oasis_data_dir)
    seasonal_map = loader.load_monthly_demand(target_month)
    
    # Load Data
    df = pd.read_csv(SCORECARD_FILE)
    
    # Convert to Recs
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            # Map Unit_Price to selling_price for engine
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size', None)) else 1),
            'moq_floor': 0,
            'historical_order_count': 0, # Reset for greenfield simulation
            'is_staple_override': str(row.get('Is_Staple', 'False')).upper() == 'TRUE', # Optional if engine checks file
            # v2.9: Pass margin_pct so engine can calculate actual costs
            'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else None,
            'recommended_quantity': 0,
            'reasoning': ''
        }
        recommendations.append(rec)
        
    engine = get_engine()
    
    # Run Logic
    # v10.1: Re-enable enrichment but in Greenfield mode (keeps global intelligence, skips store history)
    engine.enrich_product_data(recommendations, is_greenfield=True)
    
    products_map = {r['product_name']: r for r in recommendations}
    
    result = engine.apply_greenfield_allocation(recommendations, budget, seasonal_demand_map=seasonal_map)
    raw_recs = result['recommendations']
    allocation_summary = result['summary']
    
    # Golden Logic v10.0 Parity
    from oasis.logic.order_logic_guards import apply_safety_guards
    final_recs = apply_safety_guards(raw_recs, products_map, allocation_mode="initial_load")
    
    # Convert back to DataFrame
    results = []
    
    product_data_map: dict[str, dict[str, float | None]] = {}
    for _, row in df.iterrows():
        product_name = row.get('Product')
        if product_name:
            product_data_map[product_name] = {
                'margin_pct': row.get('Margin_Pct') if pd.notnull(row.get('Margin_Pct')) else None
            }

    # v2.9: Budget Correction (Pruning) - Ensure we don't exceed budget after guards
    def get_cost_estimate(r):
        qty = float(r.get('recommended_quantity', 0))
        if qty <= 0: return 0.0
        
        # Consistent with OrderEngine calculation
        price = float(r.get('selling_price', 0))
        cost_price = float(r.get('cost_price', 0.0))
        
        if cost_price <= 0.0:
            # Fallback if enrichment didn't find cost
            cost_price = price * 0.75
            
        return qty * cost_price

    # Pre-calculate costs and sort by priority
    recs_with_cost = []
    for r in final_recs:
        r['estimated_cost'] = get_cost_estimate(r) if not r.get('is_consignment', False) else 0.0
        recs_with_cost.append(r)
    
    # Sort: Staples first, then high ADS
    recs_with_cost.sort(key=lambda x: engine.staple_priority_sort(x))
    
    current_total_cash = sum(r['estimated_cost'] for r in recs_with_cost)
    
    # v10.2: REMOVED Post-Allocation Budget Pruning
    # The OrderEngine already strictly manages the budget internally. 
    # Safety guards (like pack rounding) might push the final cost slightly over budget 
    # (+1-5%) to avoid breaking packs, which is the correct retail behavior.
    # Pruning here was indiscriminately killing items and causing massive under-allocation.
    if current_total_cash > budget:
        st.info(f"Note: Final optimal basket (KES {current_total_cash:,.0f}) slightly exceeds base budget (KES {budget:,.0f}) due to minimum pack-size rounding requirements.")
    
    for r in recs_with_cost:
        qty: float = float(r.get('recommended_quantity', 0))
        if qty > 0:
            price: float = float(r.get('selling_price', 0))
            # Re-check logic flag
            is_consignment = r.get('is_consignment', False)
            
            # v2.8: Use the engine's exact calculation method to mathematically
            # guarantee 100% sync between engine budget and UI presentation.
            cost_price = float(engine._get_actual_cost_price(r, price))
            
            cost = float(qty) * float(cost_price)
            revenue = float(qty) * float(price)
            
            if is_consignment:
                funding_source = "CONSIGNMENT"
            else:
                funding_source = "CASH"

            results.append({
                "Product": r['product_name'],
                "Department": r['product_category'],
                "Qty": qty,
                "Allocated_Cost": cost,
                "Expected_Revenue": revenue,
                "Reasoning": r['reasoning'],
                "Type": funding_source,
                "Avg_Daily_Sales": r.get('avg_daily_sales', 0)
            })
            
    # Calculate totals outside the loop to avoid type checker issues with AugAssign
    total_cash_spend = sum(float(item["Allocated_Cost"]) for item in results if item["Type"] == "CASH")
    total_consignment_val = sum(float(item["Allocated_Cost"]) for item in results if item["Type"] == "CONSIGNMENT")
            
    return pd.DataFrame(results), total_cash_spend, total_consignment_val, allocation_summary, seasonal_map

# --- Streamlit UI ---
# Guard set_page_config for import safety (A2 fix)
try:
    st.set_page_config(page_title="Inventory Allocation Engine", layout="wide")
except Exception:
    pass  # Already called by importing app (e.g. integrated_app.py)

st.title("🛒 Dynamic Inventory Allocation Engine (v2.0 Logic)")
st.markdown("Powered by **OrderEngine 2.0**: Two-Pass Allocation with Efficiency Guards.")

# Sidebar
st.sidebar.header("Configuration")
budget = st.sidebar.slider("Capital Budget (KES)", min_value=50000, max_value=200000000, value=300000, step=10000)

if st.sidebar.button("Run Simulation"):
    with st.spinner("Running Allocation Logic..."):
        basket_df, cash_spend, consignment_val, alloc_summary, _ = load_and_run_allocation(budget)

    if not basket_df.empty:
        # Top Metrics
        est_revenue = basket_df["Expected_Revenue"].sum()
        total_value = cash_spend + consignment_val
        roi = ((est_revenue - total_value) / total_value) * 100 if total_value > 0 else 0
        
        # Calculate Capital Recovery (Days to ROI)
        total_qty = basket_df["Qty"].sum()
        total_sales = basket_df["Avg_Daily_Sales"].sum()
        avg_turnover = (total_qty / total_sales) if total_sales > 0 else 0
        
        c1, c2, c3, c4, c5, c6, c7 = st.columns(7)
        c1.metric("Budget Target", f"KES {budget:,.0f}")
        c2.metric("Cash Used", f"KES {cash_spend:,.0f}", delta=f"{cash_spend-budget:,.0f}")
        c3.metric("Consignment Val", f"KES {consignment_val:,.0f}", delta="Free Capital")
        c4.metric("Est. Revenue", f"KES {est_revenue:,.0f}", delta=f"{roi:.1f}% ROI")
        c5.metric("Days to ROI", f"{avg_turnover:.1f} Days", help="Average time to rotate stock and recover capital")
        c6.metric("Total SKUs", len(basket_df))
        
        # New: Risk Analysis Metric
        risk_buffered_count = basket_df[basket_df['Reasoning'].str.contains("RISK BUFFER", na=False)].shape[0]
        if risk_buffered_count > 0:
             c7.metric("Risk Buffers", f"{risk_buffered_count} Items", delta="Safety Stock", help="Items with Volatile Demand or Unreliable Suppliers received extra stock.")
        else:
             c7.metric("Risk Buffers", "0 Items")

        # v2.6: Display Allocation Summary
        st.info(f"**Utilization**: {alloc_summary['utilization_pct']:.1f}% | **Skipped**: {alloc_summary['total_skipped']} items")


        # Info Box (Dynamic Profile based on Budget)
        # Access the profile manager from the engine instance
        engine = get_engine()
        profile = engine.profile_manager.get_profile(budget)
        
        tier_name = profile['tier_name']
        depth = profile['depth_days']
        cap = profile['price_ceiling']
        
        strategy_desc = f"**{tier_name} Strategy**: Price Cap {cap:,.0f} KES, Depth {depth} Days, Max {profile['max_packs']} Packs."
        
        st.info(f"**Engine Active Profile**: {strategy_desc}")
        
        # Visualizations
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("Department Spend")
            dept_summ = basket_df.groupby("Department")["Allocated_Cost"].sum().reset_index()
            fig_dept = px.pie(dept_summ, values="Allocated_Cost", names="Department", hole=0.3)
            st.plotly_chart(fig_dept, use_container_width=True)
            
        with col_right:
            st.subheader("Pack Count Distribution")
            fig_hist = px.histogram(basket_df, x="Qty", title="Distribution of Pack Quantities")
            st.plotly_chart(fig_hist, use_container_width=True)

        # Detailed Table
        st.subheader("Generated Order Basket")
        st.dataframe(basket_df.sort_values("Allocated_Cost", ascending=False), height=500)
        
        # Download
        csv = basket_df.to_csv(index=False).encode('utf-8')
        st.download_button(
            "Download Order (CSV)",
            csv,
            "Allocated_Basket_v2.csv",
            "text/csv",
            key='download-csv'
        )
        
    else:
        st.warning("No allocation generated. Check data files or budget settings.")
else:
    st.info("Adjust budget and click 'Run Simulation' to start.")
