import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
import asyncio

# Ensure app path is in sys.path
sys.path.append(os.getcwd())

from app.logic.order_engine import OrderEngine

# Configuration
# Use current directory for data, compatible with both local and cloud
DATA_DIR = os.getcwd() # Was: r"c:\Users\iLink\.gemini\antigravity\scratch"
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"

# --- Helper Logic ---
@st.cache_resource
def get_engine():
    return OrderEngine(DATA_DIR)

@st.cache_data
def load_and_run_allocation(budget):
    if not os.path.exists(SCORECARD_FILE):
        return None
    
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
            'pack_size': 1,
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
    
    # Run Logic (now returns dict with 'recommendations' and 'summary')
    result = engine.apply_greenfield_allocation(recommendations, budget)
    final_recs = result['recommendations']
    allocation_summary = result['summary']
    
    # Convert back to DataFrame
    results = []
    total_cash_spend = 0.0
    total_consignment_val = 0.0
    
    # v2.8: Performance Optimization - Create lookup dictionary once
    # Instead of nested loop (O(n²)), use dictionary lookup (O(n))
    product_data_map = {}
    for _, row in df.iterrows():
        product_name = row.get('Product')
        if product_name:
            product_data_map[product_name] = {
                'margin_pct': row.get('Margin_Pct') if pd.notnull(row.get('Margin_Pct')) else None
            }
    
    for r in final_recs:
        qty = r['recommended_quantity']
        if qty > 0:
            price = r['selling_price']
            # Re-check logic flag
            is_consignment = r.get('is_consignment', False)
            
            # v2.8: Optimized cost calculation with O(1) dictionary lookup
            # Priority: GRN cost → Margin calculation → 0.75 estimate
            cost_price = None
            
            # 1. Try GRN database (most accurate - actual purchase prices)
            if cost_price is None and hasattr(engine, 'grn_db'):
                p_name = r['product_name']
                p_barcode = str(r.get('barcode', '')).strip()
                grn_key = p_barcode if p_barcode else engine.normalize_product_name(p_name)
                grn_stat = engine.grn_db.get(grn_key)
                if grn_stat and grn_stat.get('avg_cost'):
                    cost_price = grn_stat['avg_cost']
            
            # 2. Try Margin% from pre-built dictionary (O(1) lookup)
            if cost_price is None:
                product_info = product_data_map.get(r['product_name'])
                if product_info:
                    margin_pct = product_info['margin_pct']
                    if margin_pct is not None and margin_pct >= 0 and margin_pct < 100:
                        cost_price = price * (1 - margin_pct / 100.0)
            
            # 3. Fallback to 25% margin estimate
            if cost_price is None or cost_price <= 0:
                cost_price = price * 0.75
            
            cost = qty * cost_price
            revenue = qty * price 
            
            if is_consignment:
                total_consignment_val += cost
                funding_source = "CONSIGNMENT"
            else:
                total_cash_spend += cost
                funding_source = "CASH"

            results.append({
                "Product": r['product_name'],
                "Department": r['product_category'],
                "Qty": qty,
                "Allocated_Cost": cost,
                "Expected_Revenue": revenue,
                "Reasoning": r['reasoning'],
                "Type": funding_source
            })
            
    return pd.DataFrame(results), total_cash_spend, total_consignment_val, allocation_summary

# --- Streamlit UI ---
st.set_page_config(page_title="Inventory Allocation Engine", layout="wide")

st.title("🛒 Dynamic Inventory Allocation Engine (v2.0 Logic)")
st.markdown("Powered by **OrderEngine 2.0**: Two-Pass Allocation with Efficiency Guards.")

# Sidebar
st.sidebar.header("Configuration")
budget = st.sidebar.slider("Capital Budget ($)", min_value=50000, max_value=150000000, value=300000, step=10000)

if st.sidebar.button("Run Simulation"):
    with st.spinner("Running Allocation Logic..."):
        basket_df, cash_spend, consignment_val, alloc_summary = load_and_run_allocation(budget)

    if basket_df is not None and not basket_df.empty:
        # Top Metrics
        est_revenue = basket_df["Expected_Revenue"].sum()
        total_value = cash_spend + consignment_val
        roi = ((est_revenue - total_value) / total_value) * 100 if total_value > 0 else 0
        
        c1, c2, c3, c4, c5, c6 = st.columns(6)
        c1.metric("Budget Target", f"${budget:,.0f}")
        c2.metric("Cash Used", f"${cash_spend:,.0f}", delta=f"{cash_spend-budget:,.0f}")
        c3.metric("Consignment Val", f"${consignment_val:,.0f}", delta="Free Capital")
        c4.metric("Est. Revenue", f"${est_revenue:,.0f}", delta=f"{roi:.1f}% ROI")
        c5.metric("Total SKUs", len(basket_df))
        
        # New: Risk Analysis Metric
        risk_buffered_count = basket_df[basket_df['Reasoning'].str.contains("RISK BUFFER", na=False)].shape[0]
        if risk_buffered_count > 0:
             c6.metric("Risk Buffers Active", f"{risk_buffered_count} Items", delta="Safety Stock Added", help="Items with Volatile Demand or Unreliable Suppliers received extra stock.")

        # v2.6: Display Allocation Summary
        st.info(f"**Utilization**: {alloc_summary['utilization_pct']:.1f}% | **Skipped**: {alloc_summary['total_skipped']} items")


        # Info Box (Dynamic Profile based on Budget)
        # Access the profile manager from the engine instance
        engine = get_engine()
        profile = engine.profile_manager.get_profile(budget)
        
        tier_name = profile['tier_name']
        depth = profile['depth_days']
        cap = profile['price_ceiling']
        
        strategy_desc = f"**{tier_name} Strategy**: Price Cap {cap:,.0f}/=, Depth {depth} Days, Max {profile['max_packs']} Packs."
        
        st.info(f"**Engine Active Profile**: {strategy_desc}")
        
        # Visualizations
        col_left, col_right = st.columns(2)
        
        with col_left:
            st.subheader("Department Spend")
            dept_summ = basket_df.groupby("Department")["Allocated_Cost"].sum().reset_index()
            fig_dept = px.pie(dept_summ, values="Allocated_Cost", names="Department", hole=0.3)
            st.plotly_chart(fig_dept, width='stretch')
            
        with col_right:
            st.subheader("Pack Count Distribution")
            fig_hist = px.histogram(basket_df, x="Qty", title="Distribution of Pack Quantities")
            st.plotly_chart(fig_hist, width='stretch')

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
