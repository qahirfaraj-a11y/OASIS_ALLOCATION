import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

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
    return OrderEngine(str(DATA_DIR))

@st.cache_data
def load_and_run_allocation(budget, target_month="JAN", _scorecard_mtime=0.0):
    """BUG 5 FIX: _scorecard_mtime busts the cache when the CSV is modified."""
    if not os.path.exists(SCORECARD_FILE):
        # Must match the 6-tuple shape of the normal return path below
        return pd.DataFrame(), 0.0, 0.0, {}, {}, []
    
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
            'supplier_name': row.get('Supplier'),
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
    
    # v10.9: PERFECT SYNC - Cost Consolidation
    def get_stratified_cost_price(r, engine):
        if r.get('cost_price') is not None:
            return float(r['cost_price'])
        return float(engine._get_actual_cost_price(r, float(r.get('selling_price', 0))))

    # 1. Enrich all records with stratified cost prices
    for r in final_recs:
        r['cost_price'] = get_stratified_cost_price(r, engine)
        r['estimated_cost'] = round(float(r.get('recommended_quantity', 0)) * r['cost_price'], 2) if not r.get('is_consignment', False) else 0.0

    # 2. Sort authorized list
    recs_with_cost = sorted(final_recs, key=lambda x: engine.staple_priority_sort(x))
    
    current_total_cash = sum(r['estimated_cost'] for r in recs_with_cost)
    
    if current_total_cash > budget:
        st.info(f"Note: Final optimal basket (KES {current_total_cash:,.0f}) slightly exceeds base budget (KES {budget:,.0f}) due to minimum pack-size rounding requirements.")
    
    # 3. Build Results and Final Summary
    results = []
    total_cash_spend = 0.0
    total_consignment_val = 0.0

    for r in recs_with_cost:
        qty = float(r.get('recommended_quantity', 0))
        if qty <= 0: continue
        
        cost = round(qty * r['cost_price'], 2)
        price = float(r.get('selling_price', 0))
        is_cons = r.get('is_consignment', False)
        
        if is_cons:
            total_consignment_val = round(total_consignment_val + cost, 2)
        else:
            total_cash_spend = round(total_cash_spend + cost, 2)

        results.append({
            "Product": r['product_name'],
            "Department": r['product_category'],
            "Qty": qty,
            "Allocated_Cost": cost,
            "Expected_Revenue": qty * price,
            "Reasoning": r['reasoning'],
            "Type": "CONSIGNMENT" if is_cons else "CASH",
            "Avg_Daily_Sales": r.get('avg_daily_sales', 0)
        })
            
    # Update allocation_summary for metadata parity
    allocation_summary['total_cash_used'] = total_cash_spend
    allocation_summary['total_consignment_value'] = total_consignment_val
    allocation_summary['utilization_pct'] = round((total_cash_spend / budget) * 100, 2) if budget > 0 else 0
    
    return pd.DataFrame(results), total_cash_spend, total_consignment_val, allocation_summary, seasonal_map, recs_with_cost

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
target_month = st.sidebar.selectbox("Seasonal Base Month", ["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"], index=0)

if st.sidebar.button("Run Greenfield Allocation", type="primary"):
    with st.spinner(f"Simulating Greenfield Order for KES {budget:,.0f} ({target_month})..."):
        # BUG 5 FIX: Pass file mtime to bust cache when scorecard is updated
        sc_mtime = os.path.getmtime(SCORECARD_FILE) if os.path.exists(SCORECARD_FILE) else 0.0
        basket_df, cash_spend, consignment_val, alloc_summary, _, recs_with_cost = load_and_run_allocation(budget, target_month=target_month, _scorecard_mtime=sc_mtime)
        
        import copy
        st.session_state['basket_df'] = basket_df
        st.session_state['cash_spend'] = cash_spend
        st.session_state['consignment_val'] = consignment_val
        st.session_state['alloc_summary'] = alloc_summary
        st.session_state['recs_with_cost'] = copy.deepcopy(recs_with_cost)
        st.session_state['has_allocation'] = True

if st.session_state.get('has_allocation') and not st.session_state['basket_df'].empty:
    basket_df = st.session_state['basket_df']
    cash_spend = st.session_state['cash_spend']
    consignment_val = st.session_state['consignment_val']
    alloc_summary = st.session_state['alloc_summary']
    # Top Metrics
    est_revenue = basket_df["Expected_Revenue"].sum()
    total_value = cash_spend + consignment_val
    # BUG 10 FIX: Use cash_spend (actual capital deployed) instead of total_value
    # (which includes free consignment) to avoid deflating the ROI metric.
    roi = ((est_revenue - cash_spend) / cash_spend) * 100 if cash_spend > 0 else 0
    
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
        st.plotly_chart(fig_dept, width="stretch") # Updated from None to 'stretch' for Streamlit compatibility
        
    with col_right:
        st.subheader("Pack Count Distribution")
        fig_hist = px.histogram(basket_df, x="Qty", title="Distribution of Pack Quantities")
        st.plotly_chart(fig_hist, width="stretch") # Updated from None to 'stretch' for Streamlit compatibility

    # Mop-Up Button Logic
    st.markdown("---")
    if st.button("🪄 Run Consolidation Mop-Up", type="secondary"):
        with st.spinner("Running K-Core Pruning and Knapsack Mop-Up..."):
            engine = get_engine()
            current_recs = st.session_state['recs_with_cost']
            updated_recs = engine.run_mop_up_engine(current_recs, budget)
            
            # Rebuild Results
            results = []
            new_cash_spend = 0.0
            new_cons_val = 0.0
            
            for r in updated_recs:
                qty = float(r.get('recommended_quantity', 0))
                if qty <= 0: continue
                cost = round(qty * r['cost_price'], 2)
                price = float(r.get('selling_price', 0))
                is_cons = r.get('is_consignment', False)
                if is_cons:
                    new_cons_val += cost
                else:
                    new_cash_spend += cost
                    
                results.append({
                    "Product": r['product_name'],
                    "Department": r['product_category'],
                    "Qty": qty,
                    "Allocated_Cost": cost,
                    "Expected_Revenue": qty * price,
                    "Reasoning": r.get('reasoning', ''),
                    "Mop_Up_Action": r.get('mop_up_action', ''),
                    "Type": "CONSIGNMENT" if is_cons else "CASH",
                    "Avg_Daily_Sales": r.get('avg_daily_sales', 0)
                })
            
            st.session_state['basket_df'] = pd.DataFrame(results)
            st.session_state['cash_spend'] = new_cash_spend
            st.session_state['consignment_val'] = new_cons_val
            st.session_state['alloc_summary']['total_cash_used'] = new_cash_spend
            st.session_state['alloc_summary']['utilization_pct'] = round((new_cash_spend / budget) * 100, 2)
            st.rerun()

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
    
elif st.session_state.get('has_allocation'):
    st.warning("No allocation generated. Check data files or budget settings.")
else:
    st.info("Adjust budget and click 'Run Simulation' to start.")
