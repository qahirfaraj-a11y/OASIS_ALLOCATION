import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os

# Ensure app path is in sys.path
sys.path.append(os.getcwd())

from pathlib import Path

from oasis.logic.order_engine import OrderEngine
from oasis.logic.greenfield_runner import (
    find_latest_scorecard as _find_latest_scorecard,
    load_scorecard_recommendations,
    run_greenfield_allocation,
)

# Use directory of this script for data
DATA_DIR = Path(__file__).parent.resolve()

SCORECARD_FILE = _find_latest_scorecard(str(DATA_DIR)) or os.path.join(
    DATA_DIR, "Full_Product_Allocation_Scorecard_v3.csv"
)

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

    # Shared greenfield pipeline (enrich → allocate → safety guards → costs).
    engine = get_engine()
    recommendations = load_scorecard_recommendations(SCORECARD_FILE)
    result = run_greenfield_allocation(engine, recommendations, budget,
                                       seasonal_demand_map=seasonal_map)

    if result.cash_spend > budget:
        st.info(f"Note: Final optimal basket (KES {result.cash_spend:,.0f}) slightly exceeds base budget (KES {budget:,.0f}) due to minimum pack-size rounding requirements.")

    # Authorized list sorted by staple priority (used by the mop-up button)
    recs_with_cost = sorted(result.recommendations, key=lambda x: engine.staple_priority_sort(x))

    return (result.basket, result.cash_spend, result.consignment_value,
            result.summary, seasonal_map, recs_with_cost)

# --- Streamlit UI ---
# Guard set_page_config for import safety (A2 fix)
try:
    st.set_page_config(page_title="Inventory Allocation Engine", layout="wide")
except Exception:
    pass  # Already called by importing app (e.g. integrated_app.py)

# ── Unified auth gate (U2) ──────────────────────────────────────────────
# Gate only when run as a standalone app. When imported by a host app
# (e.g. integrated_app.py), the host owns the auth gate — re-gating here
# would render a login mid-page.
if __name__ == "__main__":
    from oasis.ui.auth import require_login
    _AUTH_DB = os.getenv(
        "OASIS_DB_PATH",
        os.path.join(str(DATA_DIR), "oasis", "data", "mock_pos_erp.db"),
    )
    require_login(st, _AUTH_DB, app_title="Allocation Engine")

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

    # A1: per-stage breakdown of where SKUs dropped out of the basket
    _by_stage = alloc_summary.get('skipped_by_stage') or {}
    if _by_stage:
        _stage_labels = {
            "pass1": "Pass 1 (width filter)",
            "liquidity_prune": "Pass 1.5 (liquidity recovery)",
            "premium_trim": "Premium cap",
            "anchor_mov": "Pass 3 (anchor MOV)",
            "safety_guards": "Safety guards",
        }
        with st.expander(f"Why {alloc_summary['total_skipped']} SKUs were skipped", expanded=False):
            _stage_df = pd.DataFrame(
                [{"Stage": _stage_labels.get(k, k), "SKUs": v}
                 for k, v in sorted(_by_stage.items(), key=lambda x: -x[1])]
            )
            st.dataframe(_stage_df, use_container_width=True, hide_index=True)
            _reasons = alloc_summary.get('skip_reasons') or {}
            if _reasons:
                st.caption("By reason: " + " · ".join(
                    f"{k}: {v}" for k, v in sorted(_reasons.items(), key=lambda x: -x[1])
                ))


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
