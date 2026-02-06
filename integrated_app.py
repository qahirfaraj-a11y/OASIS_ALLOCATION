import streamlit as st
import pandas as pd
import plotly.express as px
import sys
import os
import asyncio
import numpy as np

# Ensure app path
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine
from oasis.logic.simulation_bridge import SimulationOrderUtil
from retail_simulator import RetailSimulator, SKUState, STORE_UNIVERSES

# --- Helper Logic ---
@st.cache_resource
def get_engine():
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    return OrderEngine(data_path)

@st.cache_resource
def get_bridge():
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    return SimulationOrderUtil(data_path)

def convert_recommendations_to_skustate(recommendations, demand_scale_factor=1.0):
    """
    Bridge Function: Converts Allocation App "Basket" (Dicts) -> Retail Simulator "SKUState" objects.
    Uses OrderEngine intelligence to enrich data (CV, Lead Time, etc).
    """
    sku_states = []
    bridge = get_bridge() 
    
    # Pre-process into list for enrichment
    raw_items = []
    for rec in recommendations:
        if rec['Qty'] > 0:
            raw_items.append({
                'product_name': rec['Product'],
                'department': rec['Department'],
                'avg_daily_sales': rec.get('Avg_Daily_Sales', 0)
            })
            
    # Enrich Data (Bulk Operation)
    # This fetches Lead Time, CV, Freshness from JSON databases
    enriched_items = bridge.engine.enrich_product_data(raw_items)
    
    # Map back to Basket Quantity
    # We need to map by product name since enrichment returns a list
    enriched_map = {item['product_name']: item for item in enriched_items}
    
    for rec in recommendations:
        qty = rec['Qty']
        if qty > 0:
            p_name = rec['Product']
            enriched = enriched_map.get(p_name, {})
            
            price = rec['Expected_Revenue'] / qty if qty > 0 else 0
            cost = rec['Allocated_Cost'] / qty if qty > 0 else 0
            ads = rec.get('Avg_Daily_Sales', 0)
            
            # Use Enriched Data or Fallback
            sku = SKUState(
                product_name=p_name,
                supplier=enriched.get('supplier_name', "Unknown"),
                department=rec['Department'],
                unit_price=price,
                cost_price=cost,
                avg_daily_sales=ads,
                demand_cv=enriched.get('demand_cv', 0.5), # Real CV from engine
                lead_time_days=enriched.get('lead_time_days', 2), # Real Lead Time (Default 2 for consistency)
                current_stock=qty, 
                is_fresh=enriched.get('is_fresh', False), # Real Freshness flag
                reorder_point_override=enriched.get('reorder_point') # Override with OrderEngine logic
            )
            sku_states.append(sku)
            
    return sku_states

# --- Streamlit UI ---
st.set_page_config(page_title="Oasis Integration", layout="wide")

st.title("🏝️ Oasis Retail Lifecycle")

# Session State for Workflow
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'basket' not in st.session_state:
    st.session_state.basket = None
if 'budget' not in st.session_state:
    st.session_state.budget = 300000

# Wizard Navigation
col_nav1, col_nav2 = st.columns(2)
with col_nav1:
    if st.button("Step 1: Allocation", use_container_width=True, type="primary" if st.session_state.step==1 else "secondary"):
        st.session_state.step = 1
with col_nav2:
    if st.button("Step 2: Simulation Lab", use_container_width=True, type="primary" if st.session_state.step==2 else "secondary"):
        st.session_state.step = 2

st.divider()

# --- STEP 1: ALLOCATION ---
if st.session_state.step == 1:
    st.header("Step 1: The Architect (Day 1 Allocation)")
    st.markdown("Generate the optimal opening stock for your budget.")
    
    budget = st.slider("Capital Budget ($)", 50000, 10000000, st.session_state.budget, 10000)
    st.session_state.budget = budget
    
    if st.button("Generate Opening Order"):
        with st.spinner("Running OrderEngine v4.0..."):
            # Re-use logic from allocation_app.py (Simplified here for prototype)
            # We need to actually run the allocation to get the basket
            from allocation_app import load_and_run_allocation
            basket_df, cash, consign, summary = load_and_run_allocation(budget)
            
            st.session_state.basket = basket_df
            st.session_state.summary = summary
            st.success("Allocation Generated!")
            
    if st.session_state.basket is not None:
        df = st.session_state.basket
        st.dataframe(df, height=300)
        st.metric("Total Items", len(df), delta=f"${df['Allocated_Cost'].sum():,.0f}")
        
        if st.button("Proceed to Simulation ->"):
            st.session_state.step = 2
            st.rerun()

# --- STEP 2: SIMULATION ---
elif st.session_state.step == 2:
    st.header("Step 2: The Proving Ground (Simulation)")
    
    if st.session_state.basket is None:
        st.warning("Please generate an Allocation in Step 1 first.")
    else:
        st.markdown(f"Running simulation with **{len(st.session_state.basket)} items** from Day 1.")
        
        c1, c2, c3 = st.columns(3)
        with c1:
            days = st.number_input("Duration (Days)", 30, 90, 30)
        with c2:
            archetype = st.selectbox("Store Archetype", ["Standard", "Student Hub (High Impulse)", "Residential (Weekend Spike)"])
        with c3:
            shock = st.checkbox("Running Black Swan Event?")
            if shock:
                event_type = st.selectbox("Event", ["Supplier Failure", "Demand Surge"])
        
        if st.button("Run Simulation"):
            with st.spinner("Simulating Reality..."):
                # Convert Basket to SKUState
                # Note: This checks `allocation_app` output format
                initial_skus = convert_recommendations_to_skustate(st.session_state.basket.to_dict('records'))
                
                # Dynamic Config Selection based on Budget
                budget_val = st.session_state.budget
                
                if budget_val < 150_000:
                    tier_key = "Micro_100k"
                elif budget_val < 500_000:
                    tier_key = "Small_200k"
                elif budget_val < 5_000_000:
                    tier_key = "Medium_1M"
                elif budget_val < 50_000_000:
                    tier_key = "Large_10M"
                else:
                    tier_key = "Mega_100M"
                
                config = STORE_UNIVERSES[tier_key].copy() # Copy to avoid mutating global
                config["budget"] = budget_val 
                
                st.info(f"Using Simulation Profile: **{tier_key}** (Reorder Every {config['reorder_frequency_days']} Days)")
                
                # Run Sim
                sim = RetailSimulator("Custom Scenario", config, seed=42, bridge=get_bridge(), initial_skus=initial_skus)
                
                # TODO: Inject Archetype/Shock logic here (Phase 2)
                
                result = sim.run(days)
                
                st.subheader("Simulation Results")
                m1, m2, m3, m4, m5 = st.columns(5)
                m1.metric("Fill Rate", f"{result.avg_fill_rate:.1f}%")
                m2.metric("Stockout Rate", f"{result.stockout_rate:.2f}%")
                m3.metric("Revenue", f"${result.total_revenue:,.0f}")
                m4.metric("Turns", f"{result.inventory_turnover:.1f}x")
                m5.metric("ROI", f"{result.roi:.1f}%")
                
                st.caption(f"Scenario: {archetype} | Events: {event_type if shock else 'None'}")
