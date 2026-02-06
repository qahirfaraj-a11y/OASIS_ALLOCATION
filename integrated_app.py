import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
import numpy as np
from pathlib import Path

# Ensure app path
sys.path.insert(0, str(Path(__file__).parent))

from oasis.logic.order_engine import OrderEngine
from oasis.logic.simulation_bridge import SimulationOrderUtil
from retail_simulator import RetailSimulator, SKUState, STORE_UNIVERSES

# Try to import supplier analytics
try:
    from oasis.analytics.supplier_analytics import (
        get_top_suppliers_by_department,
        get_major_categories,
        analyze_supplier_failure_impact,
        load_scorecard_data
    )
    SUPPLIER_ANALYTICS_AVAILABLE = True
except ImportError:
    SUPPLIER_ANALYTICS_AVAILABLE = False


# --- Helper Logic ---
@st.cache_resource
def get_engine():
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    return OrderEngine(data_path)

@st.cache_resource
def get_bridge():
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    return SimulationOrderUtil(data_path)

@st.cache_data
def get_scorecard_data():
    """Load scorecard data for supplier analytics."""
    if SUPPLIER_ANALYTICS_AVAILABLE:
        try:
            return load_scorecard_data()
        except:
            return None
    return None


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
    enriched_items = bridge.engine.enrich_product_data(raw_items)
    enriched_map = {item['product_name']: item for item in enriched_items}
    
    for rec in recommendations:
        qty = rec['Qty']
        if qty > 0:
            p_name = rec['Product']
            enriched = enriched_map.get(p_name, {})
            
            price = rec['Expected_Revenue'] / qty if qty > 0 else 0
            cost = rec['Allocated_Cost'] / qty if qty > 0 else 0
            ads = rec.get('Avg_Daily_Sales', 0)
            
            sku = SKUState(
                product_name=p_name,
                supplier=enriched.get('supplier_name', "Unknown"),
                department=rec['Department'],
                unit_price=price,
                cost_price=cost,
                avg_daily_sales=ads,
                demand_cv=enriched.get('demand_cv', 0.5),
                lead_time_days=enriched.get('lead_time_days', 2),
                current_stock=qty, 
                is_fresh=enriched.get('is_fresh', False),
                reorder_point_override=enriched.get('reorder_point')
            )
            sku_states.append(sku)
            
    return sku_states


# --- Streamlit UI ---
st.set_page_config(page_title="Oasis Retail Lifecycle", layout="wide", page_icon="🏝️")

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .stMetric {
        background-color: #f0f2f6;
        padding: 10px;
        border-radius: 8px;
        border-left: 4px solid #667eea;
    }
</style>
""", unsafe_allow_html=True)

st.markdown('<p class="main-header">🏝️ Oasis Retail Lifecycle</p>', unsafe_allow_html=True)

# Session State for Workflow
if 'step' not in st.session_state:
    st.session_state.step = 1
if 'basket' not in st.session_state:
    st.session_state.basket = None
if 'budget' not in st.session_state:
    st.session_state.budget = 300000
if 'sim_result' not in st.session_state:
    st.session_state.sim_result = None

# Wizard Navigation with Tabs
tab1, tab2, tab3 = st.tabs(["📦 Step 1: Allocation", "🧪 Step 2: Simulation Lab", "🔍 Supplier Analysis"])

# --- TAB 1: ALLOCATION ---
with tab1:
    st.header("The Architect (Day 1 Allocation)")
    st.markdown("Generate the optimal opening stock for your budget.")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        budget = st.slider("Capital Budget (KES)", 50000, 10000000, st.session_state.budget, 10000,
                          format="KES %d")
        st.session_state.budget = budget
        
        # Store type selection
        is_online = st.checkbox("🌐 Online Store Mode", help="Enable special dynamics for e-commerce (fresh/artisanal boost)")
    
    with col2:
        st.info(f"**Budget:** KES {budget:,}")
        if is_online:
            st.success("Online Mode Active\n- Fresh category boost\n- Artisanal boost\n- Higher supplier risk")
    
    if st.button("🚀 Generate Opening Order", type="primary"):
        with st.spinner("Running OrderEngine v4.0..."):
            try:
                from allocation_app import load_and_run_allocation
                basket_df, cash, consign, summary = load_and_run_allocation(budget)
                
                st.session_state.basket = basket_df
                st.session_state.summary = summary
                st.session_state.is_online = is_online
                st.success(f"Allocation Generated! {len(basket_df)} SKUs")
            except Exception as e:
                st.error(f"Allocation failed: {e}")
            
    if st.session_state.basket is not None:
        df = st.session_state.basket
        
        col_m1, col_m2, col_m3 = st.columns(3)
        col_m1.metric("Total SKUs", len(df))
        col_m2.metric("Total Cost", f"KES {df['Allocated_Cost'].sum():,.0f}")
        col_m3.metric("Expected Revenue", f"KES {df['Expected_Revenue'].sum():,.0f}")
        
        st.dataframe(df, height=300, use_container_width=True)


# --- TAB 2: SIMULATION ---
with tab2:
    st.header("The Proving Ground (Simulation)")
    
    if st.session_state.basket is None:
        st.warning("⚠️ Please generate an Allocation in Step 1 first.")
    else:
        st.markdown(f"Running simulation with **{len(st.session_state.basket)} items** from Day 1.")
        
        # Simulation Controls
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("⚙️ Simulation Settings")
            
            days = st.slider("Duration (Days)", 7, 90, 30)
            
            # Enhanced Archetype Selection with Online
            archetype_options = [
                "Standard",
                "Student Hub (High Impulse)", 
                "Residential (Weekend Spike)",
                "🌐 Online Store (Fresh/Artisanal Boost)"
            ]
            archetype = st.selectbox("Store Archetype", archetype_options)
            
            # Dynamic tier selection based on budget
            budget_val = st.session_state.budget
            if archetype == "🌐 Online Store (Fresh/Artisanal Boost)":
                tier_key = "Online_5M"
                st.info("🌐 **Online Mode**: Fresh +35%, Artisanal +25%, Higher Supplier Risk")
            elif budget_val < 150_000:
                tier_key = "Micro_100k"
            elif budget_val < 500_000:
                tier_key = "Small_200k"
            elif budget_val < 5_000_000:
                tier_key = "Medium_1M"
            elif budget_val < 50_000_000:
                tier_key = "Large_10M"
            else:
                tier_key = "Mega_100M"
        
        with col2:
            st.subheader("🦢 Black Swan Events")
            
            shock = st.checkbox("Enable Black Swan Event")
            
            selected_supplier = None
            selected_category = None
            failure_day = 10
            failure_duration = 14
            
            if shock:
                event_type = st.selectbox("Event Type", ["Supplier Failure", "Demand Surge", "Competitor Entry"])
                
                if event_type == "Supplier Failure" and SUPPLIER_ANALYTICS_AVAILABLE:
                    st.markdown("---")
                    st.markdown("**🎯 Target Specific Supplier:**")
                    
                    try:
                        categories = get_major_categories()
                        selected_category = st.selectbox("Product Category", categories)
                        
                        # Get top 10 suppliers for this category
                        df_scorecard = get_scorecard_data()
                        if df_scorecard is not None:
                            suppliers = get_top_suppliers_by_department(df_scorecard, selected_category, 10)
                            
                            if suppliers:
                                supplier_options = [f"{s.supplier_name} ({s.share_pct:.1f}%)" for s in suppliers]
                                selected_idx = st.selectbox("Top 10 Suppliers", range(len(supplier_options)),
                                                           format_func=lambda x: supplier_options[x])
                                selected_supplier = suppliers[selected_idx].supplier_name
                                
                                # Show impact preview
                                impact = analyze_supplier_failure_impact(df_scorecard, selected_supplier, selected_category)
                                
                                st.warning(f"**Impact Preview:**\n"
                                          f"- Revenue at Risk: KES {impact['revenue_at_risk']:,.0f}\n"
                                          f"- Coverage Loss: {impact['coverage_loss_pct']:.1f}%\n"
                                          f"- Severity: {impact['estimated_stockout_severity']}")
                    except Exception as e:
                        st.error(f"Could not load supplier data: {e}")
                    
                    failure_day = st.slider("Failure Start Day", 1, days-5, 10)
                    failure_duration = st.slider("Failure Duration (Days)", 1, 30, 14)
                elif event_type == "Supplier Failure":
                    st.info("Supplier analytics not available. Using random supplier failure.")
        
        # Run Simulation Button
        if st.button("🚀 Run Simulation", type="primary", use_container_width=True):
            with st.spinner("Simulating Reality..."):
                try:
                    # Convert Basket to SKUState
                    initial_skus = convert_recommendations_to_skustate(st.session_state.basket.to_dict('records'))
                    
                    config = STORE_UNIVERSES[tier_key].copy()
                    config["budget"] = budget_val 
                    
                    st.info(f"Using Profile: **{tier_key}** | Reorder Every {config['reorder_frequency_days']} Days")
                    
                    # Run Sim
                    sim = RetailSimulator("Custom Scenario", config, seed=42, bridge=get_bridge(), initial_skus=initial_skus)
                    result = sim.run(days)
                    
                    st.session_state.sim_result = result
                    st.session_state.sim_config = {
                        'tier': tier_key,
                        'days': days,
                        'archetype': archetype,
                        'shock': shock,
                        'supplier': selected_supplier if shock else None
                    }
                    
                except Exception as e:
                    st.error(f"Simulation failed: {e}")
                    import traceback
                    st.code(traceback.format_exc())
        
        # Display Results
        if st.session_state.sim_result is not None:
            result = st.session_state.sim_result
            config = st.session_state.get('sim_config', {})
            
            st.divider()
            st.subheader("📊 Simulation Results")
            
            m1, m2, m3, m4, m5 = st.columns(5)
            m1.metric("Fill Rate", f"{result.avg_fill_rate:.1f}%",
                     delta="Good" if result.avg_fill_rate > 95 else "Low")
            m2.metric("Stockout Rate", f"{result.stockout_rate:.2f}%")
            m3.metric("Revenue", f"KES {result.total_revenue:,.0f}")
            m4.metric("Turns", f"{result.inventory_turnover:.1f}x")
            m5.metric("ROI", f"{result.roi:.1f}%")
            
            st.caption(f"Scenario: {config.get('archetype', 'N/A')} | "
                      f"Days: {config.get('days', 'N/A')} | "
                      f"Supplier Event: {config.get('supplier', 'None')}")
            
            # Daily Chart
            if hasattr(result, 'daily_logs') and result.daily_logs:
                daily_df = pd.DataFrame(result.daily_logs)
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=daily_df['day'],
                    y=daily_df['fill_rate'],
                    mode='lines+markers',
                    name='Fill Rate %',
                    line=dict(color='#667eea', width=2)
                ))
                fig.add_trace(go.Bar(
                    x=daily_df['day'],
                    y=daily_df['stockouts'],
                    name='Stockouts',
                    marker_color='#ef4444',
                    opacity=0.6,
                    yaxis='y2'
                ))
                fig.update_layout(
                    title="Daily Performance",
                    xaxis_title="Day",
                    yaxis=dict(title="Fill Rate %", side='left', range=[80, 105]),
                    yaxis2=dict(title="Stockouts", side='right', overlaying='y'),
                    hovermode='x unified',
                    height=350
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # === EXCEL EXPORT SECTION ===
            st.divider()
            st.subheader("📥 Export Results")
            
            # Prepare Excel data
            from io import BytesIO
            
            def create_excel_download(result, config):
                """Create Excel file with multiple sheets for download."""
                output = BytesIO()
                
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    # Sheet 1: Summary
                    summary_data = {
                        'Metric': ['Fill Rate', 'Stockout Rate', 'Total Revenue', 'Lost Sales', 
                                  'Inventory Turns', 'ROI', 'Days Simulated', 'SKUs Tracked'],
                        'Value': [f"{result.avg_fill_rate:.1f}%", f"{result.stockout_rate:.2f}%",
                                 f"KES {result.total_revenue:,.0f}", f"KES {result.lost_sales:,.0f}",
                                 f"{result.inventory_turnover:.1f}x", f"{result.roi:.1f}%",
                                 config.get('days', 'N/A'), len(result.daily_logs) if result.daily_logs else 'N/A']
                    }
                    pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
                    
                    # Sheet 2: Daily Performance
                    if hasattr(result, 'daily_logs') and result.daily_logs:
                        daily_df = pd.DataFrame(result.daily_logs)
                        daily_df.to_excel(writer, sheet_name='Daily_Performance', index=False)
                    
                    # Sheet 3: Daily Orders/Replenishments
                    if hasattr(result, 'order_logs') and result.order_logs:
                        orders_df = pd.DataFrame(result.order_logs)
                        orders_df.to_excel(writer, sheet_name='Daily_Replenishments', index=False)
                    
                    # Sheet 4: Stockout Details
                    if hasattr(result, 'stockout_details') and result.stockout_details:
                        stockout_df = pd.DataFrame(result.stockout_details)
                        stockout_df.to_excel(writer, sheet_name='Stockout_Details', index=False)
                    
                    # Sheet 5: SKU Final State
                    if hasattr(result, 'sku_final_states') and result.sku_final_states:
                        sku_df = pd.DataFrame(result.sku_final_states)
                        sku_df.to_excel(writer, sheet_name='SKU_Final_State', index=False)
                
                output.seek(0)
                return output.getvalue()
            
            # Create download button
            try:
                excel_data = create_excel_download(result, config)
                
                from datetime import datetime
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"simulation_results_{config.get('tier', 'custom')}_{timestamp}.xlsx"
                
                col_dl1, col_dl2 = st.columns([1, 2])
                with col_dl1:
                    st.download_button(
                        label="📥 Download Excel Report",
                        data=excel_data,
                        file_name=filename,
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                        type="primary"
                    )
                with col_dl2:
                    st.caption(f"Includes: Summary, Daily Performance, Replenishments, Stockouts, SKU States")
                    
            except Exception as e:
                st.warning(f"Could not generate Excel: {e}")
with tab3:
    st.header("Supplier Concentration Analysis")
    
    if not SUPPLIER_ANALYTICS_AVAILABLE:
        st.warning("Supplier analytics module not available.")
    else:
        try:
            df_scorecard = get_scorecard_data()
            
            if df_scorecard is None:
                st.error("Could not load scorecard data.")
            else:
                categories = get_major_categories()
                selected_cat = st.selectbox("Select Category", categories, key="analysis_cat")
                
                suppliers = get_top_suppliers_by_department(df_scorecard, selected_cat, 10)
                
                if suppliers:
                    col1, col2 = st.columns([1, 1])
                    
                    with col1:
                        st.subheader(f"Top 10 Suppliers: {selected_cat}")
                        
                        sup_df = pd.DataFrame([{
                            "Supplier": s.supplier_name[:30],
                            "Share %": f"{s.share_pct:.1f}%",
                            "SKUs": s.sku_count,
                            "Revenue": f"KES {s.revenue_potential:,.0f}"
                        } for s in suppliers])
                        
                        st.dataframe(sup_df, hide_index=True, use_container_width=True)
                    
                    with col2:
                        # Pie chart
                        pie_data = pd.DataFrame([{
                            "Supplier": s.supplier_name[:20],
                            "Share": s.share_pct
                        } for s in suppliers[:7]])
                        
                        fig = px.pie(pie_data, values='Share', names='Supplier',
                                    title=f"Market Share - {selected_cat}")
                        st.plotly_chart(fig, use_container_width=True)
                    
                    # Concentration Warning
                    top_share = suppliers[0].share_pct
                    if top_share > 40:
                        st.error(f"⚠️ **CRITICAL**: {suppliers[0].supplier_name} controls {top_share:.1f}% of {selected_cat}")
                    elif top_share > 25:
                        st.warning(f"⚠️ **HIGH RISK**: {suppliers[0].supplier_name} controls {top_share:.1f}%")
                    else:
                        st.success(f"✅ Healthy diversity in {selected_cat}")
                        
        except Exception as e:
            st.error(f"Error loading supplier data: {e}")

# Footer
st.divider()
st.caption("OASIS v4.0 | Retail Simulation Engine | [GitHub](https://github.com/qahirfaraj-a11y/OASIS_ALLOCATION)")
