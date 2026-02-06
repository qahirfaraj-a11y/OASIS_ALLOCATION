"""
OASIS Retail Simulation Dashboard
==================================
Streamlit-compatible web app for stock allocation and simulation.
Designed for Streamlit Cloud deployment.

Usage:
    streamlit run streamlit_app.py
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
import os
from pathlib import Path

# Ensure app path
sys.path.insert(0, str(Path(__file__).parent))

# Try to import simulation modules
try:
    from retail_simulator import RetailSimulator, STORE_UNIVERSES, print_simulation_summary
    from oasis.analytics.supplier_analytics import (
        get_top_suppliers_by_department, 
        get_major_categories,
        analyze_supplier_failure_impact,
        load_scorecard_data
    )
    SIMULATION_AVAILABLE = True
except ImportError as e:
    SIMULATION_AVAILABLE = False
    IMPORT_ERROR = str(e)

# --- Page Config ---
st.set_page_config(
    page_title="OASIS Retail Simulation",
    page_icon="🏝️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# --- Custom CSS ---
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
    }
    .stMetric {
        background-color: #f8f9fa;
        padding: 1rem;
        border-radius: 8px;
        border-left: 4px solid #667eea;
    }
</style>
""", unsafe_allow_html=True)


def render_simulation_tab():
    """Main simulation dashboard tab."""
    st.header("📊 Retail Simulation Engine")
    
    if not SIMULATION_AVAILABLE:
        st.error(f"Simulation modules not available: {IMPORT_ERROR}")
        st.info("Please ensure all dependencies are installed.")
        return
    
    # Sidebar Controls
    with st.sidebar:
        st.header("⚙️ Simulation Settings")
        
        store_type = st.selectbox(
            "Store Archetype",
            options=list(STORE_UNIVERSES.keys()),
            format_func=lambda x: f"{x} ({STORE_UNIVERSES[x]['description']})"
        )
        
        days = st.slider("Simulation Duration (Days)", 7, 90, 30)
        
        st.divider()
        
        st.subheader("🦢 Black Swan Events")
        enable_failure = st.checkbox("Enable Supplier Failure")
        
        if enable_failure:
            categories = get_major_categories()
            selected_category = st.selectbox("Category", categories)
            
            try:
                df = load_scorecard_data()
                suppliers = get_top_suppliers_by_department(df, selected_category, 10)
                supplier_names = [s.supplier_name for s in suppliers]
                selected_supplier = st.selectbox("Supplier to Fail", supplier_names)
                
                failure_day = st.slider("Failure Start Day", 1, days-5, 10)
                failure_duration = st.slider("Failure Duration (Days)", 1, 30, 14)
            except Exception as e:
                st.warning(f"Could not load suppliers: {e}")
                enable_failure = False
    
    # Main Content Area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader(f"🏪 {store_type} Configuration")
        config = STORE_UNIVERSES[store_type]
        
        config_df = pd.DataFrame({
            "Parameter": ["Budget", "Safety Days", "Max SKUs", "Reorder Frequency", "Demand Scale"],
            "Value": [
                f"KES {config['budget']:,}",
                f"{config['safety_days']} days",
                f"{config['max_skus']} SKUs",
                f"Every {config['reorder_frequency_days']} days",
                f"{config['demand_scale_factor']*100:.1f}%"
            ]
        })
        st.dataframe(config_df, hide_index=True, use_container_width=True)
    
    with col2:
        if 'is_online' in config and config['is_online']:
            st.info("🌐 **Online Store Mode**\n\n"
                    f"Fresh Boost: +{(config.get('fresh_demand_boost', 1)-1)*100:.0f}%\n\n"
                    f"Artisanal Boost: +{(config.get('artisanal_demand_boost', 1)-1)*100:.0f}%")
    
    # Run Simulation Button
    if st.button("🚀 Run Simulation", type="primary", use_container_width=True):
        with st.spinner(f"Running {days}-day simulation for {store_type}..."):
            try:
                simulator = RetailSimulator(store_type, config)
                result = simulator.run(days=days)
                
                # Store result in session
                st.session_state['last_result'] = result
                st.session_state['last_store'] = store_type
                
                st.success("Simulation Complete!")
                
            except Exception as e:
                st.error(f"Simulation failed: {e}")
                return
    
    # Display Results
    if 'last_result' in st.session_state:
        result = st.session_state['last_result']
        store = st.session_state.get('last_store', 'Unknown')
        
        st.divider()
        st.subheader(f"📈 Results: {store}")
        
        # Key Metrics
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Fill Rate", f"{result.avg_fill_rate:.1f}%", 
                  delta="Good" if result.avg_fill_rate > 95 else "Low")
        m2.metric("Stockout Rate", f"{result.stockout_rate:.2f}%")
        m3.metric("Revenue", f"KES {result.total_revenue:,.0f}")
        m4.metric("Inventory Turns", f"{result.inventory_turnover:.1f}x")
        m5.metric("ROI", f"{result.roi:.1f}%")
        
        # Daily Performance Chart
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
                opacity=0.7,
                yaxis='y2'
            ))
            fig.update_layout(
                title="Daily Performance",
                xaxis_title="Day",
                yaxis=dict(title="Fill Rate %", side='left'),
                yaxis2=dict(title="Stockouts", side='right', overlaying='y'),
                hovermode='x unified',
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        # Top Stockouts Table
        if hasattr(result, 'stockout_details') and result.stockout_details:
            st.subheader("🔴 Top Stockout Items")
            stockout_df = pd.DataFrame(result.stockout_details[:10])
            st.dataframe(stockout_df, hide_index=True, use_container_width=True)


def render_supplier_analysis_tab():
    """Supplier concentration analysis tab."""
    st.header("🔍 Supplier Concentration Analysis")
    
    if not SIMULATION_AVAILABLE:
        st.error("Analytics modules not available.")
        return
    
    try:
        df = load_scorecard_data()
        categories = get_major_categories()
        
        selected_cat = st.selectbox("Select Category", categories)
        
        suppliers = get_top_suppliers_by_department(df, selected_cat, 10)
        
        if suppliers:
            # Create dataframe
            sup_df = pd.DataFrame([
                {
                    "Supplier": s.supplier_name,
                    "Market Share %": s.share_pct,
                    "SKU Count": s.sku_count,
                    "Revenue Potential": s.revenue_potential,
                    "Risk Score": s.risk_score
                }
                for s in suppliers
            ])
            
            col1, col2 = st.columns([1, 1])
            
            with col1:
                st.subheader(f"Top 10 Suppliers in {selected_cat}")
                st.dataframe(sup_df, hide_index=True, use_container_width=True)
            
            with col2:
                # Pie chart
                fig = px.pie(
                    sup_df, 
                    values='Market Share %', 
                    names='Supplier',
                    title=f"Market Share Distribution - {selected_cat}"
                )
                st.plotly_chart(fig, use_container_width=True)
            
            # Concentration Warning
            top_share = suppliers[0].share_pct if suppliers else 0
            if top_share > 40:
                st.error(f"⚠️ **CRITICAL CONCENTRATION**: {suppliers[0].supplier_name} controls {top_share:.1f}% of {selected_cat}")
            elif top_share > 25:
                st.warning(f"⚠️ **HIGH CONCENTRATION**: {suppliers[0].supplier_name} controls {top_share:.1f}% of {selected_cat}")
            else:
                st.success(f"✅ Healthy supplier diversity in {selected_cat}")
            
            # Impact Analysis
            st.subheader("📊 Failure Impact Analysis")
            selected_supplier = st.selectbox("Select Supplier for Impact Analysis", [s.supplier_name for s in suppliers])
            
            if st.button("Analyze Impact"):
                impact = analyze_supplier_failure_impact(df, selected_supplier, selected_cat)
                
                ic1, ic2, ic3, ic4 = st.columns(4)
                ic1.metric("Affected SKUs", impact['affected_sku_count'])
                ic2.metric("Revenue at Risk", f"KES {impact['revenue_at_risk']:,.0f}")
                ic3.metric("Coverage Loss", f"{impact['coverage_loss_pct']:.1f}%")
                ic4.metric("Severity", impact['estimated_stockout_severity'])
                
    except Exception as e:
        st.error(f"Error loading data: {e}")


def render_about_tab():
    """About and documentation tab."""
    st.header("📖 About OASIS")
    
    st.markdown("""
    ## OASIS Retail Simulation Engine
    
    **O**ptimized **A**llocation **S**ystem for **I**nventory **S**imulation
    
    ### Features
    
    - **Day 1 Allocation**: Generate optimal opening stock based on budget
    - **Multi-Day Simulation**: Test inventory performance over time
    - **Black Swan Events**: Simulate supplier failures and demand shocks
    - **Supplier Analytics**: Identify concentration risks
    - **Online Store Mode**: Specialized model for e-commerce
    
    ### Store Archetypes
    
    | Tier | Budget | SKUs | Use Case |
    |------|--------|------|----------|
    | Micro_100k | 100K | 200 | Small Kiosk/Duka |
    | Small_200k | 200K | 300 | Mini-Mart |
    | Medium_1M | 1M | 800 | Medium Supermarket |
    | Large_10M | 10M | 5000 | Large Supermarket |
    | Mega_100M | 100M | 15000 | Hypermarket |
    | Online_5M | 5M | 2000 | Online Grocery |
    
    ### Technical Stack
    
    - Python 3.10+
    - Streamlit for UI
    - Pandas/NumPy for data processing
    - Plotly for visualization
    
    ---
    
    Built by OASIS Team | [GitHub](https://github.com/qahirfaraj-a11y/OASIS_ALLOCATION)
    """)


# --- Main App ---
def main():
    st.markdown('<p class="main-header">🏝️ OASIS Retail Simulation</p>', unsafe_allow_html=True)
    
    tab1, tab2, tab3 = st.tabs(["📊 Simulation", "🔍 Supplier Analysis", "📖 About"])
    
    with tab1:
        render_simulation_tab()
    
    with tab2:
        render_supplier_analysis_tab()
    
    with tab3:
        render_about_tab()


if __name__ == "__main__":
    main()
