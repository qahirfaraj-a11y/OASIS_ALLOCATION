"""
O.A.S.I.S. Client Approval Dashboard
Separate Streamlit tool for Phase 4-6 of the playbook.
Procurement managers review and approve auto-generated daily POs.
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys
import json
import io
from datetime import datetime
from oasis.logic.shadow_mode import ShadowModeEngine

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="O.A.S.I.S. PO Approval Center", layout="wide", initial_sidebar_state="expanded")

st.title("O.A.S.I.S. Purchase Order Approval Center")
st.markdown("**Daily Procurement Workflow** - Review, modify, and approve auto-generated purchase orders.")

base_dir = os.path.dirname(os.path.abspath(__file__))
pipeline_dir = os.path.join(base_dir, 'pipeline_logs')
governance_dir = os.path.join(base_dir, 'amit_governance')
stores_path = os.path.join(base_dir, 'store_coords.json')

# AP5: Ensure required directories exist
os.makedirs(pipeline_dir, exist_ok=True)
os.makedirs(governance_dir, exist_ok=True)

# AP1: Load Store List with fallback if file is missing
try:
    with open(stores_path, 'r') as f:
        STORES_MAP = json.load(f)
except (FileNotFoundError, json.JSONDecodeError):
    st.warning("⚠️ `store_coords.json` not found. Using default store list.")
    STORES_MAP = {"DEFAULT": {"name": "Default Store", "lat": 0, "lon": 0}}
STORE_OPTIONS = {f"{k} - {v['name']}": k for k, v in STORES_MAP.items()}

# Sidebar Config
st.sidebar.header("0. Target Network Node")
selected_store_label = st.sidebar.selectbox("Active Store", list(STORE_OPTIONS.keys()))
selected_store_id = STORE_OPTIONS[selected_store_label]

st.sidebar.divider()
st.sidebar.header("1. Data Extraction")
extraction_mode = st.sidebar.radio("Extraction Source", ["File Dump Watcher", "Live SQL Database"])

# AP2: Dynamic scorecard discovery with version glob fallback
from pathlib import Path as _ApprovalPath
_approval_sc_candidates = list(_ApprovalPath(base_dir).glob("Full_Product_Allocation_Scorecard_v*.csv"))
if _approval_sc_candidates:
    def _get_sc_ver(p):
        try: return int(p.stem.split('_v')[-1])
        except: return 0
    _approval_scorecard_path = str(max(_approval_sc_candidates, key=_get_sc_ver))
else:
    _approval_scorecard_path = os.path.join(base_dir, 'Full_Product_Allocation_Scorecard_v7.csv')

config = {
    'data_dir': base_dir,
    'scorecard_path': _approval_scorecard_path,
    'amit_enabled': True,
    'lata_enabled': True,
    'dharam_enabled': True,
}

if extraction_mode == "Live SQL Database":
    config['extraction_mode'] = 'sql'
    with st.sidebar.expander("SQL Credentials", expanded=True):
        config['sql_server'] = st.text_input("Server IP / Name", "localhost")
        config['sql_db'] = st.text_input("Database Name", "iRetailDB")
        config['sql_trusted'] = st.checkbox("Use Windows Transport Auth", True)
        if not config['sql_trusted']:
            config['sql_user'] = st.text_input("Username")
            config['sql_pass'] = st.text_input("Password", type="password")
else:
    config['extraction_mode'] = 'file'
    st.sidebar.info("Listening to drops in: oasis/data/inbound_drops")

st.sidebar.divider()
st.sidebar.header("2. Pipeline Configuration")
config['revenue_core_only'] = st.sidebar.toggle("Revenue Core Only (Top 20%)", value=False,
                                  help="Limit ordering to top 20% fastest-moving items (Phase 4 mode)")
config['shadow_mode'] = st.sidebar.toggle("Shadow Mode", value=True,
                                 help="When ON, POs are logged but NOT dispatched to suppliers")

st.sidebar.divider()
st.sidebar.header("3. Pipeline Control")
config['store_id'] = selected_store_id
run_pipeline = st.sidebar.button("Run Daily Pipeline Now", use_container_width=True)

if run_pipeline:
    from oasis.logic.daily_pipeline import DailyPipeline

    with st.spinner("Running O.A.S.I.S. Daily Pipeline..."):
        pipeline = DailyPipeline(config)
        result = pipeline.run_daily_cycle()

    if result['status'] == 'COMPLETED':
        st.sidebar.success("Pipeline Complete!")
    else:
        st.sidebar.error(f"Pipeline Failed. Check logs.")

    # Display step log
    for step in result.get('steps', []):
        icon = "OK" if step['status'] == 'OK' else ("WARN" if step['status'] == 'WARNING' else "FAIL")
        st.sidebar.text(f"[{icon}] {step['step']}: {step['detail'][:50]}")

# Main Content: Load and display latest PO
st.divider()

# Find latest PO file for the SELECTED STORE
po_files = []
if os.path.exists(pipeline_dir):
    # Search for files starting with 'daily_po_{store_id}' or fallback to 'daily_po_'
    store_prefix = f'daily_po_{selected_store_id}'
    po_files = sorted([f for f in os.listdir(pipeline_dir) if f.startswith(store_prefix)], reverse=True)
    
    # Fallback for existing legacy files if no store-specific file found
    if not po_files:
        po_files = sorted([f for f in os.listdir(pipeline_dir) if f.startswith('daily_po_')], reverse=True)

if po_files:
    latest_po_path = os.path.join(pipeline_dir, po_files[0])
    
    # Initialize session state for the dataframe to allow reactivity
    if 'active_po_file' not in st.session_state or st.session_state.active_po_file != po_files[0]:
        st.session_state.po_df = pd.read_csv(latest_po_path)
        st.session_state.active_po_file = po_files[0]

    po_df = st.session_state.po_df

    st.header(f"Daily Purchase Order: {po_files[0].replace('daily_po_', '').replace('.csv', '')}")

    # Summary metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total Line Items", len(po_df))
    c2.metric("Total PO Value", f"KES {po_df.get('Shadow_Order_Value', pd.Series([0])).sum():,.2f}")
    suppliers = po_df['Supplier'].nunique() if 'Supplier' in po_df.columns else 0
    c3.metric("Suppliers", suppliers)
    mode_label = "SHADOW" if config.get('shadow_mode', True) else "LIVE"
    c4.metric("Mode", mode_label)

    st.divider()

    tab1, tab2, tab3, tab4 = st.tabs(["By Supplier", "Full PO Detail", "Divergence Analysis", "AMIT Blocked Items"])

    with tab1:
        st.subheader("Order Grouped by Supplier")
        if 'Supplier' in po_df.columns:
            supp_summary = po_df.groupby('Supplier').agg(
                Items=('Item_Name', 'count'),
                Total_Qty=('Shadow_Order_Qty', 'sum'),
                Total_Value=('Shadow_Order_Value', 'sum')
            ).sort_values('Total_Value', ascending=False).reset_index()
            st.dataframe(supp_summary, use_container_width=True, height=400)

            fig = px.bar(supp_summary.head(15), x='Supplier', y='Total_Value',
                         color_discrete_sequence=['#2ecc71'])
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, use_container_width=True)

    with tab2:
        st.subheader("Full Purchase Order Lines")
        
        # Allow editing directly in dataframe
        # We catch the returned value from data_editor to update session state
        edited_df = st.data_editor(
            st.session_state.po_df, 
            use_container_width=True, 
            height=500, 
            num_rows="dynamic",
            key="po_editor"
        )
        
        # Trigger a rerun if data changed to update metrics at top
        if not edited_df.equals(st.session_state.po_df):
            # Recalculate Value if Qty or Cost changed
            if 'Shadow_Order_Qty' in edited_df.columns and 'Unit_Cost' in edited_df.columns:
                edited_df['Shadow_Order_Value'] = edited_df['Shadow_Order_Qty'] * edited_df['Unit_Cost']
            
            st.session_state.po_df = edited_df
            st.rerun()

        col_a, col_b, col_c = st.columns(3)
        with col_a:
            if st.button("Approve & Save", use_container_width=True, type="primary"):
                approved_path = os.path.join(pipeline_dir, f'approved_po_{selected_store_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
                st.session_state.po_df.to_csv(approved_path, index=False)
                st.success(f"PO Approved locally.")

        with col_b:
            if st.button("Dispatch to ERP", use_container_width=True, help="Push to iRetail SQL Staging"):
                from oasis.logic.iretail_integration import IRetailBridge
                bridge = IRetailBridge(
                    server=config.get('sql_server', 'localhost'),
                    database=config.get('sql_db', 'iRetailDB'),
                    trusted_connection=config.get('sql_trusted', True)
                )
                
                # Mocking the push for simulation safety as per plan
                st.info("DRY RUN: Sending PO to iRetail Staging Queue...")
                order_data = st.session_state.po_df.to_dict('records')
                # In real scenario: bridge.push_purchase_order(order_data)
                st.success(f"Pushed {len(order_data)} lines to Production Staging.")

        with col_c:
            # Download as Excel
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
                st.session_state.po_df.to_excel(writer, sheet_name='Approved PO', index=False)
            output.seek(0)
            st.download_button(
                label="Download Approved PO (Excel)",
                data=output,
                file_name=f"OASIS_Approved_PO_{datetime.now().strftime('%Y%m%d')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True,
            )

    with tab3:
        st.subheader("Divergence Analysis: AI Recommendations vs Human Buyer")
        
        # Load mock human PO for comparison if not provided
        human_po_path = os.path.join(base_dir, 'mock_human_po.csv')
        
        if os.path.exists(human_po_path):
            with st.spinner("Generating Divergence Report..."):
                engine = ShadowModeEngine(base_dir)
                # We use the current session_state po_df (in case they edited it)
                engine.shadow_po = st.session_state.po_df.copy()
                engine.ingest_human_orders(human_po_path)
                comparison = engine.generate_comparison()
                stats = engine.get_summary_stats()

            # Divergence Summary Cards
            m1, m2, m3, m4 = st.columns(4)
            m1.metric("Human Misses", stats.get('human_missed', 0), delta_color="inverse")
            m2.metric("Over-Ordered Items", stats.get('human_over_ordered', 0), delta_color="inverse")
            m3.metric("Aligned Items", stats.get('aligned', 0))
            m4.metric("Holding Cost Risk", f"KES {stats.get('over_order_waste_risk', 0):,.2f}", delta_color="inverse")

            st.divider()

            # Visualization
            col1, col2 = st.columns(2)
            
            with col1:
                st.write("**Divergence Type Distribution**")
                fig_div = px.pie(comparison, names='Divergence', color='Divergence',
                                color_map={
                                    'HUMAN_MISSED': '#e74c3c', 
                                    'HUMAN_OVER_ORDERED': '#f1c40f',
                                    'ALIGNED': '#2ecc71',
                                    'NO_ORDER': '#95a5a6'
                                })
                st.plotly_chart(fig_div, use_container_width=True)

            with col2:
                st.write("**Top Material Discrepancies (Value)**")
                # Highlight where human and AI differ most in value
                comparison['Value_Diff'] = abs(
                    comparison['Shadow_Order_Value'] - (
                        comparison['Human_Order_Qty'] * (comparison['Unit_Cost'].fillna(0) if 'Unit_Cost' in comparison.columns else 0)
                    )
                )
                top_diff = comparison.sort_values('Value_Diff', ascending=False).head(10)
                fig_diff = px.bar(top_diff, x='Item_Name', y=['Shadow_Order_Value', 'Value_Diff'], 
                                 barmode='group', title="AI vs Diff")
                st.plotly_chart(fig_diff, use_container_width=True)

            st.write("**Deep Dive: Divergence Details**")
            st.dataframe(comparison[['Item_Name', 'Shadow_Order_Qty', 'Human_Order_Qty', 'Divergence', 'Divergence_Detail']], 
                        use_container_width=True, height=400)
        else:
            st.warning("No Human Purchase Order found for comparison. Please upload a `human_po.csv` to enable Divergence Analysis.")

    with tab4:
        st.subheader("AMIT Negative List (Blocked from Ordering)")
        neg_path = os.path.join(governance_dir, 'amit_negative_list.csv')
        if os.path.exists(neg_path):
            neg_df = pd.read_csv(neg_path)
            
            # Filter for current store if data allows (assuming scorecard is store-specific)
            st.metric("Items Blocked (Governance)", len(neg_df))
            if 'Capital_Trapped' in neg_df.columns:
                st.metric("Capital Trapped in Dead Stock", f"KES {neg_df['Capital_Trapped'].sum():,.2f}")
            
            st.write("Review blocked items. Select 'Release' to allow a one-time purchase override.")
            neg_df['Release_for_Purchase'] = False
            
            edited_neg = st.data_editor(
                neg_df[['Item_Name', 'SOH', 'ADS', 'Capital_Trapped', 'Classification', 'Release_for_Purchase']],
                use_container_width=True, 
                height=400,
                key="amit_editor"
            )
            
            # Inject overrides into PO
            overrides = edited_neg[edited_neg['Release_for_Purchase'] == True].copy()
            if not overrides.empty:
                if st.button(f"Inject {len(overrides)} Overrides into Draft PO"):
                    # Add necessary columns for PO format
                    overrides['Shadow_Order_Qty'] = 1 # Default or manual logic
                    if 'Unit_Cost' not in overrides.columns: overrides['Unit_Cost'] = 100 
                    overrides['Shadow_Order_Value'] = overrides['Shadow_Order_Qty'] * overrides['Unit_Cost']
                    overrides['Order_Reason'] = 'MANAGER_OVERRIDE'
                    overrides['Supplier'] = 'OVERRIDE'
                    
                    st.session_state.po_df = pd.concat([st.session_state.po_df, overrides], ignore_index=True)
                    st.success("Overrides injected. Please review 'Full PO Detail' tab.")
                    st.rerun()
        else:
            st.info("No AMIT Negative List generated yet. Run the pipeline with AMIT enabled.")

else:
    st.info("No purchase orders generated yet. Click 'Run Daily Pipeline Now' in the sidebar to begin.")

# Pipeline History
st.divider()
st.header("Pipeline Run History")
if os.path.exists(pipeline_dir):
    log_files = sorted([f for f in os.listdir(pipeline_dir) if f.startswith('pipeline_run_')], reverse=True)
    if log_files:
        for lf in log_files[:5]:
            with open(os.path.join(pipeline_dir, lf)) as f:
                log_data = json.load(f)
            status_icon = "PASS" if log_data.get('status') == 'COMPLETED' else 'FAIL'
            with st.expander(f"[{status_icon}] {log_data.get('run_id', lf)} - {log_data.get('status', 'UNKNOWN')}"):
                for step in log_data.get('steps', []):
                    st.text(f"  [{step['status']}] {step['step']}: {step['detail']}")
    else:
        st.text("No pipeline runs yet.")
