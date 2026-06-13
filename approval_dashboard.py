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
from datetime import datetime, timedelta
from oasis.logic.shadow_mode import ShadowModeEngine

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

st.set_page_config(page_title="O.A.S.I.S. PO Approval Center", layout="wide", initial_sidebar_state="expanded")

# ── Unified auth gate (U2) ──────────────────────────────────────────────
# This dashboard authorizes purchase-order spend; it must never be open.
# Restricted to roles that carry can_approve_po (ops_admin, regional_manager).
from oasis.ui.auth import require_login, logout as _oasis_logout  # noqa: E402
_AUTH_DB = os.getenv(
    "OASIS_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "oasis", "data", "mock_pos_erp.db"),
)
_oasis_user = require_login(
    st, _AUTH_DB, app_title="PO Approval Center",
    allowed_roles=["ops_admin", "regional_manager"],
)
with st.sidebar:
    st.caption(f"Signed in as {_oasis_user.get('display_name', _oasis_user['username'])} · {_oasis_user['role']}")
    if st.button("Log out", key="oasis_logout_btn"):
        _oasis_logout(st, _AUTH_DB)
        st.rerun()

st.title("O.A.S.I.S. Purchase Order Approval Center")
st.markdown("**Daily Procurement Workflow** - Review, modify, and approve auto-generated purchase orders.")

base_dir = os.path.dirname(os.path.abspath(__file__))
# Redirect pipeline_logs and governance tracking directly into the central oasis/data pipeline
pipeline_dir = os.path.join(base_dir, 'oasis', 'data', 'pipeline_logs')
governance_dir = os.path.join(base_dir, 'oasis', 'data', 'amit_governance')
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
        except (ValueError, IndexError): return 0
    _approval_scorecard_path = str(max(_approval_sc_candidates, key=_get_sc_ver))
else:
    _approval_scorecard_path = os.path.join(base_dir, 'Full_Product_Allocation_Scorecard_v7.csv')

config = {
    'data_dir': os.path.join(base_dir, 'oasis', 'data'),
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
run_pipeline = st.sidebar.button("Run Daily Pipeline Now", width='stretch')

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
        icon = "OK" if step['status'] == 'OK' else ("WARN" if step['status'] == 'WARNING' else ("SKIP" if step['status'] == 'SKIPPED' else "FAIL"))
        st.sidebar.text(f"[{icon}] {step['step']}: {step['detail'][:50]}")

st.sidebar.divider()
st.sidebar.header("4. Inventory Reconstructor")
target_extrap_date = st.sidebar.date_input("Target Stock Date", value=datetime(2026, 1, 25))
if st.sidebar.button("Reconstruct Stock for Date", width='stretch', help="Extrapolate SOH using GRNs and Sales Burn Rates"):
    from oasis.logic.extrapolate_stock import run_extrapolation
    with st.spinner(f"Reconstructing stock for {target_extrap_date}..."):
        try:
            date_str = target_extrap_date.strftime("%Y-%m-%d")
            out_file = run_extrapolation(date_str)
            st.sidebar.success(f"Scorecard Generated: {os.path.basename(out_file)}")
            st.sidebar.info("You can now select this file as 'Active Scorecard' if you rerun the app.")
        except Exception as e:
            st.sidebar.error(f"Extrapolation Failed: {e}")

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

    tab1, tab2, tab3, tab4, tab5 = st.tabs(["By Supplier", "Full PO Detail", "Divergence Analysis", "AMIT Blocked Items", "🧪 Simulation Lab"])

    with tab1:
        st.subheader("Order Grouped by Supplier")
        if 'Supplier' in po_df.columns:
            supp_summary = po_df.groupby('Supplier').agg(
                Items=('Item_Name', 'count'),
                Total_Qty=('Shadow_Order_Qty', 'sum'),
                Total_Value=('Shadow_Order_Value', 'sum')
            ).sort_values('Total_Value', ascending=False).reset_index()
            st.dataframe(supp_summary, width='stretch', height=400)

            fig = px.bar(supp_summary.head(15), x='Supplier', y='Total_Value',
                         color_discrete_sequence=['#2ecc71'])
            fig.update_layout(xaxis_tickangle=-45)
            st.plotly_chart(fig, width='stretch')

    with tab2:
        st.subheader("Full Purchase Order Lines")
        
        # Allow editing directly in dataframe
        # We catch the returned value from data_editor to update session state
        edited_df = st.data_editor(
            st.session_state.po_df, 
            width='stretch', 
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
            if st.button("Approve & Save", width='stretch', type="primary"):
                approved_path = os.path.join(pipeline_dir, f'approved_po_{selected_store_id}_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
                st.session_state.po_df.to_csv(approved_path, index=False)
                st.success(f"PO Approved locally.")

        with col_b:
            if st.button("Dispatch to ERP", width='stretch', help="Push to iRetail SQL Staging"):
                from oasis.logic.iretail_integration import IRetailBridge
                bridge = IRetailBridge(
                    server=config.get('sql_server', 'localhost'),
                    database=config.get('sql_db', 'iRetailDB'),
                    trusted_connection=config.get('sql_trusted', True)
                )
                
                # BUG 3 FIX: Gate backend push on Shadow Mode toggling
                order_data = st.session_state.po_df.to_dict('records')
                if config.get('shadow_mode', True):
                    st.info("DRY RUN (Shadow Mode): Sending PO to iRetail Staging Queue...")
                    st.success(f"Simulated push of {len(order_data)} lines to Production Staging.")
                else:
                    with st.spinner("Pushing to ERP (LIVE)..."):
                        pushed_rows = bridge.push_purchase_order(order_data)
                        st.success(f"LIVE DISPATCH: Pushed {pushed_rows} lines to Production Staging.")

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
                width='stretch',
            )

    with tab3:
        st.subheader("Divergence Analysis: AI Recommendations vs Human Buyer")
        
        # Discover available human POs/GRNs
        data_dir = os.path.join(base_dir, 'oasis', 'data')
        all_files = os.listdir(data_dir)
        # We prioritize 'grnd' files as they contain line-item details, but keep 'po' and 'csv' as options
        available_pos = [f for f in all_files if (f.startswith('grnd') or f.startswith('po_') or f.endswith('.csv')) and not f.startswith('daily_po_')]
        
        selected_pos = st.multiselect("Select Human Inbound Data (GRNs preferred for line items)", 
                                     sorted(available_pos), 
                                     default=[f for f in available_pos if 'grnd' in f.lower()][:1])
        
        if selected_pos:
            full_po_paths = [os.path.join(data_dir, f) for f in selected_pos]
            with st.spinner("Generating Divergence Report..."):
                engine = ShadowModeEngine(base_dir)
                # We use the current session_state po_df (in case they edited it)
                engine.shadow_po = st.session_state.po_df.copy()
                engine.ingest_human_orders(full_po_paths)
                comparison = engine.generate_comparison()
                stats = engine.get_summary_stats()
                
                # Check for error logged by engine if columns were missing
                if stats.get('human_total_value', 0) == 0 and not comparison.empty and 'Human PO data invalid' in str(comparison.iloc[0].get('Divergence_Detail', '')):
                    st.error("Selected files (like PO headers) do not contain line-item data. Please select 'grnd' files for comparison.")
        else:
            st.info("Please select at least one human PO file to compare.")
            comparison = pd.DataFrame()
            stats = {}

        if not comparison.empty:
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
                if not comparison.empty and 'Divergence' in comparison.columns:
                    fig_div = px.pie(comparison, names='Divergence', color='Divergence',
                                    color_discrete_map={
                                        'HUMAN_MISSED': '#e74c3c', 
                                        'HUMAN_OVER_ORDERED': '#f1c40f',
                                        'ALIGNED': '#2ecc71',
                                        'NO_ORDER': '#95a5a6'
                                    })
                    st.plotly_chart(fig_div, width='stretch')
                else:
                    st.info("Not enough data to calculate divergence distribution.")

            with col2:
                st.write("**Top Material Discrepancies (Value)**")
                if not comparison.empty:
                    human_qty = comparison.get('Human_Order_Qty', pd.Series(0, index=comparison.index))
                    unit_cost = comparison['Unit_Cost'].fillna(0) if 'Unit_Cost' in comparison.columns else 0
                        
                    comparison['Value_Diff'] = abs(comparison.get('Shadow_Order_Value', 0) - (human_qty * unit_cost))
                    top_diff = comparison.sort_values('Value_Diff', ascending=False).head(10)
                    fig_diff = px.bar(top_diff, x='Item_Name', y=['Shadow_Order_Value', 'Value_Diff'], 
                                     barmode='group', title="AI vs Diff")
                    st.plotly_chart(fig_diff, width='stretch')
                else:
                    st.info("No comparative discrepancies found.")

            st.write("**Deep Dive: Divergence Details**")
            if not comparison.empty:
                cols_to_show = [c for c in ['Item_Name', 'Shadow_Order_Qty', 'Human_Order_Qty', 'Divergence', 'Divergence_Detail'] if c in comparison.columns]
                st.dataframe(comparison[cols_to_show], width=1200, height=400)
            else:
                st.info("No records to display.")

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
                width='stretch', 
                height=400,
                key="amit_editor"
            )
            
            # Inject overrides into PO
            overrides = edited_neg[edited_neg['Release_for_Purchase'] == True].copy()
            if not overrides.empty:
                if st.button(f"Inject {len(overrides)} Overrides into Draft PO"):
                    # Add necessary columns for PO format
                    # C2 FIX: Look up actual costs from GRN intelligence cache instead of hardcoded KES 100
                    grn_cache_path = os.path.join(base_dir, 'oasis', 'data', 'grn_intelligence_cache.json')
                    cost_lookup = {}
                    if os.path.exists(grn_cache_path):
                        try:
                            with open(grn_cache_path, 'r') as _gc:
                                grn_cache = json.load(_gc)
                            for item_key, item_data in grn_cache.items():
                                if isinstance(item_data, dict):
                                    cost_lookup[item_key] = item_data.get('avg_cost', item_data.get('unit_cost', 100.0))
                        except (json.JSONDecodeError, IOError):
                            st.warning("⚠️ GRN cost cache could not be loaded. Using fallback costs.")

                    if 'ADS' in overrides.columns:
                        overrides['Shadow_Order_Qty'] = (overrides['ADS'] * 7).clip(lower=1).astype(int) 
                    else:
                        overrides['Shadow_Order_Qty'] = 1
                        
                    if 'Unit_Cost' not in overrides.columns:
                        # Map costs from GRN cache by item name, fallback to KES 100 only if not found
                        overrides['Unit_Cost'] = overrides['Item_Name'].map(cost_lookup).fillna(100.0)
                    else:
                        overrides['Unit_Cost'] = overrides['Unit_Cost'].fillna(
                            overrides['Item_Name'].map(cost_lookup)
                        ).fillna(100.0)
                        
                    overrides['Shadow_Order_Value'] = overrides['Shadow_Order_Qty'] * overrides['Unit_Cost']
                    overrides['Order_Reason'] = 'MANAGER_OVERRIDE'
                    
                    if 'Supplier' not in overrides.columns: overrides['Supplier'] = 'OVERRIDE'
                    
                    st.session_state.po_df = pd.concat([st.session_state.po_df, overrides], ignore_index=True)
                    st.success("Overrides injected. Please review 'Full PO Detail' tab.")
                    st.rerun()
    with tab5:
        st.subheader("O.A.S.I.S. Simulation Lab (High-Fidelity Backtest)")
        st.write("Replay historical procurement cycles to audit AI performance against human buyers.")
        
        c1, c2 = st.columns(2)
        with c1:
            sim_start = st.date_input("Simulation Start", value=datetime(2026, 1, 20))
        with c2:
            sim_end = st.date_input("Simulation End", value=datetime(2026, 1, 22))
            
        if st.button("🚀 Run Network-Wide Backtest", width='stretch', type="primary"):
            from oasis.logic.simulation_pipeline import SimulationEngine
            
            sim_engine = SimulationEngine(config)
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            days_to_sim = (sim_end - sim_start).days + 1
            results = []
            
            for i in range(days_to_sim):
                curr_date = sim_start + timedelta(days=i)
                date_str = curr_date.strftime("%Y-%m-%d")
                status_text.text(f"Processing {date_str} ({i+1}/{days_to_sim})...")
                
                # We call the run_simulation logic per day for UI feedback
                # Using a single-day simulation call
                report = sim_engine.run_simulation(date_str, date_str)
                results.append(report['daily_records'][0])
                progress_bar.progress((i + 1) / days_to_sim)
                
            st.success(f"Simulation Complete for {days_to_sim} days!")
            
            # Aggregate stats
            summary = sim_engine._aggregate_stats(results)
            
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("Cumulative Savings Opportunity", f"KES {summary['cumulative_holding_risk']:,.2f}")
            k2.metric("Total Aligned Items", summary['total_aligned'])
            k3.metric("Human Over-Orders", summary['total_human_over_ordered'], delta_color="inverse")
            k4.metric("Human Under-Orders (Misses)", summary['total_human_missed'], delta_color="inverse")
            
            # Charts
            sim_df = pd.DataFrame([
                {
                    'Date': r['date'], 
                    'Missed': r['comparison'].get('human_missed', 0),
                    'OverOrdered': r['comparison'].get('human_over_ordered', 0),
                    'Aligned': r['comparison'].get('aligned', 0),
                    'Risk': r['comparison'].get('over_order_waste_risk', 0)
                } for r in results
            ])
            
            st.divider()
            st.write("**Daily Divergence Trend**")
            fig_trend = px.line(sim_df, x='Date', y=['Missed', 'OverOrdered', 'Aligned'], 
                               title="Day-over-Day Alignment", markers=True)
            st.plotly_chart(fig_trend, width='stretch')
            
            st.write("**Daily Financial Risk (Cumulative Waste)**")
            fig_risk = px.bar(sim_df, x='Date', y='Risk', title="Inventory Waste Risk (KES)",
                             color_discrete_sequence=['#e74c3c'])
            st.plotly_chart(fig_risk, width='stretch')


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
