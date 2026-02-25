"""
OASIS Operations Command Center
================================
A "Day in the Life" demo dashboard showing real-time engine reasoning
across all 4 operational pillars: Sales Monitoring, Transfer Intelligence,
Stock Review, and Smart Ordering.

Run:  streamlit run ops_dashboard.py
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import os
import sys
import json
import sqlite3
import random
import math
import io
import gc
from datetime import datetime, date, timedelta

# Add project root
sys.path.insert(0, os.getcwd())

from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter
from oasis.logic.alert_monitor import AlertMonitor
from oasis.logic.order_engine import OrderEngine
from oasis.llm.inference import RuleBasedLLM
from oasis.data.supplier_calendar import SupplierCalendar
from oasis.logic.order_engine import apply_safety_guards
from oasis.simulation.black_swan_events import SupplierRiskAnalyzer, SupplierFailureEvent, SCENARIO_TEMPLATES
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService, NetworkPlan
from oasis.logic.transfer_state import TransferRecord
from intraday_sim import IntraDaySimulator

# ─────────────────────────────────────────────────────────────────────
# Page Config & Styling
# ─────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="OASIS Command Center",
    page_icon="🔮",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    .stApp { font-family: 'Inter', sans-serif; }
    
    /* Dark premium cards */
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        border: 1px solid rgba(255,255,255,0.08);
        border-radius: 12px;
        padding: 20px;
        margin: 8px 0;
        color: white;
    }
    .metric-card h3 { margin: 0; font-size: 0.85em; color: #888; font-weight: 400; }
    .metric-card .value { font-size: 2em; font-weight: 700; margin: 4px 0; }
    .metric-card .sub { font-size: 0.8em; color: #aaa; }
    
    /* Alert cards */
    .alert-card {
        background: linear-gradient(135deg, #3d0000 0%, #5c1a1a 100%);
        border: 1px solid #ff4444;
        border-radius: 10px;
        padding: 16px;
        margin: 8px 0;
        color: #ffcccc;
    }
    .alert-card .alert-title { color: #ff6666; font-weight: 600; font-size: 1.1em; }
    
    /* Transfer cards */
    .transfer-card {
        background: linear-gradient(135deg, #0a1628 0%, #132743 100%);
        border: 1px solid #2e6ba6;
        border-radius: 10px;
        padding: 16px;
        margin: 8px 0;
        color: #cce0ff;
    }
    
    /* Reasoning expander */
    .reasoning-box {
        background: #0d1117;
        border: 1px solid #30363d;
        border-radius: 8px;
        padding: 12px;
        font-family: 'Courier New', monospace;
        font-size: 0.85em;
        color: #c9d1d9;
        white-space: pre-wrap;
    }
    
    /* Status badge */
    .badge-green { background: #1a4d1a; color: #4caf50; padding: 2px 10px; border-radius: 12px; font-size: 0.8em; }
    .badge-yellow { background: #4d3d00; color: #ffc107; padding: 2px 10px; border-radius: 12px; font-size: 0.8em; }
    .badge-red { background: #4d0000; color: #f44336; padding: 2px 10px; border-radius: 12px; font-size: 0.8em; }
    
    /* Header bar */
    .header-bar {
        background: linear-gradient(90deg, #0f0c29, #302b63, #24243e);
        padding: 20px 30px;
        border-radius: 12px;
        margin-bottom: 20px;
        display: flex;
        justify-content: space-between;
        align-items: center;
    }
    .header-bar h1 { margin: 0; font-size: 1.6em; color: white; }
    .header-bar .subtitle { color: #aaa; font-size: 0.9em; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────
# Data Loading (Cached)
# ─────────────────────────────────────────────────────────────────────
DATA_DIR = os.path.join(os.getcwd(), "oasis", "data")
DB_PATH = os.path.join(DATA_DIR, "mock_pos_erp.db")
if not os.path.exists(DB_PATH):
    # Fallback to the lightweight database for Cloud deployment
    DB_PATH = os.path.join(DATA_DIR, "mock_pos_erp_lite.db")

@st.cache_resource
def get_connector():
    """Create a cached UniversalConnector to mock DB."""
    uri = f"sqlite:///{DB_PATH}"
    mapper = SchemaMapper.for_pos_erp()
    return UniversalConnector(uri, mapper)

@st.cache_resource
def get_adapter():
    """Create a cached PosErpAdapter."""
    return PosErpAdapter(get_connector())

@st.cache_data(ttl=60)
def load_sales_data(org_cd: str, days: int = 90):
    """Load sales history as DataFrame."""
    adapter = get_adapter()
    return adapter.fetch_sales_history(org_cd, days=days)

@st.cache_data(ttl=60)
def load_products(org_cd: str):
    """Load enriched product list."""
    adapter = get_adapter()
    return adapter.fetch_enriched_products(org_cd)

@st.cache_data(ttl=60)
def load_stock(org_cd: str):
    """Load stock snapshot."""
    adapter = get_adapter()
    return adapter.fetch_stock_snapshot(org_cd)

@st.cache_data(ttl=60)
def load_sales_intel(org_cd: str):
    """Load sales intelligence."""
    adapter = get_adapter()
    return adapter.fetch_sales_intelligence(org_cd, days=300)

@st.cache_data(ttl=300)
def load_all_stocks():
    """Load stock for all orgs in the DB."""
    adapter = get_adapter()
    orgs = adapter.fetch_all_organizations()
    result = {}
    for o in orgs:
        org_cd = o["ORG_CD"]
        result[org_cd] = adapter.fetch_stock_snapshot(org_cd)
    return result

@st.cache_data(ttl=300)
def load_orgs():
    """Load organization list."""
    adapter = get_adapter()
    return adapter.fetch_all_organizations()

@st.cache_resource
def get_order_engine():
    """Create a cached OrderEngine and load local databases."""
    engine = OrderEngine(DATA_DIR)
    engine.load_local_databases()
    return engine

@st.cache_resource
def get_calendar():
    """Load Supplier Calendar."""
    cal_path = os.path.join(os.getcwd(), "Supplier_Order_Calendar_2026.xlsx")
    cal = SupplierCalendar(cal_path)
    cal.load()
    return cal


# ─────────────────────────────────────────────────────────────────────
# Header
# ─────────────────────────────────────────────────────────────────────
st.markdown("""
<div class="header-bar">
    <div>
        <h1>🔮 OASIS Command Center</h1>
        <div class="subtitle">Operations, Allocation, Sales Intelligence & Simulation</div>
    </div>
</div>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────
# Sidebar: Day Simulator
# ─────────────────────────────────────────────────────────────────────
st.sidebar.markdown("## 🎛️ Day Simulator")

# Date Picker
sim_date = st.sidebar.date_input("📅 Simulation Date", value=date(2026, 1, 1))

# Store picker
orgs = load_orgs()
org_names = {o["ORG_CD"]: o["ORG_NAME"] for o in orgs}
selected_org = st.sidebar.selectbox(
    "📍 Store",
    list(org_names.keys()),
    format_func=lambda x: f"{org_names[x]} ({x})"
)

# Time of day
sim_hour = st.sidebar.slider(
    "🕐 Time of Day",
    min_value=6, max_value=22, value=14,
    format="%d:00",
    help="Simulate what the store sees at this hour"
)

# ── IntraDaySimulator: initialise once per session, cache in session_state ──
if 'intraday_sim' not in st.session_state:
    try:
        with st.spinner("🔄 Loading intra-day simulation engine…"):
            st.session_state['intraday_sim'] = IntraDaySimulator.from_db(DB_PATH)
        st.session_state['intraday_sim_error'] = None
    except Exception as _e:
        st.session_state['intraday_sim'] = None
        st.session_state['intraday_sim_error'] = str(_e)

_sim = st.session_state.get('intraday_sim')
_sim_err = st.session_state.get('intraday_sim_error')

# Re-run sim for current hour (cached inside the simulator object)
_sim_state = None
if _sim:
    try:
        _sim_state = _sim.advance_to_hour(sim_hour)
    except Exception as _ex:
        _sim_err = str(_ex)

# 🔴 LIVE SIM badge in sidebar
if _sim:
    n_so = sum(s.n_stockouts for s in _sim_state['hour_stats'].values()) if _sim_state else 0
    n_tr = sum(s.n_transfers for s in _sim_state['hour_stats'].values()) if _sim_state else 0
    st.sidebar.markdown(f"""
    <div style="background:#7b2d2d22; border:1px solid #ef5350; border-radius:8px;
                padding:8px 12px; margin-top:8px; font-size:13px;">
        🔴 <strong>LIVE SIM ACTIVE</strong><br/>
        <span style="color:#888;">{sim_hour:02d}:00 &nbsp;▸&nbsp; {n_so} stockouts &nbsp;▸&nbsp; {n_tr} transfers</span>
    </div>
    """, unsafe_allow_html=True)
elif _sim_err:
    st.sidebar.warning(f"⚠️ Sim: {_sim_err[:60]}")

# Phase indicator
if sim_hour < 10:
    phase = "☀️ **Morning** — Stock Review & Ordering"
    phase_color = "#4a8c1c"
elif sim_hour < 17:
    phase = "🕐 **Midday** — Live Monitoring"
    phase_color = "#2e6ba6"
elif sim_hour < 20:
    phase = "⚡ **Peak** — Alert Response"
    phase_color = "#d94f00"
else:
    phase = "🌙 **Evening** — End-of-Day Close"
    phase_color = "#6b4c9a"

st.sidebar.markdown(f"""
<div style="background: {phase_color}22; border-left: 4px solid {phase_color}; 
            padding: 10px 14px; border-radius: 0 8px 8px 0; margin: 10px 0;">
    {phase}
</div>
""", unsafe_allow_html=True)

# Connection status
st.sidebar.markdown("---")
st.sidebar.markdown("### 🔗 ERP Connection")
if os.path.exists(DB_PATH):
    db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
    st.sidebar.success(f"Connected to Mock POS/ERP  \n`{db_size:.1f} MB`")
else:
    st.sidebar.error("Mock DB not found. Run:  \n`python -m oasis.logic.mock_pos_erp`")
    st.stop()

store_name = org_names.get(selected_org, selected_org)


# ─────────────────────────────────────────────────────────────────────
# Main Tabs
# ─────────────────────────────────────────────────────────────────────
tab_sales, tab_transfer, tab_stock, tab_ordering = st.tabs([
    "📊 Live Sales Feed",
    "🔄 Transfer Intelligence",
    "📦 End-of-Day Stock",
    "🛒 Smart Ordering"
])


# =====================================================================
# TAB 1: LIVE SALES FEED
# =====================================================================
with tab_sales:
    st.markdown(f"### 📊 Live Sales — {store_name}")
    
    # Load sales data (for historical total SKUs count)
    df_sales = load_sales_data(selected_org, days=90)
    
    if df_sales.empty and not _sim_state:
        st.warning("No sales data available.")
    else:
        # If simulation is active, use simulated rows for the current store
        if _sim_state:
            df_visible = pd.DataFrame([
                r for r in _sim_state['sales_rows'] 
                if r['org_cd'] == selected_org
            ])
            # Map column names to match expected schema if needed
            if not df_visible.empty:
                df_visible = df_visible.rename(columns={
                    'itm_cd': 'itm_cd', 'name': 'item_name', 'dept': 'department',
                    'price': 'sell_price', 'revenue': 'net_amt', 'qty': 'qty',
                    'hour': 'sim_hour'
                })
            else:
                df_visible = pd.DataFrame(columns=['itm_cd', 'item_name', 'department', 'qty', 'sell_price', 'net_amt', 'sim_hour'])
        else:
            # Fallback to historical simulation (the original logic)
            df_sales["hour"] = pd.to_datetime(df_sales["bill_dt"]).dt.hour.fillna(12)
            latest_date = df_sales["bill_dt"].max()
            df_today = df_sales[df_sales["bill_dt"] == latest_date].copy()
            if df_today.empty:
                unique_dates = sorted(df_sales["bill_dt"].unique())
                last_dates = unique_dates[-3:] if len(unique_dates) >= 3 else unique_dates
                df_today = df_sales[df_sales["bill_dt"].isin(last_dates)].copy()
            np.random.seed(42)
            n = len(df_today)
            hours = np.clip(np.random.normal(14, 3, n), 6, 22).astype(int)
            df_today["sim_hour"] = hours
            df_visible = df_today[df_today["sim_hour"] <= sim_hour]

        # ── Metrics Row ──
        col1, col2, col3, col4 = st.columns(4)
        total_revenue = df_visible["net_amt"].sum() if not df_visible.empty else 0
        total_units = df_visible["qty"].sum() if not df_visible.empty else 0
        total_txns = len(df_visible)
        unique_items = df_visible["itm_cd"].nunique() if not df_visible.empty else 0
        
        with col1:
            st.markdown(f"""
            <div class="metric-card">
                <h3>Revenue (Today)</h3>
                <div class="value" style="color: #4caf50;">KES {total_revenue:,.0f}</div>
                <div class="sub">Up to {sim_hour}:00</div>
            </div>""", unsafe_allow_html=True)
        with col2:
            st.markdown(f"""
            <div class="metric-card">
                <h3>Units Sold</h3>
                <div class="value" style="color: #2196f3;">{total_units:,.0f}</div>
                <div class="sub">{total_txns:,} line items</div>
            </div>""", unsafe_allow_html=True)
        with col3:
            avg_basket = total_revenue / max(1, total_txns) * 5 if total_txns > 0 else 0
            st.markdown(f"""
            <div class="metric-card">
                <h3>Avg Basket Value</h3>
                <div class="value" style="color: #ff9800;">KES {avg_basket:,.0f}</div>
                <div class="sub">per transaction</div>
            </div>""", unsafe_allow_html=True)
        with col4:
            n_skus_db = unique_items
            n_skus_total = df_sales["itm_cd"].nunique() if not df_sales.empty else 2000
            st.markdown(f"""
            <div class="metric-card">
                <h3>Active SKUs</h3>
                <div class="value" style="color: #ab47bc;">{n_skus_db:,}</div>
                <div class="sub">of {n_skus_total:,} total</div>
            </div>""", unsafe_allow_html=True)

        # ── IntraDay Sim overlay: stockout warning banner ──
        if _sim_state:
            store_so = [e for e in _sim_state['stockouts'] if e.org_cd == selected_org]
            if store_so:
                top5 = sorted(store_so, key=lambda e: e.lost_sales_kes, reverse=True)[:5]
                items_str = ", ".join(f"<em>{e.product_name[:30]}</em>" for e in top5)
                st.markdown(f"""
                <div style="background:#7b2d2d33; border-left:4px solid #ef5350;
                            padding:10px 16px; border-radius:0 8px 8px 0; margin:10px 0;">
                    ⚠️ <strong>{len(store_so)} live stockout(s) at {sim_hour:02d}:00</strong>
                    &mdash; {items_str}
                </div>""", unsafe_allow_html=True)
        
        # ── Hourly Revenue Chart ──
        st.markdown("#### ⏰ Hourly Revenue Pattern")
        if _sim_state:
            chart_data = []
            prev_rev, prev_units = 0.0, 0
            for h in range(6, sim_hour + 1):
                st_h = _sim.advance_to_hour(h)
                stats = st_h['hour_stats'].get(selected_org)
                if stats:
                    rev = stats.total_revenue - prev_rev
                    uni = stats.total_units - prev_units
                    chart_data.append({"sim_hour": h, "revenue": max(0, rev), "units": max(0, uni)})
                    prev_rev = stats.total_revenue
                    prev_units = stats.total_units
                else:
                    chart_data.append({"sim_hour": h, "revenue": 0.0, "units": 0})
            hourly = pd.DataFrame(chart_data)
            _sim_state = _sim.advance_to_hour(sim_hour) # restore
        else:
            if not df_visible.empty:
                hourly = df_visible.groupby("sim_hour").agg(
                    revenue=("net_amt", "sum"),
                    units=("qty", "sum"),
                ).reset_index()
            else:
                hourly = pd.DataFrame(columns=["sim_hour", "revenue", "units"])

        all_hours = pd.DataFrame({"sim_hour": range(6, 23)})
        hourly = all_hours.merge(hourly, on="sim_hour", how="left").fillna(0)
        hourly["status"] = hourly["sim_hour"].apply(
            lambda h: "Past" if h <= sim_hour else "Future"
        )
        
        fig_hourly = px.bar(
            hourly, x="sim_hour", y="revenue", color="status",
            color_discrete_map={"Past": "#4caf50", "Future": "#333333"},
            labels={"sim_hour": "Hour", "revenue": "Revenue (KES)"},
        )
        fig_hourly.update_layout(
            plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
            font_color="#c9d1d9", showlegend=False,
            margin=dict(t=20, b=40, l=60, r=20), height=280,
            xaxis=dict(dtick=1, gridcolor="#1a1a2e"),
            yaxis=dict(gridcolor="#1a1a2e"),
        )
        fig_hourly.add_vline(x=sim_hour, line_dash="dash", line_color="#ff9800", 
                            annotation_text="NOW", annotation_font_color="#ff9800")
        st.plotly_chart(fig_hourly, use_container_width=True)

        # ── Top Movers + Spike Detection ──
        col_movers, col_alerts = st.columns([3, 2])
        if not df_visible.empty:
            with col_movers:
                st.markdown("#### 🔥 Top Movers")
                top_items = df_visible.groupby(["itm_cd", "item_name"]).agg(
                    units=("qty", "sum"),
                    revenue=("net_amt", "sum"),
                ).sort_values("units", ascending=False).head(15).reset_index()
                
                intel = load_sales_intel(selected_org)
                top_items["ads"] = top_items["item_name"].apply(
                    lambda n: intel.get(n, {}).get("avg_daily_sales", 0)
                )
                top_items["velocity_ratio"] = np.where(
                    top_items["ads"] > 0,
                    (top_items["units"] / (top_items["ads"] * (max(0, sim_hour-6)/14))).round(1),
                    0
                )
                
                def color_velocity(val):
                    if val > 3: return "background: #5c1a1a; color: #ff6666;"
                    elif val > 1.5: return "background: #4d3d00; color: #ffc107;"
                    else: return "background: #1a4d1a; color: #4caf50;"
                
                display_df = top_items[["item_name", "units", "revenue", "velocity_ratio"]].copy()
                display_df.columns = ["Product", "Units Sold", "Revenue (KES)", "Velocity Ratio"]
                display_df["Revenue (KES)"] = display_df["Revenue (KES)"].apply(lambda x: f"{x:,.0f}")
                
                st.dataframe(
                    display_df.style.applymap(color_velocity, subset=["Velocity Ratio"]),
                    use_container_width=True, hide_index=True, height=400,
                )
            
            with col_alerts:
                st.markdown("#### ⚠️ Velocity Alerts")
                monitor = AlertMonitor(spike_threshold_pct=200.0)
                spike_items = top_items[top_items["velocity_ratio"] > 2.0]
                realtime_batch = []
                hist_stats = {}
                for _, row in spike_items.iterrows():
                    realtime_batch.append({"sku": row["itm_cd"], "qty": row["units"]})
                    item_intel = intel.get(row["item_name"], {})
                    hist_stats[row["itm_cd"]] = {
                        "avg_daily_sales": item_intel.get("avg_daily_sales", 1),
                        "product_name": row["item_name"],
                    }
                alerts = monitor.check_velocity_spikes(realtime_batch, hist_stats)
                if alerts:
                    for alert in alerts[:5]:
                        st.markdown(f"""
                        <div class="alert-card">
                            <div class="alert-title">⚠️ {alert['type']}</div>
                            <strong>{alert['product_name']}</strong><br/>
                            <span style="font-size: 0.9em;">{alert['message']}</span><br/>
                            <span style="color: #ffa500; font-size: 0.85em;">💡 {alert['recommended_action']}</span>
                        </div>""", unsafe_allow_html=True)
                else:
                    st.markdown("""
                    <div class="metric-card">
                        <h3>Status</h3>
                        <div class="value" style="color: #4caf50; font-size: 1.3em;">✅ All Normal</div>
                        <div class="sub">No velocity spikes detected</div>
                    </div>""", unsafe_allow_html=True)

# =====================================================================
# TAB 2: TRANSFER INTELLIGENCE
# =====================================================================
@st.cache_resource
def get_gnn_resources():
    """Load NetworkSimulator + StoreGraphNetwork if available."""
    network_path = os.path.join(os.getcwd(), "stores_network.json")
    if not os.path.exists(network_path): return None, None
    try:
        import torch
        from network_simulation import NetworkSimulator
        from models.store_gnn import StoreGraphNetwork
        sim = NetworkSimulator(network_path)
        stub = sim.get_feature_matrix()
        model = StoreGraphNetwork(in_features=stub.shape[1], edge_dim=1)
        pt_path = os.path.join(os.getcwd(), "st_gat_v2.pt")
        if os.path.exists(pt_path):
            sd = torch.load(pt_path, map_location="cpu")
            # Logic to handle minor state_dict mismatches if necessary
            model.load_state_dict(sd, strict=False)
        model.eval()
        return model, sim
    except Exception as e:
        return None, str(e)

with tab_transfer:
    st.markdown(f"### 🔄 Transfer Intelligence — Intra-Day Stockout Prevention")

    # ── SECTION 0: Live Simulation Transfer Opportunities ────────────
    if _sim_state:
        st.markdown("#### 🔴 LIVE SIMULATION: Intra-Day Transfers")
        st.caption("Real-time transfer opportunities detected by the simulator as items stock out across the network.")
        
        sim_transfers = [t for t in _sim_state['transfers'] if t.to_org == selected_org]
        
        if sim_transfers:
            cols = st.columns(2)
            for i, t in enumerate(sim_transfers[:6]):
                with cols[i % 2]:
                    urgency_color = "#f44336" if t.urgency == "CRITICAL" else "#ff9800"
                    st.markdown(f"""
                    <div class="transfer-card" style="border-left: 5px solid {urgency_color};">
                        <div style="font-weight:bold; font-size:1.1em;">{t.product_name}</div>
                        <div style="font-size:0.9em; color:#888;">{t.department}</div>
                        <hr style="margin:8px 0; border:0; border-top:1px solid #333;"/>
                        <div style="display:flex; justify-content:space-between;">
                            <span>From: <strong>{t.from_name}</strong></span>
                            <span>Qty: <strong>{t.transfer_qty}</strong></span>
                        </div>
                        <div style="font-size:0.85em; margin-top:4px;">
                            Saves: <span style="color:#4caf50;">KES {t.value_kes:,.0f}</span> in lost sales
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
            if st.button("🚀 Execute Live Sim Transfers", key="exec_sim_xfer"):
                st.success(f"Dispatched {len(sim_transfers)} inter-branch transfers!")
        else:
            st.success("✅ No urgent inter-branch transfers required for the current hour.")
        st.markdown("---")

    # ── SECTION A: Neural-Network Transfer Hub (ST-GAT) ──────────────
    gnn_model, gnn_sim = get_gnn_resources()
    if gnn_model is not None and gnn_sim is not None:
        import torch
        st.markdown("#### 🧠 ST-GAT Network Intelligence")
        # Run GNN inference
        x_t = gnn_sim.get_feature_matrix()
        T = 30
        x_seq = x_t.unsqueeze(0).unsqueeze(0).expand(1, T, -1, -1)
        traffic_mat = gnn_sim.get_traffic_matrix()
        if sim_hour >= 17: traffic_mat = traffic_mat + 0.3
        with torch.no_grad():
            gnn_out = gnn_model(x_seq, gnn_sim.adj, traffic_mat)
        
        gnn_stores = gnn_sim.stores_data
        gnn_ids = [s['store_id'] for s in gnn_stores]
        risk_scores = gnn_out['risk'].squeeze().tolist()
        if not isinstance(risk_scores, list): risk_scores = [risk_scores]
        
        risk_cols = st.columns(min(len(gnn_ids), 5))
        for ci, (sid, rscore) in enumerate(zip(gnn_ids, risk_scores)):
            color = "#f44336" if rscore > 0.7 else ("#ff9800" if rscore > 0.4 else "#4caf50")
            icon = "🔴" if rscore > 0.7 else ("🟠" if rscore > 0.4 else "🟢")
            with risk_cols[ci % len(risk_cols)]:
                st.markdown(f"""
                <div class="metric-card" style="text-align:center;">
                    <h3>{icon} {sid}</h3>
                    <div class="value" style="color:{color};font-size:1.8em;">{rscore:.2f}</div>
                    <div class="sub">Risk Score</div>
                </div>""", unsafe_allow_html=True)
        
        transfer_mat = gnn_out['transfer'][0]
        traffic_sq = traffic_mat.squeeze(-1)
        gnn_recs = []
        for si, src in enumerate(gnn_stores):
            for dj, dst in enumerate(gnn_stores):
                if si == dj: continue
                score = transfer_mat[si, dj].item()
                fric = traffic_sq[si, dj].item()
                if score > 0.45:
                    profit_pulse = score * 1000
                    friction_pen = fric * 400
                    net_gain = profit_pulse - friction_pen
                    gnn_recs.append({
                        "From": src['store_id'], "To": dst['store_id'],
                        "Score": f"{score:.2f}", "Net Gain": f"KES {net_gain:,.0f}",
                        "_net_gain": net_gain
                    })
        if gnn_recs:
            gnn_recs.sort(key=lambda x: -x["_net_gain"])
            st.dataframe(pd.DataFrame(gnn_recs).drop(columns=["_net_gain"]).head(8), use_container_width=True, hide_index=True)
        st.markdown("---")

    # ── SECTION B: Item-Level Intra-Day Heuristic ────────
    st.markdown("#### ⏱️ Item-Level Intra-Day Stockout Risk (ADS Heuristic)")
    all_stocks = load_all_stocks()
    hours_remaining = max(1, 22 - sim_hour)
    comparison_data = []
    for org_cd, stocks in all_stocks.items():
        org_intel = load_sales_intel(org_cd)
        for item in stocks:
            name = item.get("product_name", "Unknown")
            qty = float(item.get("current_stocks", 0))
            ads = org_intel.get(name, {}).get("avg_daily_sales", 0)
            if ads > 0:
                if qty <= 0:
                    # Append 0.0 so we can sort properly
                    comparison_data.append({
                        "Product": name, "Store": org_names.get(org_cd, org_cd),
                        "Stock": 0, "ADS": round(ads, 1), "Hours to SO": 0.0
                    })
                else:
                    hours_to_so = qty / (ads / 16)
                    if hours_to_so <= hours_remaining:
                        comparison_data.append({
                            "Product": name, "Store": org_names.get(org_cd, org_cd),
                            "Stock": qty, "ADS": round(ads, 1), "Hours to SO": round(hours_to_so, 1)
                        })
    if comparison_data:
        df_comp = pd.DataFrame(comparison_data).sort_values("Hours to SO")
        df_comp["Hours to SO"] = df_comp["Hours to SO"].apply(
            lambda x: "ALREADY OUT" if isinstance(x, (int, float)) and x <= 0 else x
        )
        st.dataframe(df_comp, use_container_width=True, hide_index=True)
    else:
        st.success("✅ No heuristic stockouts projected for the rest of the day.")

# =====================================================================
# TAB 3: END-OF-DAY STOCK REVIEW
# =====================================================================
with tab_stock:
    st.markdown(f"### 📦 Stock Review — {store_name}")
    products = load_products(selected_org)
    if products:
        df_p = pd.DataFrame(products)
        
        # Defensive: Ensure required columns exist for calculation
        for col in ["avg_daily_sales", "current_stocks"]:
            if col not in df_p.columns:
                df_p[col] = 0.0
                
        df_p["days_cover"] = np.where(df_p["avg_daily_sales"] > 0, (df_p["current_stocks"] / df_p["avg_daily_sales"]).round(1), 999)
        df_p["health"] = df_p["days_cover"].apply(lambda d: "🔴 Stockout" if d < 0.5 else "🟡 Critical" if d < 2 else "🟢 Healthy" if d < 30 else "⚪ Overstock")
        
        # ── SECTION: Stock Metrics ──
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("🟢 Healthy", len(df_p[df_p["health"] == "🟢 Healthy"]))
        c2.metric("🟡 Critical", len(df_p[df_p["health"] == "🟡 Critical"]))
        c3.metric("🔴 Stockout", len(df_p[df_p["health"] == "🔴 Stockout"]))
        c4.metric("⚪ Overstock", len(df_p[df_p["health"] == "⚪ Overstock"]))
        
        st.markdown("#### 📋 Stock Detail")
        disp_cols = ["product_name", "department", "current_stocks", "avg_daily_sales", "days_cover", "health"]
        df_disp = df_p[disp_cols].sort_values("days_cover").copy()
        df_disp.columns = ["Product", "Department", "Stock Qty", "ADS", "Days Cover", "Health"]
        st.dataframe(df_disp, use_container_width=True, hide_index=True)
        
        st.markdown("#### 📈 Stock Volume vs Demand")
        fig_stock = px.scatter(df_p[df_p["days_cover"] < 100], x="avg_daily_sales", y="current_stocks", color="health",
                               hover_name="product_name", color_discrete_map={"🟢 Healthy": "#4caf50", "🟡 Critical": "#ffc107", "🔴 Stockout": "#f44336", "⚪ Overstock": "#888"})
        fig_stock.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117", font_color="#c9d1d9")
        st.plotly_chart(fig_stock, use_container_width=True)

# =====================================================================
# TAB 4: SMART ORDERING
# =====================================================================
with tab_ordering:
    st.markdown(f"### 🛒 Smart Ordering — {store_name}")
    engine = get_order_engine()
    calendar = get_calendar()
    products = load_products(selected_org)
    if products:
        st.info("OASIS Ordering Engine logic is currently active for replenishment.")
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        
        sim_util = SimulationOrderUtil(DATA_DIR)
        enriched = sim_util.prepare_sku_data(products)
        raw_recs = sim_util.calculate_order_quantity(enriched)
        final_recs = sim_util.finalize_orders(raw_recs)
        
        # ── Consolidated Network Transfer Overlay (NEW) ──
        st.markdown("---")
        enable_network = st.checkbox(
            "🔄 Apply Network Transfer Optimization",
            help="Identifies items that can be fulfilled via inter-branch transfer "
                 "instead of supplier order. Per-store engine logic is NOT modified."
        )

        network_plan = None
        if enable_network:
            with st.spinner("🌐 Running network-level optimization across all stores..."):
                try:
                    # Run all stores' engines to get their raw orders
                    all_store_orders = {}
                    all_stock_data = {}
                    for o in orgs:
                        o_cd = o["ORG_CD"]
                        o_products = load_products(o_cd)
                        if not o_products:
                            continue
                        
                        # Memory Optimization: Trim product data for the network map
                        # We only need key fields for transfer logic
                        trimmed_products = []
                        for p in o_products:
                            trimmed_products.append({
                                'itm_cd': p.get('item_code', p.get('itm_cd')),
                                'product_name': p.get('product_name'),
                                'current_stocks': p.get('current_stocks'),
                                'avg_daily_sales': p.get('avg_daily_sales'),
                                'selling_price': p.get('selling_price'),
                                'department': p.get('department'),
                                'is_fresh': p.get('is_fresh'),
                                'supplier_name': p.get('supplier_name'),
                                'estimated_delivery_days': p.get('estimated_delivery_days')
                            })
                        all_stock_data[o_cd] = trimmed_products
                        
                        # Run per-store engine (UNTOUCHED)
                        o_enriched = sim_util.prepare_sku_data(list(o_products))
                        o_raw = sim_util.calculate_order_quantity(o_enriched)
                        o_final = sim_util.finalize_orders(o_raw)
                        all_store_orders[o_cd] = o_final
                        
                        # Explicitly clear large objects if possible
                        del o_products
                        del o_enriched
                        del o_raw
                        gc.collect()

                    # Build consolidated service
                    cts = ConsolidatedTransferService(
                        org_names=org_names,
                        stock_data=all_stock_data,
                    )
                    network_plan = cts.optimize_network(all_store_orders)

                    # Apply adjusted orders for the selected store
                    if selected_org in network_plan.adjusted_orders:
                        final_recs = network_plan.adjusted_orders[selected_org]
                except Exception as e:
                    st.warning(f"⚠️ Network optimization failed: {e}")
                    network_plan = None

        # ── Network Optimization Results ──
        if network_plan:
            # Summary metrics
            mc1, mc2, mc3, mc4 = st.columns(4)
            with mc1:
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Transfers Found</h3>
                    <div class="value" style="color: #2196f3;">{network_plan.total_items_transferred}</div>
                    <div class="sub">across network</div>
                </div>""", unsafe_allow_html=True)
            with mc2:
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Units via Transfer</h3>
                    <div class="value" style="color: #4caf50;">{network_plan.total_units_transferred:,.0f}</div>
                    <div class="sub">saved from supplier PO</div>
                </div>""", unsafe_allow_html=True)
            with mc3:
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Orders Reduced</h3>
                    <div class="value" style="color: #ff9800;">{network_plan.total_orders_reduced}</div>
                    <div class="sub">items fulfilled by network</div>
                </div>""", unsafe_allow_html=True)
            with mc4:
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Est. Savings</h3>
                    <div class="value" style="color: #4caf50;">KES {network_plan.estimated_savings_kes:,.0f}</div>
                    <div class="sub">vs supplier ordering</div>
                </div>""", unsafe_allow_html=True)

            # Transfer recommendations for this store
            store_transfers = [t for t in network_plan.transfers if t.to_org == selected_org]
            if store_transfers:
                st.markdown("#### 🔄 Incoming Transfers")
                tf_data = [{
                    "Product": t.product_name,
                    "From": org_names.get(t.from_org, t.from_org),
                    "Qty": t.qty,
                    "Urgency": t.urgency,
                    "ETA": f"{t.eta_hours:.0f}h",
                } for t in store_transfers]
                st.dataframe(pd.DataFrame(tf_data), use_container_width=True, hide_index=True)

            # Donor compensation (items this store needs to order extra)
            donor_adds = network_plan.donor_additions.get(selected_org, [])
            if donor_adds:
                st.markdown("#### 📦 Donor Replenishment (Extra Orders)")
                st.caption("These items need to be added to your PO because stock was donated to another branch.")
                da_data = [{
                    "Product": d['product_name'],
                    "Extra Qty": d['recommended_quantity'],
                    "Reason": d['reasoning'],
                } for d in donor_adds]
                st.dataframe(pd.DataFrame(da_data), use_container_width=True, hide_index=True)

            st.markdown("---")

        # ── Standard Order Display (per-store engine output, possibly adjusted) ──
        # UI Toggle
        show_all = st.checkbox("Show Blocked/Zero Quantity Items (Display AI Reasoning)")
        
        if show_all:
            recs_to_show = final_recs
        else:
            recs_to_show = [r for r in final_recs if r.get('recommended_quantity', 0) > 0]
        
        if recs_to_show:
            display_cols = ["product_name", "recommended_quantity", "reasoning"]
            st.dataframe(pd.DataFrame(recs_to_show)[display_cols].head(50), use_container_width=True, hide_index=True)
        else:
            st.info("No orders recommended at this time based on current stock and intelligence.")
            
        # Keep export to only positive orders
        pos_recs = [r for r in final_recs if r.get('recommended_quantity', 0) > 0]
        if pos_recs:
            df_csv = pd.DataFrame(pos_recs)
            csv_data = df_csv.to_csv(index=False)
            st.download_button("📥 Export Purchase Orders", data=csv_data, file_name="po.csv", type="primary")

# ─────────────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.markdown("""
<div style="text-align: center; color: #666; font-size: 0.75em; padding: 10px;">
    OASIS Engine v3.0<br/>
    Operations · Allocation · Sales Intelligence · Simulation<br/>
    © 2026
</div>
""", unsafe_allow_html=True)
