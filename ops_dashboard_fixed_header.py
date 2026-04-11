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
    """Load stock for all orgs."""
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
    """Create a cached OrderEngine and load ERP data."""
    engine = OrderEngine(DATA_DIR)
    connector = get_connector()
    engine.load_from_erp(connector, "ORG001")
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
    
    # Load sales data
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
            avg_basket = total_revenue / max(1, total_txns) * 5  # approx per bill
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
