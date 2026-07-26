"""
OASIS Operations Command Center
================================
A "Day in the Life" demo dashboard showing real-time engine reasoning
across all 4 operational pillars: Sales Monitoring, Transfer Intelligence,
Stock Review, and Smart Ordering.

Run:  streamlit run ops_dashboard.py
"""

import streamlit as st
import os
import sys
import json
import sqlite3
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import asyncio
import random
import math
import io
import gc
import tempfile
import time
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Any, Tuple
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add project root
sys.path.insert(0, os.getcwd())

from oasis.logic.db_connector import UniversalConnector, SchemaMapper, load_system_config, load_system_config_full, save_system_config, ensure_oasis_tables
from oasis.logic.pos_erp_adapter import PosErpAdapter
from oasis.logic.alert_monitor import AlertMonitor
from oasis.logic.order_engine import OrderEngine
from oasis.llm.inference import RuleBasedLLM
from oasis.data.supplier_calendar import SupplierCalendar
from oasis.logic.order_engine import apply_safety_guards
from oasis.simulation.black_swan_events import SupplierRiskAnalyzer, SupplierFailureEvent, SCENARIO_TEMPLATES
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService, NetworkPlan
from oasis.logic.transfer_state import TransferRecord
from oasis.logic.auth_manager import get_user_permissions, get_all_users
from oasis.ui.auth import require_login
from oasis.logic.audit_logger import (
    log_action, get_recent_logs, get_action_summary,
    ACTION_LOGIN, ACTION_LOGOUT, ACTION_PO_GENERATED, ACTION_PO_EXPORTED,
    ACTION_TRANSFER_EXECUTED, ACTION_FILE_PROCESSED, ACTION_CONFIG_CHANGED,
    ENTITY_PO, ENTITY_TRANSFER, ENTITY_FILE, ENTITY_CONFIG, ENTITY_SESSION
)
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

# ── License enforcement (locked installs stop here) ─────────────────
from oasis.logic.license_manager import console_gate as _license_gate  # noqa: E402
_license_gate(st, "core")
from oasis.ui.onboarding import data_source_badge as _src_badge  # noqa: E402
_src_badge(st)

# Cross-links to the sibling consoles (suite polish)
from oasis.ui.home import suite_links as _suite_links  # noqa: E402
_suite_links(st, "command")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=Inter:wght@300;400;500;600;700&display=swap');
    
    :root {
        --glass-bg: rgba(255, 255, 255, 0.03);
        --glass-border: rgba(255, 255, 255, 0.1);
        --neon-emerald: #00ff88;
        --neon-amber: #ffaa00;
        --neon-ruby: #ff4444;
        --deep-space: #0b0e14;
    }

    .stApp { 
        font-family: 'Inter', sans-serif;
        background-color: var(--deep-space);
        color: #e0e0e0;
    }

    h1, h2, h3 { font-family: 'Outfit', sans-serif; letter-spacing: -0.5px; }
    
    /* Glassmorphism Cards */
    .metric-card {
        background: var(--glass-bg);
        backdrop-filter: blur(12px);
        -webkit-backdrop-filter: blur(12px);
        border: 1px solid var(--glass-border);
        border-radius: 16px;
        padding: 24px;
        margin: 10px 0;
        transition: all 0.3s ease;
    }
    .metric-card:hover {
        border-color: rgba(0, 255, 136, 0.3);
        transform: translateY(-2px);
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.8);
    }
    .metric-card h3 { margin: 0; font-size: 0.8em; color: #888; font-weight: 500; text-transform: uppercase; }
    .metric-card .value { font-size: 2.2em; font-weight: 700; margin: 6px 0; color: #fff; }
    .metric-card .sub { font-size: 0.85em; color: #aaa; }
    
    /* Neon Status Widget */
    .pulse-box {
        background: rgba(0, 255, 136, 0.05);
        border: 1px solid rgba(0, 255, 136, 0.2);
        border-radius: 12px;
        padding: 12px;
        margin: 15px 0;
    }
    .pulse-dot {
        height: 8px;
        width: 8px;
        background-color: var(--neon-emerald);
        border-radius: 50%;
        display: inline-block;
        margin-right: 8px;
        box-shadow: 0 0 12px var(--neon-emerald);
        animation: pulse 2s infinite;
    }
    @keyframes pulse {
        0% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(0, 255, 136, 0.7); }
        70% { transform: scale(1); box-shadow: 0 0 0 10px rgba(0, 255, 136, 0); }
        100% { transform: scale(0.95); box-shadow: 0 0 0 0 rgba(0, 255, 136, 0); }
    }

    /* Alert cards (Glass Ruby) */
    .alert-card {
        background: rgba(255, 68, 68, 0.05);
        backdrop-filter: blur(8px);
        border: 1px solid rgba(255, 68, 68, 0.3);
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
    }
    .alert-card .alert-title { color: var(--neon-ruby); font-weight: 600; font-size: 1.1em; }
    
    /* Transfer cards (Glass Azure) */
    .transfer-card {
        background: rgba(0, 136, 255, 0.05);
        backdrop-filter: blur(8px);
        border: 1px solid rgba(0, 136, 255, 0.3);
        border-radius: 12px;
        padding: 16px;
        margin: 8px 0;
    }
    
    /* Premium Header Bar */
    .header-bar {
        background: linear-gradient(90deg, rgba(15,12,41,0.8), rgba(48,43,99,0.8), rgba(36,36,62,0.8));
        backdrop-filter: blur(20px);
        padding: 24px 32px;
        border-radius: 20px;
        margin-bottom: 24px;
        border: 1px solid var(--glass-border);
        display: flex;
        justify-content: space-between;
        align-items: center;
        box-shadow: 0 8px 32px rgba(0,0,0,0.4);
    }
    .header-bar h1 { margin: 0; font-size: 1.8em; font-weight: 700; color: #fff; background: linear-gradient(to right, #fff, #888); -webkit-background-clip: text; -webkit-text-fill-color: transparent; }
    .header-bar .subtitle { color: #888; font-size: 0.95em; margin-top: 4px; }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────
# Data Loading & Production Paths
# ─────────────────────────────────────────────────────────────────────

def load_env_local(env_path=".env"):
    """Simple parser to load .env without external dependencies."""
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                key, value = line.split("=", 1)
                os.environ[key.strip()] = value.strip()

load_env_local()

DATA_DIR = os.getenv("DATA_DIR", os.path.join(os.getcwd(), "oasis", "data"))
DB_PATH = os.getenv("OASIS_DB_PATH", os.path.join(DATA_DIR, "mock_pos_erp.db"))
showcase_mode = os.getenv('OASIS_SHOWCASE_MODE', 'false').lower() == 'true'
# Live run: time-of-day is not dragged on a slider — it accrues automatically in
# line with the mock POS stream (the more sales rung up today, the later the day).
LIVE_MODE = os.getenv('OASIS_LIVE_MODE', 'false').lower() == 'true'
# Number of today's bills that maps to a full trading day (06:00 → 22:00).
LIVE_FULL_DAY_BILLS = int(os.getenv('OASIS_LIVE_FULL_DAY_BILLS', '1000'))


def _bills_today(db_path: str) -> int:
    """Count today's POS bills in the live snapshot (drives the auto clock)."""
    import sqlite3
    try:
        c = sqlite3.connect(db_path, timeout=10.0)
        try:
            n = c.execute("SELECT COUNT(*) FROM POS_SALES_HDR "
                          "WHERE BILL_DT = date('now','localtime')").fetchone()[0]
            return int(n or 0)
        finally:
            c.close()
    except Exception:
        return 0


def _live_sim_hour(db_path: str):
    """(sim_hour, bills_today): the trading hour implied by sales accrued so far."""
    bills = _bills_today(db_path)
    frac = min(1.0, bills / float(max(1, LIVE_FULL_DAY_BILLS)))
    hour = int(round(6 + frac * 16))          # 06:00 (open) → 22:00 (close)
    return max(6, min(22, hour)), bills

if not os.path.exists(DB_PATH) and not os.path.isabs(DB_PATH):
    # Fallback check
    alt_path = os.path.join(DATA_DIR, "mock_pos_erp_lite.db")
    if os.path.exists(alt_path):
        DB_PATH = alt_path

# Ensure OASIS auth/audit/config tables exist (migration-safe)
@st.cache_resource
def _init_db_schema(path: str):
    ensure_oasis_tables(path)
    return True

_init_db_schema(DB_PATH)

REGISTRY_PATH = os.path.join(DATA_DIR, "transfers_registry.json")

@st.cache_resource
def get_connector():
    """Cached UniversalConnector for OASIS's own store (users/audit/config +
    PO/transfer queues). Honors OASIS_DB_URL; falls back to the local DB_PATH.
    """
    from oasis.logic import db as oasis_db
    if os.getenv("OASIS_DB_URL"):
        uri = oasis_db.get_sqlalchemy_url()
    else:
        uri = f"sqlite:///{DB_PATH}"
    mapper = SchemaMapper.for_pos_erp()
    return UniversalConnector(uri, mapper)

@st.cache_resource
def get_pos_connector():
    """Cached UniversalConnector for the read-only POS/ERP *source* DB.

    Only distinct from the store when a POS is actually configured — by
    OASIS_POS_DB_URL or the first-run wizard's "Connect a POS" choice;
    otherwise the caller falls back to the single store connector (demo).
    """
    from oasis.logic import db as oasis_db
    mapper = SchemaMapper.for_pos_erp()
    return UniversalConnector(oasis_db.get_pos_sqlalchemy_url(), mapper)

@st.cache_resource
def get_adapter():
    """Cached PosErpAdapter: POS source for reads, OASIS store for the queues."""
    from oasis.logic import db as oasis_db
    if oasis_db.has_distinct_pos():          # env var OR wizard choice (S2)
        return PosErpAdapter(get_pos_connector(), get_connector())
    return PosErpAdapter(get_connector())

@st.cache_resource
def get_distance_map():
    """Load store coordinates for distance-aware transfers."""
    try:
        path = os.path.join(os.getcwd(), "store_coords.json")
        if os.path.exists(path):
            with open(path, 'r') as f:
                return json.load(f)
    except Exception as e:
        st.error(f"Error loading distance map: {e}")
    return {}

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

@st.cache_data(ttl=600)
def load_network_stock(org_cds: List[str]):
    """Load enriched product data for all stores in the network at once."""
    adapter = get_adapter()
    all_data = {}
    for org_cd in org_cds:
        try:
            all_data[org_cd] = adapter.fetch_enriched_products(org_cd)
        except Exception as e:
            logger.error(f"Failed to load products for {org_cd}: {e}")
            all_data[org_cd] = []
    return all_data

@st.cache_data(ttl=60)
def load_sales_intel(org_cd: str):
    """Load sales intelligence."""
    adapter = get_adapter()
    return adapter.fetch_sales_intelligence(org_cd, days=300)

@st.cache_data(ttl=3600)
def _cached_ads_map(org_cd: str):
    """Load ADS map per org using enriched products (which carry computed avg_daily_sales).
    NOTE: fetch_product_master returns 0 ADS for all items — use fetch_enriched_products.
    """
    adapter = get_adapter()
    try:
        products = adapter.fetch_enriched_products(org_cd)
        return {p.get('item_code', ''): float(p.get('avg_daily_sales', 0.0)) for p in products}
    except Exception as e:
        logger.warning(f"_cached_ads_map failed for {org_cd}: {e}")
        return {}

@st.cache_data(ttl=120)
def get_all_store_risks(sim_hour: int):
    """Run GNN to get risk scores for all stores.

    Cached (2 min TTL): multiple tabs call this per render and the torch
    inference + network-wide stock load is the most expensive computation
    in the app. The sidebar advances the simulator to the selected hour
    before any tab renders, so skipping the internal advance on a cache
    hit does not change simulator state.
    """
    gnn_model, gnn_sim = get_gnn_resources()
    if gnn_model is None or gnn_sim is None:
        return {}
    
    import torch
    import streamlit as st
    try:
        x_t = gnn_sim.get_feature_matrix()
        # --- DYNAMIC INVENTORY INJECTION ---
        all_stocks = load_all_stocks()
        
        # Check for live simulator state
        _sim = None
        if hasattr(st, 'session_state') and 'intraday_sim' in st.session_state:
            _sim = st.session_state['intraday_sim']
            
        _sim_state = None
        if _sim is not None:
            try:
                _sim_state = _sim.advance_to_hour(sim_hour)
            except Exception:
                pass

        for i, src in enumerate(gnn_sim.stores_data):
            store_id = src.get('store_id', '')
            org_cd = store_id.replace('CFP-', 'ORG')
            stocks = all_stocks.get(org_cd, [])
            
            # --- INJECT ADS ---
            ads_map = _cached_ads_map(org_cd)
            for item in stocks:
                ic = item.get('item_code', '')
                if ic in ads_map:
                    item['avg_daily_sales'] = ads_map[ic]
            # ------------------
            
            so_ratio = 0.0
            crit_ratio = 0.0
            
            if stocks:
                active_skus = sum(1 for item in stocks if float(item.get('avg_daily_sales', 0)) > 0 or float(item.get('current_stocks', 0)) > 0)
                denom = max(1, active_skus)
            else:
                denom = 1
                
            if _sim_state and stocks:
                stats = _sim_state['hour_stats'].get(org_cd)
                if stats:
                    so_ratio = stats.n_stockouts / denom
                    crit_ratio = getattr(stats, 'n_critical', 0) / denom
            elif stocks:
                # FIX: Raw stock has tiny decimals (0.0024) that are never <= 0.
                # Treat stock < 1.0 as effectively stocked-out for risk scoring.
                # Also use days-cover for critical: items with < 3 days cover are critical.
                so_count = 0
                crit_count = 0
                for item in stocks:
                    curr = float(item.get('current_stocks', 0))
                    ads = float(item.get('avg_daily_sales', 0))
                    if ads > 0:
                        days_cov = curr / ads
                        if curr < 1.0 or days_cov < 0.5:
                            so_count += 1
                        elif days_cov < 3.0:
                            crit_count += 1
                    elif curr < 1.0:
                        so_count += 1  # ADS=0 but depleted stock
                so_ratio = so_count / denom
                crit_ratio = crit_count / denom
                
            # Inject without artificial 10x amplification to avoid OOD saturation
            x_t[i, 24] = so_ratio
            x_t[i, 25] = crit_ratio
            
            # Save for inventory heuristic calculation later
            src['_so_ratio'] = so_ratio
            src['_crit_ratio'] = crit_ratio

        # -----------------------------------
        # H1 FIX: Use canonical 2-arg GCN forward (x_t, edge_index)
        with torch.no_grad():
            gnn_out = gnn_model(x_t, gnn_sim.edge_index)
        
        gnn_stores = gnn_sim.stores_data
        gnn_ids = [s['store_id'] for s in gnn_stores]
        risk_scores = gnn_out['risk'].squeeze().tolist()
        if not isinstance(risk_scores, list): risk_scores = [risk_scores]
        
        # Determine blend ratio from config
        config_data = load_system_config(DB_PATH)
        gnn_blend = float(config_data.get('gnn_risk_blend_ratio', 0.5))
        inventory_blend = 1.0 - gnn_blend
        
        # If GNN model is untrained (all scores identical), rely entirely on inventory heuristic
        _gnn_uniform = (max(risk_scores) - min(risk_scores)) < 0.02
        
        # Shared inventory-risk + sigmoidal-brake blend (SH-A S2 delegation).
        # Math is identical to the former inline block; the so/crit ratios are
        # still derived here (incl. the live intraday-sim hour_stats path above),
        # only the formula now lives in one place.
        from oasis.logic import gnn_service
        final_scores = {}
        for i, (sid, rscore) in enumerate(zip(gnn_ids, risk_scores)):
            src = gnn_stores[i]
            so_ratio = src.get('_so_ratio', 0.0)
            crit_ratio = src.get('_crit_ratio', 0.0)
            inv_risk = gnn_service.risk_from_ratios(so_ratio, crit_ratio)
            final_scores[sid] = gnn_service.blend_risk(
                rscore, inv_risk, gnn_blend, gnn_uniform=_gnn_uniform)

        return final_scores
    except Exception as e:
        logger.error(f"GNN risk calculation failed: {e}")
        return {}

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


# ── Authentication Gate ──
# Goes through the shared oasis.ui.auth gate so this console takes part in suite
# SSO. require_login() adopts a ?sid= handed over by a sibling console before it
# ever renders a login form; previously this file ran an entirely parallel auth
# stack (its own login screen calling authenticate() directly), so the suite bar
# linked here WITH a sid and the user still landed on a second login form
# (deep-analysis finding S5). The session key is the same either way —
# oasis.ui.auth.USER_KEY is "user" — so everything below reads unchanged.

if showcase_mode and not st.session_state.get("user"):
    st.toast("🛡️ Showcase Mode Active: sign in as 'ops_admin'.", icon="🔐")
    st.caption("🔒 **System Isolation Enabled (Showcase Mode)** — demo accounts: "
               "`ops_admin` (full), `regional_mgr`, `branch_mgr`. "
               "Password: value of `OASIS_SEED_PASSWORD`.")

require_login(st, DB_PATH, app_title="Command Center")

# ── User is authenticated ──
# Fallback for import-time or unauthenticated state to prevent subscript errors
current_user = st.session_state.get('user')
if not current_user:
    # CC-A fix: fail CLOSED. This branch is only reached on import/unauthenticated
    # state (the auth gate st.stop()s first on the normal run path); it must not
    # grant any tab access if it is ever reached.
    current_user = {
        'username': 'system_import',
        'display_name': 'System Import',
        'role': 'guest',
        'permissions': {'tabs': {}, 'can_view_all_stores': False}
    }

user_perms = current_user.get('permissions', {})
user_role = current_user.get('role', 'unknown')
user_org = current_user.get('assigned_org')  # None for regional/admin

# ─────────────────────────────────────────────────────────────────────
# Header (with user info)
# ─────────────────────────────────────────────────────────────────────
role_labels = {
    'ops_admin': '🔧 Operations Admin',
    'regional_manager': '🌐 Regional Manager',
    'branch_manager': '🏪 Branch Manager'
}

# ── Notification System ──
from oasis.logic.notification_service import NotificationService

if 'notification_service' not in st.session_state:
    st.session_state['notification_service'] = NotificationService(get_connector(), None)

# ── Scheduler Service ──
from oasis.logic.scheduler_service import OasisScheduler
if 'oasis_scheduler' not in st.session_state:
    st.session_state['oasis_scheduler'] = OasisScheduler(DB_PATH)
    
notif_service = st.session_state['notification_service']

# Determine context for alerts
alert_org_cd = None if user_perms.get("can_view_all_stores") else user_org
active_alerts = notif_service.get_active_alerts(org_cd=alert_org_cd, user_role=user_role, username=current_user['username'])

unread_alerts = [a for a in active_alerts if not a.get("is_read")]

if unread_alerts:
    # Use a toast for the newest unread alert as a push notification paradigm
    st.toast(f"🔔 {len(unread_alerts)} new system alert(s)! {unread_alerts[0]['title']}", icon="🔔")

# Contextual Branding
store_name = user_org if user_org else "Network"
if user_org == "ORG001": store_name = "Rhapta Road"
elif user_org == "ORG002": store_name = "Flagship Store"

col_title, col_alerts = st.columns([5, 1])

# Replace the old static header HTML with dynamic layout
st.markdown(f"""
<div class="header-bar">
    <div style="display: flex; align-items: center;">
        <div style="margin-right: 20px;">
            <h1 style="background: linear-gradient(135deg, #00ff88 0%, #0088ff 100%); -webkit-background-clip: text; -webkit-text-fill-color: transparent; font-size: 2.2em; font-weight: 800; margin: 0;">O.A.S.I.S.</h1>
            <div style="color: #888; font-size: 0.8em; text-transform: uppercase; letter-spacing: 2px; margin-top: -5px;">Autonomous Supply Intelligence System</div>
        </div>
        <div style="height: 40px; width: 1px; background: rgba(255,255,255,0.1); margin: 0 25px;"></div>
        <div>
            <div style="color: #666; font-size: 0.75em; text-transform: uppercase; letter-spacing: 1px;">Neural Activity</div>
            <div style="color: #00ff88; font-size: 0.85em; display: flex; align-items: center;">
                <span class="pulse-dot" style="height: 6px; width: 6px; margin-bottom: 0;"></span>
                Analyzing {store_name} Logstream...
            </div>
        </div>
    </div>
    <div style="text-align: right;">
        <div style="color: #fff; font-weight: 600; font-size: 1.1em; font-family: 'Outfit';">{current_user['display_name']}</div>
        <div style="color: #0088ff; font-size: 0.75em; font-weight: 700; text-transform: uppercase; letter-spacing: 1px;">{role_labels.get(user_role, user_role)}</div>
    </div>
</div>
""", unsafe_allow_html=True)

with col_title:
    pass # Empty to push alerts to the right

with col_alerts:
    if active_alerts:
        btn_label = f"🔔 Alerts ({len(unread_alerts)})" if unread_alerts else "🔕 Inbox (0)"
        type_str = "primary" if unread_alerts else "secondary"
        with st.popover(btn_label):
            st.markdown("### System Notifications")
            for alert in active_alerts:
                # Basic styling for alerts
                color = "#f44336" if alert['urgency'] == "HIGH" else "#ff9800"
                st.markdown(f"<div style='border-left: 3px solid {color}; padding-left: 10px; margin-bottom: 10px;'>"
                            f"<strong>{alert['title']}</strong><br/>"
                            f"<span style='font-size:0.85em;color:#aaa;'>{alert['message']}</span><br/>"
                            f"<span style='font-size:0.7em;color:#666;'>{alert['timestamp']}</span>"
                            f"</div>", unsafe_allow_html=True)
                if not alert['is_read']:
                    notif_service.mark_as_read(current_user['username'], alert['id'])
            if st.button("Dismiss All", use_container_width=True):
                st.rerun()

st.markdown("<br/>", unsafe_allow_html=True)


# ─────────────────────────────────────────────────────────────────────
# Sidebar: Day Simulator & Narrative
# ─────────────────────────────────────────────────────────────────────
if showcase_mode:
    st.sidebar.markdown("## 📖 Showcase Narrative")
    with st.sidebar.expander("The Scenario", expanded=True):
        st.markdown("""
        **The Crisis**: High-impact stockouts in Fresh & Staples are costing **18.4%** in lost revenue. 
        Meanwhile, capital is trapped in "Dead Stock" (Slow-movers).
        
        **The Oasis Fix**:
        1. **Detect** demand spikes in real-time.
        2. **Rebalance** surplus using Inter-Branch Transfers.
        3. **Optimize** POs based on predictive velocity.
        """)
    st.sidebar.markdown("---")

st.sidebar.markdown("## 🎛️ Multi-Day Simulator")

# Date Picker — drives multi-day simulation
_BASE_SIM_DATE = date(2026, 1, 1)
sim_date = st.sidebar.date_input("📅 Simulation Date", value=_BASE_SIM_DATE)
sim_day = max(1, (sim_date - _BASE_SIM_DATE).days + 1)

# Store picker (role-filtered)
orgs = load_orgs()
org_names = {o["ORG_CD"]: o["ORG_NAME"] for o in orgs}

# Branch managers can only see their assigned store
if not user_perms['can_view_all_stores'] and user_org:
    available_orgs = [user_org] if user_org in org_names else list(org_names.keys())[:1]
else:
    available_orgs = list(org_names.keys())

selected_org = st.sidebar.selectbox(
    "📍 Store",
    available_orgs,
    format_func=lambda x: f"{org_names.get(x, x)} ({x})"
)

# Time of day — auto-accrues with the live POS in a live run; manual slider otherwise
if LIVE_MODE:
    sim_hour, _bills_today_n = _live_sim_hour(DB_PATH)
    _close_pct = min(100, int(100 * _bills_today_n / max(1, LIVE_FULL_DAY_BILLS)))
    st.sidebar.markdown(
        f"""<div style="background:#1c3a5e22; border:1px solid #2e6ba6; border-radius:8px;
                    padding:8px 12px;">
            🟢 <strong>LIVE — time of day auto-accruing</strong><br/>
            <span style="font-size:20px; font-weight:700;">{sim_hour:02d}:00</span>
            <span style="color:#888;"> &nbsp;·&nbsp; {_bills_today_n:,} sales today &nbsp;·&nbsp; {_close_pct}% of trading day</span>
        </div>""", unsafe_allow_html=True)
    st.sidebar.caption("Tracks the mock POS stream — pull latest to advance. "
                       "Set OASIS_LIVE_FULL_DAY_BILLS to change the day length.")
    
    auto_refresh = st.sidebar.checkbox("⏱ Auto-refresh (Live Streaming)", value=True)
    st.session_state['auto_refresh'] = auto_refresh

    if auto_refresh:
        refresh_sec = st.sidebar.slider("Refresh Interval (seconds)", 5, 60, 10, help="Increase this if the app is lagging while rendering metrics")
        from streamlit_autorefresh import st_autorefresh
        st.cache_data.clear() # Clear cache on refresh so we get live data
        st_autorefresh(interval=refresh_sec * 1000, key="data_autorefresh")

    if st.sidebar.button("🔄 Pull latest POS bills", use_container_width=True):
        st.cache_data.clear()   # bust TTL caches so the just-committed bills show
        st.rerun()
else:
    sim_hour = st.sidebar.slider(
        "🕐 Time of Day",
        min_value=6, max_value=22, value=14,
        format="%d:00",
        help="Simulate what the store sees at this hour"
    )

# ── IntraDaySimulator ───────────────────────────────────────────────────────
# Live run: read the POS stream DIRECTLY. The synthetic intra-day simulator is
# bypassed entirely so every tab shows real streamed bills/stock (not modelled
# rows) — this is what makes the dashboard react to live sale signals.
if LIVE_MODE:
    st.session_state.pop('intraday_sim', None)
    _sim = None
    _sim_state = None
    _sim_err = None
else:
    # initialise once per session, cache in session_state
    if 'intraday_sim' not in st.session_state or st.sidebar.button("♻️ Reset Simulator"):
        try:
            with st.spinner("🔄 Loading intra-day simulation engine…"):
                _sim_db = os.path.join(DATA_DIR, "mock_pos_erp_showcase.db")
                if not os.path.exists(_sim_db):
                    _sim_db = DB_PATH
                st.session_state['intraday_sim'] = IntraDaySimulator.from_db(_sim_db, registry_path=REGISTRY_PATH)
            st.session_state['intraday_sim_error'] = None
        except Exception as _e:
            st.session_state['intraday_sim'] = None
            st.session_state['intraday_sim_error'] = str(_e)

    _sim = st.session_state.get('intraday_sim')
    _sim_err = st.session_state.get('intraday_sim_error')

    # Advance to the selected day first, then to the selected hour
    _sim_state = None
    if _sim:
        try:
            if sim_day > 1:
                _sim.advance_to_day(sim_day)
            _sim_state = _sim.advance_to_hour(sim_hour)
        except Exception as _ex:
            _sim_err = str(_ex)

# 🔴 LIVE SIM badge in sidebar
if _sim:
    n_so = sum(s.n_stockouts for s in _sim_state['hour_stats'].values()) if _sim_state else 0
    n_tr = sum(s.n_transfers for s in _sim_state['hour_stats'].values()) if _sim_state else 0
    _day_label = f"Day {sim_day} · " if sim_day > 1 else ""
    st.sidebar.markdown(f"""
    <div style="background:#7b2d2d22; border:1px solid #ef5350; border-radius:8px;
                padding:8px 12px; margin-top:8px; font-size:13px;">
        🔴 <strong>LIVE SIM ACTIVE</strong><br/>
        <span style="color:#888;">{_day_label}{sim_hour:02d}:00 &nbsp;▸&nbsp; {n_so} stockouts &nbsp;▸&nbsp; {n_tr} transfers</span>
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

# Connection status with health check
st.sidebar.markdown("---")
st.sidebar.markdown("### 🔗 ERP Connection")
if os.path.exists(DB_PATH):
    try:
        connector = get_connector()
        health = connector.health_check()
        db_size = os.path.getsize(DB_PATH) / (1024 * 1024)
        status_color = "#00ff88" if health['status'] == 'healthy' else "#ffaa00"
        
        st.sidebar.markdown(f"""
        <div class="pulse-box">
            <div style="display: flex; align-items: center; margin-bottom: 8px;">
                <span class="pulse-dot"></span>
                <span style="font-size: 0.9em; font-weight: 500; color: #fff;">O.A.S.I.S. Neural Pulse</span>
            </div>
            <div style="font-size: 0.8em; color: #888; margin-left: 16px;">
                Engine State: <span style="color: #00ff88;">OPTIMAL</span><br/>
                Sync Latency: <span style="color: #00ff88;">{health['latency_ms']}ms</span>
            </div>
        </div>
        
        <div style="background: rgba(255,255,255,0.02); border: 1px solid {status_color}33; border-radius:12px; padding:12px; font-size:13px; margin-top: 10px;">
            <div style="color: {status_color}; font-weight: 600; margin-bottom: 4px; display: flex; align-items: center;">
                <span style="margin-right: 6px;">🔗</span> ERP UPLINK: {health['status'].upper()}
            </div>
            <div style="color: #888;">
                Tables: {health['tables_found']} mapped<br/>
                DB Load: {db_size:.1f} MB
            </div>
        </div>
        """, unsafe_allow_html=True)
    except Exception as e:
        st.sidebar.error(f"🔴 Uplink Failure: {str(e)[:60]}")
else:
    st.sidebar.error("Database connection unavailable.")

# Master Control Hub Uplink
st.sidebar.markdown("---")
st.sidebar.markdown("### 🌐 Master Hub Uplink")
hub_status_color = "#00ff88"
st.sidebar.markdown(f"""
<div style="background: rgba(255,255,255,0.02); border: 1px solid {hub_status_color}33; border-radius:12px; padding:12px; font-size:13px; margin-top: 10px;">
    <div style="color: {hub_status_color}; font-weight: 600; margin-bottom: 4px; display: flex; align-items: center;">
        <span style="margin-right: 6px;">🛰️</span> HUB STATE: ACTIVE
    </div>
    <div style="color: #888;">
        Last Pulse: 45s ago<br/>
        Anonymization: <span style="color: #00ff88;">ENABLED</span>
    </div>
</div>
""", unsafe_allow_html=True)

# Proactive Rebalancing Settings
st.sidebar.markdown("---")
st.sidebar.markdown("### ⚙️ Push Rebalancing Settings")
if 'cold_node_days' not in st.session_state:
    st.session_state['cold_node_days'] = 60
if 'hot_node_days' not in st.session_state:
    st.session_state['hot_node_days'] = 14

st.session_state['cold_node_days'] = st.sidebar.number_input(
    "Cold Node Threshold (> Days)", min_value=30, max_value=365, value=st.session_state['cold_node_days'], step=5,
    help="SKUs with more than this many days of coverage will be considered dead capital (Cold Node) and eligible for proactive push transfers."
)
st.session_state['hot_node_days'] = st.sidebar.number_input(
    "Hot Node Threshold (< Days)", min_value=1, max_value=60, value=st.session_state['hot_node_days'], step=1,
    help="SKUs with less than this many days of coverage in a store with steady sales will be eligible to receive proactive push transfers."
)

# Daily Pipeline
st.sidebar.markdown("---")
st.sidebar.markdown("### 🛠️ Automation")
if st.sidebar.button("▶️ Run Daily Pipeline", use_container_width=True):
    with st.spinner("Executing Daily Pipeline..."):
        from oasis.logic.daily_pipeline import DailyPipeline
        pipeline = DailyPipeline({
            'data_dir': DATA_DIR,
            'shadow_mode': True,
            'amit_enabled': True,
            'lata_enabled': True,
            'dharam_enabled': True,
            'scorecard_path': os.path.join(DATA_DIR, "Full_Product_Allocation_Scorecard_vSim.csv")
        })
        result = pipeline.run_daily_cycle()
        if result.get('status') == 'COMPLETED':
            st.sidebar.success("Pipeline Completed!")
        else:
            st.sidebar.error(f"Pipeline Failed: {result.get('status')}")

# Logout button
st.sidebar.markdown("---")
if st.sidebar.button("🚪 Logout", use_container_width=True):
    log_action(DB_PATH, current_user['username'], ACTION_LOGOUT, ENTITY_SESSION)
    st.session_state['user'] = None
    st.rerun()

# Audit Log viewer (ops_admin and regional_manager)
if user_perms.get('can_view_audit_log'):
    with st.sidebar.expander("📋 Recent Activity", expanded=False):
        audit_df = get_recent_logs(DB_PATH, limit=20)
        if not audit_df.empty:
            for _, row in audit_df.iterrows():
                action_icon = {
                    'LOGIN': '🔑', 'LOGOUT': '🚪', 'PO_GENERATED': '📋',
                    'PO_EXPORTED': '📥', 'TRANSFER_EXECUTED': '🔄',
                    'FILE_PROCESSED': '📂', 'CONFIG_CHANGED': '⚙️', 'PO_APPROVED': '✅'
                }.get(row.get('ACTION', ''), '📌')
                ts = str(row.get('CREATED_DT', ''))[:16]
                st.markdown(
                    f"<div style='font-size:0.8em; color:#aaa; padding:2px 0;'>"
                    f"{action_icon} <strong>{row.get('USERNAME','')}</strong> · {row.get('ACTION','')} · {ts}</div>",
                    unsafe_allow_html=True
                )
        else:
            st.caption("No recent activity.")

store_name = org_names.get(selected_org, selected_org)


# ─────────────────────────────────────────────────────────────────────
# Main Tabs (Role-Based)
# ─────────────────────────────────────────────────────────────────────
tab_labels = []
tab_keys = []

if user_perms['tabs'].get('executive_roi') or showcase_mode:
    tab_labels.insert(0, "🏆 Executive ROI Overview")
    tab_keys.insert(0, "executive_roi")
if user_perms['tabs'].get('live_sales'):
    tab_labels.append("📊 Live Sales Feed")
    tab_keys.append("live_sales")
if user_perms['tabs'].get('transfer_intelligence'):
    tab_labels.append("🔄 Transfer Intelligence")
    tab_keys.append("transfer_intelligence")
if user_perms['tabs'].get('stock_review'):
    tab_labels.append("📦 End-of-Day Stock")
    tab_keys.append("stock_review")
if user_perms['tabs'].get('smart_ordering'):
    tab_labels.append("🛒 Smart Ordering")
    tab_keys.append("smart_ordering")
if user_perms['tabs'].get('oasis_processor'):
    tab_labels.append("🚀 OASIS Processor")
    tab_keys.append("oasis_processor")
if user_perms['tabs'].get('allocation_engine'):
    tab_labels.append("🧮 Allocation Engine")
    tab_keys.append("allocation_engine")
if user_perms['tabs'].get('simulation_validation'):
    tab_labels.append("🧪 Simulation Lab")
    tab_keys.append("simulation_validation")
if user_perms['tabs'].get('analytics'):
    tab_labels.append("📈 Analytics")
    tab_keys.append("analytics")
if user_perms['tabs'].get('settings'):
    tab_labels.append("⚙️ Settings")
    tab_keys.append("settings")

# Always show Supplier Intelligence for ops_admin and regional_manager
if user_role in ('ops_admin', 'regional_manager') or showcase_mode:
    tab_labels.append("🔬 Supplier Intelligence")
    tab_keys.append("supplier_intelligence")
if not tab_labels:
    tab_labels.append("Dashboard")
    tab_keys.append("default")

tabs = st.tabs(tab_labels)
tab_map = {key: tabs[i] for i, key in enumerate(tab_keys)}

# ── Module gating: premium tabs render an upsell stub when unlicensed ──
from oasis.logic.license_manager import allowed_modules as _allowed_modules  # noqa: E402
from oasis.logic.license_manager import render_upsell as _render_upsell  # noqa: E402

TAB_MODULES = {
    "smart_ordering": "ordering",
    "supplier_intelligence": "ordering",
    "transfer_intelligence": "network",
    "allocation_engine": "network",
}
if "_allowed_mods" not in st.session_state:
    st.session_state["_allowed_mods"] = _allowed_modules()
_MODS = st.session_state["_allowed_mods"]


def _mod_ok(tab_key: str) -> bool:
    return TAB_MODULES.get(tab_key, "core") in _MODS


for _key, _mod in TAB_MODULES.items():
    if _key in tab_map and _mod not in _MODS:
        with tab_map[_key]:
            _render_upsell(st, _mod)

# ─────────────────────────────────────────────────────────────────────
# TAB: Executive ROI Overview (THE SHOWCASE)
# ─────────────────────────────────────────────────────────────────────
if "executive_roi" in tab_map:
    with tab_map["executive_roi"]:
        st.markdown(f"### 🏆 Executive ROI Overview — {store_name}")

        # CC-C fix: compute real network figures from live stock instead of
        # hard-coding them. Dead-stock (AMIT rule: ADS<0.2 & SOH>15) and
        # stockout (ADS>0 & SOH<1) counts, plus capital trapped in dead stock
        # (the recoverable figure), all from the enriched product feed.
        _roi_products = load_products(selected_org) or []
        _roi_total_skus = len(_roi_products)
        _roi_dead = 0
        _roi_trapped = 0.0
        _roi_stockout = 0
        for _p in _roi_products:
            _ads = float(_p.get('avg_daily_sales', 0) or 0)
            _soh = float(_p.get('current_stocks', _p.get('current_stock', 0)) or 0)
            _cost = float(_p.get('cost_price', _p.get('wac', 0)) or 0) or \
                float(_p.get('selling_price', 0) or 0) * 0.75
            if _ads < 0.2 and _soh > 15:
                _roi_dead += 1
                _roi_trapped += _soh * _cost
            if _ads > 0 and _soh < 1:
                _roi_stockout += 1
        _roi_dead_pct = round(_roi_dead / _roi_total_skus * 100, 1) if _roi_total_skus else 0.0
        _roi_so_pct = round(_roi_stockout / _roi_total_skus * 100, 1) if _roi_total_skus else 0.0

        # Pull showcase data from config if exists
        showcase_savings = "0"
        is_showcase = showcase_mode
        try:
            config_data = load_system_config(DB_PATH)
            if 'showcase_roi_savings' in config_data:
                showcase_savings = config_data['showcase_roi_savings']
                is_showcase = True
        except Exception:
            showcase_savings = '0'
            is_showcase = False

        if is_showcase:
            st.success(f"🌟 **Demo Showcase Active**: High-impact scenario loaded for {store_name}.")
        
        # --- LIVE NETWORK SNAPSHOT (real figures from the enriched feed) ---
        _dead_color = "#00ff88" if _roi_dead_pct < 5 else "#ff4444"
        _so_color = "#00ff88" if _roi_so_pct < 2 else "#ff4444"
        st.markdown(f"""
        <div style="background: rgba(0, 255, 136, 0.02); border: 1px solid rgba(0, 255, 136, 0.1); border-radius: 20px; padding: 25px; margin-bottom: 30px; box-shadow: 0 4px 24px rgba(0,0,0,0.2);">
            <div style="display: flex; justify-content: space-between; align-items: start;">
                <div>
                    <h4 style="margin: 0; color: var(--neon-emerald); font-size: 0.9em; text-transform: uppercase; letter-spacing: 1px;">Live Network Snapshot</h4>
                    <p style="color: #888; font-size: 0.8em; margin-top: 4px;">{_roi_total_skus:,} active SKUs analyzed for {store_name}.</p>
                </div>
                <div style="text-align: right;">
                    <span class="badge-green">LIVE DATA</span>
                </div>
            </div>
            <div style="height: 1px; background: linear-gradient(90deg, var(--neon-emerald) 0%, rgba(0,255,136,0) 100%); margin: 20px 0; opacity: 0.3;"></div>
            <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px;">
                <div style="text-align: center;">
                    <div style="font-size: 1.6em; font-weight: 700; color: #fff; font-family: 'Outfit';">{_roi_total_skus:,}</div>
                    <div style="font-size: 0.7em; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Active SKUs</div>
                </div>
                <div style="text-align: center;">
                    <div style="font-size: 1.6em; font-weight: 700; color: {_dead_color}; font-family: 'Outfit';">{_roi_dead_pct}%</div>
                    <div style="font-size: 0.7em; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Dead Stock (&lt;5% target)</div>
                </div>
                <div style="text-align: center;">
                    <div style="font-size: 1.6em; font-weight: 700; color: {_so_color}; font-family: 'Outfit';">{_roi_so_pct}%</div>
                    <div style="font-size: 0.7em; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Stockout (&lt;2% target)</div>
                </div>
                <div style="text-align: center;">
                    <div style="font-size: 1.6em; font-weight: 700; color: var(--neon-amber); font-family: 'Outfit';">KES {_roi_trapped:,.0f}</div>
                    <div style="font-size: 0.7em; color: #666; text-transform: uppercase; letter-spacing: 0.5px;">Capital Trapped (Recoverable)</div>
                </div>
            </div>
        </div>
        """, unsafe_allow_html=True)
        
        # --- COMPARATIVE METRICS (real figures; demo only in showcase mode) ---
        m1, m2, m3 = st.columns(3)

        # Live stock availability: % of active SKUs not stocked out.
        _avail = round(100.0 - _roi_so_pct, 1)

        # Fulfillment: measured from Shadow-mode alignment if logs exist; demo
        # value only in showcase mode; otherwise honestly N/A.
        _fulfil = None
        if showcase_mode:
            _fulfil = 98.8
        else:
            try:
                _sld = os.path.join(os.getcwd(), 'shadow_logs')
                _lc = sorted([f for f in os.listdir(_sld) if f.startswith('shadow_comparison_')])
                if _lc:
                    _cdf = pd.read_csv(os.path.join(_sld, _lc[-1]))
                    _al = len(_cdf[_cdf.get('Divergence', pd.Series()) == 'ALIGNED'])
                    _fulfil = round(_al / max(len(_cdf), 1) * 100, 1)
            except Exception:
                _fulfil = None

        with m1:
            if _fulfil is not None:
                st.markdown(f"""
                <div class="metric-card">
                    <h3>Fulfillment Rate</h3>
                    <div class="value" style="color: var(--neon-emerald);">{_fulfil}%</div>
                    <div class="sub">{'Demo showcase' if showcase_mode else 'From Shadow-mode alignment'}</div>
                </div>""", unsafe_allow_html=True)
            else:
                st.markdown("""
                <div class="metric-card">
                    <h3>Fulfillment Rate</h3>
                    <div class="value" style="color: #888;">—</div>
                    <div class="sub">Run Shadow Mode (Phase 2) to measure</div>
                </div>""", unsafe_allow_html=True)

        with m2:
            _av_color = "var(--neon-emerald)" if _avail >= 98 else ("var(--neon-amber)" if _avail >= 95 else "var(--neon-ruby)")
            st.markdown(f"""
            <div class="metric-card">
                <h3>Stock Availability</h3>
                <div class="value" style="color: {_av_color};">{_avail}%</div>
                <div class="sub">{_roi_stockout} of {_roi_total_skus:,} SKUs out of stock</div>
            </div>""", unsafe_allow_html=True)

        with m3:
            _cap = showcase_savings if (showcase_mode and showcase_savings != "0") else f"{_roi_trapped:,.0f}"
            st.markdown(f"""
            <div class="metric-card">
                <h3>Recoverable Capital</h3>
                <div class="value" style="color: var(--neon-amber);">KES {_cap}</div>
                <div class="sub">Trapped in dead stock (AMIT rule)</div>
            </div>""", unsafe_allow_html=True)

        st.markdown("---")

        # Real weekly revenue trend (replaces the former hard-coded projection).
        st.markdown("#### 💹 Weekly Revenue Trend (last 90 days)")
        _roi_sales = load_sales_data(selected_org, days=90)
        if _roi_sales is not None and not _roi_sales.empty and 'net_amt' in _roi_sales.columns:
            _rs = _roi_sales.copy()
            _rs['bill_dt'] = pd.to_datetime(_rs['bill_dt'], errors='coerce')
            _rs = _rs.dropna(subset=['bill_dt'])
            _rs['week'] = _rs['bill_dt'].dt.strftime('%Y-W%U')
            _trend = _rs.groupby('week')['net_amt'].sum().reset_index().sort_values('week')
            fig_roi = go.Figure()
            fig_roi.add_trace(go.Scatter(
                x=_trend['week'], y=_trend['net_amt'], name="Weekly Revenue",
                mode='lines+markers', line=dict(color='#4caf50', width=3),
                fill='tozeroy', fillcolor='rgba(76,175,80,0.1)'))
            fig_roi.update_layout(
                xaxis_title="Week", yaxis_title="Revenue (KES)",
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                font_color="#c9d1d9", height=380, margin=dict(t=20))
            st.plotly_chart(fig_roi, use_container_width=True)
        else:
            st.info("No sales history available for this store yet.")

        # Data-derived insight (replaces the former fixed narrative claim).
        if _roi_total_skus:
            _msg = (f"💡 **Insight**: {_roi_dead} SKUs ({_roi_dead_pct}%) are dead "
                    f"stock holding **KES {_roi_trapped:,.0f}** of recoverable capital; "
                    f"{_roi_stockout} SKUs ({_roi_so_pct}%) are stocked out.")
            if _roi_dead_pct < 5 and _roi_so_pct < 2:
                st.success(_msg + " Both within Playbook targets (dead <5%, stockout <2%).")
            else:
                st.info(_msg + " Targets: dead stock <5%, stockout <2%.")
        else:
            st.info("💡 No live stock data for this store yet — connect the ERP feed to populate ROI metrics.")


# =====================================================================
# TAB 1: LIVE SALES FEED
# =====================================================================
if "live_sales" in tab_map:
 with tab_map["live_sales"]:
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
            if LIVE_MODE:
                # In live mode, today's sales are the actual streamed sales.
                latest_date = df_sales["bill_dt"].max()
                df_today = df_sales[df_sales["bill_dt"] == latest_date].copy()
                if not df_today.empty:
                    # Reverse because fetch_sales_history sorts DESC
                    df_today = df_today.iloc[::-1].reset_index(drop=True)
                    if sim_hour > 6:
                        frac = np.linspace(0, 1, len(df_today))
                        df_today["sim_hour"] = np.clip(np.round(6 + frac * (sim_hour - 6)), 6, 22).astype(int)
                    else:
                        df_today["sim_hour"] = 6
                df_visible = df_today
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

        # ── Multi-Day Trends (visible when sim_day > 1) ──
        if _sim and sim_day > 1:
            st.markdown("#### 📅 Multi-Day Trends")
            st.caption(f"Day-over-day performance from Day 1 → Day {sim_day}")
            trends = _sim.get_multi_day_trends(sim_day)
            closed_trends = [t for t in trends if t.get('status') == 'closed']

            if closed_trends:
                # Summary metrics for completed days
                ct1, ct2, ct3 = st.columns(3)
                total_rev_all = sum(t.get('revenue', 0) for t in closed_trends)
                total_so_all = sum(t.get('stockouts', 0) for t in closed_trends)
                total_lost_all = sum(t.get('lost_sales', 0) for t in closed_trends)

                with ct1:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>Cumulative Revenue</h3>
                        <div class="value" style="color: #4caf50;">KES {total_rev_all:,.0f}</div>
                        <div class="sub">{len(closed_trends)} completed day(s)</div>
                    </div>""", unsafe_allow_html=True)
                with ct2:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>Total Stockouts</h3>
                        <div class="value" style="color: #f44336;">{total_so_all:,}</div>
                        <div class="sub">across all days</div>
                    </div>""", unsafe_allow_html=True)
                with ct3:
                    st.markdown(f"""
                    <div class="metric-card">
                        <h3>Total Lost Sales</h3>
                        <div class="value" style="color: #ff9800;">KES {total_lost_all:,.0f}</div>
                        <div class="sub">opportunity cost</div>
                    </div>""", unsafe_allow_html=True)

                # Day-over-day line chart
                df_trends = pd.DataFrame(closed_trends)
                fig_trend = go.Figure()
                fig_trend.add_trace(go.Bar(
                    x=df_trends['day'], y=df_trends['revenue'],
                    name='Revenue (KES)', marker_color='#4caf50', opacity=0.6
                ))
                fig_trend.add_trace(go.Scatter(
                    x=df_trends['day'], y=df_trends['stockouts'],
                    name='Stockouts', yaxis='y2',
                    mode='lines+markers', line=dict(color='#f44336', width=2)
                ))
                fig_trend.update_layout(
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117",
                    font_color="#c9d1d9", height=300,
                    margin=dict(t=30, b=40, l=60, r=60),
                    xaxis=dict(title="Day", dtick=1, gridcolor="#1a1a2e"),
                    yaxis=dict(title="Revenue (KES)", gridcolor="#1a1a2e"),
                    yaxis2=dict(title="Stockouts", overlaying='y', side='right',
                                gridcolor="#1a1a2e"),
                    legend=dict(orientation="h", y=1.12),
                )
                st.plotly_chart(fig_trend, use_container_width=True)
            else:
                st.info("📊 Multi-day trend data will appear after at least one full day is completed.")

        # ── Top Movers + Spike Detection ──
        col_movers, col_alerts = st.columns([3, 2])
        if not df_visible.empty:
            with col_movers:
                st.markdown("#### 🔥 Top Movers")
                # Calculate velocity for ALL items first
                all_items = df_visible.groupby(["itm_cd", "item_name"]).agg(
                    units=("qty", "sum"),
                    revenue=("net_amt", "sum"),
                ).reset_index()
                
                intel = load_sales_intel(selected_org)
                all_items["ads"] = all_items["item_name"].apply(
                    lambda n: intel.get(n, {}).get("avg_daily_sales", 0)
                )
                
                # Prevent division by zero and handle elapsed time
                elapsed_hours = float(max(0.5, sim_hour - 6)) # Minimum 30 mins to avoid inf
                
                all_items["velocity_ratio"] = np.where(
                    all_items["ads"] > 0,
                    (all_items["units"] / (all_items["ads"] * (elapsed_hours / 14.0))).round(1),
                    0
                )
                
                # Top Movers UI is top 15 by volume
                top_items = all_items.sort_values("units", ascending=False).head(15).copy()
                
                def color_velocity(val):
                    if val > 3: return "background: #5c1a1a; color: #ff6666;"
                    elif val > 1.5: return "background: #4d3d00; color: #ffc107;"
                    else: return "background: #1a4d1a; color: #4caf50;"
                
                display_df = top_items[["item_name", "units", "revenue", "velocity_ratio"]].copy()
                display_df.columns = ["Product", "Units Sold", "Revenue (KES)", "Velocity Ratio"]
                display_df["Revenue (KES)"] = display_df["Revenue (KES)"].apply(lambda x: f"{x:,.0f}")
                
                st.dataframe(
                    display_df.style.map(color_velocity, subset=["Velocity Ratio"]),
                    use_container_width=True, hide_index=True, height=400,
                )
            
            with col_alerts:
                st.markdown("#### ⚠️ Velocity Alerts")
                monitor = AlertMonitor(spike_threshold_pct=200.0)
                spike_items = all_items[all_items["velocity_ratio"] > 2.0].sort_values("velocity_ratio", ascending=False)
                realtime_batch = []
                hist_stats = {}
                for _, row in spike_items.iterrows():
                    realtime_batch.append({"sku": row["itm_cd"], "qty": row["units"]})
                    item_intel = intel.get(row["item_name"], {})
                    hist_stats[row["itm_cd"]] = {
                        "avg_daily_sales": item_intel.get("avg_daily_sales", 1),
                        "product_name": row["item_name"],
                    }
                alerts = monitor.check_velocity_spikes(realtime_batch, hist_stats, elapsed_hours=elapsed_hours)
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
    """Load NetworkSimulator + StoreGraphNetwork via the shared service (SH-A S2).

    Delegates to oasis.logic.gnn_service._load_model — the single guarded loader
    (conv1 dimension guard, trained/untrained/unavailable status). Returns the
    same (model, sim) shape this dashboard expects; on an untrained checkpoint
    the (random-init) model + sim are still returned, exactly as before, so the
    downstream uniform-GNN detection takes over.
    """
    from oasis.logic import gnn_service
    model, sim, status = gnn_service._load_model()
    if model is None or sim is None:
        return None, None
    if status == "trained":
        logger.info("Successfully loaded GNN model weights (via gnn_service).")
    else:
        logger.warning(f"GNN model status={status}; using inventory-led risk.")
    return model, sim

if "transfer_intelligence" in tab_map and _mod_ok("transfer_intelligence"):
 with tab_map["transfer_intelligence"]:
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
                adapter = get_adapter()
                # Group by donor store — a batch can span multiple donors, and each
                # transfer request must be attributed to the store actually shipping.
                from collections import defaultdict as _dd
                _by_donor = _dd(list)
                for t in sim_transfers:
                    _by_donor[t.from_org].append({
                        "item_code": t.itm_cd, "product_name": t.product_name,
                        "transfer_qty": t.transfer_qty, "transfer_value": t.value_kes,
                        "urgency": t.urgency,
                    })
                _pushed_total = 0
                for _donor_org, _donor_items in _by_donor.items():
                    if adapter.push_transfer_request(_donor_org, selected_org, _donor_items):
                        _pushed_total += len(_donor_items)
                if _pushed_total:
                    log_action(DB_PATH, current_user["username"], "TRANSFER_EXECUTED", ENTITY_TRANSFER, f"TX_BATCH_{int(time.time())}", selected_org, {"items": _pushed_total, "donors": len(_by_donor)})
                    st.success(f"Dispatched {_pushed_total} inter-branch transfers from {len(_by_donor)} donor store(s)!")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("No transfers could be dispatched — adapter rejected all donor requests.")
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
        # H1 FIX: Use canonical 2-arg GCN forward (x_t, edge_index)
        with torch.no_grad():
            gnn_out = gnn_model(x_t, gnn_sim.edge_index)
            # Patch: Inject transfer scores which are not returned by the base GNN forward
            gnn_out['transfer'] = gnn_model.get_all_transfer_scores(gnn_out['embeddings']).unsqueeze(0)
        
        # Get traffic matrix from the simulation
        traffic_mat = gnn_sim.get_traffic_matrix()
        
        gnn_stores = gnn_sim.stores_data
        gnn_ids = [s['store_id'] for s in gnn_stores]
        # Use blended risk scores which incorporate inventory-aware risk blending
        risk_scores_map = get_all_store_risks(sim_hour)
        risk_scores = [risk_scores_map.get(sid, gnn_out['risk'][i].item()) for i, sid in enumerate(gnn_ids)]
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
                if score > 0.25:  # Lowered from 0.45 — 5-store network scores cluster below 0.45
                    # Dimensionless ranking signal (GNN score minus traffic
                    # friction) — NOT currency. Do not present it as KES.
                    profit_pulse = score * 1000
                    friction_pen = fric * 400
                    net_gain = profit_pulse - friction_pen
                    gnn_recs.append({
                        "From": src['store_id'], "To": dst['store_id'],
                        "Score": f"{score:.2f}", "Priority Index": f"{net_gain:,.0f}",
                        "_net_gain": net_gain
                    })
        if gnn_recs:
            gnn_recs.sort(key=lambda x: -x["_net_gain"])
            st.dataframe(pd.DataFrame(gnn_recs).drop(columns=["_net_gain"]).head(8), use_container_width=True, hide_index=True)
        st.markdown("---")

    # ── SECTION B: Item-Level Intra-Day Heuristic ────────
    st.markdown("#### ⏱️ Item-Level Intra-Day Stockout Risk (ADS Heuristic)")
    st.caption("Shows ALL items with < 3 days of unit-based cover — including items that may trigger MOQ failures in ordering.")
    # FIX: Use unit-based ADS from enriched products (not KES-based from sales_intel).
    # FIX: Show items with < 3 days cover (not just items stocking out today).
    # This surfaces the 437 MOQ-failure items that have demand but insufficient stock.
    _heur_enriched = load_network_stock(tuple([o["ORG_CD"] for o in orgs]))
    comparison_data = []
    for _h_org, _h_prods in _heur_enriched.items():
        for _h_item in _h_prods:
            name = _h_item.get("product_name", "Unknown")
            qty  = float(_h_item.get("current_stocks", 0) or 0)
            ads  = float(_h_item.get("avg_daily_sales", 0) or 0)
            uom  = str(_h_item.get("uom", "EA")).upper()
            if ads > 0:
                days_cover = qty / ads
                hours_to_so = days_cover * 16  # 16h trading day
                # Show items with < 3 days cover (critical inventory)
                if days_cover < 3.0:
                    # Severity tag
                    if qty < 1.0:
                        severity = "⛔ DEPLETED"
                    elif days_cover < 0.5:
                        severity = "🔴 CRITICAL (<½ day)"
                    elif days_cover < 1.0:
                        severity = "🟠 URGENT (<1 day)"
                    else:
                        severity = "🟡 LOW (<3 days)"
                    comparison_data.append({
                        "Severity": severity,
                        "Product": name,
                        "Store": org_names.get(_h_org, _h_org),
                        "Dept": str(_h_item.get("department") or _h_item.get("category") or ""),
                        "Stock": round(qty, 1) if uom == "KG" else int(round(qty)),
                        "ADS (units/day)": round(ads, 2),
                        "Days Cover": round(days_cover, 1),
                        "Hours to SO": round(hours_to_so, 1),
                        "UOM": uom,
                    })
            elif qty < 1.0:
                # ADS=0 but effectively depleted — flag for manual review
                comparison_data.append({
                    "Severity": "⚪ NO ADS (depleted)",
                    "Product": name,
                    "Store": org_names.get(_h_org, _h_org),
                    "Dept": str(_h_item.get("department") or _h_item.get("category") or ""),
                    "Stock": round(qty, 4),
                    "ADS (units/day)": 0,
                    "Days Cover": 0,
                    "Hours to SO": 0,
                    "UOM": str(_h_item.get("uom", "EA")).upper(),
                })
    if comparison_data:
        # Show critical items first, then sort by days cover
        _sev_order = {"⛔ DEPLETED": 0, "🔴 CRITICAL (<½ day)": 1, "🟠 URGENT (<1 day)": 2, "🟡 LOW (<3 days)": 3, "⚪ NO ADS (depleted)": 4}
        comparison_data.sort(key=lambda x: (_sev_order.get(x["Severity"], 99), x["Days Cover"]))
        df_comp = pd.DataFrame(comparison_data)
        # Summary metrics
        _n_depleted = sum(1 for c in comparison_data if "DEPLETED" in c["Severity"] or "CRITICAL" in c["Severity"])
        _n_urgent = sum(1 for c in comparison_data if "URGENT" in c["Severity"])
        _n_low = sum(1 for c in comparison_data if "LOW" in c["Severity"])
        _hm1, _hm2, _hm3, _hm4 = st.columns(4)
        _hm1.metric("⛔ Depleted/Critical", _n_depleted)
        _hm2.metric("🟠 Urgent (<1 day)", _n_urgent)
        _hm3.metric("🟡 Low (<3 days)", _n_low)
        _hm4.metric("Total At-Risk", len(comparison_data))
        st.dataframe(df_comp, use_container_width=True, hide_index=True, height=400)
    else:
        st.success("✅ No unit-level stockouts projected for the rest of the trading day.")

    # ── SECTION B2: Network Transfer Opportunities (CTS-Powered) ─────
    st.markdown("---")
    st.markdown("#### 🌐 Live Network Transfer Opportunities")
    st.caption("Real-time cross-store analysis: identifies items overstocked at donor stores that can plug stockout gaps at recipient stores.")

    with st.spinner("🔄 Running cross-store transfer intelligence scan..."):
        try:
            from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
            from oasis.logic.moq_failure_store import load_moq_failures

            _all_orgs = [o["ORG_CD"] for o in orgs]
            _org_name_map = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}
            _risk_map = get_all_store_risks(sim_hour)

            # Enriched products for all stores (real ADS & real stock quantities)
            _net_stock = load_network_stock(tuple(_all_orgs))

            # Gap 5: MOQ failures become pull triggers (timestamped store, auto-expiring)
            _moq_failures = {}
            try:
                _moq_failures = load_moq_failures(os.path.join(DATA_DIR, "moq_failures.json"))
            except Exception:
                pass

            # Pending transfers (REQUESTED / IN_TRANSIT) count as committed
            # supply: donors lose that stock, recipients gain it. This stops
            # the scan from regenerating transfers that are already queued.
            _pending_records = []
            try:
                _pending_df = get_adapter().fetch_transfers(None)
                if not _pending_df.empty:
                    _pending_records = _pending_df.to_dict("records")
            except Exception as _pend_err:
                logger.warning(f"Pending transfer lookup failed: {_pend_err}")

            # Single scan implementation — shared with Smart Ordering's CTS.
            _cts_scan = ConsolidatedTransferService(
                org_names=_org_name_map,
                stock_data=_net_stock,
                registry_path=REGISTRY_PATH,
                distance_map=get_distance_map(),
                cold_node_days=st.session_state.get('cold_node_days', 60),
                hot_node_days=st.session_state.get('hot_node_days', 14),
            )
            _scan = _cts_scan.scan_network_opportunities(
                moq_failures=_moq_failures,
                pending_transfers=_pending_records,
            )

            # Summary metrics row
            _total_excess_skus = sum(s.get("overstock", 0) for s in _scan.store_stats.values())
            _total_deficit_skus = sum(s.get("deficits", 0) for s in _scan.store_stats.values())
            _n_push = sum(1 for o in _scan.opportunities if o.type == "PUSH")
            _mc1, _mc2, _mc3, _mc4 = st.columns(4)
            _mc1.metric("🏪 Stores Scanned", len(_all_orgs))
            _mc2.metric("📦 Overstock SKUs (network)", f"{_total_excess_skus:,}")
            _mc3.metric("⚠️ Deficit SKUs (pull triggers)", f"{_total_deficit_skus:,}")
            _mc4.metric("🔃 Push Opportunities (cold→hot)", f"{_n_push:,}")
            if _scan.pending_outbound_units > 0:
                st.caption(
                    f"🚚 {_scan.pending_outbound_units:,.0f} units already committed in "
                    f"REQUESTED/IN_TRANSIT transfers are excluded from donor stock."
                )

            # Per-store overstock / deficit breakdown
            st.markdown("##### 📊 Store-Level Inventory Health")
            _health_rows = []
            for _oc in _all_orgs:
                _stats = _scan.store_stats.get(_oc, {})
                _risk = _risk_map.get(_oc, _risk_map.get(f"CFP-{_oc.replace('ORG','')}", 0.0))
                _health_rows.append({
                    "Store": _org_name_map.get(_oc, _oc),
                    "Org": _oc,
                    "Total SKUs": _stats.get("total_skus", 0),
                    "Overstock (excess>0)": _stats.get("overstock", 0),
                    "Pull Deficits (<7d)": _stats.get("deficits", 0),
                    "Push Opps (cold→hot)": _stats.get("push_from", 0),
                    "Risk Score": round(_risk, 3),
                    "Status": "🔴 High Risk" if _risk > 0.5 else ("🟠 Moderate" if _risk > 0.25 else "🟢 Stable"),
                })
            _health_df = pd.DataFrame(_health_rows).sort_values("Pull Deficits (<7d)", ascending=False)
            st.dataframe(_health_df, use_container_width=True, hide_index=True)

            # ── Recommended transfers (display rows from the unified scan) ──
            st.markdown("##### 🔄 Recommended Item-Level Transfers")
            _xfer_opps = []
            for _o in _scan.opportunities:
                _icon = "🔴 PULL" if _o.type == "PULL" else "🔵 PUSH"
                if _o.manual_only:
                    _icon += " (🖐️ MANUAL ONLY)"
                _xfer_opps.append({
                    "Type":             _icon,
                    "Product":          _o.product_name[:45],
                    "From":             _org_name_map.get(_o.from_org, _o.from_org),
                    "From Org":         _o.from_org,
                    "To":               _org_name_map.get(_o.to_org, _o.to_org),
                    "To Org":           _o.to_org,
                    "Transfer Qty":     _o.transfer_qty,
                    "Donor Days Cover": _o.donor_days_cover,
                    "Rcpt Days Cover":  _o.recipient_days_cover,
                    "Donor Excess":     _o.donor_excess,
                    "Value (KES)":      _o.value_kes,
                    "Department":       _o.department,
                    "Supplier":         _o.supplier,
                    "_itm_cd":          _o.itm_cd,
                    "_rec_org":         _o.to_org,
                    "_don_org":         _o.from_org,
                    "_is_fresh":        _o.manual_only,
                })

            if _xfer_opps:
                st.success(f"✅ **{len(_xfer_opps)} transfer opportunities identified** across the network.")

                # Summary stat strip
                _total_value  = sum(x["Value (KES)"] for x in _xfer_opps)
                _unique_items = len({x["Product"] for x in _xfer_opps})
                _unique_pairs = len({(x["From Org"], x["To Org"]) for x in _xfer_opps})
                _s1, _s2, _s3 = st.columns(3)
                _s1.metric("💰 Total Transfer Value", f"KES {_total_value:,.0f}")
                _s2.metric("📦 Unique SKUs", _unique_items)
                _s3.metric("🔗 Store Pairs", _unique_pairs)

                # Department filter
                _all_depts = sorted({x["Department"] for x in _xfer_opps if x["Department"]})
                _sel_dept  = st.multiselect("Filter by Department", _all_depts, default=[], key="xfer_dept_filter")
                _show_opps = [x for x in _xfer_opps if (not _sel_dept or x["Department"] in _sel_dept)]

                # Export logic
                _disp_cols = ["Type", "Product", "From", "To", "Transfer Qty", "Donor Days Cover", "Rcpt Days Cover", "Donor Excess", "Value (KES)", "Department", "Supplier"]
                _full_df = pd.DataFrame(_show_opps)[_disp_cols] if _show_opps else pd.DataFrame(columns=_disp_cols)
                
                # Add Download Button for the full list
                if not _full_df.empty:
                    _csv_data = _full_df.to_csv(index=False).encode('utf-8')
                    st.download_button(
                        label="📥 Download Full Opportunities List (CSV)",
                        data=_csv_data,
                        file_name="transfer_opportunities.csv",
                        mime="text/csv",
                    )

                # Display table (limit to 100 in the UI for performance)
                st.caption(f"Showing top {min(100, len(_full_df))} of {len(_full_df)} opportunities. Use the download button to get the full list.")
                _xfer_df = _full_df.head(100).copy()
                _xfer_df["Value (KES)"] = _xfer_df["Value (KES)"].apply(lambda v: f"{v:,.0f}")
                st.dataframe(_xfer_df, use_container_width=True, hide_index=True, height=400)

                # ── Queue Transfers to DB button ────────────────────────
                st.markdown("---")
                _col_btn1, _col_btn2 = st.columns([1, 3])
                with _col_btn1:
                    _max_queue = st.number_input("Max transfers to queue", min_value=1, max_value=500, value=min(50, len(_xfer_opps)), step=10, key="xfer_queue_limit")
                with _col_btn2:
                    st.caption("Queuing saves transfer requests to the database so store managers can review and dispatch.")
                if st.button(f"🚀 Queue Top {_max_queue} Transfers to Database", key="btn_queue_xfers", type="primary", use_container_width=True):
                    _queued = 0
                    _adapter_q = get_adapter()
                    for _xo in _xfer_opps[:_max_queue]:
                        if _xo.get("_is_fresh", False):
                            continue
                        try:
                            _items_payload = [{
                                "item_code":     _xo["_itm_cd"],
                                "product_name":  _xo["Product"],
                                "transfer_qty":  _xo["Transfer Qty"],
                                "transfer_value": _xo["Value (KES)"],
                                "urgency": "HIGH" if _xo["Rcpt Days Cover"] <= 1 else "MEDIUM",
                            }]
                            if _adapter_q.push_transfer_request(_xo["From Org"], _xo["To Org"], _items_payload):
                                _queued += 1
                        except Exception as _eq:
                            pass
                    if _queued > 0:
                        log_action(DB_PATH, current_user["username"], ACTION_TRANSFER_EXECUTED, ENTITY_TRANSFER,
                                   f"BATCH_{int(time.time())}", selected_org, {"count": _queued})
                        st.success(f"✅ {_queued} transfers queued to database successfully!")
                        load_all_stocks.clear()
                        load_network_stock.clear()
                        time.sleep(1)
                        st.rerun()
                    else:
                        st.warning("No transfers were queued. The adapter may not support push_transfer_request for your DB schema.")

            else:
                st.info("ℹ️ No profitable transfer opportunities found at this time. Either all stores are well-stocked, or no donors have sufficient excess above their safety stock threshold.")
                st.caption("💡 Tip: Lower the Cold Node threshold in the sidebar to see more potential transfers.")

        except Exception as _xfer_err:
            st.error(f"Transfer scan error: {_xfer_err}")
            import traceback
            st.code(traceback.format_exc(), language="python")

    st.markdown("---")

    # ── SECTION C: Transfer Status Tracking ──────────────
    st.markdown("#### 🚚 Transfer Execution & Status")
    adapter = get_adapter()
    org_filter = selected_org if not user_perms.get("can_view_all_stores") else None
    df_transfers = adapter.fetch_transfers(org_filter)
    
    if not df_transfers.empty:
        # Action handler for marking received
        action_col, tbl_col = st.columns([1, 4])
        with action_col:
            st.caption("Update Status")
            transfer_id = st.number_input("Transfer ID", min_value=1, step=1)
            new_status = st.selectbox("Status", ["IN_TRANSIT", "RECEIVED"])
            if st.button("Update Status", use_container_width=True):
                if adapter.update_transfer_status(transfer_id, new_status):
                    log_action(DB_PATH, current_user['username'], "TRANSFER_EXECUTED", ENTITY_TRANSFER,
                               f"TX_{transfer_id}", selected_org, {"status": new_status})
                    st.success(f"Transfer {transfer_id} marked as {new_status}")
                    time.sleep(1)
                    st.rerun()
                else:
                    st.error("Failed to update (ID not found or DB error).")
        
        with tbl_col:
            # Color code status
            def color_status(val):
                if val == 'REQUESTED': return 'color: #ff9800;'
                elif val == 'IN_TRANSIT': return 'color: #2196f3;'
                elif val == 'RECEIVED': return 'color: #4caf50;'
                return ''
                
            disp_df = df_transfers.copy()
            # formatting
            disp_df["VALUE_KES"] = disp_df["VALUE_KES"].apply(lambda x: f"{x:,.0f}")
            if "COMPLETED_DT" in disp_df.columns:
                disp_df["COMPLETED_DT"] = disp_df["COMPLETED_DT"].fillna("-")
                
            st.dataframe(disp_df.style.map(color_status, subset=["STATUS"]), 
                         use_container_width=True, hide_index=True, height=300)
    else:
        st.info("No transfer records found.")

# =====================================================================
# TAB 3: END-OF-DAY STOCK REVIEW
# =====================================================================
if "stock_review" in tab_map:
 with tab_map["stock_review"]:
    st.markdown(f"### 📦 Stock Review — {store_name}")
    products = load_products(selected_org)
    if products:
        df_p = pd.DataFrame(products)
        
        # Defensive: Ensure required columns exist for calculation
        for col in ["avg_daily_sales", "current_stocks"]:
            if col not in df_p.columns:
                df_p[col] = 0.0
                
        df_p["days_cover"] = np.where(df_p["avg_daily_sales"] > 0, (df_p["current_stocks"] / df_p["avg_daily_sales"]).round(1), 999)
        def get_health(row):
            d = row["days_cover"]
            is_fresh = bool(row.get("is_fresh", False)) or any(k in str(row.get("department", "")).upper() for k in ["MILK", "DAIRY", "FRESH", "MEAT", "BREAD", "BAKERY"])
            overstock_limit = 14.0 if is_fresh else 30.0
            if d < 0.5: return "🔴 Stockout"
            if d < 2: return "🟡 Critical"
            if d < overstock_limit: return "🟢 Healthy"
            return "⚪ Overstock"
            
        df_p["health"] = df_p.apply(get_health, axis=1)
        
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
if "smart_ordering" in tab_map and _mod_ok("smart_ordering"):
 with tab_map["smart_ordering"]:
    st.markdown(f"### 🛒 Smart Ordering — {store_name}")
    engine = get_order_engine()
    can_approve = user_perms.get("can_approve_po", False)
    if can_approve:
        t_gen, t_app = st.tabs(["🛠️ Generate Orders", "✅ Pending Approvals"])
    else:
        from contextlib import nullcontext
        t_gen = nullcontext()
        t_app = None

    with t_gen:
        calendar = get_calendar()
        
        # G11 Fix: Stock refresh button to clear cache before PO generation
        rc1, rc2 = st.columns([3, 1])
        with rc1:
            st.info("OASIS Ordering Engine logic is currently active for replenishment.")
        with rc2:
            if st.button("🔄 Refresh Stock", help="Clear cached data and reload latest stock levels"):
                load_products.clear()
                load_network_stock.clear()
                load_all_stocks.clear()
                for _k in [k for k in st.session_state.keys()
                           if str(k).startswith("so_pipeline_")]:
                    st.session_state.pop(_k, None)
                st.rerun()
        
        products = load_products(selected_org)
        if products:
            from oasis.logic.simulation_bridge import SimulationOrderUtil
            from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService

            # The full pipeline (enrich → order calc → network optimization →
            # MOQ gate) runs once per store and is cached in session_state.
            # Widget interactions no longer re-run network optimization or
            # rewrite the MOQ failure store as a side effect of rendering.
            _so_key = f"so_pipeline_{selected_org}"
            _gen_col1, _gen_col2 = st.columns([3, 1])
            with _gen_col2:
                if st.button("⚙️ Regenerate Orders", key="btn_regen_orders",
                             help="Re-run the ordering pipeline with current stock and settings"):
                    st.session_state.pop(_so_key, None)
            with _gen_col1:
                if _so_key in st.session_state:
                    st.caption(
                        f"Showing recommendations generated at "
                        f"{st.session_state[_so_key].get('generated_at', '?')} — "
                        f"use Regenerate Orders to refresh."
                    )

            if _so_key not in st.session_state:
                with st.spinner("🧮 Running ordering pipeline (engine → network → MOQ gate)..."):
                    # G4 Fix: Load ordering thresholds from session state (editable via Settings)
                    ordering_thresholds = st.session_state.get('ordering_thresholds', None)

                    risk_scores_map = get_all_store_risks(sim_hour)
                    store_risk = risk_scores_map.get(selected_org, 0.0)  # blended — for display
                    sim_util = SimulationOrderUtil(DATA_DIR, thresholds=ordering_thresholds, engine=engine)
                    enriched = sim_util.prepare_sku_data(products)
                    # Gate-compliant ORDERING risk: inventory-only until the GNN is
                    # validated (OASIS_GNN_ORDERING_WEIGHT). Closes F2 — the
                    # unvalidated GNN no longer shifts live PO quantities; unified
                    # with the Operations Console.
                    from oasis.logic import gnn_service as _gnn_service
                    _ordering_risk = _gnn_service.ordering_risk(products, gnn_risk_score=store_risk)
                    raw_recs = sim_util.calculate_order_quantity(enriched, gnn_risk_score=_ordering_risk, use_real_date=True)
                    finalized_recs = sim_util.finalize_orders(raw_recs)

                    # ── UNIFIED NETWORK TRANSFER OPTIMIZATION ──
                    all_org_cds = [o["ORG_CD"] for o in orgs]
                    org_name_map = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}

                    # Use enriched products (carry real avg_daily_sales) so the network map
                    # can compute meaningful excess and safety stock per donor.
                    # Raw fetch_stock_snapshot has 0 ADS — making every store look like a donor.
                    enriched_network_stock = load_network_stock(tuple(all_org_cds))

                    cts = ConsolidatedTransferService(
                        org_names=org_name_map,
                        stock_data=enriched_network_stock,
                        registry_path=REGISTRY_PATH,
                        distance_map=get_distance_map(),
                        cold_node_days=st.session_state.get('cold_node_days', 60),
                        hot_node_days=st.session_state.get('hot_node_days', 14)
                    )

                    # Pass the raw recommendations into the network layer first
                    network_plan = cts.optimize_network(
                        {selected_org: finalized_recs},
                        risk_scores=risk_scores_map
                    )

                    # Adjusted orders (original minus transfer fulfillments),
                    # then the Minimum Order Threshold gate
                    network_adjusted_recs = network_plan.adjusted_orders.get(selected_org, [])
                    mot_result = sim_util.apply_minimum_order_gate(network_adjusted_recs)

                    # Gap 5: Feed MOQ-failed items directly to Transfer deficit list.
                    # Replace-per-org semantics with timestamps + 7-day expiry — the
                    # latest run is the complete truth for this store (no more
                    # unbounded append growth / stale triggers).
                    try:
                        from oasis.logic.moq_failure_store import record_moq_failures
                        _moq_path = os.path.join(DATA_DIR, "moq_failures.json")
                        record_moq_failures(_moq_path, selected_org, mot_result['transfer_recs'] or [])
                    except Exception as _moq_err:
                        logger.warning(f"MOQ failure store update failed: {_moq_err}")

                    st.session_state[_so_key] = {
                        'generated_at': datetime.now().strftime("%H:%M:%S"),
                        'sim_util': sim_util,
                        'enriched': enriched,
                        'store_risk': store_risk,
                        'org_name_map': org_name_map,
                        'enriched_network_stock': enriched_network_stock,
                        'network_plan': network_plan,
                        'po_recs': mot_result['po_recs'],
                        'dropped_recs': mot_result['transfer_recs'],
                        'supplier_summary': mot_result['supplier_summary'],
                    }

            _so = st.session_state[_so_key]
            sim_util = _so['sim_util']
            enriched = _so['enriched']
            store_risk = _so['store_risk']
            org_name_map = _so['org_name_map']
            enriched_network_stock = _so['enriched_network_stock']
            network_plan = _so['network_plan']
            po_recs = _so['po_recs']
            dropped_recs = _so['dropped_recs']
            supplier_summary = _so['supplier_summary']
            final_recs = po_recs

            # ── G17 Fix: PO Dedup Check ──
            from datetime import date as _date
            try:
                existing_pos = adapter.fetch_pending_pos(selected_org)
                today_pos = [po for po in existing_pos if str(po.get('PO_DATE', ''))[:10] == str(_date.today())]
                if today_pos:
                    st.markdown(f"""
                    <div class="alert-card">
                        <div class="alert-title">⚠️ Duplicate PO Warning</div>
                        <strong>{len(today_pos)} PO(s)</strong> already exist for {selected_org} today.<br/>
                        <span style="font-size: 0.85em; color: #888;">AI suggests careful review to avoid double-ordering.</span>
                    </div>""", unsafe_allow_html=True)
            except Exception:
                pass  # Non-critical

            # Show on-order awareness info
            on_order_count = sum(1 for r in enriched if r.get('on_order_qty', 0) > 0)
            if on_order_count > 0:
                st.markdown(f"""
                <div class="transfer-card">
                    <div style="font-weight: 600; color: var(--neon-emerald);">📦 On-Order Intelligence Active</div>
                    <span style="font-size: 0.85em; color: #888;">Adjusting quantities for <strong>{on_order_count} SKUs</strong> currently in transit.</span>
                </div>""", unsafe_allow_html=True)
                
            # ── Network Intelligence Results ──
            store_transfers = [t for t in network_plan.transfers if t.to_org == selected_org]
            donor_adds = network_plan.donor_additions.get(selected_org, [])
            
            if store_transfers or donor_adds or dropped_recs:
                st.markdown("---")
                st.markdown("### 🌐 Network Transfer Intelligence")
                
                if store_transfers:
                    st.success(f"✅ {len(store_transfers)} items will be fulfilled via network transfers instead of supplier orders!")
                    # Value transfers at each item's real selling price from the
                    # network stock data (was a flat KES 500/unit placeholder).
                    _price_by_itm = {}
                    for _ps in enriched_network_stock.values():
                        for _pp in _ps:
                            _pk = str(_pp.get('item_code', _pp.get('itm_cd', '')) or '')
                            if _pk and _pk not in _price_by_itm:
                                _price_by_itm[_pk] = float(_pp.get('selling_price', 0) or 0)
                    tf_data = [{
                        "Product": t.product_name,
                        "From": org_name_map.get(t.from_org, t.from_org),
                        "Qty": t.qty,
                        "Urgency": t.urgency,
                        "Est. Value (KES)": f"{(t.qty * _price_by_itm.get(str(t.itm_cd), 0)):,.0f}"
                    } for t in store_transfers]
                    st.dataframe(pd.DataFrame(tf_data), use_container_width=True, hide_index=True)
                    
                if donor_adds:
                    st.info(f"📦 {len(donor_adds)} items added to your PO to compensate for your donations to other branches.")
                    da_data = [{
                        "Product": d['product_name'],
                        "Extra Qty": d['recommended_quantity'],
                        "Reason": d['reasoning'],
                    } for d in donor_adds]
                    st.dataframe(pd.DataFrame(da_data), use_container_width=True, hide_index=True)
                    
                if dropped_recs:
                    with st.expander(f"⚠️ {len(dropped_recs)} items failed Minimum Order Quantity (Click to review)", expanded=False):
                        st.caption("These items could not be ordered because the demand is less than the supplier's Minimum Order Quantity and no network donor had excess stock. Consider manual transfers or supplier exceptions.")
                        dropped_data = [{
                            "Product": d['product_name'],
                            "Supplier": d.get('supplier_name', 'Unknown'),
                            "Qty Needed": d['recommended_quantity'],
                            "Reason": d['reasoning'],
                        } for d in dropped_recs]
                        st.dataframe(pd.DataFrame(dropped_data), use_container_width=True, hide_index=True)
            
            st.markdown("---")
            # Downstream display uses the final PO-only items
            final_recs = po_recs

            # =============================================================
            # 🔮 CHAOS & DISRUPTION SCENARIOS (Phase 3.2)
            # =============================================================
            with st.expander("🔮 Chaos & Disruption Scenarios", expanded=False):
                st.caption("Simulate supply chain disruptions and see how PO recommendations adapt in real-time.")

                scenario_type = st.selectbox(
                    "Scenario Type",
                    ["None", "🚫 Supplier Failure", "🏪 Competitor Entry", "💸 Price War"],
                    key="disruption_scenario_type"
                )

                disruption_active = scenario_type != "None"
                disrupted_recs = None

                if scenario_type == "🚫 Supplier Failure":
                    # Build inventory dict for SupplierRiskAnalyzer
                    inventory_for_analysis = {}
                    for p in products:
                        name = p.get('product_name', 'Unknown')
                        inventory_for_analysis[name] = {
                            'department': p.get('department', 'UNKNOWN'),
                            'supplier': p.get('supplier_name', 'UNKNOWN'),
                            'avg_daily_sales': p.get('avg_daily_sales', 0),
                            'price': p.get('selling_price', p.get('sell_price', 0)),
                        }

                    analyzer = SupplierRiskAnalyzer()
                    critical = analyzer.identify_critical_suppliers(inventory_for_analysis)

                    if critical:
                        supplier_options = [f"{c['supplier']} — {c['department']} ({c['share_pct']:.0f}% share, KES {c['revenue_at_risk']:,.0f} at risk)" for c in critical]
                        sel_idx = st.selectbox("Target Supplier", range(len(supplier_options)),
                                               format_func=lambda x: supplier_options[x],
                                               key="disruption_supplier")
                        sel_supplier = critical[sel_idx]['supplier']
                        sel_dept = critical[sel_idx]['department']

                        d_col1, d_col2 = st.columns(2)
                        with d_col1:
                            failure_mode = st.selectbox("Failure Mode", ["Complete (No Supply)", "Partial (50% Capacity)", "Delayed (2× Lead Time)"],
                                                        key="disruption_mode")
                        with d_col2:
                            failure_duration = st.slider("Duration (days)", 3, 30, 14, key="disruption_duration")

                        if st.button("⚡ Apply Supplier Disruption", key="apply_disruption", type="primary"):
                            with st.spinner("Recalculating orders with disruption..."):
                                # Create modified product list with disruption effects
                                disrupted_products = []
                                affected_count = 0
                                for p in list(enriched):
                                    p_copy = dict(p)
                                    p_supplier = str(p_copy.get('supplier_name', '')).upper().strip()
                                    if sel_supplier.upper() in p_supplier:
                                        affected_count += 1
                                        if "Complete" in failure_mode:
                                            # Max out safety stock — force emergency order
                                            p_copy['lead_time_days'] = p_copy.get('lead_time_days', 3) + failure_duration
                                            p_copy['demand_cv'] = min(2.0, p_copy.get('demand_cv', 0.5) * 2.0)
                                        elif "Partial" in failure_mode:
                                            p_copy['lead_time_days'] = p_copy.get('lead_time_days', 3) + int(failure_duration * 0.5)
                                            p_copy['demand_cv'] = min(1.5, p_copy.get('demand_cv', 0.5) * 1.5)
                                        else:  # Delayed
                                            p_copy['lead_time_days'] = p_copy.get('lead_time_days', 3) * 2
                                    disrupted_products.append(p_copy)

                                disrupted_raw = sim_util.calculate_order_quantity(disrupted_products, gnn_risk_score=store_risk)
                                disrupted_recs = sim_util.finalize_orders(disrupted_raw)

                                # Show impact comparison
                                st.markdown(f"#### ⚡ Disruption Impact: **{sel_supplier}** ({failure_mode})")
                                st.markdown(f"**{affected_count}** SKUs affected across **{sel_dept}**")

                                baseline_qty = sum(r.get('recommended_quantity', 0) for r in final_recs if r.get('recommended_quantity', 0) > 0)
                                disrupted_qty = sum(r.get('recommended_quantity', 0) for r in disrupted_recs if r.get('recommended_quantity', 0) > 0)
                                delta = disrupted_qty - baseline_qty

                                ic1, ic2, ic3 = st.columns(3)
                                with ic1:
                                    st.metric("Baseline PO Qty", f"{baseline_qty:,.0f}")
                                with ic2:
                                    st.metric("Disrupted PO Qty", f"{disrupted_qty:,.0f}", delta=f"{delta:+,.0f}")
                                with ic3:
                                    pct_change = (delta / max(1, baseline_qty)) * 100
                                    st.metric("Change", f"{pct_change:+.1f}%",
                                              delta="More safety stock" if delta > 0 else "Reduced scope")

                                # Replace final_recs with disrupted version
                                final_recs = disrupted_recs
                                st.success("✅ PO recommendations below now reflect the disruption scenario.")
                    else:
                        st.info("No critical single-source suppliers found in the current inventory.")

                elif scenario_type == "🏪 Competitor Entry":
                    template_names = {
                        "carrefour_100m": "🟥 Carrefour @ 100m (-6% sales)",
                        "naivas_200m": "🟧 Naivas @ 200m (-4.5% sales)",
                        "quickmart_500m": "🟨 QuickMart @ 500m (-3% sales)",
                        "competitor_exit_nearby": "🟩 Tuskys Exit @ 200m (+4% sales)",
                    }
                    sel_template = st.selectbox("Competitive Scenario", list(template_names.keys()),
                                                 format_func=lambda k: template_names[k],
                                                 key="disruption_competitor")
                    event = SCENARIO_TEMPLATES[sel_template]
                    st.info(f"**{event.competitor_name}** — {event.impact_pct:+.1f}% YoY impact, "
                            f"{event.ramp_up_days} day ramp-up, {event.distance_meters}m away")

                    if st.button("⚡ Apply Competitive Event", key="apply_competitor", type="primary"):
                        with st.spinner("Recalculating orders with competitive pressure..."):
                            day_1_mult = event.get_multiplier_for_day(15)
                            disrupted_products = []
                            for p in list(enriched):
                                p_copy = dict(p)
                                dept = str(p_copy.get('department', '')).upper()
                                mult = event.get_multiplier_for_day(15, dept)
                                p_copy['avg_daily_sales'] = p_copy.get('avg_daily_sales', 0) * mult
                                disrupted_products.append(p_copy)

                            disrupted_raw = sim_util.calculate_order_quantity(disrupted_products, gnn_risk_score=store_risk)
                            disrupted_recs = sim_util.finalize_orders(disrupted_raw)

                            baseline_qty = sum(r.get('recommended_quantity', 0) for r in final_recs if r.get('recommended_quantity', 0) > 0)
                            disrupted_qty = sum(r.get('recommended_quantity', 0) for r in disrupted_recs if r.get('recommended_quantity', 0) > 0)
                            delta = disrupted_qty - baseline_qty

                            st.markdown(f"#### 🏪 Competitive Impact: **{event.competitor_name}** at Day 15")
                            ic1, ic2, ic3 = st.columns(3)
                            with ic1:
                                st.metric("Baseline PO Qty", f"{baseline_qty:,.0f}")
                            with ic2:
                                st.metric("Adjusted PO Qty", f"{disrupted_qty:,.0f}", delta=f"{delta:+,.0f}")
                            with ic3:
                                st.metric("Demand Multiplier", f"{day_1_mult:.3f}")

                            final_recs = disrupted_recs
                            st.success("✅ PO recommendations adjusted for competitive pressure.")

                elif scenario_type == "💸 Price War":
                    event = SCENARIO_TEMPLATES["price_war_aggressive"]
                    st.warning(f"**Aggressive Price War** — {event.impact_pct:+.1f}% YoY impact on commodities. "
                               f"Cooking Oil, Rice, Sugar, Flour hit hardest.")

                    if st.button("⚡ Apply Price War", key="apply_pricewar", type="primary"):
                        with st.spinner("Recalculating..."):
                            disrupted_products = []
                            for p in list(enriched):
                                p_copy = dict(p)
                                dept = str(p_copy.get('department', '')).upper()
                                mult = event.get_multiplier_for_day(7, dept)
                                p_copy['avg_daily_sales'] = p_copy.get('avg_daily_sales', 0) * mult
                                disrupted_products.append(p_copy)

                            disrupted_raw = sim_util.calculate_order_quantity(disrupted_products, gnn_risk_score=store_risk)
                            disrupted_recs = sim_util.finalize_orders(disrupted_raw)

                            baseline_qty = sum(r.get('recommended_quantity', 0) for r in final_recs if r.get('recommended_quantity', 0) > 0)
                            disrupted_qty = sum(r.get('recommended_quantity', 0) for r in disrupted_recs if r.get('recommended_quantity', 0) > 0)
                            delta = disrupted_qty - baseline_qty

                            st.markdown("#### 💸 Price War Impact (Day 7, Full Ramp)")
                            ic1, ic2 = st.columns(2)
                            with ic1:
                                st.metric("Baseline PO Qty", f"{baseline_qty:,.0f}")
                            with ic2:
                                st.metric("Adjusted PO Qty", f"{disrupted_qty:,.0f}", delta=f"{delta:+,.0f}")

                            final_recs = disrupted_recs
                            st.success("✅ PO recommendations adjusted for price war scenario.")

            # Removed redundant Network Transfer Overlay (now integrated natively above)
    
            # ── Standard Order Display (per-store engine output, possibly adjusted) ──
            # UI Toggle
            show_all = st.checkbox("Show Blocked/Zero Quantity Items (Display AI Reasoning)")
            
            if show_all:
                recs_to_show = final_recs
            else:
                recs_to_show = [r for r in final_recs if r.get('recommended_quantity', 0) > 0]
            
            if recs_to_show:
                df_recs = pd.DataFrame(recs_to_show)
                
                # Extract supplier names cleanly
                if 'supplier_name' not in df_recs.columns:
                    df_recs['supplier_name'] = 'UNKNOWN'
                
                df_recs['supplier'] = df_recs['supplier_name'].fillna('UNKNOWN').astype(str)
                suppliers = sorted([s for s in df_recs['supplier'].unique() if s])
                
                if suppliers:
                    tabs = st.tabs(suppliers)
                    for i, supp in enumerate(suppliers):
                        with tabs[i]:
                            supp_df = df_recs[df_recs['supplier'] == supp]
                            display_cols = ["product_name", "recommended_quantity", "reasoning"]
                            st.dataframe(supp_df[display_cols], use_container_width=True, hide_index=True)
                else:
                    display_cols = ["product_name", "recommended_quantity", "reasoning"]
                    st.dataframe(df_recs[display_cols].head(50), use_container_width=True, hide_index=True)
            else:
                st.info("No orders recommended at this time based on current stock and intelligence.")
                
            # Keep export to only positive orders
            pos_recs = [r for r in final_recs if r.get('recommended_quantity', 0) > 0]
            if pos_recs:
                colA, colB = st.columns(2)
                with colA:
                    if st.button("🚀 Push to PENDING Approvals", type="primary", use_container_width=True):
                        with st.spinner("Pushing..."):
                            adapter = get_adapter()
                            pushed = adapter.push_purchase_order(selected_org, pos_recs)
                            if pushed:
                                log_action(DB_PATH, current_user['username'], ACTION_PO_GENERATED,
                                           ENTITY_PO, f"PO_{selected_org}_{int(time.time())}", selected_org,
                                           {"items": pushed})
                                st.success(f"Sent {pushed} items to pending approvals.")
                                time.sleep(1)
                                st.rerun()
                with colB:
                    # FETCH BARCODES AND SUPPLIERS FROM DB FOR ACCURATE EXPORT
                    export_data = []
                    try:
                        # Central DB factory — honors OASIS_DB_URL/OASIS_DB_PATH
                        # (was a hardcoded relative SQLite path that broke when
                        # the app ran from a different working directory).
                        from oasis.logic import db as oasis_db
                        _conn = oasis_db.get_raw_connection()
                        for r in pos_recs:
                            _itm = r.get('itm_cd', r.get('item_code', ''))
                            _res = _conn.execute(
                                "SELECT I.SCAN_ITM_CD, S.SUPPLIER_NAME FROM ITEM_MST I "
                                "LEFT JOIN SUPPLIER_MST S ON I.SUPPLIER_CD = S.SUPPLIER_CD "
                                "WHERE I.ITM_CD = ?", (_itm,)
                            ).fetchone()
                            
                            _bcode = _res[0] if _res and _res[0] else 'N/A'
                            _supp = _res[1] if _res and _res[1] else r.get('supplier', 'Unknown Supplier')
                            
                            export_data.append({
                                'Barcode': _bcode,
                                'Supplier': _supp,
                                'Item Code': _itm,
                                'Product Name': r.get('product_name', ''),
                                'Order Qty': r.get('recommended_quantity', 0),
                                'Cost Est': r.get('cost_est', 0),
                                'Reasoning': r.get('reasoning', '')
                            })
                        _conn.close()
                    except Exception as e:
                        # Fallback if DB fails
                        for r in pos_recs:
                            export_data.append({
                                'Barcode': 'N/A',
                                'Supplier': r.get('supplier', 'Unknown'),
                                'Item Code': r.get('itm_cd', ''),
                                'Product Name': r.get('product_name', ''),
                                'Order Qty': r.get('recommended_quantity', 0),
                                'Cost Est': r.get('cost_est', 0),
                                'Reasoning': r.get('reasoning', '')
                            })

                    df_csv = pd.DataFrame(export_data)
                    csv_data = df_csv.to_csv(index=False).encode('utf-8')
                    if st.download_button("📥 Export CSV Backup", data=csv_data, file_name=f"po_{selected_org}.csv", use_container_width=True):
                        pass


    if t_app is not None:
        with t_app:
            st.markdown("#### 📋 Purchase Orders Awaiting Approval")
            adapter = get_adapter()
            org_filter = selected_org if not user_perms.get("can_view_all_stores") else None
            df_pending = adapter.fetch_pending_pos(org_filter)
            
            if df_pending.empty:
                st.info("No pending purchase orders waiting for approval.")
            else:
                st.caption("You can edit the **QUANTITY** column. A reason is required if you modify the quantity.")
                
                edit_df = df_pending.copy()
                edit_df.insert(0, "Select", False)
                edit_df.insert(len(edit_df.columns), "Reason", "")
                
                edited_df = st.data_editor(
                    edit_df,
                    hide_index=True,
                    use_container_width=True,
                    disabled=[c for c in edit_df.columns if c not in ["Select", "QUANTITY", "Reason"]],
                    column_config={"Select": st.column_config.CheckboxColumn("Select", required=True)}
                )
                
                col1, col2 = st.columns(2)
                with col1:
                    if st.button("✅ Approve Selected", type="primary", use_container_width=True):
                        selected = edited_df[edited_df["Select"] == True]
                        if not selected.empty:
                            count = 0
                            for _, row in selected.iterrows():
                                po_id = row["PO_ID"]
                                orig_qty = df_pending[df_pending["PO_ID"] == po_id].iloc[0]["QUANTITY"]
                                new_qty = row["QUANTITY"]
                                reason = row["Reason"] if new_qty != orig_qty else None
                                
                                if adapter.update_po_status(po_id, "APPROVED", current_user["username"], new_qty, reason):
                                    count += 1
                                    log_action(DB_PATH, current_user["username"], "PO_APPROVED", ENTITY_PO,
                                               f"PO_ID_{po_id}", row["ORG_CD"], {"new_qty": new_qty, "reason": reason})
                            st.success(f"Approved {count} purchase orders.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.warning("No rows selected.")
                with col2:
                    if st.button("❌ Reject Selected", use_container_width=True):
                        selected = edited_df[edited_df["Select"] == True]
                        if not selected.empty:
                            count = 0
                            for _, row in selected.iterrows():
                                po_id = row["PO_ID"]
                                if adapter.update_po_status(po_id, "REJECTED", current_user["username"]):
                                    count += 1
                                    log_action(DB_PATH, current_user["username"], "PO_REJECTED", ENTITY_PO,
                                               f"PO_ID_{po_id}", row["ORG_CD"], {})
                            st.success(f"Rejected {count} purchase orders.")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.warning("No rows selected.")
# TAB 5: 🚀 OASIS PROCESSOR (BATCH FILE PROCESSING)
# =====================================================================
if "oasis_processor" in tab_map:
 with tab_map["oasis_processor"]:
    st.markdown("### 🚀 Batch Inventory Processor")
    st.info("Upload Picking Lists or GRN files (Excel/CSV) to generate intelligence-driven order recommendations in bulk.")
    
    uploaded_files = st.file_uploader(
        "Upload Inventory Files", 
        type=["xlsx", "xls", "csv"], 
        accept_multiple_files=True,
        help="You can upload multiple files at once. Each will be processed individually."
    )
    
    if uploaded_files:
        st.write(f"📁 **{len(uploaded_files)} files selected.**")
        
        if st.button("▶️ Process All Orders", type="primary"):
            progress_bar = st.progress(0)
            status_text = st.empty()
            
            all_recommendations = []
            results_container = st.container()
            
            # Use RuleBasedLLM for consistency and reliability
            llm = RuleBasedLLM()
            engine = OrderEngine(DATA_DIR)
            engine.load_local_databases()
            
            for i, uploaded_file in enumerate(uploaded_files):
                file_name = uploaded_file.name
                status_text.text(f"Processing {file_name}...")
                
                try:
                    # Save uploaded file to temp path for processing
                    with tempfile.NamedTemporaryFile(delete=False, suffix=os.path.splitext(file_name)[1]) as tmp:
                        tmp.write(uploaded_file.getbuffer())
                        tmp_path = tmp.name
                    
                    # 1. Parse
                    status_text.text(f"[{file_name}] Parsing...")
                    products = engine.parse_inventory_file(tmp_path)
                    
                    # 2. Enrich
                    status_text.text(f"[{file_name}] Enriching with Intelligence...")
                    products = engine.enrich_product_data(products)
                    
                    # 3. AI Analysis (Rule-Based)
                    status_text.text(f"[{file_name}] Running Decision Engine...")
                    recommendations = asyncio.run(llm.analyze(products))
                    all_recommendations.extend(recommendations)
                    
                    # 4. Generate Output Report
                    output_name = f"processed_{file_name}"
                    output_path = os.path.join(tempfile.gettempdir(), output_name)
                    engine.generate_excel_report(tmp_path, recommendations, output_path)
                    
                    # 5. Provide Download
                    with open(output_path, "rb") as f:
                        processed_data = f.read()
                        
                    with results_container:
                        col1, col2 = st.columns([3, 1])
                        col1.write(f"✅ **{file_name}** ({len(products)} products)")
                        col2.download_button(
                            label=f"📥 Download Report",
                            data=processed_data,
                            file_name=output_name,
                            key=f"dl_{file_name}_{i}"
                        )
                    
                    # Audit log
                    log_action(DB_PATH, current_user['username'], ACTION_FILE_PROCESSED,
                               ENTITY_FILE, file_name, selected_org,
                               {"products": len(products), "recommendations": len(recommendations)})
                    
                    # Cleanup
                    os.unlink(tmp_path)
                    
                except Exception as ex:
                    st.error(f"Error processing {file_name}: {ex}")
                
                # Update progress
                progress = (i + 1) / len(uploaded_files)
                progress_bar.progress(progress)
            
            status_text.success(f"Processing Complete! {len(uploaded_files)} files processed.")
            
            # Show summary table of top recommendations across all files
            if all_recommendations:
                st.markdown("---")
                st.markdown("#### 🔦 Top Recommendations (Overview)")
                df_summary = pd.DataFrame(all_recommendations)
                # Ensure columns exist
                for c in ["product_name", "recommended_quantity", "reasoning"]:
                    if c not in df_summary.columns: df_summary[c] = ""
                
                display_cols = ["product_name", "recommended_quantity", "reasoning"]
                st.dataframe(
                    df_summary[df_summary["recommended_quantity"] > 0][display_cols].sort_values("recommended_quantity", ascending=False).head(50),
                    use_container_width=True,
                    hide_index=True
                )
    else:
        # Guidance for user
        st.write("---")
        st.markdown("""
        #### 💡 How it works
        1. **Upload** your current Picking List or GRN files.
        2. **Process**: The system enriches your data with ADS, trends, and risk scores.
        3. **Download**: Get an enhanced version of your file with the 'Recommended Qty' and AI logic already populated.
        """)


# =====================================================================
# TAB 6: 🧮 ALLOCATION ENGINE (Phase 4.1)
# =====================================================================
if "allocation_engine" in tab_map and _mod_ok("allocation_engine"):
 with tab_map["allocation_engine"]:
    st.markdown(f"### 🧮 Allocation Engine — Budget-Constrained Order Generation")
    st.caption("Two-Pass allocation with efficiency guards. Powered by OrderEngine 2.0.")

    # --- Scorecard Discovery (shared runner) ---
    from oasis.logic.greenfield_runner import find_latest_scorecard as _find_sc
    _alloc_search_dir = os.path.dirname(os.path.abspath(__file__))
    _alloc_scorecard = _find_sc(_alloc_search_dir)

    if _alloc_scorecard is None or not os.path.exists(_alloc_scorecard):
        st.warning("⚠️ No `Full_Product_Allocation_Scorecard_v*.csv` found. Upload a scorecard or place one in the project directory.")
    else:
        st.success(f"📄 Scorecard: `{os.path.basename(_alloc_scorecard)}`")

        # --- Controls ---
        alloc_col1, alloc_col2, alloc_col3 = st.columns([2, 1, 1])
        with alloc_col1:
            alloc_budget = st.slider(
                "💰 Capital Budget (KES)", min_value=50_000, max_value=200_000_000,
                value=3_000_000, step=100_000, key="alloc_budget",
                help="Total cash available for purchasing inventory"
            )
        with alloc_col2:
            alloc_month = st.selectbox("📅 Target Month", ["JAN","FEB","MAR","APR","MAY","JUN","JUL","AUG","SEP","OCT","NOV","DEC"], key="alloc_month")
        with alloc_col3:
            st.metric("Scorecard SKUs", f"{len(pd.read_csv(_alloc_scorecard)):,}")

        if st.button("🚀 Run Allocation", type="primary", key="run_alloc_engine", use_container_width=True):
            with st.spinner("Running Two-Pass Allocation Logic..."):
                try:
                    from oasis.simulation.data_loader import HistoricalDataLoader
                    from oasis.logic.greenfield_runner import (
                        load_scorecard_recommendations, run_greenfield_allocation,
                    )
                    _alloc_loader = HistoricalDataLoader(os.path.dirname(os.path.abspath(__file__)))
                    _alloc_seasonal = _alloc_loader.load_monthly_demand(alloc_month)

                    # Same shared pipeline as allocation_app.py — enrichment
                    # and safety guards now applied here too (previously skipped).
                    _alloc_engine = get_order_engine()
                    _alloc_recs = load_scorecard_recommendations(_alloc_scorecard)
                    _gf = run_greenfield_allocation(
                        _alloc_engine, _alloc_recs, alloc_budget,
                        seasonal_demand_map=_alloc_seasonal,
                    )

                    if not _gf.is_empty:
                        st.session_state['alloc_basket'] = _gf.basket
                        st.session_state['alloc_cash'] = _gf.cash_spend
                        st.session_state['alloc_consign'] = _gf.consignment_value
                        st.session_state['alloc_summary'] = _gf.summary
                        st.session_state['alloc_budget'] = alloc_budget
                        st.success(f"✅ Allocation complete! {len(_gf.basket)} SKUs in basket.")
                    else:
                        st.warning("No items were allocated. Try increasing the budget.")
                except Exception as _alloc_ex:
                    st.error(f"Allocation failed: {_alloc_ex}")
                    import traceback
                    st.code(traceback.format_exc(), language="text")

        # --- Display Results (from session state) ---
        if 'alloc_basket' in st.session_state:
            _basket_df = st.session_state['alloc_basket']
            _cash_spend = st.session_state['alloc_cash']
            _consignment_val = st.session_state['alloc_consign']
            _alloc_summary = st.session_state['alloc_summary']
            _alloc_budget_val = st.session_state.get('alloc_budget', alloc_budget)

            # --- KPI Metrics Row ---
            _est_revenue = _basket_df["Expected_Revenue"].sum()
            _total_value = _cash_spend + _consignment_val
            _roi = ((_est_revenue - _total_value) / _total_value) * 100 if _total_value > 0 else 0
            _total_qty = _basket_df["Qty"].sum()
            _total_ads = _basket_df["Avg_Daily_Sales"].sum()
            _avg_turnover = (_total_qty / _total_ads) if _total_ads > 0 else 0

            st.markdown("#### 📊 Allocation Results")
            m1, m2, m3, m4, m5, m6 = st.columns(6)
            m1.metric("Budget Target", f"KES {_alloc_budget_val:,.0f}")
            m2.metric("Cash Used", f"KES {_cash_spend:,.0f}", delta=f"{_cash_spend - _alloc_budget_val:,.0f}")
            m3.metric("Consignment", f"KES {_consignment_val:,.0f}", delta="Free Capital")
            m4.metric("Est. Revenue", f"KES {_est_revenue:,.0f}", delta=f"{_roi:.1f}% ROI")
            m5.metric("Days to ROI", f"{_avg_turnover:.1f}")
            m6.metric("Total SKUs", f"{len(_basket_df):,}")

            # Utilization bar
            _util_pct = _alloc_summary.get('utilization_pct', 0)
            st.progress(min(_util_pct / 100.0, 1.0), text=f"Budget Utilization: {_util_pct:.1f}% | Skipped: {_alloc_summary.get('total_skipped', 0)} items")

            # A1: per-stage breakdown of where SKUs dropped out of the basket
            _by_stage = _alloc_summary.get('skipped_by_stage') or {}
            if _by_stage:
                _stage_labels = {
                    "pass1": "Pass 1 (width filter)",
                    "liquidity_prune": "Pass 1.5 (liquidity recovery)",
                    "premium_trim": "Premium cap",
                    "anchor_mov": "Pass 3 (anchor MOV)",
                    "safety_guards": "Safety guards",
                }
                with st.expander(f"Why {_alloc_summary.get('total_skipped', 0)} SKUs were skipped", expanded=False):
                    _stage_df = pd.DataFrame(
                        [{"Stage": _stage_labels.get(k, k), "SKUs": v}
                         for k, v in sorted(_by_stage.items(), key=lambda x: -x[1])]
                    )
                    st.dataframe(_stage_df, use_container_width=True, hide_index=True)
                    _reasons = _alloc_summary.get('skip_reasons') or {}
                    if _reasons:
                        st.caption("By reason: " + " · ".join(
                            f"{k}: {v}" for k, v in sorted(_reasons.items(), key=lambda x: -x[1])
                        ))

            # Engine profile info
            try:
                _eng = get_order_engine()
                _profile = _eng.profile_manager.get_profile(_alloc_budget_val)
                st.info(f"**Engine Profile**: {_profile['tier_name']} — Price Cap {_profile['price_ceiling']:,.0f}/=, Depth {_profile['depth_days']} Days, Max {_profile['max_packs']} Packs")
            except Exception:
                pass

            # --- Charts ---
            _chart_l, _chart_r = st.columns(2)
            with _chart_l:
                st.markdown("##### Department Spend")
                _dept_sum = _basket_df.groupby("Department")["Allocated_Cost"].sum().reset_index()
                _fig_dept = px.pie(_dept_sum, values="Allocated_Cost", names="Department", hole=0.4,
                                   color_discrete_sequence=px.colors.qualitative.Set2)
                _fig_dept.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117", font_color="#c9d1d9",
                                        margin=dict(t=20, b=20, l=20, r=20), height=320)
                st.plotly_chart(_fig_dept, use_container_width=True)
            with _chart_r:
                st.markdown("##### Pack Quantity Distribution")
                _fig_hist = px.histogram(_basket_df, x="Qty", nbins=40, color_discrete_sequence=["#2196f3"])
                _fig_hist.update_layout(plot_bgcolor="#0d1117", paper_bgcolor="#0d1117", font_color="#c9d1d9",
                                        margin=dict(t=20, b=40, l=60, r=20), height=320,
                                        xaxis=dict(gridcolor="#1a1a2e"), yaxis=dict(gridcolor="#1a1a2e"))
                st.plotly_chart(_fig_hist, use_container_width=True)

            # --- Basket Table ---
            st.markdown("##### 📋 Generated Order Basket")
            st.dataframe(
                _basket_df.sort_values("Allocated_Cost", ascending=False),
                height=450, use_container_width=True, hide_index=True
            )

            # --- Download ---
            _csv = _basket_df.to_csv(index=False).encode('utf-8')
            st.download_button(
                "📥 Download Order Basket (CSV)", _csv,
                f"Allocation_{alloc_month}_{alloc_budget:,.0f}.csv", "text/csv",
                key='alloc-download-csv', use_container_width=True
            )


# =====================================================================
# TAB 7: 🧪 SIMULATION LAB (Phase 3.3)
# =====================================================================
if "simulation_validation" in tab_map:
 with tab_map["simulation_validation"]:
    st.markdown(f"### 🧪 Simulation Validation Lab — {store_name}")
    st.caption("Compare heuristic ordering vs GNN-adjusted ordering across a multi-day Monte Carlo simulation.")

    try:
        from retail_simulator import RetailSimulator, SKUState, STORE_UNIVERSES
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        SIM_AVAILABLE = True
    except ImportError as sim_err:
        SIM_AVAILABLE = False
        st.error(f"Simulation modules not available: {sim_err}")

    if SIM_AVAILABLE:
        # Configuration
        sim_col1, sim_col2 = st.columns(2)
        with sim_col1:
            sim_days = st.slider("Simulation Duration (Days)", 7, 60, 30, key="sim_lab_days")
            sim_archetype = st.selectbox("Store Archetype", [
                "Standard", "Student Hub (High Impulse)", "Residential (Weekend Spike)"
            ], key="sim_lab_archetype")

        with sim_col2:
            sim_enable_swan = st.checkbox("🦢 Enable Black Swan Event", key="sim_lab_swan")
            if sim_enable_swan:
                swan_day = st.slider("Event Start Day", 1, max(1, sim_days - 5), 10, key="sim_lab_swan_day")
                swan_dur = st.slider("Event Duration (Days)", 3, 21, 14, key="sim_lab_swan_dur")
            else:
                swan_day, swan_dur = 0, 0

        if st.button("🚀 Run Comparison Simulation", type="primary", key="run_sim_lab", use_container_width=True):
            with st.spinner("Running dual simulations (this may take 30-60 seconds)..."):
                try:
                    sim_util = SimulationOrderUtil(DATA_DIR)
                    products = load_products(selected_org)
                    if not products:
                        st.error("No products found for this store.")
                    else:
                        enriched = sim_util.prepare_sku_data(products)

                        # Convert to SKUState objects
                        sku_states = []
                        for p in enriched:
                            try:
                                sku = SKUState(
                                    product_name=p.get('product_name', 'Unknown'),
                                    supplier=p.get('supplier_name', 'Unknown'),
                                    department=p.get('department', 'UNKNOWN'),
                                    unit_price=float(p.get('selling_price', p.get('sell_price', 100))),
                                    cost_price=float(p.get('wac', p.get('cost_price', 50))),
                                    avg_daily_sales=float(p.get('avg_daily_sales', 0)),
                                    demand_cv=float(p.get('demand_cv', 0.5)),
                                    lead_time_days=int(p.get('lead_time_days', 3)),
                                    current_stock=float(p.get('current_stocks', 0)),
                                    is_fresh=bool(p.get('is_fresh', False)),
                                )
                                if sku.avg_daily_sales > 0:
                                    sku_states.append(sku)
                            except Exception:
                                continue

                        if not sku_states:
                            st.warning("No valid SKUs could be constructed for simulation.")
                        else:
                            budget_est = sum(s.cost_price * s.current_stock for s in sku_states)
                            # Select a reasonable tier
                            if budget_est < 500_000:
                                tier_key = "Small_200k"
                            elif budget_est < 5_000_000:
                                tier_key = "Medium_1M"
                            else:
                                tier_key = "Large_10M"

                            config = STORE_UNIVERSES.get(tier_key, STORE_UNIVERSES.get("Medium_1M", {})).copy()
                            config["budget"] = budget_est

                            bridge = SimulationOrderUtil(DATA_DIR)

                            # Run 1: Heuristic (no GNN risk)
                            sim_heuristic = RetailSimulator(
                                "Heuristic Baseline", config, seed=42,
                                bridge=bridge, initial_skus=sku_states
                            )
                            result_heur = sim_heuristic.run(sim_days)

                            # Run 2: GNN-Adjusted — inject the store's GNN risk
                            # so the bridge inflates safety stock (CC-D fix: the
                            # risk was previously computed but never passed, so
                            # this run was identical to the heuristic one).
                            risk_scores_map = get_all_store_risks(sim_hour)
                            gnn_risk = risk_scores_map.get(selected_org, 0.0)
                            sim_gnn = RetailSimulator(
                                "GNN-Adjusted", config, seed=42,
                                bridge=bridge, initial_skus=sku_states,
                                gnn_risk_score=gnn_risk
                            )
                            result_gnn = sim_gnn.run(sim_days)

                            st.session_state['sim_lab_results'] = {
                                'heuristic': result_heur,
                                'gnn': result_gnn,
                                'gnn_risk': gnn_risk,
                                'days': sim_days,
                                'tier': tier_key,
                            }
                            st.success("✅ Dual simulation complete!")

                except Exception as sim_ex:
                    st.error(f"Simulation failed: {sim_ex}")
                    import traceback
                    st.code(traceback.format_exc())

        # Display results if available
        if 'sim_lab_results' in st.session_state:
            results = st.session_state['sim_lab_results']
            r_heur = results['heuristic']
            r_gnn = results['gnn']

            st.markdown("---")
            st.markdown("#### 📊 Side-by-Side Comparison")

            # Metric cards
            mc1, mc2, mc3, mc4 = st.columns(4)
            with mc1:
                delta_fr = r_gnn.avg_fill_rate - r_heur.avg_fill_rate
                st.metric("Fill Rate (Heuristic)", f"{r_heur.avg_fill_rate:.1f}%")
                st.metric("Fill Rate (GNN)", f"{r_gnn.avg_fill_rate:.1f}%",
                          delta=f"{delta_fr:+.1f}%")
            with mc2:
                st.metric("Stockout Rate (Heuristic)", f"{r_heur.stockout_rate:.2f}%")
                st.metric("Stockout Rate (GNN)", f"{r_gnn.stockout_rate:.2f}%")
            with mc3:
                st.metric("Revenue (Heuristic)", f"KES {r_heur.total_revenue:,.0f}")
                rev_delta = r_gnn.total_revenue - r_heur.total_revenue
                st.metric("Revenue (GNN)", f"KES {r_gnn.total_revenue:,.0f}",
                          delta=f"KES {rev_delta:+,.0f}")
            with mc4:
                st.metric("ROI (Heuristic)", f"{r_heur.roi:.1f}%")
                st.metric("ROI (GNN)", f"{r_gnn.roi:.1f}%",
                          delta=f"{r_gnn.roi - r_heur.roi:+.1f}%")

            # Charts
            if hasattr(r_heur, 'daily_logs') and r_heur.daily_logs:
                chart_col1, chart_col2 = st.columns(2)

                with chart_col1:
                    st.markdown("##### 📉 Heuristic Ordering")
                    heur_df = pd.DataFrame(r_heur.daily_logs)
                    fig_h = go.Figure()
                    fig_h.add_trace(go.Scatter(
                        x=heur_df['day'], y=heur_df['fill_rate'],
                        mode='lines+markers', name='Fill Rate %',
                        line=dict(color='#ff9800', width=2)
                    ))
                    if 'stockout_count' in heur_df.columns:
                        fig_h.add_trace(go.Bar(
                            x=heur_df['day'], y=heur_df['stockout_count'],
                            name='Stockouts', marker_color='#ef4444', opacity=0.5, yaxis='y2'
                        ))
                    fig_h.update_layout(
                        plot_bgcolor="#0d1117", paper_bgcolor="#0d1117", font_color="#c9d1d9",
                        yaxis=dict(title="Fill Rate %", range=[80, 105]),
                        yaxis2=dict(title="Stockouts", side='right', overlaying='y'),
                        height=300, margin=dict(t=20, b=40),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_h, use_container_width=True)

                with chart_col2:
                    st.markdown("##### 🧠 GNN-Adjusted Ordering")
                    gnn_df = pd.DataFrame(r_gnn.daily_logs)
                    fig_g = go.Figure()
                    fig_g.add_trace(go.Scatter(
                        x=gnn_df['day'], y=gnn_df['fill_rate'],
                        mode='lines+markers', name='Fill Rate %',
                        line=dict(color='#4caf50', width=2)
                    ))
                    if 'stockout_count' in gnn_df.columns:
                        fig_g.add_trace(go.Bar(
                            x=gnn_df['day'], y=gnn_df['stockout_count'],
                            name='Stockouts', marker_color='#ef4444', opacity=0.5, yaxis='y2'
                        ))
                    fig_g.update_layout(
                        plot_bgcolor="#0d1117", paper_bgcolor="#0d1117", font_color="#c9d1d9",
                        yaxis=dict(title="Fill Rate %", range=[80, 105]),
                        yaxis2=dict(title="Stockouts", side='right', overlaying='y'),
                        height=300, margin=dict(t=20, b=40),
                        showlegend=False,
                    )
                    st.plotly_chart(fig_g, use_container_width=True)

            # Value proposition summary
            st.markdown("---")
            st.markdown("#### 💰 OASIS Value Proposition")
            rev_uplift = r_gnn.total_revenue - r_heur.total_revenue
            so_diff = r_heur.stockout_rate - r_gnn.stockout_rate
            st.markdown(f"""
            <div class="metric-card" style="text-align:center;">
                <h3>Projected Annual Impact (Extrapolated)</h3>
                <div class="value" style="color: #4caf50; font-size: 1.5em;">
                    KES {rev_uplift * (365 / max(1, results['days'])):,.0f}
                </div>
                <div class="sub">
                    Additional revenue through GNN-optimized ordering<br/>
                    Stockout reduction: {so_diff:.2f}% points
                </div>
            </div>""", unsafe_allow_html=True)


# =====================================================================
# TAB 7: 📈 ANALYTICS & KPIs (Phase 3.4)
# =====================================================================
if "analytics" in tab_map:
 with tab_map["analytics"]:
    st.markdown(f"### 📈 Analytics & KPIs — {store_name}")
    st.caption("Historical performance metrics and operational KPI tracking.")

    try:
        adapter = get_adapter()
        connector = get_connector()

        # ── Weekly Revenue Trend ──
        st.markdown("#### 📊 Weekly Revenue Trend")
        df_sales = load_sales_data(selected_org, days=90)

        if not df_sales.empty:
            df_sales['bill_dt'] = pd.to_datetime(df_sales['bill_dt'])
            df_sales['week'] = df_sales['bill_dt'].dt.isocalendar().week.astype(int)
            df_sales['year_week'] = df_sales['bill_dt'].dt.strftime('%Y-W%U')

            weekly = df_sales.groupby('year_week').agg(
                revenue=('net_amt', 'sum'),
                units=('qty', 'sum'),
                transactions=('bill_dt', 'count'),
            ).reset_index().sort_values('year_week')

            fig_weekly = go.Figure()
            fig_weekly.add_trace(go.Scatter(
                x=weekly['year_week'], y=weekly['revenue'],
                mode='lines+markers', name='Revenue',
                line=dict(color='#4caf50', width=2),
                fill='tozeroy', fillcolor='rgba(76,175,80,0.1)',
            ))
            fig_weekly.update_layout(
                plot_bgcolor="#0d1117", paper_bgcolor="#0d1117", font_color="#c9d1d9",
                xaxis_title="Week", yaxis_title="Revenue (KES)",
                height=300, margin=dict(t=20, b=40),
            )
            st.plotly_chart(fig_weekly, use_container_width=True)

            # ── Weekly delta
            if len(weekly) >= 2:
                latest_rev = weekly['revenue'].iloc[-1]
                prev_rev = weekly['revenue'].iloc[-2]
                wow_delta = latest_rev - prev_rev
                wow_pct = (wow_delta / max(1, prev_rev)) * 100

                kc1, kc2, kc3, kc4 = st.columns(4)
                with kc1:
                    st.metric("Latest Week Revenue", f"KES {latest_rev:,.0f}",
                              delta=f"KES {wow_delta:+,.0f} WoW")
                with kc2:
                    st.metric("WoW Change", f"{wow_pct:+.1f}%")
                with kc3:
                    avg_weekly = weekly['revenue'].mean()
                    st.metric("Avg Weekly Revenue", f"KES {avg_weekly:,.0f}")
                with kc4:
                    st.metric("Total Weeks", f"{len(weekly)}")
        else:
            st.info("No sales data available for trend analysis.")

        st.markdown("---")

        # ── Operational KPIs ──
        st.markdown("#### 🎯 Operational KPIs")

        # PO Stats
        try:
            # CC-E fix: the table is INTEGRATION_PURCHASE_ORDERS (the old name
            # INTEGRATION_PO_RECOMMENDATIONS does not exist, so this KPI was
            # silently 0); quantity column is QUANTITY.
            with connector.get_connection() as conn:
                po_df = pd.read_sql("SELECT * FROM INTEGRATION_PURCHASE_ORDERS WHERE ORG_CD = :org",
                                    conn, params={"org": selected_org})
                po_count = len(po_df)
                po_value = po_df['QUANTITY'].sum() if 'QUANTITY' in po_df.columns else 0
        except Exception:
            po_count, po_value = 0, 0

        # Transfer Stats
        try:
            tf_df = adapter.fetch_transfers(selected_org)
            tf_count = len(tf_df) if not tf_df.empty else 0
            tf_value = tf_df['VALUE_KES'].sum() if not tf_df.empty and 'VALUE_KES' in tf_df.columns else 0
        except Exception:
            tf_count, tf_value = 0, 0

        # Stockout Analysis
        all_stocks = load_all_stocks()
        store_stocks = all_stocks.get(selected_org, [])
        total_skus = len(store_stocks) if store_stocks else 0
        stockout_skus = sum(1 for s in store_stocks if float(s.get('current_stocks', 0)) <= 0)
        critical_skus = sum(1 for s in store_stocks if 0 < float(s.get('current_stocks', 0)) <= 5)

        ok1, ok2, ok3, ok4 = st.columns(4)
        with ok1:
            st.markdown(f"""
            <div class="metric-card">
                <h3>📋 Purchase Orders</h3>
                <div class="value" style="color: #2196f3;">{po_count}</div>
                <div class="sub">Total PO lines generated</div>
            </div>""", unsafe_allow_html=True)
        with ok2:
            st.markdown(f"""
            <div class="metric-card">
                <h3>🔄 Transfers</h3>
                <div class="value" style="color: #ff9800;">{tf_count}</div>
                <div class="sub">Inter-branch movements</div>
            </div>""", unsafe_allow_html=True)
        with ok3:
            so_pct = (stockout_skus / max(1, total_skus)) * 100
            so_color = "#f44336" if so_pct > 5 else ("#ff9800" if so_pct > 2 else "#4caf50")
            st.markdown(f"""
            <div class="metric-card">
                <h3>🚨 Stockouts</h3>
                <div class="value" style="color: {so_color};">{stockout_skus}</div>
                <div class="sub">{so_pct:.1f}% of {total_skus} SKUs</div>
            </div>""", unsafe_allow_html=True)
        with ok4:
            st.markdown(f"""
            <div class="metric-card">
                <h3>⚠️ Critical Stock</h3>
                <div class="value" style="color: #ffc107;">{critical_skus}</div>
                <div class="sub">SKUs with ≤5 units</div>
            </div>""", unsafe_allow_html=True)

        # ── Department Stockout Heatmap ──
        st.markdown("---")
        st.markdown("#### 🗺️ Department Stockout Heatmap")

        if store_stocks:
            dept_stats = {}
            for s in store_stocks:
                dept = s.get('department', 'UNKNOWN')
                if dept not in dept_stats:
                    dept_stats[dept] = {'total': 0, 'stockout': 0, 'critical': 0}
                dept_stats[dept]['total'] += 1
                qty = float(s.get('current_stocks', 0))
                if qty <= 0:
                    dept_stats[dept]['stockout'] += 1
                elif qty <= 5:
                    dept_stats[dept]['critical'] += 1

            heatmap_data = []
            for dept, stats in sorted(dept_stats.items(), key=lambda x: x[1]['stockout'], reverse=True):
                if stats['total'] > 0:
                    heatmap_data.append({
                        'Department': dept,
                        'SKUs': stats['total'],
                        'Stockouts': stats['stockout'],
                        'Critical': stats['critical'],
                        'Stockout %': round(stats['stockout'] / stats['total'] * 100, 1),
                        'Health Score': round((1 - (stats['stockout'] + stats['critical'] * 0.5) / stats['total']) * 100, 1),
                    })

            if heatmap_data:
                df_heat = pd.DataFrame(heatmap_data)

                fig_heat = px.bar(
                    df_heat.head(15), x='Stockout %', y='Department',
                    orientation='h', color='Health Score',
                    color_continuous_scale=['#f44336', '#ff9800', '#4caf50'],
                    range_color=[0, 100],
                )
                fig_heat.update_layout(
                    plot_bgcolor="#0d1117", paper_bgcolor="#0d1117", font_color="#c9d1d9",
                    height=400, margin=dict(t=20, l=150),
                    yaxis=dict(autorange="reversed"),
                )
                st.plotly_chart(fig_heat, use_container_width=True)

                # Data table
                with st.expander("📋 Full Department Breakdown"):
                    st.dataframe(df_heat, use_container_width=True, hide_index=True)
        else:
            st.info("No stock data available for heatmap analysis.")

    except Exception as analytics_err:
        st.error(f"Analytics error: {analytics_err}")


# =====================================================================
# TAB 8: ⚙️ SETTINGS (OPS_ADMIN ONLY)
# =====================================================================
if "settings" in tab_map:
 with tab_map["settings"]:
    st.markdown("### ⚙️ System Settings")
    st.caption(f"Logged in as **{current_user['display_name']}** ({role_labels.get(user_role, user_role)})")
    
    # ── Config Editor ──
    configs = load_system_config_full(DB_PATH)
    
    if configs:
        # Group by CONFIG_GROUP
        groups = {}
        for cfg in configs:
            g = cfg.get('CONFIG_GROUP', 'general')
            if g not in groups:
                groups[g] = []
            groups[g].append(cfg)
        
        group_icons = {'alerting': '🚨', 'ordering': '🛒', 'transfers': '🔄', 'general': '⚙️'}
        
        with st.form("config_form"):
            updated_values = {}
            
            for group_name, items in groups.items():
                icon = group_icons.get(group_name, '📌')
                st.markdown(f"#### {icon} {group_name.title()}")
                
                cols = st.columns(2)
                for idx, cfg in enumerate(items):
                    with cols[idx % 2]:
                        key = cfg['CONFIG_KEY']
                        desc = cfg.get('DESCRIPTION', key)
                        current_val = cfg['CONFIG_VALUE']
                        new_val = st.text_input(
                            desc,
                            value=current_val,
                            key=f"cfg_{key}",
                            help=f"Key: {key} · Last updated by {cfg.get('UPDATED_BY', 'system')}"
                        )
                        updated_values[key] = new_val
                
                st.markdown("---")
            
            save_btn = st.form_submit_button("💾 Save All Settings", type="primary")
            
            if save_btn:
                changes_made = 0
                for key, new_val in updated_values.items():
                    old_val = next((c['CONFIG_VALUE'] for c in configs if c['CONFIG_KEY'] == key), None)
                    if old_val != new_val:
                        save_system_config(DB_PATH, key, new_val, current_user['username'])
                        changes_made += 1
                        log_action(DB_PATH, current_user['username'], ACTION_CONFIG_CHANGED,
                                   ENTITY_CONFIG, key, None,
                                   {"old_value": old_val, "new_value": new_val})
                
                if changes_made > 0:
                    st.success(f"✅ Saved {changes_made} configuration change(s).")
                else:
                    st.info("No changes detected.")
    else:
        st.warning("No configuration entries found. The database may need to be rebuilt.")
    
    # ── G4 Fix: Ordering Threshold Tuning ──
    st.markdown("---")
    st.markdown("#### 🎚️ Ordering Thresholds")
    st.caption("Fine-tune the Smart Ordering engine's decision thresholds. Changes apply immediately.")
    
    current_thresholds = st.session_state.get('ordering_thresholds', {
        'fresh_stale_days': 120,
        'dry_dead_days': 200,
        'dry_dead_min_sales': 5,
        'key_sku_boost_pct': 0.20,
        'critical_stockout_days': 2.0,
        'min_order_units': 10,
        'min_order_value_kes': 5000,
    })
    
    tc1, tc2, tc3 = st.columns(3)
    with tc1:
        fresh_stale = st.number_input(
            "Fresh Stale Threshold (days)", min_value=30, max_value=365,
            value=int(current_thresholds.get('fresh_stale_days', 120)),
            help="Fresh items with no sales after this many days are blocked from ordering"
        )
        dead_stock = st.number_input(
            "Dead Stock Threshold (days)", min_value=60, max_value=730,
            value=int(current_thresholds.get('dry_dead_days', 200)),
            help="Dry items with no sales after this many days are blocked from ordering"
        )
    with tc2:
        dead_min_sales = st.number_input(
            "Dead Stock Min Sales", min_value=0, max_value=100,
            value=int(current_thresholds.get('dry_dead_min_sales', 5)),
            help="Minimum 90-day sales to avoid dead stock classification"
        )
        sku_boost = st.slider(
            "Key SKU Boost %", min_value=0, max_value=50,
            value=int(current_thresholds.get('key_sku_boost_pct', 0.20) * 100),
            help="Extra volume buffer for top-500 SKUs"
        )
    with tc3:
        crit_days = st.number_input(
            "Critical Stockout (days)", min_value=0.5, max_value=10.0,
            value=float(current_thresholds.get('critical_stockout_days', 2.0)),
            step=0.5,
            help="Below this coverage, order is forced regardless of schedule"
        )
    
    # Phase C: Minimum Order Threshold controls
    st.markdown("##### 📦 Minimum Order Threshold (MOT)")
    st.caption("Orders below this threshold are routed to inter-store transfers instead of supplier POs.")
    mc1, mc2 = st.columns(2)
    with mc1:
        mot_units = st.number_input(
            "Min Order Units", min_value=1, max_value=200,
            value=int(current_thresholds.get('min_order_units', 10)),
            help="Supplier picking list must have at least this many total units"
        )
    with mc2:
        mot_value = st.number_input(
            "Min Order Value (KES)", min_value=500, max_value=100000, step=500,
            value=int(current_thresholds.get('min_order_value_kes', 5000)),
            help="Supplier picking list must exceed this total value"
        )
    
    if st.button("💾 Apply Ordering Thresholds"):
        st.session_state['ordering_thresholds'] = {
            'fresh_stale_days': fresh_stale,
            'dry_dead_days': dead_stock,
            'dry_dead_min_sales': dead_min_sales,
            'key_sku_boost_pct': sku_boost / 100.0,
            'critical_stockout_days': crit_days,
            'min_order_units': mot_units,
            'min_order_value_kes': mot_value,
        }
        st.success("✅ Ordering thresholds updated. Generate a new PO to see the effect.")
    
    # ── User Management (Read-Only) ──
    st.markdown("---")
    st.markdown("#### 👥 User Accounts")
    users = get_all_users(DB_PATH)
    if users:
        df_users = pd.DataFrame(users)
        role_emoji = {'ops_admin': '🔧', 'regional_manager': '🌐', 'branch_manager': '🏪'}
        if 'ROLE' in df_users.columns:
            df_users['ROLE'] = df_users['ROLE'].apply(lambda r: f"{role_emoji.get(r, '')} {r}")
        st.dataframe(df_users, use_container_width=True, hide_index=True)
    else:
        st.info("No users found.")
    
    # ── Scheduler Control Panel ──
    st.markdown("---")
    st.markdown("#### ⏰ Scheduler — Automated Tasks")
    st.caption("Configure background jobs for automated PO generation, monitoring, and reporting.")

    _sched = st.session_state.get('oasis_scheduler')
    if _sched:
        # Start/Stop toggle
        sched_col_ctrl, sched_col_status = st.columns([2, 1])
        with sched_col_ctrl:
            if _sched.is_running():
                if st.button("⏹️ Stop Scheduler", key="stop_scheduler"):
                    _sched.stop()
                    st.rerun()
            else:
                if st.button("▶️ Start Scheduler", type="primary", key="start_scheduler"):
                    _sched.start()
                    st.rerun()
        with sched_col_status:
            if _sched.is_running():
                st.markdown("<div style='background:#1a4d1a22; border:1px solid #4caf50; border-radius:8px; padding:8px 12px; text-align:center;'>🟢 <strong>RUNNING</strong></div>", unsafe_allow_html=True)
            else:
                st.markdown("<div style='background:#4d1a1a22; border:1px solid #ef5350; border-radius:8px; padding:8px 12px; text-align:center;'>🔴 <strong>STOPPED</strong></div>", unsafe_allow_html=True)

        # Job Configuration
        job_statuses = _sched.get_job_status()
        for js in job_statuses:
            with st.expander(f"{'✅' if js['enabled'] else '⬜'} {js['name']}", expanded=False):
                st.caption(js['description'])
                jc1, jc2, jc3 = st.columns([1, 1, 1])
                with jc1:
                    new_enabled = st.toggle("Enabled", value=js['enabled'], key=f"sched_en_{js['job_id']}")
                    if new_enabled != js['enabled']:
                        _sched.toggle_job(js['job_id'], new_enabled)
                        st.rerun()
                with jc2:
                    st.text_input("Cron", value=js['cron'], key=f"sched_cron_{js['job_id']}", disabled=True,
                                  help="min hour day month weekday")
                with jc3:
                    if st.button("▶️ Run Now", key=f"sched_run_{js['job_id']}"):
                        with st.spinner(f"Running {js['name']}..."):
                            result = _sched.run_now(js['job_id'])
                        st.success(result[:150])

                # Status info
                st.markdown(f"<div style='font-size:0.8em; color:#888;'>Last run: {js['last_run']} · Status: {js['last_status']} · Next: {js['next_run']}</div>", unsafe_allow_html=True)
                if js['last_result']:
                    st.code(js['last_result'][:300], language="text")
    else:
        st.warning("Scheduler not initialized.")

    # ── Audit Log Viewer ──
    st.markdown("---")
    st.markdown("#### 📋 Full Audit Trail")
    
    audit_col1, audit_col2 = st.columns(2)
    with audit_col1:
        audit_limit = st.number_input("Rows to show", min_value=10, max_value=500, value=50, step=10)
    with audit_col2:
        action_filter = st.selectbox("Filter by action", ["All", ACTION_PO_GENERATED, ACTION_PO_EXPORTED,
                                     ACTION_TRANSFER_EXECUTED, ACTION_FILE_PROCESSED,
                                     ACTION_CONFIG_CHANGED, ACTION_LOGIN, ACTION_LOGOUT])
    
    action_arg = None if action_filter == "All" else action_filter
    audit_df_full = get_recent_logs(DB_PATH, limit=int(audit_limit), action=action_arg)
    
    if not audit_df_full.empty:
        st.dataframe(audit_df_full, use_container_width=True, hide_index=True)
    else:
        st.info("No audit entries yet.")


# ─────────────────────────────────────────────────────────────────────
# TAB: Supplier Intelligence (U5 — Previously Dormant)
# ─────────────────────────────────────────────────────────────────────
if "supplier_intelligence" in tab_map and _mod_ok("supplier_intelligence"):
    with tab_map["supplier_intelligence"]:
        st.markdown("### 🔬 Supplier Intelligence & Concentration Risk")
        st.caption("Powered by `oasis.analytics.supplier_analytics` — Identifies single-supplier dependency risks using HHI scoring.")

        try:
            from oasis.analytics.supplier_analytics import (
                get_major_categories, get_top_suppliers_by_department,
                analyze_supplier_failure_impact,
                calculate_hhi, load_scorecard_data
            )

            categories = get_major_categories()
            selected_dept = st.selectbox("Select Department", categories, key="sa_dept")

            with st.spinner(f"Analyzing supplier concentration for {selected_dept}..."):
                try:
                    sc_df = load_scorecard_data()
                except Exception:
                    sc_df = None

                if sc_df is not None and not sc_df.empty:
                    top_suppliers = get_top_suppliers_by_department(sc_df, selected_dept, top_n=10)

                    if top_suppliers:
                        share_pcts = [s.share_pct for s in top_suppliers]
                        hhi = calculate_hhi(share_pcts)

                        if hhi > 2500:
                            hhi_label = "Highly Concentrated"
                            hhi_color = "#f44336"
                        elif hhi > 1500:
                            hhi_label = "Moderately Concentrated"
                            hhi_color = "#ff9800"
                        else:
                            hhi_label = "Unconcentrated (Healthy)"
                            hhi_color = "#00ff88"

                        col_a, col_b, col_c = st.columns(3)
                        col_a.metric("HHI Score", f"{hhi:,.0f}", hhi_label)
                        col_b.metric("Top Supplier Share", f"{top_suppliers[0].share_pct:.1f}%", top_suppliers[0].supplier_name)
                        col_c.metric("Tracked Suppliers", len(top_suppliers))

                        rows = []
                        for s in top_suppliers:
                            rows.append({
                                "Supplier": s.supplier_name,
                                "SKUs": s.sku_count,
                                "Revenue Potential": f"KES {s.revenue_potential:,.0f}",
                                "Market Share %": f"{s.share_pct:.1f}%",
                                "Risk Score": f"{s.risk_score:.2f}",
                            })
                        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

                        st.markdown("---")
                        st.markdown("#### Supplier Failure Impact Simulator")
                        supplier_names = [s.supplier_name for s in top_suppliers]
                        sim_supplier = st.selectbox("Select Supplier to Simulate Failure", supplier_names, key="sa_fail_sim")

                        if st.button("Simulate Failure", key="sa_fail_btn"):
                            impact = analyze_supplier_failure_impact(sc_df, sim_supplier, selected_dept)
                            sev_colors = {"CRITICAL": "#f44336", "HIGH": "#ff5722", "MEDIUM": "#ff9800", "LOW": "#4caf50", "NONE": "#888"}
                            sev = impact['estimated_stockout_severity']
                            col1, col2, col3 = st.columns(3)
                            col1.metric("Severity", sev)
                            col2.metric("Affected SKUs", impact['affected_sku_count'])
                            col3.metric("Revenue at Risk", f"KES {impact['revenue_at_risk']:,.0f}")
                            st.info(f"Coverage Loss: {impact['coverage_loss_pct']:.1f}% | Substitute Availability: {impact['substitute_availability']*100:.0f}%")
                    else:
                        st.warning(f"No supplier data found for {selected_dept}.")
                else:
                    st.error("Could not load scorecard data. Ensure Full_Product_Allocation_Scorecard exists.")
        except Exception as e:
            st.error(f"Supplier Intelligence module error: {e}")


# ─────────────────────────────────────────────────────────────────────
# Footer
# ─────────────────────────────────────────────────────────────────────
st.sidebar.markdown("---")
st.sidebar.markdown("""
<div style="text-align: center; color: #666; font-size: 0.75em; padding: 10px;">
    OASIS Retail Manager v4.0<br/>
    Operations · Allocation · Sales Intelligence · Simulation<br/>
</div>
""", unsafe_allow_html=True)

