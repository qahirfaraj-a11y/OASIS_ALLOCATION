import streamlit as st
import pandas as pd
import plotly.express as px
import os
import sys
from datetime import datetime

# Setup Pathing
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.exchange.exchange_registry import ExchangeRegistry
from oasis.exchange.secondary_market import SecondaryMarket
from oasis.exchange.clearing_house import ClearingHouse
from oasis.exchange.ui_utils import (
    format_kes, render_risk_badge, calculate_gpp_health, 
    render_sparkline, format_mpesa_id
)

def safe_rerun():
    if hasattr(st, "rerun"):
        st.rerun()
    else:
        st.experimental_rerun()

# --- SESSION STATE & ENGINE INIT ---
DATA_DIR = os.path.join(os.path.dirname(__file__), "oasis", "data")
if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

st.set_page_config(page_title="KUBER Financial Terminal", layout="wide", page_icon="🏦")

if 'registry' not in st.session_state:
    st.session_state.registry = ExchangeRegistry(DATA_DIR)
    st.session_state.market = SecondaryMarket(st.session_state.registry)
    st.session_state.ch = ClearingHouse(st.session_state.registry)

reg = st.session_state.registry
market = st.session_state.market
ch = st.session_state.ch

# --- STYLES (Premium Bloomberg Redesign) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;700&family=Outfit:wght@300;400;600;700&display=swap');
    
    :root {
        --terminal-green: #00ff41;
        --deep-night: #0a0b10;
        --glass-bg: rgba(255, 255, 255, 0.03);
    }

    .stApp {
        background-color: var(--deep-night);
        color: #d1d1d1;
        font-family: 'Outfit', sans-serif;
    }

    /* Glassmorphism Financial Cards */
    .metric-card {
        background: var(--glass-bg);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255,255,255,0.1);
        padding: 24px;
        border-radius: 16px;
        transition: all 0.3s ease;
    }
    .metric-card:hover {
        border-color: var(--terminal-green);
        box-shadow: 0 0 20px rgba(0, 255, 65, 0.1);
    }
    
    .financial-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.8em;
        font-weight: 700;
        color: #fff;
    }
    
    /* Bloomberg Ticker Tape style pulse */
    .pulse-bar {
        background: rgba(0,0,0,0.5);
        border-bottom: 1px solid var(--terminal-green);
        padding: 12px 20px;
        border-radius: 0 0 12px 12px;
        margin-bottom: 30px;
        display: flex;
        justify-content: space-around;
        align-items: center;
        font-family: 'JetBrains Mono', monospace;
        letter-spacing: -0.5px;
    }
    
    .ticker-label { color: #666; font-size: 0.65em; text-transform: uppercase; }
    .ticker-value { color: var(--terminal-green); font-size: 1.1em; font-weight: 700; }
</style>
""", unsafe_allow_html=True)

# --- TOP PULSE (FINTECH TICKER STYLE) ---
ledger = reg.registry["global_ledger"]
summary = reg.get_summary()

st.markdown(f"""
<div class="pulse-bar">
    <div style="text-align:center;">
        <div class="ticker-label">TVL (Total Value Locked)</div>
        <div class="ticker-value">{format_kes(summary["total_locked"])}</div>
    </div>
    <div style="text-align:center;">
        <div class="ticker-label">GPP Health</div>
        <div class="ticker-value" style="color: #00ff88;">{summary.get("gpp_coverage", 0.12):.2%}</div>
    </div>
    <div style="text-align:center;">
        <div class="ticker-label">24H Volume</div>
        <div class="ticker-value" style="color: #fff;">{format_kes(summary["total_volume"])}</div>
    </div>
    <div style="text-align:center;">
        <div class="ticker-label">Active Nodes</div>
        <div class="ticker-value" style="color: #fff;">{summary["active_count"]}</div>
    </div>
</div>
""", unsafe_allow_html=True)

# --- MAIN METRICS (Premium Glass Redesign) ---
m1, m2, m3, m4 = st.columns(4)

resilience = calculate_gpp_health(reg.registry)
res_color = "#00ff41" if resilience > 0.1 else "#ffaa00"

with m1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="ticker-label">Resilience Ratio</div>
        <div class="financial-value" style="color: {res_color};">{resilience:.1%}</div>
        <div style="font-size: 0.7em; color: #666;">Systemic Risk Buffer</div>
    </div>""", unsafe_allow_html=True)

with m2:
    st.markdown(f"""
    <div class="metric-card">
        <div class="ticker-label">Insurance Fund</div>
        <div class="financial-value">{format_kes(ledger.get("gpp_insurance_fund", 0.0))}</div>
        <div style="font-size: 0.7em; color: #666;">Global Loss Mitigation</div>
    </div>""", unsafe_allow_html=True)

with m3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="ticker-label">Liquidity Fund</div>
        <div class="financial-value">{format_kes(ledger.get("gpp_liquidity_fund", 0.0))}</div>
        <div style="font-size: 0.7em; color: #666;">Cash Reserve for P2P</div>
    </div>""", unsafe_allow_html=True)

with m4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="ticker-label">Platform Opex</div>
        <div class="financial-value">{format_kes(ledger.get("platform_fees", 0.0))}</div>
        <div style="font-size: 0.7em; color: #666;">Secondary Market Rev</div>
    </div>""", unsafe_allow_html=True)

st.divider()

# --- MAIN TABS ---
tab_market, tab_secondary, tab_ledger, tab_logs = st.tabs([
    "📍 Primary Market (POs)", 
    "💱 Secondary Market (P2P)", 
    "📊 Shadow Bank Ledger", 
    "📑 Technical Trace"
])
with tab_market:
    st.subheader("O.A.S.I.S. Inbound Projection (Awaiting Capital)")
    listed_pos = {k: v for k, v in reg.registry["active_positions"].items() if v["status"] == "LISTED"}
    
    if not listed_pos:
        st.info("No new PO projections from O.A.S.I.S. awaiting funding.")
    else:
        for pid, pos in listed_pos.items():
            with st.container():
                st.markdown(f"""
                <div class="metric-card" style="margin-bottom: 20px;">
                    <div style="display: flex; justify-content: space-between; align-items: flex-start;">
                        <div>
                            <span class="ticker-label">Asset Node</span>
                            <div style="font-size: 1.4em; font-weight: 700; color: #fff;">{pos['sku']}</div>
                        </div>
                        <div style="text-align: right;">
                            <span class="ticker-label">Projected Net Yield</span>
                            <div style="color: var(--terminal-green); font-weight: 700; font-family: 'JetBrains Mono';">12.25% (Net)</div>
                        </div>
                    </div>
                </div>""", unsafe_allow_html=True)
                
                c1, c2, c3 = st.columns([1, 2, 2])
                with c1:
                    render_risk_badge(pos['risk_tranche'])
                    st.write("")
                    st.write(f"**Exposure**: {pos['wp_score']:.2f} Wp")
                
                with c2:
                    st.write("**Forecast Velocity**")
                    render_sparkline(pos.get("velocity_history", [0]*10))
                
                with c3:
                    st.write(f"**Total Requirement**: {format_kes(pos['total_cost'])}")
                    st.progress(pos['shares_funded'])
                    st.write(f"<span class='ticker-label'>Funded: {pos['shares_funded']:.1%}</span>", unsafe_allow_html=True)
                    
                    investor_list = list(reg.registry["investors"].keys())
                    if investor_list:
                        selected_investor = st.selectbox("Commit as:", investor_list, key=f"sel_{pid}")
                        fund_amt = st.number_input("Commit Amount (KES)", min_value=10.0, max_value=pos['total_cost'], step=500.0, key=f"amt_{pid}")
                        if st.button("AUTHENTICATE & COMMIT", key=f"btn_{pid}"):
                            # 15% is the fixed gross yield for Tier 1 in this simulation
                            if reg.fund_position(selected_investor, pid, fund_amt, 0.15):
                                st.success(f"Position Funded. M-Pesa ID: {format_mpesa_id()}")
                                st.rerun()
                st.divider()

# --- TAB 2: SECONDARY MARKET (P2P Net Exchange) ---
with tab_secondary:
    st.subheader("Peer-to-Peer Secondary Market (TVL Optimization)")
    
    col_ask, col_execute = st.columns(2)
    
    with col_ask:
        st.markdown("""<div class="ticker-label" style="margin-bottom:10px;">📉 ACTIVE SELL SIDE (ASKS)</div>""", unsafe_allow_html=True)
        if not market.order_book["asks"]:
            st.info("Secondary market dormant. All holders maintaining positions.")
        else:
            for i, ask in enumerate(market.order_book["asks"]):
                with st.container(border=True):
                    st.markdown(f"""
                    <div style="font-family: 'JetBrains Mono';">
                        <span style="color: var(--terminal-green);">{ask['pos_id']}</span> | 
                        <span style="color: #fff;">{ask['fraction']:.1%} Share</span> | 
                        <span style="color: #fff;">{format_kes(ask['price_kes'])}</span>
                    </div>""", unsafe_allow_html=True)
                    if st.button(f"BUY ORDER #{i}", key=f"buy_{i}"):
                        st.info("Confirming M-Pesa Handshake...")
                        # In production this would trigger the actual matching engine
                        st.success("MATCH SUCCESSFUL")

    with col_execute:
        st.write("#### Exchange Guardians")
        st.markdown("> Triggers the GPP Safety Net (48h exit liquidity with 7% penalty).")
        if st.button("⚡ EXECUTE GPP LIQUIDITY BOT"):
            market.run_gpp_liquidity_bot()
            st.toast("GPP Engine processing liquidity requests...")
            st.rerun()
      st.rerun()

# --- TAB 3: SHADOW BANK LEDGER ---
with tab_ledger:
    st.subheader("Global Resilience & Treasury")
    
    c1, c2 = st.columns(2)
    
    with c1:
        st.write("#### GPP Reserve Distribution")
        gpp_data = {
            "Fund": ["Insurance (Waste)", "Liquidity (Exits)", "OpEx (System)"],
            "Balance": [
                ledger.get("gpp_insurance_fund", 0.0), 
                ledger.get("gpp_liquidity_fund", 0.0), 
                ledger.get("platform_fees", 0.0)
            ]
        }
        fig_gpp = px.pie(gpp_data, values="Balance", names="Fund", hole=0.5, color_discrete_sequence=px.colors.qualitative.Pastel)
        st.plotly_chart(fig_gpp, use_container_width=True)
        
    with c2:
        st.write("#### TVL by Departmental Risk")
        dept_data = []
        for pid, pos in reg.registry["active_positions"].items():
            dept_data.append({"Tranche": pos["risk_tranche"], "TVL": pos["total_cost"]})
        
        if dept_data:
            df_dept = pd.DataFrame(dept_data).groupby("Tranche").sum().reset_index()
            fig_dept = px.bar(df_dept, x="Tranche", y="TVL", color="Tranche", color_discrete_sequence=px.colors.qualitative.Bold)
            st.plotly_chart(fig_dept, use_container_width=True)
        else:
            st.info("No active funded positions.")

# --- TAB 4: TECHNICAL TRACE ---
with tab_logs:
    st.subheader("System Event Stream")
    logs = [
        f"GPP Balance updated to {format_kes(ledger['gpp_balance'])}",
        f"TVL tracked at {format_kes(summary['total_locked'])}",
        f"Total Trades processed: {ledger['total_trades']}"
    ]
    for log in reversed(logs):
        st.text(f"[{datetime.now().strftime('%H:%M:%S')}] SYS: {log}")
    
    st.divider()
    if st.button("Force Global Batch Settlement"):
        for pid in list(reg.registry["active_positions"].keys()):
            ch.process_batch_settlement(pid, 0, is_final=False) # Simplified match
        st.success("Batch Settlement triggered across all positions.")
        st.rerun()

# Sidebar: M-Pesa Bridge (NSE Kenya Inspired)
st.sidebar.header("📱 M-Pesa Simulated Bridge")
investor_ids = list(reg.registry["investors"].keys())
if investor_ids:
    target_inv = st.sidebar.selectbox("Target Investor", investor_ids)
    deposit_amt = st.sidebar.number_input("Deposit Amount (KES)", min_value=100.0, step=500.0)
    if st.sidebar.button("🔗 Simulate M-Pesa Cash-In"):
        if reg.process_cash_in(target_inv, deposit_amt):
            tx_id = format_mpesa_id()
            st.sidebar.success(f"Confirmed! TX: {tx_id}")
            safe_rerun()

st.sidebar.divider()
st.sidebar.header("Exchange Management")
if st.sidebar.button("Wipe & Reset Registry (Demo Only)"):
    if os.path.exists(os.path.join(DATA_DIR, "kuber_registry.json")):
        os.remove(os.path.join(DATA_DIR, "kuber_registry.json"))
        st.rerun()

st.sidebar.divider()
st.sidebar.markdown("### Investor Registry")
for iid, inv in reg.registry["investors"].items():
    st.sidebar.write(f"👤 {inv['name']} | Avail: {format_kes(inv['available_capital'])}")
