import streamlit as st
import pandas as pd
import plotly.express as px
import httpx
import asyncio
import time
from datetime import datetime

st.set_page_config(page_title="OASIS Master Control", layout="wide", page_icon="🛰️")

# --- STYLES (Neon Enterprise) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@400;700&display=swap');
    
    .stApp { background-color: #05070a; color: #e0e0e0; font-family: 'Outfit', sans-serif; }
    .node-card {
        background: rgba(255, 255, 255, 0.03);
        border: 1px solid rgba(255, 255, 255, 0.1);
        border-radius: 12px;
        padding: 20px;
        margin-bottom: 15px;
    }
</style>
""", unsafe_allow_html=True)

st.title("🛰️ OASIS Master Control")
st.markdown("### Regional Fleet Monitoring — Global Intelligence Layer")

HUB_URL = "http://localhost:8000"

async def fetch_nodes():
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(f"{HUB_URL}/api/v1/nodes")
            return response.json()
        except:
            return {}

nodes = asyncio.run(fetch_nodes())

if not nodes:
    st.info("No active Store Nodes detected in the universe. Awaiting initial pulse...")
else:
    # Aggregated Stats
    total_tvl = sum(n['metrics'].get('tvl_locked', 0) for n in nodes.values())
    total_val = sum(n['metrics'].get('total_inventory_value', 0) for n in nodes.values())
    avg_stockout = sum(n['metrics'].get('stockout_rate', 0) for n in nodes.values()) / len(nodes)
    
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("Nodes Active", len(nodes))
    m2.metric("Total TVL (KES)", f"{total_tvl:,.0f}")
    m3.metric("Fleet Inventory Value", f"{total_val:,.0f}")
    m4.metric("Avg Stockout Rate", f"{avg_stockout:.1%}")

    st.divider()
    
    # Node List
    st.subheader("📍 Distributed Node Status")
    for node_id, data in nodes.items():
        with st.container():
            st.markdown(f"""
            <div class="node-card">
                <div style="display: flex; justify-content: space-between; align-items: center;">
                    <div style="font-size: 1.2em; font-weight: 700; color: #00ff88;">{node_id}</div>
                    <div style="font-size: 0.8em; color: #888;">Last Seen: {data['last_seen']}</div>
                </div>
                <div style="display: grid; grid-template-columns: repeat(4, 1fr); gap: 20px; margin-top: 15px;">
                    <div>
                        <span style="font-size: 0.7em; color: #666; text-transform: uppercase;">Inventory Value</span><br/>
                        <span style="font-weight: 600;">KES {data['metrics']['total_inventory_value']:,.0f}</span>
                    </div>
                    <div>
                        <span style="font-size: 0.7em; color: #666; text-transform: uppercase;">Stockout Rate</span><br/>
                        <span style="font-weight: 600;">{data['metrics']['stockout_rate']:.1%}</span>
                    </div>
                    <div>
                        <span style="font-size: 0.7em; color: #666; text-transform: uppercase;">KUBER TVL</span><br/>
                        <span style="font-weight: 600;">KES {data['metrics']['tvl_locked']:,.0f}</span>
                    </div>
                    <div>
                        <span style="font-size: 0.7em; color: #666; text-transform: uppercase;">Health</span><br/>
                        <span style="color: #00ff88;">{data['health']['db_integrity']}</span>
                    </div>
                </div>
            </div>
            """, unsafe_allow_html=True)

# Auto-refresh
time.sleep(5)
st.rerun()
