import streamlit as st
import httpx
import pandas as pd
import plotly.express as px

st.set_page_config(page_title="Phase 1: Pitch Diagnostic", layout="wide")

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&display=swap');
    .stApp { font-family: 'Outfit', sans-serif; background-color: #0a0b10; color: #d1d1d1; }
    .metric-box { background: rgba(255, 255, 255, 0.03); backdrop-filter: blur(12px); border: 1px solid rgba(255, 255, 255, 0.05); border-radius: 16px; padding: 24px; margin-bottom: 20px;}
    .metric-label { font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }
    .metric-value { font-size: 1.8rem; font-weight: 700; color: #fff; line-height: 1.2; margin: 8px 0; }
</style>
""", unsafe_allow_html=True)

st.title("🔬 Phase 1: The Hook (Forensic Audit)")
st.info("This mosaic strictly communicates with the FastAPI `pitch-diagnostic-engine` microservice.")

if st.button("RUN DECOUPLED AUDIT REQUEST"):
    with st.spinner("Pinging API Gateway (localhost:8000/api/v1/pitch/audit/rhapta)"):
        try:
            # Contact our FastAPI Microservice!
            response = httpx.get("http://localhost:8000/api/v1/pitch/audit/rhapta", timeout=10.0)
            
            if response.status_code == 200:
                data = response.json().get("data", {})
                cat = data.get('catalog', {})
                
                st.success("✅ Audit payload received successfully from isolated API Engine!")
                
                m1, m2, m3 = st.columns(3)
                with m1:
                    st.markdown(f"""
                    <div class="metric-box">
                        <div class="metric-label">Total Capital Evaluated</div>
                        <div class="metric-value">KES {cat.get('total_capital_tied', 0):,.0f}</div>
                    </div>""", unsafe_allow_html=True)
                with m2:
                    st.markdown(f"""
                    <div class="metric-box" style="border-bottom: 2px solid #e74c3c;">
                        <div class="metric-label">Trapped Capital (Dead Stock)</div>
                        <div class="metric-value" style="color: #e74c3c;">KES {cat.get('dead_stock_value', 0):,.0f}</div>
                    </div>""", unsafe_allow_html=True)
                with m3:
                    st.markdown(f"""
                    <div class="metric-box" style="border-bottom: 2px solid #f39c12;">
                        <div class="metric-label">Revenue Bleed (Ghost Demand)</div>
                        <div class="metric-value" style="color: #f39c12;">KES {cat.get('ghost_demand_value', 0):,.0f}</div>
                    </div>""", unsafe_allow_html=True)
                
                st.write("**Note:** All heavy Neural Engine calculations were processed in the backend. 0% calculation overhead for this frontend page.")
                
            else:
                st.error(f"API Error {response.status_code}: {response.text}")
        except httpx.ConnectError:
            st.error("🚨 Could not reach API Gateway. Is the FastAPI sequence booted up (`entrypoint.py --mode bridge`) ?")
