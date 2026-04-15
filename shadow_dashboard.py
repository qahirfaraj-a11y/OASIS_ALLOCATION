import streamlit as st
import pandas as pd
import os
import glob
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- SETTINGS & THEME ---
st.set_page_config(
    page_title="O.A.S.I.S. Shadow Audit Hub",
    page_icon="⚖️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Deep Night Glassmorphism Theme
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;800&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
        color: #E0E0E0;
    }

    .stApp {
        background: radial-gradient(circle at top right, #1a1a2e, #16213e, #0f3460);
    }

    /* Glassmorphism Card */
    .glass-card {
        background: rgba(255, 255, 255, 0.05);
        backdrop-filter: blur(10px);
        border-radius: 15px;
        border: 1px solid rgba(255, 255, 255, 0.1);
        padding: 2rem;
        margin-bottom: 1.5rem;
        box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
        transition: transform 0.3s ease;
    }
    
    .glass-card:hover {
        transform: translateY(-5px);
        background: rgba(255, 255, 255, 0.08);
    }

    /* Metrics */
    .metric-title {
        color: #8892b0;
        font-size: 0.9rem;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin-bottom: 0.5rem;
    }
    .metric-value {
        font-size: 2.2rem;
        font-weight: 800;
        background: linear-gradient(90deg, #4cc9f0, #4361ee);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    .metric-sub {
        font-size: 0.8rem;
        color: #a8b2d1;
    }
    
    h1, h2, h3 {
        font-weight: 800 !important;
        letter-spacing: -1px;
    }
    
    /* System Status Dot */
    .status-dot {
        height: 10px;
        width: 10px;
        background-color: #00ff88;
        border-radius: 50%;
        display: inline-block;
        margin-right: 8px;
        box-shadow: 0 0 10px #00ff88;
    }
    </style>
""", unsafe_allow_html=True)

# --- DATA LOADING ---
LOG_DIR = "shadow_logs"
REPORTS_DIR = os.path.join("monitoring", "reports")

@st.cache_data(ttl=60)
def load_historical_data():
    files = glob.glob(os.path.join(LOG_DIR, "shadow_comparison_*.csv"))
    if not files:
        return pd.DataFrame()
    
    all_data = []
    for f in sorted(files):
        try:
            date_str = os.path.basename(f).replace("shadow_comparison_", "").replace(".csv", "")
            dt = datetime.strptime(date_str, "%Y%m%d")
            df = pd.read_csv(f)
            df['Audit_Date'] = dt
            all_data.append(df)
        except Exception as e:
            continue
    
    if not all_data:
        return pd.DataFrame()
    return pd.concat(all_data, ignore_index=True)

df_hist = load_historical_data()

# --- SIDEBAR ---
with st.sidebar:
    st.markdown("### ⚖️ O.A.S.I.S.")
    st.markdown("#### Shadow Auditor")
    st.markdown("""
    **Status:** <span class="status-dot"></span> Active<br>
    **Engine:** O.A.S.I.S. Core 4.2<br>
    **Mode:** Forensic Comparison
    """, unsafe_allow_html=True)
    
    st.divider()
    
    if not df_hist.empty:
        dates = sorted(df_hist['Audit_Date'].unique(), reverse=True)
        selected_date = st.selectbox("Select Audit Cycle", dates, format_func=lambda x: x.strftime("%Y-%m-%d"))
        df_selected = df_hist[df_hist['Audit_Date'] == selected_date]
    else:
        st.warning("No audit logs found.")
        df_selected = pd.DataFrame()

# --- HEADER ---
header_col1, header_col2 = st.columns([3, 1])
with header_col1:
    st.title("Shadow Mode Audit Hub")
    st.markdown("Comparing Algorithmic Precision vs. Human Procurement Gut.")
with header_col2:
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

if df_selected.empty:
    st.info("Awaiting the first Shadow Audit cycle. Drop a scorecard into `monitoring/inbound` to begin.")
    st.stop()

# --- EXECUTIVE SUMMARY ---
s_col1, s_col2, s_col3, s_col4 = st.columns(4)

# Calculations
total_items = len(df_selected)
aligned = len(df_selected[df_selected['Divergence'] == 'ALIGNED'])
accuracy = (aligned / total_items) * 100 if total_items > 0 else 0
missed_rev = df_selected[df_selected['Divergence'] == 'HUMAN_MISSED']['Shadow_Order_Value'].sum()

# Waste risk (17% WACC on over-ordered quantity value)
over_ordered = df_selected[df_selected['Divergence'] == 'HUMAN_OVER_ORDERED']
unit_cost_col = over_ordered['Unit_Cost'].fillna(100) if 'Unit_Cost' in over_ordered.columns else 100
waste_risk = ((over_ordered['Human_Order_Qty'] - over_ordered['Shadow_Order_Qty']) * unit_cost_col * 0.17).sum()

with s_col1:
    st.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">Engine Alignment</div>
        <div class="metric-value">{accuracy:.1f}%</div>
        <div class="metric-sub">Human vs. Algorithm parity</div>
    </div>
    """, unsafe_allow_html=True)

with s_col2:
    st.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">Missed Opportunities</div>
        <div class="metric-value">KES {missed_rev/1e6:.1f}M</div>
        <div class="metric-sub">Human missed stockout risk</div>
    </div>
    """, unsafe_allow_html=True)

with s_col3:
    st.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">Capital Waste</div>
        <div class="metric-value">KES {waste_risk/1e3:.1f}K</div>
        <div class="metric-sub">Annualized over-order cost</div>
    </div>
    """, unsafe_allow_html=True)

with s_col4:
    st.markdown(f"""
    <div class="glass-card">
        <div class="metric-title">Lines Evaluated</div>
        <div class="metric-value">{total_items:,}</div>
        <div class="metric-sub">SKUs audited this cycle</div>
    </div>
    """, unsafe_allow_html=True)

# --- CHARTS ---
c_col1, c_col2 = st.columns(2)

with c_col1:
    st.markdown("### Divergence Breakdown")
    div_counts = df_selected['Divergence'].value_counts().reset_index()
    div_counts.columns = ['Divergence', 'count']
    fig_pie = px.pie(div_counts, values='count', names='Divergence', 
                 color_discrete_sequence=['#4cc9f0', '#f72585', '#7209b7', '#3a0ca3'],
                 hole=0.4)
    fig_pie.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', 
                          font_color='#E0E0E0', showlegend=True)
    st.plotly_chart(fig_pie, use_container_width=True)

with c_col2:
    st.markdown("### Accuracy Trend (14-Day)")
    # Group by date and calculate accuracy
    trend = df_hist.groupby('Audit_Date').apply(lambda x: (len(x[x['Divergence'] == 'ALIGNED']) / len(x)) * 100).reset_index()
    trend.columns = ['Date', 'Accuracy']
    fig_line = px.line(trend, x='Date', y='Accuracy', markers=True)
    fig_line.update_traces(line_color='#4cc9f0', line_width=4)
    fig_line.update_layout(paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', 
                           font_color='#E0E0E0', yaxis_range=[0, 100])
    st.plotly_chart(fig_line, use_container_width=True)

# --- FORENSIC DATA ---
st.markdown("### Forensic Audit Table")
cols_to_show = ['Item_Name', 'Shadow_Order_Qty', 'Human_Order_Qty', 'Divergence', 'Divergence_Detail']
st.dataframe(df_selected[cols_to_show], use_container_width=True, hide_index=True)

# --- REPORT HUB ---
st.divider()
st.markdown("### 📄 Generated Audit Reports")
reports = glob.glob(os.path.join(REPORTS_DIR, "*.docx"))
if reports:
    rep_col1, rep_col2 = st.columns(2)
    for i, r in enumerate(sorted(reports, reverse=True)[:10]):
        target_col = rep_col1 if i % 2 == 0 else rep_col2
        with target_col:
            fname = os.path.basename(r)
            with open(r, "rb") as f:
                st.download_button(
                    label=f"💾 Download {fname}",
                    data=f,
                    file_name=fname,
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    key=f"dl_{i}"
                )
else:
    st.info("No Word reports generated yet. The background auditor generates these upon processing file drops.")

st.markdown("""
<div style="text-align: center; color: #8892b0; font-size: 0.8rem; margin-top: 5rem;">
    O.A.S.I.S. Forensic Auditor • Confidential Client Intelligence • © 2026
</div>
""", unsafe_allow_html=True)
