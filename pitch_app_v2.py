import streamlit as st
import pandas as pd
import io
import plotly.express as px
import plotly.graph_objects as go
import os
import sys

# P3: Graceful import fallback for pitch dependencies
try:
    from pitch_data_ingestor_v2 import ForensicOperationsIngestor
    INGESTOR_AVAILABLE = True
except ImportError as _ing_err:
    INGESTOR_AVAILABLE = False

try:
    from export_generator import generate_excel_export, generate_word_export
    EXPORT_AVAILABLE = True
except ImportError as _exp_err:
    EXPORT_AVAILABLE = False

st.set_page_config(page_title="O.A.S.I.S. Operations Forensic Audit", layout="wide", initial_sidebar_state="expanded")

# --- PREMIUM DESIGN SYSTEM (O.A.S.I.S. Standard) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;600;700&family=JetBrains+Mono:wght@400;700&display=swap');
    
    :root {
        --neon-emerald: #10b981;
        --deep-midnight: #0a0b10;
        --glass-bg: rgba(255, 255, 255, 0.03);
    }

    .stApp {
        background-color: var(--deep-midnight);
        color: #d1d1d1;
        font-family: 'Outfit', sans-serif;
    }

    /* Glassmorphism Cards */
    .metric-box {
        background: var(--glass-bg);
        backdrop-filter: blur(12px);
        border: 1px solid rgba(255, 255, 255, 0.05);
        border-radius: 16px;
        padding: 24px;
        transition: all 0.3s cubic-bezier(0.4, 0, 0.2, 1);
        margin-bottom: 20px;
    }
    .metric-box:hover {
        border-color: var(--neon-emerald);
        background: rgba(16, 185, 129, 0.02);
        box-shadow: 0 0 30px rgba(16, 185, 129, 0.05);
    }

    .metric-label { font-size: 0.75rem; color: #888; text-transform: uppercase; letter-spacing: 0.05em; font-weight: 600; }
    .metric-value { font-size: 1.8rem; font-weight: 700; color: #fff; line-height: 1.2; margin: 8px 0; }
    .metric-delta { font-size: 0.8rem; font-family: 'JetBrains Mono'; }

    /* Custom Header Pulse */
    .neural-header {
        background: linear-gradient(90deg, #fff 0%, #10b981 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-weight: 700;
        letter-spacing: -0.02em;
        font-size: 2.5rem;
    }
</style>
""", unsafe_allow_html=True)

def render_dashboard(audit):
    # --- EXECUTIVE METHODOLOGY ---
    st.markdown("""
    <div class="metric-box" style="border-left: 4px solid var(--neon-emerald);">
        <div class="metric-label">🔬 O.A.S.I.S. FORENSIC METHODOLOGY v2.0</div>
        <div style="font-size: 0.9em; color: #aaa; margin-top: 8px;">
            Analyzing logistics logs at <b>95% Service Level</b> precision to identify hidden revenue bleed (Ghost Demand) 
            and trapped capital (Dead Stock). Logic grounded in 17% retail WACC.
        </div>
    </div>""", unsafe_allow_html=True)
    
    st.markdown("### 📥 Consulting Diagnostic Packs")
    colA, colB = st.columns(2)
    with colA:
        excel_data = generate_excel_export(audit)
        st.download_button(
            label="📊 Download Raw Forensic Audit (Excel)",
            data=excel_data,
            file_name="OASIS_Forensic_Dataset.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )
    with colB:
        word_data = generate_word_export(audit)
        st.download_button(
            label="📄 Download Executive Summary (Word)",
            data=word_data,
            file_name="OASIS_Executive_Diagnostic.docx",
            mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            use_container_width=True
        )
    st.divider()
    
    tab1, tab2, tab3 = st.tabs(["🔴 The Revenue Bleed (Store)", "🚚 Supplier Hostility (Supply Chain)", "📉 Network Entropy (Logistics)"])
    
    # ---------------- TAB 1: Revenue Bleed (AMIT & DHARAM) ---------------- #
    with tab1:
        st.header("Retail Floor Inefficiency")
        cat = audit.get('catalog', {})
        if not cat:
            st.warning("No POS log data available to analyze floor inefficiency.")
        else:
            # High Impact Metric Tiles
            m1, m2, m3 = st.columns(3)
            with m1:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Total Capital Evaluated</div>
                    <div class="metric-value">KES {cat.get('total_capital_tied', 0):,.0f}</div>
                    <div class="metric-delta" style="color: #666;">Snapshot of Floor Assets</div>
                </div>""", unsafe_allow_html=True)
            
            with m2:
                st.markdown(f"""
                <div class="metric-box" style="border-bottom: 2px solid #e74c3c;">
                    <div class="metric-label">Trapped Capital (Dead Stock)</div>
                    <div class="metric-value" style="color: #e74c3c;">KES {cat.get('dead_stock_value', 0):,.0f}</div>
                    <div class="metric-delta" style="color: #e74c3c;">{cat.get('dead_stock_count', 0)} SKU Inefficiencies</div>
                </div>""", unsafe_allow_html=True)
            
            with m3:
                st.markdown(f"""
                <div class="metric-box" style="border-bottom: 2px solid #f39c12;">
                    <div class="metric-label">Revenue Bleed (Ghost Demand)</div>
                    <div class="metric-value" style="color: #f39c12;">KES {cat.get('ghost_demand_value', 0):,.0f}</div>
                    <div class="metric-delta" style="color: #f39c12;">{cat.get('ghost_demand_count', 0)} Missing Fast Movers</div>
                </div>""", unsafe_allow_html=True)
            
            st.divider()
            col_c1, col_c2 = st.columns(2)
            
            with col_c1:
                st.subheader("Capital Allocation Health")
                healthy = max(0, cat.get('total_capital_tied', 0) - cat.get('dead_stock_value', 0))
                if healthy > 0 or cat.get('dead_stock_value', 0) > 0:
                    fig1 = px.pie(
                        names=['Working Capital (Healthy)', 'Trapped Capital (Dead Stock)'],
                        values=[healthy, cat.get('dead_stock_value', 0)],
                        color_discrete_sequence=['#2ecc71', '#e74c3c'], hole=0.5
                    )
                    st.plotly_chart(fig1, use_container_width=True)
                st.info("💡 **OASIS Solution [AMIT ENGINE]:** OASIS algorithmically blocks these dead stock pieces from re-ordering, immediately liquidating the red zone.")
                
            with col_c2:
                st.subheader("Top 'Ghost Demand' Bleeders")
                if cat.get('ghost_demand_list'):
                    gd_df = pd.DataFrame(cat['ghost_demand_list']).head(10)
                    fig2 = px.bar(
                        gd_df, y='item_name', x='est_lost_revenue',
                        orientation='h', color_discrete_sequence=['#f39c12']
                    )
                    fig2.update_layout(yaxis={'categoryorder':'total ascending'})
                    st.plotly_chart(fig2, use_container_width=True)
                st.info("💡 **OASIS Solution [DHARAM ENGINE]:** OASIS intelligently patches demand gaps when fast-movers are missing, ensuring they are always fully funded next cycle.")
            
            with st.expander("🔬 The Math of O.A.S.I.S. (Forensic Integrity)"):
                st.latex(r"Ghost\ Demand\ Loss = ADS \times Recovery\ Window \times Unit\ Price")
                st.markdown("""
                **Strict Propagation Methodology:**
                - **Recovery Window:** Unlike human buyers who guess, O.A.S.I.S. calculates the specific window required to restore 95% confidence.
                - **Formula:** $Window = Avg\_Lead\_Time + (1.645 \times \sigma_{Lead\_Time})$
                - **Logic:** This ensures the store is protected against the specific volatility of the assigned supplier.
                """)

    # ---------------- TAB 2: Supplier Toxicity (LATA) ---------------- #
    with tab2:
        st.header("Supplier Risk & Toxicity Index")
        sup = audit.get('suppliers', {})
        if not sup:
            st.warning("No GRN log data available to analyze supplier variance.")
        else:
            # High Impact Metric Tiles
            m1, m2, m3 = st.columns(3)
            with m1:
                st.markdown(f"""
                <div class="metric-box">
                    <div class="metric-label">Suppliers Scanned</div>
                    <div class="metric-value">{sup.get('total_suppliers', 0)}</div>
                    <div class="metric-delta" style="color: #666;">Active Logistics Nodes</div>
                </div>""", unsafe_allow_html=True)
            
            with m2:
                st.markdown(f"""
                <div class="metric-box" style="border-bottom: 2px solid #e74c3c;">
                    <div class="metric-label">Toxic / At-Risk Vendors</div>
                    <div class="metric-value" style="color: #e74c3c;">{sup.get('criminal_count', 0)}</div>
                    <div class="metric-delta" style="color: #e74c3c;">STI Score > 0.15</div>
                </div>""", unsafe_allow_html=True)
            
            with m3:
                # P2: Safety check for supplier_list key
                supplier_list = sup.get('supplier_list', [])
                if supplier_list:
                    sdf = pd.DataFrame(supplier_list)
                    avg_sti = sdf['sti_score'].mean() if 'sti_score' in sdf.columns else 0
                else:
                    sdf = pd.DataFrame()
                    avg_sti = 0
                st.markdown(f"""
                <div class="metric-box" style="border-bottom: 2px solid #fff;">
                    <div class="metric-label">Network Toxicity Avg</div>
                    <div class="metric-value">{avg_sti:.2f}</div>
                    <div class="metric-delta" style="color: #666;">0.0 = Baseline Optimal</div>
                </div>""", unsafe_allow_html=True)
            
            st.divider()
            
            st.subheader("The 'Supplier Hostility' Spectrum")
            if sdf.empty:
                st.warning("No supplier data available for scatter analysis.")
            else:
                fig3 = px.scatter(
                    sdf, x='lead_variance', y='fulfillment', color='sti_score',
                    hover_name='supplier', size='orders',
                    custom_data=['short_supply_returns'],
                    color_continuous_scale='RdYlGn_r',
                    labels={'lead_variance': 'Lead Time Variance (Days)', 'fulfillment': 'Order Fulfillment %', 'sti_score': 'Toxicity Index'}
                )
                fig3.update_traces(hovertemplate="<b>%{hovertext}</b><br>Fulfillment: %{y}%<br>Lead Var: %{x} days<br>Short-Supply Returns: %{customdata[0]}<br>STI: %{marker.color:.2f}")
                fig3.add_hline(y=85, line_dash="dash", line_color="gray", annotation_text="Reliable Target")
                fig3.add_vline(x=3, line_dash="dash", line_color="gray", annotation_text="Variance Threshold")
                st.plotly_chart(fig3, use_container_width=True)
            
            st.warning(f"**Actionable Logic:** O.A.S.I.S. does not use flat safety buffers. It dynamically inflates inventory funding for the {sup.get('criminal_count', 0)} vendors with high STI scores to preserve 95% service level.")
            st.info("💡 **OASIS Solution [LATA ENGINE]:** O.A.S.I.S uses a neural shield to mathematically throttle ordering patterns for these bad actors.")

            with st.expander("🔬 The Math of O.A.S.I.S. (Forensic Integrity)"):
                st.latex(r"STI = (Failure\ Rate \times 0.7) + (Lead\ Time\ Volatility \times 0.3)")
                st.markdown("""
                **Supplier Toxicity Methodology:**
                - **Failure Rate:** $(1 - Fulfillment\%)$ - Direct impact on shelf availability.
                - **Lead Time Volatility:** $\sigma_{LT} / Avg_{LT}$ - Direct impact on safety stock inflation.
                - **Index:** A score > 0.15 identifies a vendor that is statistically damaging to the retailer's working capital.
                """)

    # ---------------- TAB 3: Network Entropy ---------------- #
    with tab3:
        st.header("Network Friction Costs")
        net = audit.get('network', {})
        
        m1, m2, m3, m4 = st.columns(4)
        with m1:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-label">Return Events</div>
                <div class="metric-value">{net.get('shrink_events', 0)}</div>
            </div>""", unsafe_allow_html=True)
        with m2:
            st.markdown(f"""
            <div class="metric-box" style="border-bottom: 2px solid #e74c3c;">
                <div class="metric-label">Returns Cost</div>
                <div class="metric-value" style="color: #e74c3c;">KES {net.get('shrink_cost', 0):,.0f}</div>
            </div>""", unsafe_allow_html=True)
        with m3:
            st.markdown(f"""
            <div class="metric-box">
                <div class="metric-label">Lateral Transfers</div>
                <div class="metric-value">{net.get('transfer_events', 0)}</div>
            </div>""", unsafe_allow_html=True)
        with m4:
            st.markdown(f"""
            <div class="metric-box" style="border-bottom: 2px solid #e74c3c;">
                <div class="metric-label">Transfer Friction</div>
                <div class="metric-value" style="color: #e74c3c;">KES {net.get('transfer_cost', 0):,.0f}</div>
            </div>""", unsafe_allow_html=True)
        
        st.divider()
        
        total_entropy = net.get('entropy_cost_est', 0)
        if total_entropy > 0:
            st.error(f"**Total Network Entropy Cost: KES {total_entropy:,.0f}**")
            
            w_cost = net.get('wastage_cost', 0)
            f_cost = net.get('friction_cost', 0)
            t_cost = net.get('transfer_cost', 0)
            
            col_z1, col_z2 = st.columns(2)
            with col_z1:
                st.subheader("Entropy Breakdown")
                fig = px.pie(
                    names=['Internal Wastage (Expiry/Damage)', 'Operational Friction (Short-Supply)', 'Inter-Branch Transfers'],
                    values=[w_cost, f_cost, t_cost],
                    color_discrete_sequence=['#e74c3c', '#f39c12', '#3498db'], hole=0.4
                )
                st.plotly_chart(fig, use_container_width=True)
            
            with col_z2:
                st.subheader("Operational Semantic Audit")
                st.write(f"- **Internal Wastage:** KES {w_cost:,.2f}")
                st.write(f"- **Correction Friction:** KES {f_cost:,.2f}")
                st.write(f"- **Logistical Friction:** KES {t_cost:,.2f}")
                st.info("💡 **O.A.S.I.S. Note:** Empties and Bundle-banding PRTS units were excluded from this forensic cost calculation as they are operationally neutral.")
        
        st.markdown("""
        ### What does this mean?
        - **Returns / Spoilage:** Goods sent back to suppliers or written off due to expiry, damage, or theft. This is pure cash lost.
        - **Transfers:** Moving stock from Branch A to Branch B means **Initial Allocation Failed**. The buyer guessed wrong, and now you are spending logistical money to fix it.
        
        💡 **OASIS Solution [THE ALLOCATOR]:** OASIS solves allocation mathematically at the root. It doesn't guess. It uses physics-based algorithms to place the exact optimal quantity per store, severely reducing lateral transfers and dead stock expiry.
        """)


# --- UI Layout (Premium Header) ---
st.markdown("""
<div style="margin-bottom: 40px;">
    <div class="neural-header">O.A.S.I.S. FORENSIC DIAGNOSTIC</div>
    <div style="color: #666; font-size: 0.9em; letter-spacing: 0.1em; margin-top: -5px;">
        NEURAL AUDIT ENGINE // VERSION 2.0 // DEEP LOGISTICS INTELLIGENCE
    </div>
</div>
""", unsafe_allow_html=True)

# Sidebar for Setup (Premium Neural Pulse)
st.sidebar.markdown(f"""
<div class="metric-box" style="padding: 15px; border-left: 3px solid var(--neon-emerald);">
    <div style="display: flex; align-items: center; gap: 10px;">
        <div class="pulse" style="width: 10px; height: 10px; background: var(--neon-emerald); border-radius: 50%; box-shadow: 0 0 10px var(--neon-emerald);"></div>
        <div class="metric-label" style="margin: 0;">Engine Pulse: ACTIVE</div>
    </div>
    <div style="font-size: 0.7em; color: #666; margin-top: 5px;">Forensic Depth: 95% SL Target</div>
</div>""", unsafe_allow_html=True)

st.sidebar.header("🔌 Data Ingestion")
st.sidebar.markdown("Upload target operational logs:")
pos_file = st.sidebar.file_uploader("1. POS System Log", type=["csv", "json", "xls", "xlsx"])
grn_file = st.sidebar.file_uploader("2. GRN/Purchasing Log", type=["csv", "json", "xls", "xlsx"])
shrink_file = st.sidebar.file_uploader("3. Shrink/Adjustments Log", type=["csv", "json", "xls", "xlsx"])
trans_file = st.sidebar.file_uploader("4. Branch Transfers Log", type=["csv", "json", "xls", "xlsx"])

st.sidebar.divider()
st.sidebar.markdown("### 🏆 Live Case Study")
use_rhapta = st.sidebar.button("RUN RHAPTA SITE AUDIT", use_container_width=True)

if use_rhapta:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    cache_path = os.path.join(base_dir, "rhapta_demo_preloaded.json")
    
    if os.path.exists(cache_path):
        import json
        with open(cache_path, 'r') as f:
            audit = json.load(f)
        
        gen_at = audit.get('generated_at', 'Unknown')
        st.sidebar.caption(f"Last Forensic Scan: {gen_at}")
        
        if st.sidebar.button("♻️ Rebuild Forensic Cache", use_container_width=True):
            with st.spinner("Regenerating 14k SKU Graph..."):
                import subprocess
                subprocess.run([sys.executable, "cache_rhapta_demo.py"])
                st.rerun()
                
        st.sidebar.success("Audit Complete (Instant Load)! ")
        render_dashboard(audit)
    else:
        st.error("Pre-loaded audit cache missing. Please run `cache_rhapta_demo.py` first.")

elif pos_file is not None or grn_file is not None:
    if not INGESTOR_AVAILABLE:
        st.error("⚠️ `pitch_data_ingestor_v2` module not found. Cannot run custom forensic scans.")
    else:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        ingestor = ForensicOperationsIngestor(base_dir)
        with st.spinner("Initializing Custom Forensic Scan..."):
            ingestor.load_logs(pos_file=pos_file, grn_file=grn_file, shrink_file=shrink_file, transfer_file=trans_file)
            ingestor.run_pos_analysis()
            ingestor.run_supplier_analysis()
            ingestor.run_network_analysis()
            audit = ingestor.get_full_audit()
        st.sidebar.success("Custom Audit Complete!")
        render_dashboard(audit)

else:
    st.info("Awaiting Data Logs. Upload files or click 'Run Mock Store Audit' to begin.")
