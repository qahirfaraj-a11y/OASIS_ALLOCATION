import streamlit as st
import pandas as pd
import numpy as np
import torch
import plotly.express as px
import plotly.graph_objects as go
import pydeck as pdk
import json
import math
import os
import sys

# Add models to path
sys.path.append(os.getcwd())

from models.store_gnn import StoreGraphNetwork
# from models.train_store_gnn import generate_traffic_friction # Not needed if we get from sim

# Page Config
st.set_page_config(page_title="ST-GAT Market Pulse", layout="wide", page_icon="🧠")

# Custom CSS
st.markdown("""
<style>
    .metric-card {
        background-color: #1E1E1E;
        padding: 15px;
        border-radius: 10px;
        border-left: 5px solid #FF4B4B;
    }
    .risk-high { color: #FF4B4B; font-weight: bold; }
    .risk-med { color: #FFA500; font-weight: bold; }
    .risk-low { color: #00FF00; font-weight: bold; }
</style>
""", unsafe_allow_html=True)

# --- 1. Load Model & Simulation ---
@st.cache_resource
def load_resources():
    network_path = "stores_network.json"
    if not os.path.exists(network_path):
        st.error(f"Network file not found: {network_path}")
        return None, None

    # Init Simulator
    from network_simulation import NetworkSimulator
    sim = NetworkSimulator(network_path)
    
    # Load Model
    # Get Feature Dim from one sample
    stub_feat = sim.get_feature_matrix()
    F_in = stub_feat.shape[1]
    
    model = StoreGraphNetwork(in_features=F_in, edge_dim=1)
    
    if os.path.exists("st_gat_v2.pt"):
        try:
            state_dict = torch.load("st_gat_v2.pt")
            
            # --- PATCH: Handle Dimension Mismatch (29 -> 30) ---
            # The model now expects 30 inputs (added Salary Hit), but checkpoint has 29.
            # We pad the weight matrix with zeros for the new feature so it has no initial effect.
            key = 'temporal_lstm.weight_ih_l0'
            if key in state_dict:
                weight = state_dict[key]
                if weight.shape[1] == 29:
                    print("⚠️ Patching Checkpoint: Extending input dimension 29 -> 30 (Salary Hit)")
                    # Pad the last dimension with zeros
                    # Shape: [Hidden*4, Input] -> [256, 29]
                    # We need [256, 30]
                    padding = torch.zeros(weight.shape[0], 1)
                    new_weight = torch.cat([weight, padding], dim=1)
                    state_dict[key] = new_weight
            
            model.load_state_dict(state_dict, strict=False)
            print("✅ Model weights loaded successfully (with patch).")
        except Exception as e:
            st.error(f"Failed to load model weights: {e}")
            st.warning("Proceeding with random initialization.")
    else:
        st.warning("⚠️ Model weights not found. Using random initialization.")
    
    model.eval()
    return model, sim

model, sim = load_resources()

if not model or not sim:
    st.stop()

# --- 2. Simulation Controls & Scenarios ---
st.sidebar.header("🎛️ Simulation Controls")

# Scenario: Rush Hour
rush_hour = st.sidebar.checkbox("🚦 Simulate Rush Hour", value=False, help="Increases Traffic Friction by 50%")

# Scenario: Heavy Rain (Slider)
rain_intensity = st.sidebar.slider("🌧️ Rain Intensity", 0.0, 1.0, 0.0, 0.1)

# Scenario: Budget Stress
budget_scale = st.sidebar.slider("💰 Global Budget Scaling", 0.5, 2.0, 1.0, 0.1)

# Multi-Day Control
sim_days = st.sidebar.slider("Simulation Step Size (Days)", 1, 30, 7)

col_ctrl, col_stat = st.columns([1, 4])
with col_ctrl:
    if st.button("▶️ Run Simulation", type="primary"):
        progress_bar = st.sidebar.progress(0)
        status_text = st.sidebar.empty()
        
        for d in range(sim_days):
            sim.step()
            progress = (d + 1) / sim_days
            progress_bar.progress(progress)
            status_text.text(f"Simulating Day {sim.current_day}...")
            
        status_text.text(f"Completed {sim_days} Days!")
        st.rerun()

with col_stat:
    st.info(f"📅 Day: **{sim.current_day}** | Agents: **{len(sim.simulators)}** | Network Status: **{'RUSH HOUR' if rush_hour else 'Normal'}**")

# --- 3. Inference Loop (What-If Engine) ---
# Get Base State
x_t = sim.get_feature_matrix() # [N, F]

# APPLY SCENARIOS (Data Injection)
if rain_intensity > 0:
    # Index 28 is Weather
    x_t[:, 28] = rain_intensity

if abs(budget_scale - 1.0) > 0.01:
    # Index 17 is Monthly Budget 
    shift = math.log10(budget_scale) / 10.0
    x_t[:, 17] += shift

# Reshape for LSTM
T = 30
x_seq = x_t.unsqueeze(0).unsqueeze(0).expand(1, T, -1, -1) 

# Get Traffic & Edges (Directed)
traffic = sim.get_traffic_matrix() # [N, N, 1]
if rush_hour:
    traffic += 0.5 # Global friction

edge_attr = traffic
adj = sim.adj

# Run Inference
with torch.no_grad():
    outputs = model(x_seq, adj, edge_attr)

stores = sim.stores_data

# --- 4. Sidebar: Risk Alert Panel ---
st.sidebar.header("🚨 Risk Triage Panel")

# Extract Risk Scores
risk_scores = outputs['risk'].squeeze().tolist() 
store_ids = [s['store_id'] for s in stores]
risk_data = pd.DataFrame({
    "Store": store_ids,
    "Risk Score": risk_scores
}).sort_values("Risk Score", ascending=False)

for _, row in risk_data.iterrows():
    r = row['Risk Score']
    color = "risk-high" if r > 0.7 else ("risk-med" if r > 0.4 else "risk-low")
    icon = "🔴" if r > 0.7 else ("🟠" if r > 0.4 else "🟢")
    
    st.sidebar.markdown(f"""
    <div style='padding: 5px; border-bottom: 1px solid #333;'>
        <span style='font-size: 1.2em;'>{icon}</span> 
        <strong>{row['Store']}</strong> 
        <span class='{color}' style='float: right;'>{r:.2f}</span>
    </div>
    """, unsafe_allow_html=True)
    
selected_store = st.sidebar.selectbox("Drill Down Store", ["ALL STORES"] + store_ids)

# --- 5. Main Panel: The Live Graph Map ---
tab_map, tab_intel, tab_cluster = st.tabs(["🗺️ Live Network Map", "🧠 Store Intelligence", "🔗 Cluster Analysis"])

with tab_map:
    col_map, col_details = st.columns([2, 1])
    
    with col_map:
        st.subheader("Live Graph Layer (Attention & Friction)")
        
        # Prepare Map Data
        map_data = []
        for i, s in enumerate(stores):
            size = s.get('floor_area_sqft', 10000) / 100
            demand_factor = outputs['demand_mu'][0, i].mean().item() 
            
            map_data.append({
                "name": s['store_id'],
                "lat": s['latitude'],
                "lon": s['longitude'],
                "size": size,
                "demand": demand_factor,
                "color": [255, int(255 * (1-risk_scores[i])), 0, 160] 
            })
            
        # Attention Arcs (Cyan)
        sel_idx = store_ids.index(selected_store) if selected_store != "ALL STORES" else 0
        attn_matrix = outputs['attention'][0, :, :, 0] 
        
        arcs = []
        if selected_store != "ALL STORES":
             for src_idx, src_store in enumerate(stores):
                if src_idx == sel_idx: continue
                weight = attn_matrix[sel_idx, src_idx].item()
                if weight > 0.05: 
                    arcs.append({
                        "source": [src_store['longitude'], src_store['latitude']],
                        "target": [stores[sel_idx]['longitude'], stores[sel_idx]['latitude']],
                        "weight": weight * 10, 
                        "color": [0, 255, 255, int(weight * 255 * 5)] 
                    })

        # Traffic Paths with GRADIENT (Green -> Red)
        traffic_dict = traffic.squeeze()
        paths = []
        show_traffic = st.checkbox("Show Traffic Friction", value=True)
        
        if show_traffic:
            for i, s_src in enumerate(stores):
                for j, s_dst in enumerate(stores):
                    if i == j: continue
                    fric = traffic_dict[i, j].item()
                    
                    if fric > 0.1: # Only plot relevant edges
                        # Gradient Logic: Green(Low) -> Yellow(Med) -> Red(High)
                        # 0.0 - 0.3: Green
                        # 0.3 - 0.6: Yellow
                        # 0.6 - 1.0: Red
                        if fric < 0.3:
                            color = [0, 255, 0, 150]
                        elif fric < 0.6:
                            color = [255, 255, 0, 150]
                        else:
                            color = [255, 0, 0, 200]
                            
                        paths.append({
                            "path": [[s_src['longitude'], s_src['latitude']], [s_dst['longitude'], s_dst['latitude']]],
                            "width": fric * 30, 
                            "color": color
                        })
            
        # PyDeck Layers
        layer_nodes = pdk.Layer(
            "ScatterplotLayer", map_data,
            get_position=["lon", "lat"], get_color="color", get_radius="size", pickable=True,
        )
        layer_arcs = pdk.Layer(
            "ArcLayer", arcs,
            get_source_position="source", get_target_position="target", get_width="weight",
            get_source_color="color", get_target_color="color",
        )
        layer_traffic = pdk.Layer(
            "PathLayer", paths,
            get_path="path", get_width="width", get_color="color", pickable=True
        )
        
        layers = [layer_traffic, layer_arcs, layer_nodes] if show_traffic else [layer_arcs, layer_nodes]
        
        view_state = pdk.ViewState(latitude=-1.29, longitude=36.82, zoom=11, pitch=45)
        
        r = pdk.Deck(layers=layers, initial_view_state=view_state, tooltip={"text": "{name}\nRisk: {risk_score}"})
        st.pydeck_chart(r)

    with col_details:
        st.subheader(f"📊 Insights: {selected_store}")
        
        if selected_store != "ALL STORES":
            # --- ZINB Fan Chart ---
            st.markdown("**Probabilistic Demand (Next 30 Days)**")
            
            dept_map = {i: f"Dept {i}" for i in range(20)}
            sel_dept = st.selectbox("Select Department", list(dept_map.keys()), format_func=lambda x: dept_map[x])
            
            mu = outputs['demand_mu'][0, sel_idx, sel_dept].item()
            alpha = outputs['demand_alpha'][0, sel_idx, sel_dept].item()
            pi = outputs['demand_pi'][0, sel_idx, sel_dept].item()
            
            # Gauge for Pi (Zero Probability)
            fig_gauge = go.Figure(go.Indicator(
                mode = "gauge+number",
                value = pi * 100,
                title = {'text': "Zero-Sale Risk (%)"},
                gauge = {'axis': {'range': [0, 100]},
                         'bar': {'color': "darkred" if pi > 0.5 else "green"},
                         'steps': [{'range': [0, 30], 'color': "lightgreen"},
                                   {'range': [30, 70], 'color': "yellow"}]}
            ))
            fig_gauge.update_layout(height=150, margin=dict(l=20, r=20, t=30, b=20))
            st.plotly_chart(fig_gauge, use_container_width=True)
            
            # Fan Chart
            days = np.arange(1, 31)
            means = np.array([mu] * 30) 
            std_devs = np.array([np.sqrt(mu + alpha * mu**2)] * 30) * np.linspace(1, 2, 30) 
            
            fig_fan = go.Figure()
            fig_fan.add_trace(go.Scatter(
                x=days, y=means + 1.96*std_devs, mode='lines', line=dict(width=0), showlegend=False
            ))
            fig_fan.add_trace(go.Scatter(
                x=days, y=np.maximum(0, means - 1.96*std_devs), mode='lines', line=dict(width=0),
                fill='tonexty', fillcolor='rgba(0,100,255,0.2)', name='95% Confidence (Alpha)'
            ))
            fig_fan.add_trace(go.Scatter(
                x=days, y=means, mode='lines', line=dict(color='blue', width=3), name='Forecast Mean (Mu)'
            ))
            fig_fan.update_layout(height=250, margin=dict(l=0, r=0, t=0, b=0))
            st.plotly_chart(fig_fan, key="fan_chart", use_container_width=True)
            
            # --- Radar Chart with Network Average ---
            st.markdown("---")
            st.subheader("Store DNA vs Network")
            
            # Calc Network Average
            avg_feat_vec = torch.mean(x_t, dim=0).tolist()
            store_feat_vec = x_t[sel_idx].tolist()
            
            categories = ['Sales Rank', 'Demand Scale', 'Floor Area', 'Budget', 'Footfall', 'Affluence', 
                          'Payday (Sin)', 'Brand Str', 'Supp Diversity', 'Rainfall']
            indices = [14, 15, 16, 17, 18, 19, 20, 24, 25, 28]
            
            vals_store = [store_feat_vec[i] for i in indices]
            vals_avg = [avg_feat_vec[i] for i in indices]
            
            fig_radar = go.Figure()
            fig_radar.add_trace(go.Scatterpolar(
                r=vals_avg, theta=categories, fill='toself', name='Network Avg',
                line=dict(color='gray', dash='dot')
            ))
            fig_radar.add_trace(go.Scatterpolar(
                r=vals_store, theta=categories, fill='toself', name=selected_store,
                line=dict(color='blue')
            ))
            fig_radar.update_layout(polar=dict(radialaxis=dict(visible=True, range=[0, 1])),
                                    margin=dict(t=30, b=20, l=40, r=40), height=300)
            st.plotly_chart(fig_radar, use_container_width=True)

# --- TAB 2: Store Intelligence (Velocity) ---
with tab_intel:
    st.header(f"🧠 Store Intelligence: {selected_store}")
    
    # Aggregation Logic
    if selected_store == "ALL STORES":
        all_skus = []
        for s_sim in sim.simulators.values():
            for sku in s_sim.skus.values():
                all_skus.append({
                    "Product": sku.product_name,
                    "Category": sku.department,
                    "Total Sales (Units)": sku.total_sales,
                    "Revenue": sku.total_sales * sku.unit_price,
                    "Stockouts": sku.stockout_days
                })
        df_skus = pd.DataFrame(all_skus)
        # Group by Product to sum across stores
        df_skus = df_skus.groupby(["Product", "Category"]).sum().reset_index()
    else:
        sim_instance = sim.simulators[selected_store]
        sku_data = []
        for sku in sim_instance.skus.values():
            sku_data.append({
                "Product": sku.product_name,
                "Category": sku.department,
                "Total Sales (Units)": sku.total_sales,
                "Revenue": sku.total_sales * sku.unit_price,
                "Stockouts": sku.stockout_days
            })
        df_skus = pd.DataFrame(sku_data)
    
    if not df_skus.empty:
        col_vel1, col_vel2 = st.columns(2)
        with col_vel1:
            st.subheader("🔥 Top Movers (Velocity)")
            top_qty = df_skus.sort_values("Total Sales (Units)", ascending=False).head(15)
            st.dataframe(top_qty[["Product", "Category", "Total Sales (Units)", "Stockouts"]], hide_index=True)
            
        with col_vel2:
            st.subheader("💰 Top Revenue Drivers")
            top_rev = df_skus.sort_values("Revenue", ascending=False).head(15)
            top_rev["Revenue"] = top_rev["Revenue"].apply(lambda x: f"${x:,.0f}")
            st.dataframe(top_rev[["Product", "Category", "Revenue"]], hide_index=True)
            
        st.subheader("📂 Sales by Category")
        cat_stats = df_skus.groupby("Category")[["Revenue", "Total Sales (Units)"]].sum().reset_index()
        fig_cat = px.bar(cat_stats, x="Category", y="Revenue", title="Revenue by Department")
        st.plotly_chart(fig_cat, use_container_width=True)
    else:
        st.warning("No sales data yet. Run the simulation!")

# --- TAB 3: Cluster Analysis ---
with tab_cluster:
    st.header("🔗 Store Clusters & Hidden Relationships")
    # (Existing cluster code remains valid)
    try:
        from sklearn.decomposition import PCA
        from sklearn.cluster import KMeans
        X_np = x_t.cpu().numpy()
        pca = PCA(n_components=2)
        components = pca.fit_transform(X_np)
        kmeans = KMeans(n_clusters=4, random_state=42)
        clusters = kmeans.fit_predict(X_np)
        
        df_pca = pd.DataFrame({
            "PC1": components[:, 0], "PC2": components[:, 1],
            "Store": [s['store_id'] for s in stores],
            "Region": [s.get('region', 'Unknown') for s in stores],
            "Cluster": [f"Group {c}" for c in clusters], "Risk": risk_scores
        })
        
        col_pca, col_exp = st.columns([3, 1])
        with col_pca:
            fig_pca = px.scatter(
                df_pca, x="PC1", y="PC2", color="Cluster", symbol="Region",
                hover_data=["Store", "Risk"], size=[10]*len(stores),
                title="Store Similarity Map (PCA)"
            )
            st.plotly_chart(fig_pca, use_container_width=True)
    except:
        st.error("Install sklearn for clusters.")

# --- 7. Transfer Hub (Actionable) ---
st.sidebar.markdown("---")
st.sidebar.subheader("🚚 Transfer Hub")

if selected_store != "ALL STORES":
    transfer_scores = outputs['transfer'][0] 
    transfers_out = []
    
    # Calculate Recommendations
    for j, target_store in enumerate(stores):
        if j == sel_idx: continue
        score = transfer_scores[sel_idx, j].item()
        
        # Friction Check
        fric = traffic[sel_idx, j, 0].item()
        
        if score > 0.5:
            # Est Revenue Gain (Mock model based on demand diff)
            est_gain = (score * 1000) * (1.0 - fric) 
            
            transfers_out.append({
                "Target": target_store['store_id'], 
                "Score": f"{score:.2f}",
                "Friction": f"{fric:.2f}",
                "Profit": f"${est_gain:.0f}"
            })

    if transfers_out:
        t_df = pd.DataFrame(transfers_out).sort_values("Score", ascending=False).head(5)
        st.sidebar.dataframe(t_df, hide_index=True)
        
        if st.sidebar.button("✅ Commit Transfers"):
            st.sidebar.success(f"Dispatched {len(t_df)} transfers to ERP!")
    else:
        st.sidebar.write("No transfers recommended.")
else:
    st.sidebar.info("Select a store to see transfer options.")

