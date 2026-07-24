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
import folium
from streamlit_folium import st_folium
from folium.plugins import HeatMap, Draw

# --- Helper Functions ---
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Calculate great-circle distance between two points on Earth.
    Coordinates in decimal degrees (lat negative=South, lon positive=East).
    """
    R = 6371.0  # Earth radius in km
    
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    
    return R * c


# Add models to path
sys.path.append(os.getcwd())

# Model construction + loading now lives in oasis.logic.gnn_service (SH-A S3);
# this dashboard no longer builds StoreGraphNetwork directly.

# Page Config
st.set_page_config(page_title="ST-GAT Market Pulse", layout="wide", page_icon="🧠")

# ── License gate: Market Intelligence ships in the Network module ───────
from oasis.logic.license_manager import allowed_modules, console_gate, render_upsell  # noqa: E402
console_gate(st, "core")
from oasis.ui.onboarding import data_source_badge  # noqa: E402
data_source_badge(st)
if "network" not in allowed_modules():
    render_upsell(st, "network")
    st.stop()

# ── Unified auth gate (U2) ──────────────────────────────────────────────
from oasis.ui.auth import require_login  # noqa: E402 (must follow set_page_config)
_AUTH_DB = os.getenv(
    "OASIS_DB_PATH",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "oasis", "data", "mock_pos_erp.db"),
)
require_login(st, _AUTH_DB, app_title="ST-GAT Market Pulse",
              allowed_roles=["ops_admin", "regional_manager"])

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
    """Load the GNN + simulator via the shared service (SH-A S3).

    Delegates to oasis.logic.gnn_service._load_model so the loader, the conv1
    dimension guard, and the trained/untrained/unavailable status all live in
    ONE place (shared with the Command Center). Returns (model, sim); when the
    checkpoint is missing or incompatible the model is still returned (random
    init) so the dashboard renders, and the MI-B banner below — driven by
    gnn_service.model_status() — flags it as untrained.
    """
    from oasis.logic import gnn_service
    model, sim, status = gnn_service._load_model()
    if status == "unavailable" or model is None or sim is None:
        st.error(
            "GNN resources unavailable — `stores_network.json` is missing or "
            "torch could not be loaded."
        )
        return None, None
    return model, sim

model, sim = load_resources()

if not model or not sim:
    st.stop()

# MI-B fix: be explicit when the GNN is untrained. A randomly-initialized model
# still produces confident-looking risk/demand/transfer numbers — so flag it
# loudly rather than letting the dashboard imply trained intelligence.
from oasis.logic import gnn_service as _gnn_service
if _gnn_service.model_status() != "trained":
    st.error(
        "⚠️ **GNN is UNTRAINED (random initialization).** `st_gat_v2.pt` was "
        "missing or incompatible, so all risk scores, demand forecasts, and "
        "transfer recommendations below are **illustrative only** — do not act "
        "on them until a trained checkpoint is loaded."
    )

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
    # Use x_t [N, F] and sim.edge_index [2, E] for the static GCN
    outputs = model(x_t, sim.edge_index)
    
    # --- PATCH: Inject Transfer Scores (Not returned by base GNN forward) ---
    outputs['transfer'] = model.get_all_transfer_scores(outputs['embeddings']).unsqueeze(0)


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
tab_map, tab_intel, tab_cluster, tab_expansion, tab_neural = st.tabs([
    "🗺️ Live Network Map", 
    "🧠 Store Intelligence", 
    "🔗 Cluster Analysis",
    "📍 Expansion & Site Selection",
    "🕸️ Neural Ecosystem"
])

with tab_map:
    col_map, col_details = st.columns([2, 1])
    
    with col_map:
        st.subheader("Live Graph Layer (Traffic Friction)")
        # MI-C note: the current model is a GCN with no attention heads, so the
        # "attention arcs" below are derived from an identity matrix and render
        # nothing. Kept inert (not removed) pending an attention-capable model;
        # the meaningful overlay here is traffic friction.
        
        # Prepare Map Data
        map_data = []
        for i, s in enumerate(stores):
            size = s.get('floor_area_sqft', 10000) / 100
            demand_factor = outputs['demand'][i].mean().item() 
            
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
        # Mock attention matrix for GCN (which lacks attention heads)
        attn_matrix = torch.eye(len(stores))
        
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
            
            mu = outputs['demand'][sel_idx, sel_dept].item()
            # M6 FIX: Derive ZINB params from model output instead of hardcoded constants
            demand_dist = outputs['demand'][sel_idx].detach().numpy()
            risk_val = outputs['risk'][sel_idx].item()
            alpha = max(0.1, float(np.std(demand_dist) / (np.mean(demand_dist) + 1e-6)))  # Dispersion from demand variance
            pi = min(0.95, max(0.01, risk_val * 0.5))  # Zero-inflation from risk score
            
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
    
    # Ensure simulators are hydrated if we need detail
    if not sim.is_hydrated:
        with st.spinner("Lazy Hydration: Loading Store SKUs..."):
            sim.hydrate_simulators()
            
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
        if not df_skus.empty:
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
            top_rev["Revenue"] = top_rev["Revenue"].apply(lambda x: f"KES {x:,.0f}")
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
    except Exception as e:
        st.error(f"Cluster Analysis Error: {e}")
        st.info("Ensure scikit-learn is installed in the current environment.")

# --- TAB 4: Expansion & Site Selection (NEW) ---
@st.cache_data(show_spinner="Calculating Territory Gap Index (ML Optimized)...")
def get_expansion_grid(_engine, model_loaded):
    # Map coordinates for Nairobi and surroundings
    grid_data = []
    lats = np.linspace(-1.45, -1.20, 25)
    lons = np.linspace(36.70, 36.95, 25)
    
    for lat in lats:
        for lon in lons:
            # We no longer pass radii as the engine uses Huff/ML now
            score = _engine.calculate_gap_index(lat, lon)
            grid_data.append({
                "lat": lat, "lon": lon, "score": score,
                "color": [255 * (1-score), 255 * score, 0, 100]
            })
    return grid_data

with tab_expansion:
    st.header("📍 Strategic Expansion Engine (V2)")
    st.markdown("""
    This engine has been upgraded to **Behavioral Spatial Modeling**. 
    It replaces radial penalties with the **Huff Gravity Model** (customer capture probability) 
    and uses a **Random Forest** classifier to predict site success based on traffic isochrones.
    """)
    
    col_exp_ctrl, col_exp_map = st.columns([1, 2])
    
    with col_exp_ctrl:
        st.subheader("Expansion Parameters")
        
        # --- STATE SYNC: Handle pending updates from map BEFORE widgets render ---
        # --- STATE SYNC: Handle pending updates from map BEFORE widgets render ---
        if 'input_lat' not in st.session_state: st.session_state.input_lat = -1.3000
        if 'input_lon' not in st.session_state: st.session_state.input_lon = 36.8000

        if st.session_state.get("pending_lat") is not None:
            st.session_state.input_lat = st.session_state.pop("pending_lat")
            st.session_state.input_lon = st.session_state.pop("pending_lon")
        
        internal_can = st.slider("Internal Cannibalization Radius (km)", 0.5, 10.0, 3.0, 0.5)
        comp_friction = st.slider("Competitor Friction Radius (km)", 0.5, 5.0, 2.0, 0.5)
        
        st.markdown("---")
        st.subheader("Site Analysis tool")
        
        target_lat = st.number_input("Target Latitude", format="%.4f", key="input_lat")
        target_lon = st.number_input("Target Longitude", format="%.4f", key="input_lon")
        
        if st.button("🔍 Analyze This Site", type="secondary"):
            engine = sim.expansion_engine
            
            # Perform Analysis
            huff_prob = engine.calculate_huff_probability(target_lat, target_lon)
            final_score = engine.calculate_gap_index(target_lat, target_lon)
            
            # Isochrone Check (Travel time to nearest internal store)
            nearest_store = stores[0]
            min_dist = 999
            for s in stores:
                d = haversine_km(target_lat, target_lon, s['latitude'], s['longitude'])
                if d < min_dist:
                    min_dist = d
                    nearest_store = s
            
            travel_time = engine.estimate_travel_time(target_lat, target_lon, 
                                                      nearest_store['latitude'], nearest_store['longitude'])
            
            # Affluence lookup (dynamic based on nearest store)
            affluence = nearest_store.get('catchment_affluence_index', 3.5)
            
            # GET DETAILED ANALYSIS
            detail = engine.get_detailed_analysis(target_lat, target_lon, final_score, affluence)
            
            # Metrics Row
            m1, m2, m3 = st.columns(3)
            with m1: st.metric("ML Success Score", f"{final_score:.2f}")
            with m2: st.metric("Huff Prob (Capture)", f"{huff_prob*100:.1f}%")
            with m3: st.metric("Capital Allocation", detail['capital'])
            
            st.markdown(f"**Recommendation:** `{detail['store_type']}`")
            st.info(f"**Strategic Rationale:** {detail['rationale']}")
            
            # Nearby Stores Impact Table
            st.subheader("📍 Nearby Store Proximity & Impact")
            if detail['nearby']:
                df_nearby = pd.DataFrame(detail['nearby'])
                # Order columns for clean display
                df_nearby = df_nearby[['id', 'type', 'distance_km', 'impact']]
                df_nearby.columns = ["Target Entity", "Entity Type", "Distance (km)", "Impact Assessment"]
                st.table(df_nearby)
            else:
                st.write("No existing stores within 10km radius.")
            
            st.info(f"Analysis accounts for {len(engine.competitors)} competitors.")

    with col_exp_map:
        # Generate Heatmap Grid (Optimized with Caching)
        engine = sim.expansion_engine
        model_loaded = engine.model is not None
        grid_data = get_expansion_grid(engine, model_loaded)
        
        # Competitor Data
        comp_df = engine.competitors
        
        # --- Interactive Folium Map ---
        st.subheader("Selection Matrix (Click to Analyze Site)")
             # Render Map with FIXED dimensions and alternative stable tiles
        m = folium.Map(location=[-1.2921, 36.8219], zoom_start=11, tiles="cartodbpositron")
        
        # 1. HeatMap Layer (Safe)
        heat_data = []
        for point in grid_data:
            lat, lon, score = point.get('lat'), point.get('lon'), point.get('score')
            # Check for non-None and non-NaN values (0.0 is a valid score)
            if (lat is not None and lon is not None and score is not None and 
                not np.isnan(lat) and not np.isnan(lon) and not np.isnan(score)):
                heat_data.append([lat, lon, score])
        
        if heat_data:
            HeatMap(heat_data, radius=12, blur=8, min_opacity=0.3).add_to(m)

        # 1.5 Add Drawing Tools (Crucial for Site Selection)
        Draw(export=True).add_to(m)

        # 2. Add Markers
        for s in stores:
            if s.get('latitude') and s.get('longitude'):
                folium.CircleMarker([s['latitude'], s['longitude']], radius=5, color='blue', tooltip=f"EXISTING: {s['store_id']}").add_to(m)

        # 3. Competitor Layer (Limited)
        if not comp_df.empty:
            valid_comp = comp_df.dropna(subset=['Latitude', 'Longitude']).head(100)
            for _, row in valid_comp.iterrows(): 
                folium.CircleMarker([row['Latitude'], row['Longitude']], radius=3, color='red', tooltip=f"COMP: {row.get('Store_Name', 'Unknown')}").add_to(m)

        map_out = st_folium(
            m, 
            width='stretch',
            height=600, 
            key="expansion_map_v_stable_final_4",
            returned_objects=["last_clicked", "all_drawings"]
        )
        
        if map_out:
            has_update = False
            # Handle Single Click
            if map_out.get("last_clicked"):
                lc = map_out["last_clicked"]
                # Round to 4 decimal places
                new_lat = round(float(lc["lat"]), 4)
                new_lon = round(float(lc["lng"]), 4)
                
                if new_lat != st.session_state.input_lat or new_lon != st.session_state.input_lon:
                    st.session_state.pending_lat = new_lat
                    st.session_state.pending_lon = new_lon
                    has_update = True
                    st.toast(f"📍 New Site Captured: {new_lat:.4f}, {new_lon:.4f}")
            
            # Handle Area Selection (Last drawing)
            if map_out.get("all_drawings"):
                drawings = map_out["all_drawings"]
                if drawings:
                    last_draw = drawings[-1]
                    new_lat, new_lon = None, None
                    if last_draw['geometry']['type'] == 'Point':
                        coords = last_draw['geometry']['coordinates']
                        new_lon, new_lat = coords[0], coords[1]
                    elif last_draw['geometry']['type'] == 'Polygon':
                        coords = np.array(last_draw['geometry']['coordinates'][0])
                        center = coords.mean(axis=0)
                        new_lon, new_lat = center[0], center[1]
                    
                    if new_lat is not None:
                        # Round to 4 decimal places
                        new_lat = round(float(new_lat), 4)
                        new_lon = round(float(new_lon), 4)
                        
                        if new_lat != st.session_state.input_lat or new_lon != st.session_state.input_lon:
                            st.session_state.pending_lat = new_lat
                            st.session_state.pending_lon = new_lon
                            has_update = True
                            st.toast("📐 Area Selection Analyzed")
            
            if has_update:
                st.rerun()

        st.caption("🔵 Existing Stores | 🔴 Competitors | Heatmap: Expansion Opportunity | Tool: Use Polygon/Rectangle on left to select area")

# --- 6. Neural Ecosystem ---
@st.cache_data
def load_neural_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    nodes_path = os.path.join(base_dir, "neutral_network_export", "nodes.csv")
    edges_path = os.path.join(base_dir, "neutral_network_export", "edges.csv")
    
    if os.path.exists(nodes_path) and os.path.exists(edges_path):
        nodes = pd.read_csv(nodes_path)
        edges = pd.read_csv(edges_path)
        return nodes, edges
    return pd.DataFrame(), pd.DataFrame()

with tab_neural:
    st.header("🕸️ Neural Ecosystem & Market Intelligence")
    nodes_df, edges_df = load_neural_data()
    
    if not nodes_df.empty and not edges_df.empty:
        # A. Supplier Fragility Matrix
        st.subheader("🏢 Supplier Fragility & Dominance Matrix")
        
        # Clean supplier names
        nodes_df['supplier'] = nodes_df['supplier'].fillna('Unknown').astype(str)
        nodes_df['revenue'] = pd.to_numeric(nodes_df['revenue'], errors='coerce').fillna(0)
        nodes_df['margin_pct'] = pd.to_numeric(nodes_df['margin_pct'], errors='coerce').fillna(0)
        
        # Group by supplier
        supplier_agg = nodes_df.groupby('supplier').agg(
            total_revenue=('revenue', 'sum'),
            avg_margin=('margin_pct', 'mean'),
            sku_count=('id', 'count')
        ).reset_index()
        
        # Filter out "Unknown" and tiny suppliers for better visibility
        supplier_agg = supplier_agg[(supplier_agg['supplier'] != 'Unknown') & (supplier_agg['supplier'] != '[[Unknown]]')]
        supplier_agg = supplier_agg[supplier_agg['total_revenue'] > 0]
        
        if not supplier_agg.empty:
            fig_fragility = px.scatter(
                supplier_agg, x="total_revenue", y="avg_margin", size="sku_count", 
                color="total_revenue", hover_name="supplier",
                title="Supplier Risk vs. Reward (Size = SKU Count)",
                labels={"total_revenue": "Total Revenue (KES)", "avg_margin": "Average Margin (%)"}
            )
            st.plotly_chart(fig_fragility, use_container_width=True)
            
        # B. SKU Affinity & Substitution Engine
        st.markdown("---")
        st.subheader("🔗 SKU Affinity & Substitution Engine")
        
        try:
            import networkx as nx
            has_nx = True
        except ImportError:
            has_nx = False
            st.warning("NetworkX not installed. Visualizations will be limited to tables.")
        
        skus_with_edges = pd.concat([edges_df['source'], edges_df['target']]).unique()
        valid_skus = nodes_df[nodes_df['id'].isin(skus_with_edges)]['id'].tolist()
        
        if valid_skus:
            selected_sku = st.selectbox("Select Target SKU to Trace Network", [""] + valid_skus[:1000])
            
            if selected_sku:
                connected_edges = edges_df[(edges_df['source'] == selected_sku) | (edges_df['target'] == selected_sku)]
                st.write(f"Found {len(connected_edges)} direct relationships for **{selected_sku}**")
                
                if not connected_edges.empty and has_nx:
                    G = nx.Graph()
                    for _, row in connected_edges.iterrows():
                        G.add_edge(row['source'], row['target'], relation=row['relation'])
                    
                    pos = nx.spring_layout(G)
                    
                    edge_x = []
                    edge_y = []
                    for edge in G.edges():
                        x0, y0 = pos[edge[0]]
                        x1, y1 = pos[edge[1]]
                        edge_x.extend([x0, x1, None])
                        edge_y.extend([y0, y1, None])
                        
                    edge_trace = go.Scatter(
                        x=edge_x, y=edge_y,
                        line=dict(width=0.5, color='#888'),
                        hoverinfo='none',
                        mode='lines')

                    node_x = []
                    node_y = []
                    node_text = []
                    node_color = []
                    for node in G.nodes():
                        x, y = pos[node]
                        node_x.append(x)
                        node_y.append(y)
                        node_text.append(str(node))
                        node_color.append('red' if node == selected_sku else 'blue')

                    node_trace = go.Scatter(
                        x=node_x, y=node_y,
                        mode='markers+text',
                        hoverinfo='text',
                        text=node_text,
                        textposition="bottom center",
                        marker=dict(showscale=False, color=node_color, size=10, line_width=2)
                    )
                            
                    fig_net = go.Figure(data=[edge_trace, node_trace],
                                layout=go.Layout(
                                    title='Local SKU Affinity Network',
                                    titlefont_size=16,
                                    showlegend=False,
                                    hovermode='closest',
                                    margin=dict(b=20,l=5,r=5,t=40),
                                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                                    )
                    st.plotly_chart(fig_net, use_container_width=True)

        # C. Market Whitespace / Moat Finder
        st.markdown("---")
        st.subheader("📈 High-Velocity Market Moats (Whitespace Finder)")
        
        nodes_df['velocity_ads'] = pd.to_numeric(nodes_df['velocity_ads'], errors='coerce').fillna(0)
        sku_nodes = nodes_df[nodes_df['type'] == 'SKU'].copy()
        
        if not sku_nodes.empty:
            vel_threshold = sku_nodes['velocity_ads'].quantile(0.8)
            moat_skus = sku_nodes[(sku_nodes['margin_pct'] > 15.0) & (sku_nodes['velocity_ads'] > vel_threshold)]
            moat_skus = moat_skus.sort_values('gross_profit', ascending=False)
            
            st.dataframe(
                moat_skus[['id', 'department', 'supplier', 'price', 'margin_pct', 'velocity_ads', 'gross_profit']],
                hide_index=True,
                use_container_width=True
            )
            
    else:
        st.warning("Neural Network data (nodes.csv / edges.csv) not found in the export directory.")


# --- 7. Transfer Hub (Advisory) ---
st.sidebar.markdown("---")
st.sidebar.subheader("🚚 Transfer Hub (Advisory)")

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
                "Profit": f"KES {est_gain:.0f}"
            })

    if transfers_out:
        t_df = pd.DataFrame(transfers_out).sort_values("Score", ascending=False).head(5)
        st.sidebar.dataframe(t_df, hide_index=True)
        # MI-D fix: these are STORE-LEVEL advisory scores (no SKU/qty), so this
        # tool cannot create a real transfer. Removed the fake "Dispatched to
        # ERP" success; item-level dispatch happens in the Operations Console.
        st.sidebar.caption(
            "Advisory store-pair scores. Execute item-level transfers in the "
            "Operations Console → Transfers (writes to the ERP)."
        )
    else:
        st.sidebar.write("No transfers recommended.")
else:
    st.sidebar.info("Select a store to see transfer options.")

