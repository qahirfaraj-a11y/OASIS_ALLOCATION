
import sys
import os
import json
import torch
import numpy as np
import pandas as pd
from typing import Dict, List
import math
import pickle
import gzip
from concurrent.futures import ThreadPoolExecutor

# Ensure proper path
sys.path.append(os.getcwd())

from retail_simulator import RetailSimulator, SKUState, STORE_UNIVERSES
from models.store_gnn import store_to_features, build_edges, build_adjacency, haversine_km

# --- MOCK SENSORS ---
# --- MOCK SENSORS ---
class WeatherService:
    """Mock Weather Data Provider for Nairobi."""
    def get_current_weather(self, region: str, day: int) -> float:
        """Returns Rainfall Intensity (0.0 - 1.0)."""
        # Simulate Seasonality (Rainy Seasons: Mar-May, Oct-Dec)
        d = day % 365
        is_long_rains = 60 <= d <= 150
        is_short_rains = 280 <= d <= 350
        
        base_prob = 0.6 if (is_long_rains or is_short_rains) else 0.1
        
        # Random daily fluctuation (consistent per day)
        np.random.seed((day + hash(region)) % (2**32))
        if np.random.rand() < base_prob:
            return np.random.uniform(0.3, 1.0) # Heavy rain
        return 0.0

class TrafficService:
    """Mock Traffic Data Provider (Google Maps Proxy)."""
    
    def get_traffic_friction(self, src: dict, dst: dict, hour: int) -> float:
        """
        Returns Directed Friction (0.0 - 1.0) from Src to Dst.
        Implements Asymmetric Flow (e.g., Morning Rush into CBD).
        """
        # 1. Base Distance Friction
        dist = haversine_km(src['latitude'], src['longitude'], 
                            dst['latitude'], dst['longitude'])
        base_friction = min(dist / 50.0, 0.2) # Long distance = some friction
        
        # 2. Identify Hubs
        # Simple Proxy: CBD/Industrial Area are roughly at (-1.29, 36.82)
        # Westlands: (-1.26, 36.80)
        
        # Check if Target is a "Work Hub" (Central)
        target_lat, target_lon = dst['latitude'], dst['longitude']
        is_target_hub = (-1.30 < target_lat < -1.25) and (36.80 < target_lon < 36.85)
        
        # Check if Source is a "Work Hub"
        src_lat, src_lon = src['latitude'], src['longitude']
        is_source_hub = (-1.30 < src_lat < -1.25) and (36.80 < src_lon < 36.85)

        # 3. Asymmetric Rush Hour Logic
        # Morning (6-9 AM): Flow INTO Hubs is jammed. Flow OUT is faster.
        # Evening (4-7 PM): Flow OUT of Hubs is jammed. Flow IN is faster.
        
        hourly_penalty = 0.0
        
        if 6 <= hour <= 9:
            # Morning Rush
            if is_target_hub and not is_source_hub:
                hourly_penalty = 0.6 # Jammed INBOUND
            elif is_source_hub and not is_target_hub:
                hourly_penalty = 0.1 # Clear OUTBOUND (Counter-flow)
            else:
                hourly_penalty = 0.3 # General congestion
                
        elif 16 <= hour <= 19:
            # Evening Rush
            if is_source_hub and not is_target_hub:
                hourly_penalty = 0.7 # Jammed OUTBOUND (Mombasa Rd madness)
            elif is_target_hub and not is_source_hub:
                hourly_penalty = 0.1 # Clear INBOUND
            else:
                hourly_penalty = 0.3 # General congestion
        
        # Add random noise (accidents, matatu overlaps)
        noise = np.random.uniform(0.0, 0.1)
        
        return min(base_friction + hourly_penalty + noise, 1.0)

# Global Services
WEATHER_SVC = WeatherService()
TRAFFIC_SVC = TrafficService()


class GeospatialExpansionEngine:
    """
    Analyzes geographic regions for expansion potential by factoring in:
    1. Regional Affluence & Population Density (Baseline).
    2. Competitor Friction (Proximity to Naivas/Quickmart).
    3. Self-Cannibalization (Proximity to existing Chandarana stores).
    """
    def __init__(self, stores_data, competitor_file="competitor_network.csv"):
        self.stores_data = stores_data
        self.competitor_file = competitor_file
        self.competitors = pd.DataFrame()
        
        # Load Model
        self.model_path = "expansion_model.joblib"
        self.model = None
        if os.path.exists(self.model_path):
            import joblib
            try:
                self.model = joblib.load(self.model_path)
                print("Expansion Engine: ML Model loaded successfully.")
            except Exception as e:
                print(f"Expansion Engine: Model load failed: {e}")
        
        if os.path.exists(competitor_file):
            self.competitors = pd.read_csv(competitor_file)
            print(f"Expansion Engine: Loaded {len(self.competitors)} competitors.")
        else:
            print("Expansion Engine: Competitor file not found. Competitive friction will be 0.")

    def estimate_travel_time(self, lat1, lon1, lat2, lon2, hour=12):
        """
        Estimates travel time in minutes based on traffic friction.
        Isochrone Proxy Logic.
        """
        dist_km = haversine_km(lat1, lon1, lat2, lon2)
        
        # Get friction from Traffic Service proxy
        # We use a dummy src/dst dict for the service
        src = {'latitude': lat1, 'longitude': lon1}
        dst = {'latitude': lat2, 'longitude': lon2}
        
        # Use existing TRAFFIC_SVC for friction
        friction = TRAFFIC_SVC.get_traffic_friction(src, dst, hour)
        
        # Base speed 40km/h (1.5 min per km)
        base_time = dist_km * 1.5
        
        # Friction scales time non-linearly (up to 3x)
        total_time = base_time * (1.0 + friction * 2.0)
        
        return total_time

    def calculate_huff_probability(self, lat, lon, target_size_sqft=10000):
        """
        Huff Gravity Model implementation.
        P(i,j) = (S_j / D_ij^2) / sum(S_k / D_ik^2)
        """
        # Attractiveness (S)
        def get_attractiveness(size_sqft, chain="Unknown"):
            weight = 1.0
            if "Naivas" in chain or "Carrefour" in chain:
                weight = 1.5
            return (size_sqft / 1000.0) * weight

        # Potential Stores (Target + Internal + Competitors)
        candidates = []
        
        # 1. The hypothetical target store
        candidates.append({'S': get_attractiveness(target_size_sqft), 'dist': 0.1}) # min dist
        
        # 2. Internal Stores (Chandarana)
        for s in self.stores_data:
            dist = haversine_km(lat, lon, s['latitude'], s['longitude'])
            if dist < 10.0: # 10km catchment
                candidates.append({
                    'S': get_attractiveness(s.get('floor_area_sqft', 10000)),
                    'dist': max(dist, 0.1)
                })
        
        # 3. Competitors
        if not self.competitors.empty:
            nearby = self.competitors[
                (self.competitors['Latitude'].between(lat - 0.1, lat + 0.1)) &
                (self.competitors['Longitude'].between(lon - 0.1, lon + 0.1))
            ]
            for _, comp in nearby.iterrows():
                dist = haversine_km(lat, lon, comp['Latitude'], comp['Longitude'])
                if dist < 10.0:
                    candidates.append({
                        'S': get_attractiveness(15000, comp.get('Chain', 'Unknown')), # Assume 15k sqft for big chains
                        'dist': max(dist, 0.1)
                    })
        
        # Calculate utility: S / D^2
        total_utility = 0
        target_utility = 0
        
        for i, c in enumerate(candidates):
            utility = c['S'] / (c['dist'] ** 2)
            total_utility += utility
            if i == 0: target_utility = utility
            
        if total_utility == 0: return 0.0
        return target_utility / total_utility

    def calculate_gap_index(self, lat, lon, internal_radius_km=3.0, competitor_radius_km=2.0):
        """
        Advanced ML-driven Gap Index.
        Combines Huff Probability, Traffic-Aware Friction, and RandomForest prediction.
        """
        # 1. Huff Probability
        huff_prob = self.calculate_huff_probability(lat, lon)
        
        # 2. Competitor Context (for ML features)
        min_dist_comp = 10.0
        comp_density = 0
        if not self.competitors.empty:
             nearby = self.competitors[
                (self.competitors['Latitude'].between(lat - 0.1, lat + 0.1)) &
                (self.competitors['Longitude'].between(lon - 0.1, lon + 0.1))
            ]
             for _, comp in nearby.iterrows():
                dist = haversine_km(lat, lon, comp['Latitude'], comp['Longitude'])
                if dist < 5.0: comp_density += 1
                if dist < min_dist_comp: min_dist_comp = dist

        # 3. Traffic Friction Proxy
        src = {'latitude': lat, 'longitude': lon}
        dst = {'latitude': lat + 0.01, 'longitude': lon + 0.01} # Nearby probe
        traffic_friction = TRAFFIC_SVC.get_traffic_friction(src, dst, 12) # Peak-ish
        
        # 4. Affluence (Dynamic based on nearest existing store)
        min_dist = 999.0
        affluence = 3.5
        for s in self.stores_data:
            d = haversine_km(lat, lon, s['latitude'], s['longitude'])
            if d < min_dist:
                min_dist = d
                affluence = s.get('catchment_affluence_index', 3.5)
        
        # 5. ML Prediction (Random Forest)
        if self.model is not None:
            # Feature Vector: [huff_prob, comp_density, min_dist_comp, affluence, traffic_friction, store_type]
            cols = ['huff_prob', 'comp_density', 'min_dist_comp', 'affluence', 'traffic_friction', 'store_type']
            
            features_express = pd.DataFrame([[huff_prob, comp_density, min_dist_comp, affluence, traffic_friction, 0]], columns=cols)
            features_medium = pd.DataFrame([[huff_prob, comp_density, min_dist_comp, affluence, traffic_friction, 1]], columns=cols)
            features_hyper = pd.DataFrame([[huff_prob, comp_density, min_dist_comp, affluence, traffic_friction, 2]], columns=cols)
            
            score_express = float(self.model.predict(features_express)[0])
            score_medium = float(self.model.predict(features_medium)[0])
            score_hyper = float(self.model.predict(features_hyper)[0])
            
            # Select the highest predicted success score across the candidate types
            ml_score = max(score_express, score_medium, score_hyper)
        else:
            # Fallback to deterministic if no model
            ml_score = huff_prob * 0.8
            
        return max(0.0, min(1.0, ml_score))

    def recommend_store_type(self, score, affluence=3.0):
        """
        Logic for store type recommendation based on score and affluence.
        """
        if score < 0.2: return "Unsuitable Location"
        
        if score > 0.8 and affluence >= 4.0:
            return "Hyper / Flagship"
        elif score > 0.6:
            return "Medium Anchor"
        elif score > 0.4:
            if affluence < 3.5:
                return "Gas Station Mini-Mart"
            else:
                return "Express / Neighborhood"
        else:
            return "Gas Station Mini-Mart"

    def get_detailed_analysis(self, lat, lon, score, affluence=3.5):
        """
        Returns a structured report for site selection.
        - Nearby stores (10km) and their impact.
        - Strategic Rationale.
        - Capital Allocation range (KES).
        """
        # 1. Nearby Stores & Impact (Internal + Competitors)
        nearby = []
        # Internal Stores
        for s in self.stores_data:
            dist = haversine_km(lat, lon, s['latitude'], s['longitude'])
            if dist < 10.0:
                impact = "Neutral"
                if dist < 3.0: impact = "🚨 High Cannibalization"
                elif dist < 5.0: impact = "⚠️ Moderate Cannibalization"
                elif dist < 8.0: impact = "🤝 Strategic Synergy"
                else: impact = "🌐 Network Expansion"
                
                nearby.append({
                    'id': s['store_id'],
                    'distance_km': round(dist, 2),
                    'impact': impact,
                    'type': 'INTERNAL'
                })
        
        # Competitors (within 5km for impact analysis)
        if not self.competitors.empty:
            nearby_comp = self.competitors[
                (self.competitors['Latitude'].between(lat - 0.05, lat + 0.05)) &
                (self.competitors['Longitude'].between(lon - 0.05, lon + 0.05))
            ]
            for _, comp in nearby_comp.iterrows():
                dist = haversine_km(lat, lon, comp['Latitude'], comp['Longitude'])
                if dist < 5.0:
                    nearby.append({
                        'id': comp.get('Store_Name', 'Unknown Competitor'),
                        'distance_km': round(dist, 2),
                        'impact': "🥊 Competitive Friction",
                        'type': 'COMPETITOR'
                    })
        
        # Sort by distance
        nearby = sorted(nearby, key=lambda x: x['distance_km'])

        # 2. Capital Allocation Logic (KES)
        rec_type = self.recommend_store_type(score, affluence)
        
        if rec_type == "Hyper / Flagship": lower, upper, step = 80_000_000, 150_000_000, 5_000_000
        elif rec_type == "Medium Anchor": lower, upper, step = 10_000_000, 35_000_000, 1_000_000
        elif rec_type == "Express / Neighborhood": lower, upper, step = 1_000_000, 8_000_000, 500_000
        elif rec_type == "Gas Station Mini-Mart": lower, upper, step = 200_000, 950_000, 50_000
        else: lower, upper, step = 0, 0, 100_000

        # Apply score-based scaling within the range
        mid = lower + (upper - lower) * score
        
        # Fine-grained rounding based on step
        final_low = round((mid * 0.85) / step) * step
        final_high = round((mid * 1.15) / step) * step
        
        capital_str = f"KES {final_low:,.0f} - {final_high:,.0f}"

        # 3. Strategic Rationale
        if score > 0.7:
            rationale = "High-potential 'Blue Ocean' found. Low competitor density relative to catchment power suggests high ROI."
        elif score > 0.4:
            rationale = "Solid neighborhood play. Good capture probability (Huff Model) with manageable internal overlap."
        else:
            rationale = "High friction zone. Location may suffer from over-saturation or extreme cannibalization."
        
        return {
            'nearby': nearby,
            'capital': capital_str,
            'rationale': rationale,
            'store_type': rec_type
        }



class NetworkSimulator:
    """
    Orchestrates a multi-store simulation synchronized with the ST-GAT Network.
    """
    def __init__(self, network_file: str = "stores_network.json", skip_enrichment: bool = True):
        self.network_file = network_file
        self.skip_enrichment = skip_enrichment
        with open(network_file, 'r') as f:
            self.data = json.load(f)
        self.stores_data = self.data['stores']
        self.expansion_engine = GeospatialExpansionEngine(self.stores_data)
        
        self.simulators: Dict[str, RetailSimulator] = {}
        self.current_day = 0
        self.is_hydrated = False

        # --- OPTIMIZATION: Static Graph Elements (Required for initial dashboard view) ---
        self.edge_index, _ = build_edges(self.stores_data)
        self.adj = build_adjacency(self.edge_index, num_nodes=len(self.stores_data))

        # --- OPTIMIZATION: Shared Resource Loading ---
        print("Pre-loading Shared Resources...")
        # 1. Load Master Scorecard
        try:
             scorecard_path = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv" 
             self.preloaded_df = pd.read_csv(scorecard_path)
             print(f"Loaded Master Scorecard: {len(self.preloaded_df)} rows")
        except Exception as e:
             print(f"Failed to preload scorecard: {e}")
             self.preloaded_df = None

        # 2. Shared Logic Bridge
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        data_dir = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
        self.shared_bridge = SimulationOrderUtil(data_dir)
        self.shared_bridge.engine.load_local_databases()
        
        # 3. Deferred Enrichment
        self.pre_enriched_products = None
        if not self.skip_enrichment:
            self.hydrate_simulators()
        else:
            print("Simulator initialized in LAZY mode (SKU enrichment deferred).")

    def hydrate_simulators(self):
        """Perform the heavy SKU enrichment and initialize store simulators."""
        if self.is_hydrated:
            return
        
        # --- PHASE 1: Binary Intelligence Cache ---
        cache_path = os.path.join(r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data", "st_gat_intel_cache.pkl.gz")
        scorecard_path = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"
        
        load_from_cache = False
        if os.path.exists(cache_path):
            # Invalidation Check: Is cache older than scorecard?
            if os.path.getmtime(cache_path) > os.path.getmtime(scorecard_path):
                load_from_cache = True
        
        if load_from_cache:
            print("Binary Initialization: Loading 23k+ Enriched SKUs from cache...")
            try:
                with gzip.open(cache_path, 'rb') as f:
                    self.pre_enriched_products = pickle.load(f)
                print(f"Binary Initialization: LOADED {len(self.pre_enriched_products)} products in <1s.")
            except Exception as e:
                print(f"Cache load failed: {e}. Falling back to enrichment.")
                load_from_cache = False

        if not load_from_cache and self.pre_enriched_products is None:
             print("Deep Hydration: Enriching 23k+ SKUs (First Time Only)...")
             try:
                 raw_products = []
                 for _, row in self.preloaded_df.iterrows():
                      raw_products.append({
                        'product_name': str(row.get('Product', 'Unknown')),
                        'supplier_name': str(row.get('Supplier', 'Unknown')),
                        'product_category': str(row.get('Department', 'GENERAL')),
                        'selling_price': float(row.get('Unit_Price', 0) or 0),
                        'margin_pct': float(row.get('Margin_Pct', 25) or 25),
                        'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) or 0),
                        'current_stocks': 0.0,
                        'pack_size': int(float(row.get('Pack_Size', 1) or 1)),
                        'ABC_Class': str(row.get('ABC_Class', 'C')),
                        'reliability_score': 90,
                        'is_consignment': False
                      })
                 self.pre_enriched_products = self.shared_bridge.engine.enrich_product_data(raw_products)
                 
                 # Save to Cache
                 print(f"Deep Hydration: Saving enrichment results to binary cache...")
                 with gzip.open(cache_path, 'wb') as f:
                     pickle.dump(self.pre_enriched_products, f, protocol=pickle.HIGHEST_PROTOCOL)
                 
                 print(f"Deep Hydration: {len(self.pre_enriched_products)} products ready and cached.")
             except Exception as e:
                 print(f"Deep hydration failed: {e}")

        # --- PHASE 2: Pre-compute SKU Allocation ONCE ---
        # The greenfield allocation is 800+ lines of O(N²) logic.
        # We run it once for the Mega tier and reuse the SKU list for all stores.
        import copy
        import time as _time
        
        t_alloc = _time.time()
        print("Pre-computing Greenfield Allocation (1x for all stores)...")
        
        from retail_simulator import load_scorecard_data, SKUState
        mega_config = STORE_UNIVERSES.get("Mega_100M", STORE_UNIVERSES["Medium_1M"]).copy()
        mega_budget = mega_config["budget"]
        
        # Run the heavy allocation once
        base_sku_list = load_scorecard_data(
            self.shared_bridge.engine, mega_budget, "Mega_100M", 
            demand_scale_factor=1.0,
            preloaded_data=self.preloaded_df, 
            pre_enriched_products=self.pre_enriched_products
        )
        print(f"Base Allocation: {len(base_sku_list)} SKUs in {_time.time()-t_alloc:.1f}s")
        
        # --- PHASE 3: Clone & Scale for each store ---
        print("Initializing Network Simulation Agents (Clone & Scale)...")
        
        for idx, store in enumerate(self.stores_data):
            s_id = store['store_id']
            category = store.get('store_category', 'Medium Anchor')
            
            tier = "Medium_1M"
            if "Express" in category: tier = "Small_200k"
            elif "Hyper" in category: tier = "Mega_100M"
            elif "Large" in category: tier = "Large_10M"
            elif "Boutique" in category: tier = "Small_200k"
            
            config = STORE_UNIVERSES.get(tier, STORE_UNIVERSES["Medium_1M"]).copy()
            if 'monthly_budget' in store:
                config['budget'] = store['monthly_budget']
            
            demand_scale = config.get("demand_scale_factor", 1.0)
            
            # Clone SKUs from the pre-computed base and scale demand/stock
            scaled_skus = []
            for base_sku in base_sku_list:
                sku_copy = SKUState(
                    product_name=base_sku.product_name,
                    supplier=base_sku.supplier,
                    department=base_sku.department,
                    unit_price=base_sku.unit_price,
                    cost_price=base_sku.cost_price,
                    avg_daily_sales=base_sku.avg_daily_sales * demand_scale,
                    demand_cv=base_sku.demand_cv,
                    lead_time_days=base_sku.lead_time_days,
                    current_stock=max(1, int(base_sku.current_stock * demand_scale)),
                    is_fresh=base_sku.is_fresh
                )
                scaled_skus.append(sku_copy)
            
            sim = RetailSimulator(
                tier_name=tier, 
                store_config=config, 
                seed=42 + idx,
                bridge=self.shared_bridge,
                initial_skus=scaled_skus
            )
            self.simulators[s_id] = sim
            
        self.is_hydrated = True
        print(f"Hydration SUCCESS: {len(self.simulators)} agents ready.")

    def step(self):
        """Advance simulation by 1 day using parallel vectorized acceleration."""
        if not self.is_hydrated:
            self.hydrate_simulators()
        
        self.current_day += 1
        day = self.current_day
        
        # 1. PARALLEL: Arrivals & Demand Simulation
        # These are independent per store and don't touch the shared bridge state.
        def _simulate_store_day(s_id, sim):
            sim.process_arrivals(day)
            sim.sync_to_vectorized_state() # Ensure arrivals are in NumPy workspace
            rev = sim.simulate_active_day_vectorized(day)
            sim.sync_from_vectorized_state() # Push daily sales back to objects
            return rev

        with ThreadPoolExecutor(max_workers=min(len(self.simulators), 8)) as executor:
            executor.map(lambda pair: _simulate_store_day(*pair), self.simulators.items())
        
        # 2. SERIAL: Reordering (Thread Safety Guard)
        # Reordering uses the shared_bridge which might have internal caches.
        for s_id, sim in self.simulators.items():
            if day % sim.config["reorder_frequency_days"] == 0:
                sim.place_reorders(day)
                
    def get_feature_matrix(self):
        """
        Construct [N, Features] matrix from current simulation state.
        We blend Static features with Dynamic state.
        Now includes 'Salary Hit' Step Function at Index 29.
        """
        feats_list = []
        for store in self.stores_data:
            s_id = store['store_id']
            # sim = self.simulators.get(s_id) # Unused?
            
            # Base Static Features (Length 29 in current store_gnn code)
            f_vec = store_to_features(store)
            
            # Dynamic Update: Payday Signal (Multi-Frequency)
            t = self.current_day
            
            # 1. Continuous Wave (Indices 26-27)
            sin_mo = math.sin(2 * math.pi * t / 30)
            cos_mo = math.cos(2 * math.pi * t / 30)
            
            f_vec[26] = sin_mo
            f_vec[27] = cos_mo
            
            # --- FEATURE 28: RAIN ---
            # Dashboard scnearios might override this in x_t directly, but we set baseline here.
            # Mock Service returns 0.0 or high value.
            rain = WEATHER_SVC.get_current_weather(store.get('region', 'Nairobi'), t)
            f_vec[28] = rain
            
            # --- FEATURE 29: SALARY HIT ---
            # "Explosion of bulk buying" between 28th and 5th
            day_mod = t % 30
            is_payday_window = (day_mod >= 28) or (day_mod <= 5)
            salary_hit = 1.0 if is_payday_window else 0.0
            
            f_vec[29] = salary_hit

            
            feats_list.append(f_vec)
            
        return torch.tensor(feats_list, dtype=torch.float)

    def get_traffic_matrix(self):
        """
        Generate dynamic traffic friction matrix [N, N, 1].
        Now uses ASYMMETRIC Logic (Src -> Dst).
        """
        N = len(self.stores_data)
        traffic = torch.zeros(N, N, 1)
        
        hour = (self.current_day * 24) % 24
        
        for i in range(N):
            for j in range(N):
                if i == j: continue
                
                src = self.stores_data[i]
                dst = self.stores_data[j]
                
                # Asymmetric Friction
                friction = TRAFFIC_SVC.get_traffic_friction(src, dst, hour)
                traffic[i, j, 0] = friction
            
        return traffic

