
import sys
import os
import json
import torch
import numpy as np
import pandas as pd
from typing import Dict, List
import math

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


class NetworkSimulator:
    """
    Orchestrates a multi-store simulation synchronized with the ST-GAT Network.
    """
    def __init__(self, network_file: str = "stores_network.json"):
        self.network_file = network_file
        with open(network_file, 'r') as f:
            self.data = json.load(f)
        self.stores_data = self.data['stores']
        
        self.simulators: Dict[str, RetailSimulator] = {}
        self.current_day = 0

        # --- OPTIMIZATION START ---
        print("Pre-loading Shared Resources...")
        # 1. Load Master Scorecard (Avoiding 14x reads)
        try:
             # Use localized path relative to script if possible, or absolute from config
             scorecard_path = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv" 
             self.preloaded_df = pd.read_csv(scorecard_path)
             print(f"Loaded Master Scorecard: {len(self.preloaded_df)} rows")
        except Exception as e:
             print(f"Failed to preload scorecard: {e}")
             self.preloaded_df = None

        # 2. Shared Logic Bridge (Avoiding 14x DB loads)
        # We need to import the class first (handled at top)
        # We initialize ONE bridge and pass it down
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        data_dir = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
        self.shared_bridge = SimulationOrderUtil(data_dir)
        self.shared_bridge.engine.load_local_databases()
        
        # 3. Centralized Enrichment (Avoiding 14x enrichment)
        print("Performing Centralized Product Enrichment...")
        self.pre_enriched_products = None
        if self.preloaded_df is not None:
             try:
                 # Extract raw products from DF (Copy logic from retail_simulator)
                 raw_products = []
                 for _, row in self.preloaded_df.iterrows():
                     raw_products.append({
                        'product_name': str(row.get('Product', 'Unknown')),
                        'supplier_name': str(row.get('Supplier', 'Unknown')),
                        'product_category': str(row.get('Department', 'GENERAL')),
                        'selling_price': float(row.get('Unit_Price', 0) or 0),
                        'margin_pct': float(row.get('Margin_Pct', 25) or 25),
                        'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) or 0), # RAW ADS (Scaling happens in store)
                        'current_stocks': 0.0,
                        'pack_size': int(float(row.get('Pack_Size', 1) or 1)),
                        'ABC_Class': str(row.get('ABC_Class', 'C')),
                        'reliability_score': 90,
                        'is_consignment': False
                     })
                 
                 # Enrich ONCE
                 self.pre_enriched_products = self.shared_bridge.engine.enrich_product_data(raw_products)
                 print(f"Enriched {len(self.pre_enriched_products)} products globally.")
             except Exception as e:
                 print(f"Global enrichment failed: {e}")
                 self.pre_enriched_products = None

        print("Initialized Shared Logic Bridge.")
        # --- OPTIMIZATION END ---
        
        # Initialize Simulators
        print("Initializing Network Simulation...")
        for store in self.stores_data:
            s_id = store['store_id']
            category = store.get('store_category', 'Medium Anchor')
            
            # Map Category to Tier Config
            tier = "Medium_1M" # Default
            if "Express" in category: tier = "Small_200k"
            elif "Hyper" in category: tier = "Mega_100M"
            elif "Large" in category: tier = "Large_10M"
            elif "Boutique" in category: tier = "Small_200k"
            
            config = STORE_UNIVERSES.get(tier, STORE_UNIVERSES["Medium_1M"]).copy()
            
            # Adjust budget based on store metadata if available
            if 'monthly_budget' in store:
                config['budget'] = store['monthly_budget']
            
            # Initialize Simulator
            # PASS SHARED RESOURCES
            sim = RetailSimulator(
                tier_name=tier, 
                store_config=config, 
                seed=42 + len(self.simulators),
                bridge=self.shared_bridge,           # Shared Logic
                preloaded_data=self.preloaded_df,    # Shared Data (Fallback)
                pre_enriched_products=self.pre_enriched_products # Shared Enriched Data
            )
            self.simulators[s_id] = sim
            
        print(f"Initialized {len(self.simulators)} store simulators.")
        
        # Pre-compute Static Graph Elements
        from models.store_gnn import haversine_km # Needed for TrafficService
        self.edge_index, _ = build_edges(self.stores_data)
        self.adj = build_adjacency(self.edge_index, len(self.stores_data))
        
    def step(self):
        """Advance simulation by 1 day."""
        self.current_day += 1
        day = self.current_day
        
        # Sync Step
        for s_id, sim in self.simulators.items():
            # 1. Arrivals
            sim.process_arrivals(day)
            
            # 2. Demand & Sales
            # We simulate demand for ALL SKUs
            daily_rev = 0.0
            for sku in sim.skus.values():
                demand = sim.simulate_daily_demand(sku, day_of_week=day % 7)
                sold = min(demand, sku.current_stock)
                
                sku.current_stock -= sold
                sku.total_sales += sold
                daily_rev += sold * sku.unit_price
                
                # Update tracking
                if demand > sold:
                    sku.lost_sales += (demand - sold)
                    sku.stockout_days += 1
            
            # 3. Reordering
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
            
            # 1. Continuous Wave (Indices 20-23)
            sin_mo = math.sin(2 * math.pi * t / 30)
            cos_mo = math.cos(2 * math.pi * t / 30)
            sin_wk = math.sin(2 * math.pi * t / 7)
            cos_wk = math.cos(2 * math.pi * t / 7)
            
            f_vec[20] = sin_mo
            f_vec[21] = cos_mo
            f_vec[22] = sin_wk
            f_vec[23] = cos_wk
            
            # --- FEATURE 28: RAIN ---
            # We need to ensure we are targeting Index 28.
            # store_to_features returns 29 items usually (0-28)? 
            # If len < 28, pad.
            while len(f_vec) < 28:
                f_vec.append(0.0)
                
            # Weather (Index 28)
            # Dashboard scnearios might override this in x_t directly, but we set baseline here.
            # Mock Service returns 0.0 or high value.
            rain = WEATHER_SVC.get_current_weather(store.get('region', 'Nairobi'), t)
            if len(f_vec) == 28:
                f_vec.append(rain) # Index 28
            else:
                f_vec[28] = rain
            
            # --- FEATURE 29: SALARY HIT ---
            # "Explosion of bulk buying" between 28th and 5th
            day_mod = t % 30
            is_payday_window = (day_mod >= 28) or (day_mod <= 5)
            salary_hit = 1.0 if is_payday_window else 0.0
            
            if len(f_vec) == 29:
                f_vec.append(salary_hit) # Index 29
            else:
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

