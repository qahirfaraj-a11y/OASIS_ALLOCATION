
import sys
import os
import json
import torch
import numpy as np

# Ensure proper path
sys.path.append(os.getcwd())

from network_simulation import NetworkSimulator, GeospatialExpansionEngine

def verify_upgrade():
    print("="*60)
    print(" SITE SELECTION UPGRADE VERIFICATION (V2)")
    print("="*60)
    
    network_path = "stores_network.json"
    if not os.path.exists(network_path):
        print("Error: Network file not found.")
        return

    sim = NetworkSimulator(network_path)
    engine = sim.expansion_engine
    
    # Coordinates for testing (Nairobi)
    # 1. High potential (Affluent, Served by Chandarana but maybe gap)
    # Lavington area roughly -1.28, 36.77
    lav_lat, lav_lon = -1.2882, 36.7725
    
    # 2. Competitor Heavy (Near a Naivas/Carrefour)
    # CBD -1.29, 36.82
    cbd_lat, cbd_lon = -1.2921, 36.8219
    
    test_sites = [
        {"name": "Lavington (Prime)", "lat": lav_lat, "lon": lav_lon},
        {"name": "CBD (Competitive)", "lat": cbd_lat, "lon": cbd_lon},
        {"name": "Outskirts (Low Density)", "lat": -1.40, "lon": 36.70}
    ]
    
    for site in test_sites:
        print(f"\nAnalyzing Site: {site['name']}")
        
        # 1. Huff Probability
        huff = engine.calculate_huff_probability(site['lat'], site['lon'])
        
        # 2. Travel Time to nearest Chandarana
        nearest_store = sim.stores_data[0]
        min_d = 999
        for s in sim.stores_data:
            d = np.linalg.norm(np.array([site['lat'], site['lon']]) - np.array([s['latitude'], s['longitude']]))
            if d < min_d:
                min_d = d
                nearest_store = s
        
        time = engine.estimate_travel_time(site['lat'], site['lon'], 
                                           nearest_store['latitude'], nearest_store['longitude'])
        
        # 3. Final ML Score
        score = engine.calculate_gap_index(site['lat'], site['lon'])
        
        print(f"  Huff Capture Prob: {huff*100:.2f}%")
        print(f"  Travel Time Proxy: {time:.2f} mins")
        print(f"  Final Success (ML): {score:.4f}")
        
        rec = engine.recommend_store_type(score)
        print(f"  Recommendation: {rec}")

    print("\n" + "="*60)
    print(" VERIFICATION COMPLETE")
    print("="*60)

if __name__ == "__main__":
    verify_upgrade()
