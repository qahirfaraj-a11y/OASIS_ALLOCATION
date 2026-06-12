import torch
import pandas as pd
from network_simulation import NetworkSimulator

def test_expansion_logic():
    print("Initializing NetworkSimulator in Lazy Mode...")
    sim = NetworkSimulator('stores_network.json', skip_enrichment=True)
    engine = sim.expansion_engine
    
    # 1. Coordinate close to Rhapta Road (Existing Store)
    # Latitude: -1.2657, Longitude: 36.8042
    lat_near, lon_near = -1.2658, 36.8043
    score_near = engine.calculate_gap_index(lat_near, lon_near)
    rec_near = engine.recommend_store_type(score_near)
    print(f"Near existing store: Score={score_near:.2f}, Rec={rec_near}")
    
    # 2. Coordinate in a "Gap" (e.g., further out in Karen or Lavington away from nodes)
    # Let's try Lavington: -1.2697, 36.7725 (approx)
    lat_gap, lon_gap = -1.2700, 36.7500
    score_gap = engine.calculate_gap_index(lat_gap, lon_gap)
    rec_gap = engine.recommend_store_type(score_gap)
    print(f"In a potential gap: Score={score_gap:.2f}, Rec={rec_gap}")
    
    # 3. Coordinate near a Competitor (Naivas Westlands: -1.2646, 36.8045)
    # Westlands is crowded, so score should be impacted.
    lat_comp, lon_comp = -1.2647, 36.8046
    score_comp = engine.calculate_gap_index(lat_comp, lon_comp)
    rec_comp = engine.recommend_store_type(score_comp)
    print(f"Near competitor: Score={score_comp:.2f}, Rec={rec_comp}")

    # 4. Low Affluence Check (Gas Station Mini-Mart)
    rec_low = engine.recommend_store_type(0.5, affluence=2.5)
    print(f"Low Affluence Node: Score=0.5, Affluence=2.5, Rec={rec_low}")

if __name__ == "__main__":
    test_expansion_logic()
