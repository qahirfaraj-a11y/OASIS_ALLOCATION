import os
import sys
import torch
import json
import pandas as pd
import numpy as np

# Mock large data to skip enrichment
class MockEngine:
    def load_local_databases(self): pass
    def enrich_product_data(self, products): return products

class MockBridge:
    def __init__(self):
        self.engine = MockEngine()

# Patch NetworkSimulator to use mock bridge and skip enrichment
from network_simulation import NetworkSimulator

def patched_init(self, network_file="stores_network.json"):
    self.network_file = network_file
    with open(network_file, 'r') as f:
        self.data = json.load(f)
    self.stores_data = self.data['stores']
    self.simulators = {}
    self.current_day = 0
    self.preloaded_df = pd.DataFrame()
    self.shared_bridge = MockBridge()
    self.pre_enriched_products = []
    from models.store_gnn import build_edges, build_adjacency
    self.edge_index, _ = build_edges(self.stores_data)
    self.adj = build_adjacency(self.edge_index, num_nodes=len(self.stores_data))

NetworkSimulator.__init__ = patched_init

# Now run the dashboard logic
from st_gat_dashboard import load_resources
import math

print("Loading resources...")
model, sim = load_resources()

print("Running feature extraction...")
x_t = sim.get_feature_matrix()

print("Running model inference...")
# Manual inference to see where it fails
T = 30
x_seq = x_t.unsqueeze(0).unsqueeze(0).expand(1, T, -1, -1)
adj = sim.adj
edge_attr = sim.get_traffic_matrix()

try:
    outputs = model(x_t, sim.edge_index)
    print("Inference successful")
except Exception as e:
    print(f"Inference failed: {e}")
    import traceback
    traceback.print_exc()
