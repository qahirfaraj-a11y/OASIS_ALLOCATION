import sys
import os
import traceback

# Add project root to path
sys.path.append(os.getcwd())

print("--- Reproduction Script Start ---")

try:
    from network_simulation import NetworkSimulator
    print("Initialising NetworkSimulator...")
    sim = NetworkSimulator("stores_network.json")
    print("NetworkSimulator initialised successfully.")
    
    print("Getting feature matrix...")
    feats = sim.get_feature_matrix()
    print(f"Feature matrix shape: {feats.shape}")
    
except Exception as e:
    print(f"\n!!! CAUGHT EXCEPTION: {type(e).__name__}: {e}")
    traceback.print_exc()
    sys.exit(1)

print("--- Reproduction Script Complete ---")
