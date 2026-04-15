
import time
import os
import sys
import torch

# Ensure path
sys.path.append(os.getcwd())

from network_simulation import NetworkSimulator

def benchmark():
    print("Initializing ST-GAT Network Simulator (Lazy Mode)...")
    sim = NetworkSimulator("stores_network.json")
    
    # Trigger hydration (simulates clicking 'Simulate' for the first time)
    start_h = time.time()
    sim.hydrate_simulators()
    print(f"Hydration took: {time.time()-start_h:.2f}s")

    days_to_sim = 7
    print(f"Running {days_to_sim}-day simulation...")
    
    start_s = time.time()
    for d in range(days_to_sim):
        sim.step()
    
    duration = time.time() - start_s
    print(f"Simulation of {days_to_sim} days took: {duration:.4f}s")
    print(f"Average time per day: {duration/days_to_sim:.4f}s")

if __name__ == "__main__":
    benchmark()
