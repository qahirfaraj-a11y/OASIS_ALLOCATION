"""
Store Network Generator
========================
Creates the Chandarana Foodplus store network with 14 real outlets.
Each store is a StoreNode carrying geographic, physical, financial,
and proprietary ranking attributes for use as GNN nodes.

KEY CALIBRATION NOTE:
  - ALL Chandarana stores are mega-class, carrying the full SKU range (~23K)
  - Scorecard data (ADS, GRN, PO) comes from RHAPTA ROAD (5th in sales)
  - Rhapta Road snapshot: KES 114M in stock, all SKUs stocked
  - demand_scale_factor is RELATIVE TO RHAPTA ROAD (1.0x baseline)
  - Sales ranking (user-provided): Yaya > Lavington > Diamond Plaza > ABC > Rhapta
  - Stores ranked 6th-14th are ordered by floor area

Usage:
    from store_network_generator import generate_store_network, save_network
    stores = generate_store_network()
    save_network(stores, "stores_network.json")
"""

import json
import math
import os
from dataclasses import dataclass, field, asdict
from typing import List, Dict, Optional


# =============================================================================
# Data Model
# =============================================================================

@dataclass
class StoreNode:
    """A single retail outlet node in the store network."""
    
    # Identity
    store_id: str                          # e.g. "CFP-001"
    name: str                              # e.g. "Chandarana Foodplus Signature Mall"
    
    # Geographic (Decimal Degrees: lat negative=South, lon positive=East)
    latitude: float
    longitude: float
    city: str = "Nairobi"
    region: str = ""                       # Cluster group
    
    # Physical
    floor_area_sqft: float = 0.0
    store_category: str = ""               # Physical layout descriptor
    cold_chain_capable: bool = True
    parking_capacity: int = 0
    
    # Financial
    monthly_budget: float = 114_000_000    # All stores are mega-class
    avg_monthly_revenue: float = 0.0
    demand_scale_factor: float = 1.0       # Relative to Rhapta Road baseline
    max_skus: int = 23_000                 # Full range
    
    # Sales Ranking
    sales_rank: int = 0                    # 1 = highest sales, 14 = lowest
    footfall_rank: float = 5.0             # 1-10 (10=highest footfall)
    supplier_diversity_score: float = 0.5  # 0-1 (1=most diverse)
    catchment_affluence_index: float = 3.0 # 1-5 (5=most affluent)
    brand_strength_score: float = 50.0     # 0-100
    
    # Operational
    is_online: bool = False
    is_reference_store: bool = False       # True for Rhapta Road (data source)
    reorder_frequency_days: int = 1
    min_order_value: float = 8_000
    safety_days: int = 10
    
    # Stock profile (populated by store_stock_generator)
    stock_profile: List[Dict] = field(default_factory=list)


# =============================================================================
# Cluster Affluence Profiles
# =============================================================================

CLUSTER_PROFILES = {
    "Mombasa Rd / Outer": {
        "catchment_affluence_index": 3.0,
        "parking_capacity": 300,
        "brand_strength_base": 70,
    },
    "Karen / Langata": {
        "catchment_affluence_index": 4.5,
        "parking_capacity": 150,
        "brand_strength_base": 75,
    },
    "Kilimani / Lavington": {
        "catchment_affluence_index": 4.2,
        "parking_capacity": 120,
        "brand_strength_base": 80,
    },
    "Westlands / Riverside": {
        "catchment_affluence_index": 4.0,
        "parking_capacity": 80,
        "brand_strength_base": 78,
    },
    "Parklands / Highridge": {
        "catchment_affluence_index": 3.5,
        "parking_capacity": 60,
        "brand_strength_base": 65,
    },
    "Northern Hub": {
        "catchment_affluence_index": 4.3,
        "parking_capacity": 100,
        "brand_strength_base": 72,
    },
}


# =============================================================================
# The 14 Real Stores — Ordered by Sales Rank
# =============================================================================
#
# Sales Ranking (user-confirmed top 5):
#   1. Yaya Centre
#   2. Lavington Mall
#   3. Diamond Plaza
#   4. ABC Place
#   5. Rhapta Road  (BASELINE — all scorecard data from here)
#   6-14: Ranked by floor area (descending)
#
# demand_scale_factor is relative to Rhapta Road (1.0x):
#   - Stores 1-4: scale > 1.0 (higher sales volume than Rhapta)
#   - Store 5: scale = 1.0 (Rhapta Road, baseline)
#   - Stores 6-14: scale < 1.0 (lower sales volume, proportional to floor area)

CHANDARANA_STORES = [
    # === TOP 5 BY SALES (user-confirmed ranking) ===
    {
        "store_id": "CFP-003",
        "name": "Chandarana Yaya Centre",
        "latitude": -1.2912,
        "longitude": 36.7991,
        "region": "Kilimani / Lavington",
        "floor_area_sqft": 22_500,
        "store_category": "Prime Anchor",
        "sales_rank": 1,
        "demand_scale_factor": 1.60,      # #1 in sales — ~60% more volume than Rhapta
        "footfall_rank": 9.5,
        "supplier_diversity_score": 0.92,
    },
    {
        "store_id": "CFP-005",
        "name": "Chandarana Lavington Mall",
        "latitude": -1.2785,
        "longitude": 36.7725,
        "region": "Kilimani / Lavington",
        "floor_area_sqft": 16_500,
        "store_category": "Medium Anchor",
        "sales_rank": 2,
        "demand_scale_factor": 1.40,      # #2 in sales
        "footfall_rank": 9.0,
        "supplier_diversity_score": 0.85,
    },
    {
        "store_id": "CFP-009",
        "name": "Chandarana Diamond Plaza",
        "latitude": -1.2581,
        "longitude": 36.8228,
        "region": "Parklands / Highridge",
        "floor_area_sqft": 10_000,
        "store_category": "Medium Anchor",
        "sales_rank": 3,
        "demand_scale_factor": 1.25,      # #3 in sales — compact but high density
        "footfall_rank": 8.5,
        "supplier_diversity_score": 0.78,
    },
    {
        "store_id": "CFP-006",
        "name": "Chandarana ABC Place",
        "latitude": -1.2619,
        "longitude": 36.7772,
        "region": "Westlands / Riverside",
        "floor_area_sqft": 13_500,
        "store_category": "Boutique / Large",
        "sales_rank": 4,
        "demand_scale_factor": 1.10,      # #4 in sales
        "footfall_rank": 8.5,
        "supplier_diversity_score": 0.75,
    },
    {
        "store_id": "CFP-007",
        "name": "Chandarana Rhapta Road",
        "latitude": -1.2641,
        "longitude": 36.7865,
        "region": "Westlands / Riverside",
        "floor_area_sqft": 6_500,
        "store_category": "Express / Neighborhood",
        "sales_rank": 5,
        "demand_scale_factor": 1.00,      # BASELINE — all scorecard data from here
        "footfall_rank": 8.0,
        "supplier_diversity_score": 0.70,
        "is_reference_store": True,
    },
    
    # === RANKED 6-14 BY FLOOR AREA (descending) ===
    {
        "store_id": "CFP-001",
        "name": "Chandarana Signature Mall",
        "latitude": -1.4285,
        "longitude": 36.9532,
        "region": "Mombasa Rd / Outer",
        "floor_area_sqft": 28_000,
        "store_category": "Hyper / Flagship",
        "sales_rank": 6,
        "demand_scale_factor": 0.85,      # Large space but not top-5 in sales
        "footfall_rank": 7.5,
        "supplier_diversity_score": 0.88,
    },
    {
        "store_id": "CFP-004",
        "name": "Chandarana Adlife Plaza",
        "latitude": -1.2936,
        "longitude": 36.7954,
        "region": "Kilimani / Lavington",
        "floor_area_sqft": 20_000,
        "store_category": "Large Anchor",
        "sales_rank": 7,
        "demand_scale_factor": 0.75,
        "footfall_rank": 7.0,
        "supplier_diversity_score": 0.80,
    },
    {
        "store_id": "CFP-012",
        "name": "Chandarana Rosslyn Riviera",
        "latitude": -1.2224,
        "longitude": 36.8021,
        "region": "Northern Hub",
        "floor_area_sqft": 17_500,
        "store_category": "Premium Anchor",
        "sales_rank": 8,
        "demand_scale_factor": 0.65,
        "footfall_rank": 7.0,
        "supplier_diversity_score": 0.78,
    },
    {
        "store_id": "CFP-002",
        "name": "Chandarana The Well Karen",
        "latitude": -1.3402,
        "longitude": 36.7601,
        "region": "Karen / Langata",
        "floor_area_sqft": 14_000,
        "store_category": "Medium Anchor",
        "sales_rank": 9,
        "demand_scale_factor": 0.55,
        "footfall_rank": 6.5,
        "supplier_diversity_score": 0.68,
    },
    {
        "store_id": "CFP-008",
        "name": "Chandarana Riverside Square",
        "latitude": -1.2701,
        "longitude": 36.7995,
        "region": "Westlands / Riverside",
        "floor_area_sqft": 12_000,
        "store_category": "Boutique / Large",
        "sales_rank": 10,
        "demand_scale_factor": 0.50,
        "footfall_rank": 6.0,
        "supplier_diversity_score": 0.62,
    },
    {
        "store_id": "CFP-013",
        "name": "Chandarana Ridgeways Mall",
        "latitude": -1.2312,
        "longitude": 36.8455,
        "region": "Northern Hub",
        "floor_area_sqft": 11_000,
        "store_category": "Medium Anchor",
        "sales_rank": 11,
        "demand_scale_factor": 0.45,
        "footfall_rank": 5.5,
        "supplier_diversity_score": 0.60,
    },
    {
        "store_id": "CFP-014",
        "name": "Chandarana New Muthaiga Thigiri",
        "latitude": -1.2375,
        "longitude": 36.7892,
        "region": "Northern Hub",
        "floor_area_sqft": 10_000,
        "store_category": "Medium Anchor",
        "sales_rank": 12,
        "demand_scale_factor": 0.40,
        "footfall_rank": 5.5,
        "supplier_diversity_score": 0.55,
    },
    {
        "store_id": "CFP-011",
        "name": "Chandarana Mobil Plaza Muthaiga",
        "latitude": -1.2536,
        "longitude": 36.8372,
        "region": "Northern Hub",
        "floor_area_sqft": 9_000,
        "store_category": "Medium Anchor",
        "sales_rank": 13,
        "demand_scale_factor": 0.35,
        "footfall_rank": 5.0,
        "supplier_diversity_score": 0.55,
    },
    {
        "store_id": "CFP-010",
        "name": "Chandarana Azalea Square",
        "latitude": -1.2523,
        "longitude": 36.8081,
        "region": "Parklands / Highridge",
        "floor_area_sqft": 8_000,
        "store_category": "Medium Anchor",
        "sales_rank": 14,
        "demand_scale_factor": 0.30,
        "footfall_rank": 5.0,
        "supplier_diversity_score": 0.50,
    },
]


# =============================================================================
# Haversine Distance (km)
# =============================================================================

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


# =============================================================================
# Network Generator
# =============================================================================

def generate_store_network() -> List[StoreNode]:
    """
    Build the 14-node Chandarana Foodplus store network.
    
    All stores are mega-class carrying the full product range.
    demand_scale_factor is relative to Rhapta Road (1.0x baseline).
    
    Returns:
        List of 14 StoreNode objects
    """
    stores = []
    
    # Rhapta Road baseline stock value (user-provided)
    RHAPTA_STOCK_VALUE = 114_000_000  # KES 114M
    
    for raw in CHANDARANA_STORES:
        cluster = raw.get("region", "")
        cluster_profile = CLUSTER_PROFILES.get(cluster, {})
        scale = raw.get("demand_scale_factor", 1.0)
        
        # Budget scales with demand — higher sales stores need proportionally more capital
        budget = RHAPTA_STOCK_VALUE * scale
        
        store = StoreNode(
            # Identity
            store_id=raw["store_id"],
            name=raw["name"],
            
            # Geographic
            latitude=raw["latitude"],
            longitude=raw["longitude"],
            city="Nairobi",
            region=cluster,
            
            # Physical
            floor_area_sqft=raw.get("floor_area_sqft", 10_000),
            store_category=raw.get("store_category", ""),
            cold_chain_capable=True,
            parking_capacity=cluster_profile.get("parking_capacity", 50),
            
            # Financial — all mega-class
            monthly_budget=budget,
            avg_monthly_revenue=budget * 0.08,
            demand_scale_factor=scale,
            max_skus=23_000,  # All stores carry full range
            
            # Rankings
            sales_rank=raw.get("sales_rank", 10),
            footfall_rank=raw.get("footfall_rank", 5.0),
            supplier_diversity_score=raw.get("supplier_diversity_score", 0.5),
            catchment_affluence_index=cluster_profile.get("catchment_affluence_index", 3.0),
            brand_strength_score=cluster_profile.get("brand_strength_base", 50) + 
                                 raw.get("footfall_rank", 5.0) * 2,
            
            # Operational
            is_online=False,
            is_reference_store=raw.get("is_reference_store", False),
            reorder_frequency_days=1,
            min_order_value=8_000,
            safety_days=10,
        )
        
        stores.append(store)
    
    return stores


# =============================================================================
# Distance Matrix
# =============================================================================

def build_distance_matrix(stores: List[StoreNode]) -> List[List[float]]:
    """Build an NxN distance matrix (km) between all stores."""
    n = len(stores)
    matrix = [[0.0] * n for _ in range(n)]
    
    for i in range(n):
        for j in range(i + 1, n):
            d = haversine_km(
                stores[i].latitude, stores[i].longitude,
                stores[j].latitude, stores[j].longitude
            )
            matrix[i][j] = round(d, 3)
            matrix[j][i] = round(d, 3)
    
    return matrix


# =============================================================================
# Serialization
# =============================================================================

def save_network(stores: List[StoreNode], filepath: str):
    """Save store network to JSON."""
    data = {
        "network_name": "Chandarana Foodplus Nairobi Network",
        "store_count": len(stores),
        "reference_store": "CFP-007 (Rhapta Road)",
        "reference_stock_value_kes": 114_000_000,
        "stores": [asdict(s) for s in stores],
        "distance_matrix_km": build_distance_matrix(stores),
    }
    
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)
    
    print(f"[OK] Saved {len(stores)}-node store network to {filepath}")
    return filepath


def load_network(filepath: str) -> List[StoreNode]:
    """Load store network from JSON."""
    with open(filepath, "r") as f:
        data = json.load(f)
    
    stores = []
    for s in data["stores"]:
        stock = s.pop("stock_profile", [])
        store = StoreNode(**s)
        store.stock_profile = stock
        stores.append(store)
    
    return stores


# =============================================================================
# Summary / Debug
# =============================================================================

def print_network_summary(stores: List[StoreNode]):
    """Print a human-readable summary of the store network."""
    print(f"\n{'='*90}")
    print(f"  CHANDARANA FOODPLUS STORE NETWORK -- {len(stores)} Outlets")
    print(f"  Baseline: Rhapta Road (KES 114M stock, all SKUs, demand_scale=1.0x)")
    print(f"{'='*90}")
    
    # Sort by sales rank
    sorted_stores = sorted(stores, key=lambda s: s.sales_rank)
    
    print(f"\n  {'Rank':<5} {'ID':<8} {'Store':<40} {'sqft':>7} {'Scale':>6} {'Budget (KES)':>15}")
    print(f"  {'-'*85}")
    
    for s in sorted_stores:
        ref_marker = " *" if s.is_reference_store else ""
        print(f"  {s.sales_rank:<5} {s.store_id:<8} {s.name:<40} "
              f"{s.floor_area_sqft:>6,.0f} {s.demand_scale_factor:>5.2f}x "
              f"{s.monthly_budget:>14,.0f}{ref_marker}")
    
    print(f"\n  * = Reference store (scorecard data source)")
    
    # Distance extremes
    dist_matrix = build_distance_matrix(stores)
    n = len(stores)
    min_d, max_d = float('inf'), 0
    min_pair, max_pair = ("", ""), ("", "")
    
    for i in range(n):
        for j in range(i+1, n):
            d = dist_matrix[i][j]
            if d < min_d:
                min_d = d
                min_pair = (stores[i].name, stores[j].name)
            if d > max_d:
                max_d = d
                max_pair = (stores[i].name, stores[j].name)
    
    print(f"\n  Distance: {min_pair[0]} <-> {min_pair[1]} = {min_d:.2f} km (closest)")
    print(f"  Distance: {max_pair[0]} <-> {max_pair[1]} = {max_d:.2f} km (farthest)")
    print(f"{'='*90}\n")


# =============================================================================
# CLI Entry Point
# =============================================================================

if __name__ == "__main__":
    stores = generate_store_network()
    print_network_summary(stores)
    
    output_path = os.path.join(os.path.dirname(__file__), "stores_network.json")
    save_network(stores, output_path)
