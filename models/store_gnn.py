"""
Store Graph Neural Network
============================
A GNN where each Chandarana Foodplus outlet is a node.

Architecture:
  - Manual GCN (Graph Convolutional Network) layers (no PyTorch Geometric needed)
  - Node features from store attributes (geographic, financial, rankings)
  - Edges from geographic proximity + cluster membership
  - Three prediction heads: Demand, Transfer Score, Risk Vulnerability

Usage:
    from models.store_gnn import StoreGraphNetwork, build_graph_from_network
    
    # Build graph from store network JSON
    node_features, edge_index, edge_weights, store_ids = build_graph_from_network("stores_network.json")
    
    # Create and run network
    model = StoreGraphNetwork(in_features=node_features.shape[1])
    demand_pred, embeddings = model(node_features, edge_index, edge_weights)
"""

import math
import json
import torch
import torch.nn as nn
import torch.nn.functional as F
from typing import List, Tuple, Dict, Optional


# =============================================================================
# Manual GCN Layer (No PyTorch Geometric)
# =============================================================================

class GCNConv(nn.Module):
    """
    Graph Convolutional Layer (Kipf & Welling 2017).
    Manual implementation using dense adjacency for small graphs.
    
    For N nodes with F_in features:
      H' = sigma( D^{-1/2} A_hat D^{-1/2} H W )
    
    Where A_hat = A + I (adjacency with self-loops)
    """
    
    def __init__(self, in_features: int, out_features: int, bias: bool = True):
        super().__init__()
        self.in_features = in_features
        self.out_features = out_features
        
        # Learnable weight matrix
        self.weight = nn.Parameter(torch.FloatTensor(in_features, out_features))
        if bias:
            self.bias = nn.Parameter(torch.FloatTensor(out_features))
        else:
            self.register_parameter('bias', None)
        
        self._reset_parameters()
    
    def _reset_parameters(self):
        """Xavier uniform initialization."""
        nn.init.xavier_uniform_(self.weight)
        if self.bias is not None:
            nn.init.zeros_(self.bias)
    
    def forward(self, x: torch.Tensor, adj: torch.Tensor) -> torch.Tensor:
        """
        Args:
            x: Node feature matrix [N, in_features]
            adj: Normalized adjacency matrix [N, N] (with self-loops)
        
        Returns:
            Updated node features [N, out_features]
        """
        # 1. Feature transformation: H * W
        support = torch.mm(x, self.weight)
        
        # 2. Neighborhood aggregation: A_norm * (H * W)
        output = torch.mm(adj, support)
        
        # 3. Add bias
        if self.bias is not None:
            output = output + self.bias
        
        return output


# =============================================================================
# Adjacency Matrix Utilities
# =============================================================================

def build_adjacency(edge_index: torch.Tensor, 
                    edge_weights: Optional[torch.Tensor] = None,
                    num_nodes: int = 0) -> torch.Tensor:
    """
    Build a normalized adjacency matrix from edge list.
    
    A_hat = A + I  (add self-loops)
    D_hat = degree matrix of A_hat
    A_norm = D_hat^{-1/2} * A_hat * D_hat^{-1/2}
    
    Args:
        edge_index: [2, E] tensor of (source, target) pairs
        edge_weights: Optional [E] tensor of edge weights
        num_nodes: Number of nodes
        
    Returns:
        Normalized adjacency matrix [N, N]
    """
    if num_nodes == 0:
        num_nodes = int(edge_index.max().item()) + 1
    
    # Build adjacency matrix
    adj = torch.zeros(num_nodes, num_nodes)
    
    if edge_weights is not None:
        for i in range(edge_index.shape[1]):
            src, dst = int(edge_index[0, i].item()), int(edge_index[1, i].item())
            adj[src, dst] = edge_weights[i]
            adj[dst, src] = edge_weights[i]  # Symmetric
    else:
        for i in range(edge_index.shape[1]):
            src, dst = int(edge_index[0, i].item()), int(edge_index[1, i].item())
            adj[src, dst] = 1.0
            adj[dst, src] = 1.0
    
    # Add self-loops
    adj = adj + torch.eye(num_nodes)
    
    # Degree matrix D^{-1/2}
    degree = adj.sum(dim=1)
    degree_inv_sqrt = torch.pow(degree, -0.5)
    degree_inv_sqrt[torch.isinf(degree_inv_sqrt)] = 0.0
    D_inv_sqrt = torch.diag(degree_inv_sqrt)
    
    # Symmetric normalization: D^{-1/2} A D^{-1/2}
    adj_norm = torch.mm(torch.mm(D_inv_sqrt, adj), D_inv_sqrt)
    
    return adj_norm


# =============================================================================
# Store Graph Neural Network
# =============================================================================

class StoreGraphNetwork(nn.Module):
    """
    GNN for the Chandarana store network.
    
    Architecture:
        Input (N x F_in)
          -> GCNConv(F_in -> 64) -> ReLU -> Dropout
          -> GCNConv(64 -> 32)   -> ReLU -> Dropout
          -> Store Embedding (N x 32)
          -> Task Heads:
              1. DemandHead:   Linear(32 -> n_departments)  per-dept demand index
              2. TransferHead: Bilinear(32, 32 -> 1)        pairwise transfer score
              3. RiskHead:     Linear(32 -> 1)              disruption vulnerability
    """
    
    def __init__(self, in_features: int, hidden_dim: int = 64, 
                 embed_dim: int = 32, n_departments: int = 20,
                 dropout: float = 0.3):
        super().__init__()
        
        self.in_features = in_features
        self.hidden_dim = hidden_dim
        self.embed_dim = embed_dim
        
        # Message-passing layers
        self.conv1 = GCNConv(in_features, hidden_dim)
        self.conv2 = GCNConv(hidden_dim, embed_dim)
        self.dropout = nn.Dropout(dropout)
        
        # Batch normalization for stability
        self.bn1 = nn.BatchNorm1d(hidden_dim)
        self.bn2 = nn.BatchNorm1d(embed_dim)
        
        # === Prediction Heads ===
        
        # 1. Demand Head: predict per-department demand index
        self.demand_head = nn.Sequential(
            nn.Linear(embed_dim, embed_dim),
            nn.ReLU(),
            nn.Linear(embed_dim, n_departments),
            nn.Softmax(dim=-1)  # Outputs department demand distribution
        )
        
        # 2. Transfer Head: pairwise transfer score between stores
        #    Uses bilinear form: score(i,j) = e_i^T W e_j
        self.transfer_bilinear = nn.Bilinear(embed_dim, embed_dim, 1)
        
        # 3. Risk Head: single vulnerability score per store
        self.risk_head = nn.Sequential(
            nn.Linear(embed_dim, 16),
            nn.ReLU(),
            nn.Linear(16, 1),
            nn.Sigmoid()  # 0 = resilient, 1 = vulnerable
        )
    
    def encode(self, x: torch.Tensor, adj: torch.Tensor) -> torch.Tensor:
        """
        Encode store features into embeddings via message passing.
        
        Args:
            x: Node features [N, F_in]
            adj: Normalized adjacency [N, N]
            
        Returns:
            Store embeddings [N, embed_dim]
        """
        # Layer 1
        h = self.conv1(x, adj)
        h = self.bn1(h)
        h = F.relu(h)
        h = self.dropout(h)
        
        # Layer 2
        h = self.conv2(h, adj)
        h = self.bn2(h)
        h = F.relu(h)
        h = self.dropout(h)
        
        return h
    
    def forward(self, x: torch.Tensor, edge_index: torch.Tensor,
                edge_weights: Optional[torch.Tensor] = None) -> Dict[str, torch.Tensor]:
        """
        Full forward pass.
        
        Args:
            x: Node features [N, F_in]
            edge_index: [2, E] edge list
            edge_weights: Optional [E] edge weights
            
        Returns:
            Dict with:
                'embeddings': [N, embed_dim] store embeddings
                'demand': [N, n_departments] demand distribution per store
                'risk': [N, 1] vulnerability score per store
        """
        # Build adjacency
        adj = build_adjacency(edge_index, edge_weights, num_nodes=x.shape[0])
        
        # Encode
        embeddings = self.encode(x, adj)
        
        # Predict
        demand = self.demand_head(embeddings)
        risk = self.risk_head(embeddings)
        
        return {
            'embeddings': embeddings,
            'demand': demand,
            'risk': risk,
        }
    
    def predict_transfer_score(self, embeddings: torch.Tensor, 
                                store_i: int, store_j: int) -> float:
        """
        Predict the transfer benefit score between two stores.
        High score = stock transfer from i to j is beneficial.
        
        Args:
            embeddings: [N, embed_dim] from forward()
            store_i: Source store index
            store_j: Target store index
            
        Returns:
            Transfer score (0 to 1)
        """
        e_i = embeddings[store_i].unsqueeze(0)
        e_j = embeddings[store_j].unsqueeze(0)
        score = torch.sigmoid(self.transfer_bilinear(e_i, e_j))
        return score.item()
    
    def get_all_transfer_scores(self, embeddings: torch.Tensor) -> torch.Tensor:
        """
        Compute full NxN transfer score matrix.
        
        Returns:
            [N, N] matrix where entry (i,j) = transfer benefit score
        """
        n = embeddings.shape[0]
        scores = torch.zeros(n, n)
        
        for i in range(n):
            for j in range(n):
                if i != j:
                    e_i = embeddings[i].unsqueeze(0)
                    e_j = embeddings[j].unsqueeze(0)
                    scores[i, j] = torch.sigmoid(self.transfer_bilinear(e_i, e_j)).item()
        
        return scores


# Store category encoding (one-hot) -- still useful for physical layout
CATEGORY_MAP = {
    "Hyper / Flagship": 0,
    "Prime Anchor": 1,
    "Large Anchor": 2,
    "Premium Anchor": 3,
    "Medium Anchor": 4,
    "Boutique / Large": 5,
    "Express / Neighborhood": 6,
    "Express / Historic": 7,
}

# Cluster encoding (one-hot)
CLUSTER_MAP = {
    "Mombasa Rd / Outer": 0,
    "Karen / Langata": 1,
    "Kilimani / Lavington": 2,
    "Westlands / Riverside": 3,
    "Parklands / Highridge": 4,
    "Northern Hub": 5,
}

def store_to_features(store: dict) -> List[float]:
    """
    Convert a StoreNode dict to a fixed-length feature vector.
    
    Feature layout (total = 8 + 6 + 10 = 24 features):
      [0:8]   Category one-hot (8 dims) -- physical layout descriptor
      [8:14]  Cluster one-hot (6 dims)
      [14]    sales_rank_norm (rank / 14)  -- 1=best, 14=lowest
      [15]    demand_scale (raw scale relative to Rhapta)
      [16]    floor_area_norm (sqft / 30000)
      [17]    budget_log (log10(budget) / 10)
      [18]    footfall_rank_norm (rank / 10)
      [19]    affluence_norm (index / 5)
      [20]    brand_strength_norm (score / 100)
      [21]    supplier_diversity (0-1)
      [22]    lat_norm ((lat + 1.5) * 10)
      [23]    lon_norm ((lon - 36.5) * 10)
    """
    features = []
    
    # 1. Category one-hot [8] -- still useful as physical layout descriptor
    cat_idx = CATEGORY_MAP.get(store.get("store_category", "Medium Anchor"), 4)
    cat_vec = [0.0] * len(CATEGORY_MAP)
    cat_vec[cat_idx] = 1.0
    features.extend(cat_vec)
    
    # 2. Cluster one-hot [6]
    cluster_idx = CLUSTER_MAP.get(store.get("region", ""), 0)
    cluster_vec = [0.0] * len(CLUSTER_MAP)
    cluster_vec[cluster_idx] = 1.0
    features.extend(cluster_vec)
    
    # 3. Continuous features [10]
    # Sales rank (user-confirmed ranking, normalized)
    features.append(store.get("sales_rank", 10) / 14.0)
    
    # Demand scale factor (relative to Rhapta Road)
    features.append(store.get("demand_scale_factor", 1.0))
    
    # Physical
    floor_area = store.get("floor_area_sqft", 10000)
    features.append(floor_area / 30000.0)
    
    # Financial
    budget = max(store.get("monthly_budget", 114_000_000), 1)
    features.append(math.log10(budget) / 10.0)
    
    # Rankings
    features.append(store.get("footfall_rank", 5.0) / 10.0)
    features.append(store.get("catchment_affluence_index", 3.0) / 5.0)
    features.append(store.get("brand_strength_score", 50.0) / 100.0)
    features.append(store.get("supplier_diversity_score", 0.5))
    
    # Geographic (centered around Nairobi)
    lat = store.get("latitude", -1.3)
    lon = store.get("longitude", 36.8)
    features.append((lat + 1.5) * 10)
    features.append((lon - 36.5) * 10)
    
    return features


# =============================================================================
# Edge Construction
# =============================================================================

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two points in km."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) *
         math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


def build_edges(stores: List[dict], 
                max_distance_km: float = 15.0,
                same_cluster_bonus: float = 0.3) -> Tuple[torch.Tensor, torch.Tensor]:
    """
    Build edge list and edge weights for the store graph.
    
    Edge criteria:
      1. Geographic proximity: distance <= max_distance_km
      2. Same cluster: always connected (with bonus weight)
    
    Edge weight = inverse distance (closer = stronger) + cluster bonus.
    
    Args:
        stores: List of store dicts
        max_distance_km: Maximum distance for geographic edges
        same_cluster_bonus: Extra weight for same-cluster edges
        
    Returns:
        edge_index: [2, E] tensor
        edge_weights: [E] tensor
    """
    n = len(stores)
    sources = []
    targets = []
    weights = []
    
    for i in range(n):
        for j in range(i + 1, n):
            dist = haversine_km(
                stores[i]["latitude"], stores[i]["longitude"],
                stores[j]["latitude"], stores[j]["longitude"]
            )
            
            same_cluster = stores[i].get("region", "") == stores[j].get("region", "")
            
            # Edge criteria
            if dist <= max_distance_km or same_cluster:
                # Weight: inverse distance (capped to avoid inf for very close stores)
                inv_dist = 1.0 / max(dist, 0.1)
                weight = min(inv_dist, 10.0)  # Cap at 10
                
                if same_cluster:
                    weight += same_cluster_bonus
                
                # Add both directions (undirected graph)
                sources.extend([i, j])
                targets.extend([j, i])
                weights.extend([weight, weight])
    
    edge_index = torch.tensor([sources, targets], dtype=torch.long)
    edge_weights = torch.tensor(weights, dtype=torch.float)
    
    return edge_index, edge_weights


# =============================================================================
# Build Graph from JSON
# =============================================================================

def build_graph_from_network(filepath: str, 
                              max_distance_km: float = 15.0
                              ) -> Tuple[torch.Tensor, torch.Tensor, torch.Tensor, List[str]]:
    """
    Load a store network JSON and build the full graph tensors.
    
    Returns:
        node_features: [N, F] feature matrix
        edge_index: [2, E] edge list
        edge_weights: [E] edge weights
        store_ids: List of store IDs (for mapping indices back to stores)
    """
    with open(filepath, "r") as f:
        data = json.load(f)
    
    stores = data["stores"]
    
    # Node features
    feature_list = [store_to_features(s) for s in stores]
    node_features = torch.tensor(feature_list, dtype=torch.float)
    
    # Edges
    edge_index, edge_weights = build_edges(stores, max_distance_km=max_distance_km)
    
    # Store IDs for reference
    store_ids = [s["store_id"] for s in stores]
    
    print(f"[Graph] {len(stores)} nodes, {edge_index.shape[1]} edges, "
          f"{node_features.shape[1]} features per node")
    
    return node_features, edge_index, edge_weights, store_ids


# =============================================================================
# Quick Diagnostic
# =============================================================================

if __name__ == "__main__":
    import os
    import sys
    
    # Find the network file
    network_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "stores_network.json"
    )
    
    if not os.path.exists(network_path):
        # Try current dir
        network_path = os.path.join(os.getcwd(), "stores_network.json")
    
    if not os.path.exists(network_path):
        print(f"Error: stores_network.json not found. Run store_network_generator.py first.")
        sys.exit(1)
    
    print("=" * 60)
    print("  STORE GNN -- DIAGNOSTIC RUN")
    print("=" * 60)
    
    # 1. Build graph
    node_features, edge_index, edge_weights, store_ids = build_graph_from_network(network_path)
    print(f"  Node features shape: {node_features.shape}")
    print(f"  Edge index shape:    {edge_index.shape}")
    print(f"  Edge weights shape:  {edge_weights.shape}")
    
    # 2. Create model
    model = StoreGraphNetwork(
        in_features=node_features.shape[1],
        hidden_dim=64,
        embed_dim=32,
        n_departments=20
    )
    
    total_params = sum(p.numel() for p in model.parameters())
    print(f"  Model parameters:    {total_params:,}")
    
    # 3. Forward pass
    model.eval()
    with torch.no_grad():
        outputs = model(node_features, edge_index, edge_weights)
    
    print(f"\n  Output shapes:")
    print(f"    Embeddings: {outputs['embeddings'].shape}")
    print(f"    Demand:     {outputs['demand'].shape}")
    print(f"    Risk:       {outputs['risk'].shape}")
    
    # 4. Show store embeddings similarity (cosine)
    emb = outputs['embeddings']
    emb_norm = F.normalize(emb, p=2, dim=1)
    similarity = torch.mm(emb_norm, emb_norm.t())
    
    print(f"\n  Store Embedding Similarities (Top 3 Pairs):")
    n = len(store_ids)
    pairs = []
    for i in range(n):
        for j in range(i+1, n):
            pairs.append((similarity[i, j].item(), store_ids[i], store_ids[j]))
    
    pairs.sort(reverse=True)
    for score, s1, s2 in pairs[:3]:
        print(f"    {s1} <-> {s2}: {score:.3f}")
    
    # 5. Show risk scores
    print(f"\n  Risk Vulnerability Scores:")
    risks = outputs['risk'].squeeze()
    indexed = list(zip(store_ids, risks.tolist()))
    indexed.sort(key=lambda x: -x[1])
    for sid, risk_score in indexed[:5]:
        bar = "#" * int(risk_score * 20)
        print(f"    {sid}: {risk_score:.3f} |{bar}|")
    
    # 6. Transfer scores
    print(f"\n  Transfer Score Matrix (Top 3 Pairs):")
    transfer_scores = model.get_all_transfer_scores(outputs['embeddings'])
    transfer_pairs = []
    for i in range(n):
        for j in range(n):
            if i != j:
                transfer_pairs.append((transfer_scores[i, j].item(), store_ids[i], store_ids[j]))
    
    transfer_pairs.sort(reverse=True)
    for score, s1, s2 in transfer_pairs[:3]:
        print(f"    {s1} -> {s2}: {score:.3f}")
    
    print(f"\n{'='*60}")
    print(f"  DIAGNOSTIC COMPLETE -- All systems operational")
    print(f"{'='*60}")
