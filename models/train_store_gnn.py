"""
Train Store GNN
================
Training script for the Chandarana store network GNN.

Generates training labels from store attributes and stock profiles,
then trains the network to learn:
  1. Demand distribution prediction (per-department)
  2. Risk vulnerability scoring
  3. Transfer affinity between store pairs

Usage:
    python -m models.train_store_gnn
    python -m models.train_store_gnn --epochs 200 --lr 0.005
"""

import json
import os
import sys
import argparse
import math
import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from typing import Dict, List, Tuple

from models.store_gnn import (
    StoreGraphNetwork, 
    build_graph_from_network, 
    store_to_features,
    build_edges,
    build_adjacency,
    CLUSTER_MAP,
)


# =============================================================================
# Label Generation
# =============================================================================

def generate_demand_labels(stores: List[dict], n_departments: int = 20) -> torch.Tensor:
    """
    Generate ground-truth demand distribution labels from stock profiles.
    
    For each store, compute the proportion of total scaled ADS
    that falls in each department. This gives us the "demand shape"
    of the store.
    
    Returns:
        [N, n_departments] tensor of demand distributions
    """
    # Collect all unique departments across all stores
    all_depts = set()
    for s in stores:
        for item in s.get("stock_profile", []):
            all_depts.add(item.get("department", "GENERAL"))
    
    # Sort and take top n_departments by frequency
    dept_counts = {}
    for s in stores:
        for item in s.get("stock_profile", []):
            d = item.get("department", "GENERAL")
            dept_counts[d] = dept_counts.get(d, 0) + 1
    
    top_depts = sorted(dept_counts.keys(), key=lambda x: -dept_counts[x])[:n_departments]
    dept_to_idx = {d: i for i, d in enumerate(top_depts)}
    
    # Build labels
    labels = torch.zeros(len(stores), n_departments)
    
    for si, store in enumerate(stores):
        dept_demand = [0.0] * n_departments
        
        for item in store.get("stock_profile", []):
            dept = item.get("department", "GENERAL")
            ads = item.get("ads_scaled", 0)
            
            if dept in dept_to_idx:
                dept_demand[dept_to_idx[dept]] += ads
        
        # Normalize to distribution
        total = sum(dept_demand)
        if total > 0:
            labels[si] = torch.tensor([d / total for d in dept_demand])
        else:
            # Uniform if no data
            labels[si] = torch.ones(n_departments) / n_departments
    
    return labels, top_depts


def generate_risk_labels(stores: List[dict]) -> torch.Tensor:
    """
    Generate risk vulnerability labels based on store attributes.
    
    Risk formula (heuristic):
      - Low supplier diversity -> higher risk
      - Smaller stores -> higher risk (less buffer capacity)
      - Express/Neighborhood -> higher risk (thin stock)
      - Low footfall stores -> higher risk (less demand predictability)
    
    Returns:
        [N, 1] tensor of risk scores (0=safe, 1=vulnerable)
    """
    risks = []
    
    for store in stores:
        # Component scores (each 0-1, higher = more risk)
        supplier_risk = 1.0 - store.get("supplier_diversity_score", 0.5)
        
        budget = store.get("monthly_budget", 1_000_000)
        size_risk = 1.0 / (1.0 + math.log10(max(budget, 1)) / 8)
        
        category = store.get("store_category", "Medium Anchor")
        category_risk = 0.7 if "Express" in category else 0.3
        
        footfall = store.get("footfall_rank", 5.0)
        footfall_risk = 1.0 - (footfall / 10.0)
        
        # Weighted combination
        risk = (
            0.35 * supplier_risk +
            0.25 * size_risk +
            0.20 * category_risk +
            0.20 * footfall_risk
        )
        
        risks.append(min(1.0, max(0.0, risk)))
    
    return torch.tensor(risks, dtype=torch.float).unsqueeze(1)


def generate_transfer_labels(stores: List[dict]) -> torch.Tensor:
    """
    Generate pairwise transfer affinity labels.
    
    Transfer is beneficial when:
      - Stores are in the same cluster (logistics feasible)
      - One store has excess stock capacity, the other is constrained
      - Similar product mix (shared departments)
    
    Returns:
        [N, N] tensor of transfer scores
    """
    n = len(stores)
    labels = torch.zeros(n, n)
    
    for i in range(n):
        for j in range(n):
            if i == j:
                continue
            
            # Same cluster bonus
            same_cluster = stores[i].get("region", "") == stores[j].get("region", "")
            cluster_score = 0.4 if same_cluster else 0.1
            
            # Size differential (larger -> smaller = more feasible)
            budget_i = stores[i].get("monthly_budget", 1e6)
            budget_j = stores[j].get("monthly_budget", 1e6)
            size_diff = max(0, math.log10(budget_i / max(budget_j, 1))) / 3
            
            # Product overlap (simplified: both have stock profiles)
            depts_i = set(item.get("department", "") for item in stores[i].get("stock_profile", []))
            depts_j = set(item.get("department", "") for item in stores[j].get("stock_profile", []))
            
            if depts_i and depts_j:
                overlap = len(depts_i & depts_j) / max(len(depts_i | depts_j), 1)
            else:
                overlap = 0.3  # Default moderate overlap
            
            score = 0.3 * cluster_score + 0.3 * size_diff + 0.4 * overlap
            labels[i, j] = min(1.0, max(0.0, score))
    
    return labels


# =============================================================================
# Training Loop
# =============================================================================

def train(network_path: str = "stores_network.json",
          epochs: int = 100,
          lr: float = 0.01,
          hidden_dim: int = 64,
          embed_dim: int = 32,
          n_departments: int = 20,
          save_checkpoint: bool = True):
    """
    Full training pipeline.
    
    1. Load store network
    2. Build graph (features + edges)
    3. Generate training labels
    4. Train GNN
    5. Save checkpoint
    """
    print("=" * 60)
    print("  STORE GNN TRAINING")
    print("=" * 60)
    
    # 1. Load network
    with open(network_path, "r") as f:
        data = json.load(f)
    stores = data["stores"]
    
    # 2. Build graph
    node_features, edge_index, edge_weights, store_ids = build_graph_from_network(network_path)
    n_features = node_features.shape[1]
    
    # 3. Generate labels
    print("\nGenerating training labels...")
    demand_labels, dept_names = generate_demand_labels(stores, n_departments)
    risk_labels = generate_risk_labels(stores)
    transfer_labels = generate_transfer_labels(stores)
    
    print(f"  Demand labels shape:   {demand_labels.shape}")
    print(f"  Risk labels shape:     {risk_labels.shape}")
    print(f"  Transfer labels shape: {transfer_labels.shape}")
    print(f"  Top departments: {dept_names[:5]}...")
    
    # 4. Create model
    model = StoreGraphNetwork(
        in_features=n_features,
        hidden_dim=hidden_dim,
        embed_dim=embed_dim,
        n_departments=n_departments,
    )
    
    optimizer = optim.Adam(model.parameters(), lr=lr, weight_decay=1e-4)
    scheduler = optim.lr_scheduler.StepLR(optimizer, step_size=50, gamma=0.5)
    
    # Loss functions
    demand_loss_fn = nn.KLDivLoss(reduction='batchmean')  # KL-divergence for distributions
    risk_loss_fn = nn.MSELoss()
    transfer_loss_fn = nn.MSELoss()
    
    # 5. Train
    print(f"\nTraining for {epochs} epochs (lr={lr})...")
    print("-" * 60)
    
    model.train()
    best_loss = float('inf')
    
    for epoch in range(1, epochs + 1):
        optimizer.zero_grad()
        
        # Forward
        outputs = model(node_features, edge_index, edge_weights)
        
        # Demand loss: KL-div between predicted and actual dept distribution
        # KLDivLoss expects log-probabilities as input
        demand_pred_log = torch.log(outputs['demand'] + 1e-8)
        loss_demand = demand_loss_fn(demand_pred_log, demand_labels)
        
        # Risk loss: MSE between predicted and heuristic risk scores
        loss_risk = risk_loss_fn(outputs['risk'], risk_labels)
        
        # Transfer loss: MSE on pairwise scores
        embeddings = outputs['embeddings']
        pred_transfers = model.get_all_transfer_scores(embeddings)
        loss_transfer = transfer_loss_fn(pred_transfers, transfer_labels)
        
        # Combined loss (weighted)
        total_loss = 1.0 * loss_demand + 0.5 * loss_risk + 0.3 * loss_transfer
        
        # Backward
        total_loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()
        scheduler.step()
        
        # Logging
        if epoch % 10 == 0 or epoch == 1:
            print(f"  Epoch {epoch:4d} | Loss: {total_loss:.4f} "
                  f"(Demand: {loss_demand:.4f}, Risk: {loss_risk:.4f}, "
                  f"Transfer: {loss_transfer:.4f})")
        
        if total_loss.item() < best_loss:
            best_loss = total_loss.item()
    
    # 6. Save checkpoint
    if save_checkpoint:
        checkpoint_dir = os.path.dirname(os.path.abspath(__file__))
        checkpoint_path = os.path.join(checkpoint_dir, "store_gnn_checkpoint.pt")
        
        torch.save({
            'model_state_dict': model.state_dict(),
            'optimizer_state_dict': optimizer.state_dict(),
            'epoch': epochs,
            'best_loss': best_loss,
            'n_features': n_features,
            'hidden_dim': hidden_dim,
            'embed_dim': embed_dim,
            'n_departments': n_departments,
            'store_ids': store_ids,
            'dept_names': dept_names,
        }, checkpoint_path)
        print(f"\n[OK] Checkpoint saved to: {checkpoint_path}")
    
    # 7. Final evaluation
    print(f"\n{'='*60}")
    print(f"  TRAINING COMPLETE")
    print(f"{'='*60}")
    
    model.eval()
    with torch.no_grad():
        final_outputs = model(node_features, edge_index, edge_weights)
    
    # Show final predictions
    print(f"\n  Final Risk Scores (trained):")
    risks = final_outputs['risk'].squeeze()
    for sid, risk_val in sorted(zip(store_ids, risks.tolist()), key=lambda x: -x[1]):
        bar = "#" * int(risk_val * 30)
        print(f"    {sid}: {risk_val:.3f} |{bar}|")
    
    print(f"\n  Final Demand Distributions (top dept per store):")
    demand = final_outputs['demand']
    for si, sid in enumerate(store_ids):
        top_dept_idx = demand[si].argmax().item()
        top_dept = dept_names[top_dept_idx] if top_dept_idx < len(dept_names) else f"Dept-{top_dept_idx}"
        top_pct = demand[si, top_dept_idx].item() * 100
        print(f"    {sid}: {top_dept:<30} ({top_pct:.1f}%)")
    
    print(f"\n  Best Loss: {best_loss:.4f}")
    print(f"{'='*60}")
    
    return model, final_outputs


# =============================================================================
# CLI
# =============================================================================

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Train Store GNN")
    parser.add_argument("--network", default="stores_network.json", help="Store network JSON path")
    parser.add_argument("--epochs", type=int, default=100, help="Training epochs")
    parser.add_argument("--lr", type=float, default=0.01, help="Learning rate")
    parser.add_argument("--hidden", type=int, default=64, help="Hidden layer dimension")
    parser.add_argument("--embed", type=int, default=32, help="Embedding dimension")
    parser.add_argument("--depts", type=int, default=20, help="Number of department bins")
    
    args = parser.parse_args()
    
    train(
        network_path=args.network,
        epochs=args.epochs,
        lr=args.lr,
        hidden_dim=args.hidden,
        embed_dim=args.embed,
        n_departments=args.depts,
    )
