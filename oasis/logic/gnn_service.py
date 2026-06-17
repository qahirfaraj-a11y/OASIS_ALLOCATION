"""
Shared store-risk service (SH-A).

Single source of store-risk intelligence for every O.A.S.I.S. surface. The GNN
is an *enhancement, not a dependency*: this module returns usable risk whether or
not torch and the trained checkpoint are present.

    store_risk(stock_by_org, ratio=0.5) -> {org_cd: risk 0..1}
        = sigmoidal_blend( gnn_risk (if model trained else inventory only),
                           inventory_risk )

- torch + a *trained* checkpoint present -> GNN-enhanced blended risk.
- torch missing, checkpoint missing, or model untrained (dim mismatch / random)
  -> inventory-only risk (no hard torch dependency, no silently-confident
  untrained output).

`model_status()` -> ``trained`` | ``untrained`` | ``unavailable`` lets every
surface show the right banner (the honest MI-B banner, platform-wide).

The pure pieces (`inventory_risk`, `blend_risk`, `stockout_ratios`) carry no
torch dependency and are unit tested directly. The torch path is lazy-imported
and guarded; when the checkpoint or network JSON is absent it degrades to the
pure inventory path. Logic extracted verbatim from the Command Center's
``get_all_store_risks`` (ops_dashboard.py) so S2 delegation is behaviour-
preserving.
"""

from __future__ import annotations

import os
from typing import Dict, Sequence, Tuple

# GNN blend default — matches the Command Center's gnn_risk_blend_ratio default.
DEFAULT_BLEND_RATIO = 0.5
# Below this spread the GNN's per-store risk is treated as uniform (untrained /
# out-of-distribution) and we defer entirely to the inventory heuristic.
_UNIFORM_EPS = 0.02

# Feature-matrix columns the Command Center injects live inventory signal into.
_COL_STOCKOUT = 24
_COL_CRITICAL = 25


# =============================================================================
# Pure heuristic (no torch)
# =============================================================================

def stockout_ratios(products: Sequence[dict]) -> Tuple[float, float]:
    """(stockout_ratio, critical_ratio) for one store's product list (pure).

    Mirrors get_all_store_risks (ops_dashboard.py) exactly:
      * denominator = active SKUs (ADS>0 or on-hand>0), floored at 1
      * on-hand < 1.0  -> stocked out (raw stock carries tiny decimals)
      * days-cover < 0.5 -> stockout; < 3.0 -> critical
      * ADS=0 but depleted -> stockout
    """
    products = products or []
    if not products:
        return 0.0, 0.0

    active = 0
    for it in products:
        ads = float(it.get("avg_daily_sales", 0) or 0)
        curr = float(it.get("current_stocks", it.get("current_stock", 0)) or 0)
        if ads > 0 or curr > 0:
            active += 1
    denom = max(1, active)

    so_count = 0
    crit_count = 0
    for it in products:
        curr = float(it.get("current_stocks", it.get("current_stock", 0)) or 0)
        ads = float(it.get("avg_daily_sales", 0) or 0)
        if ads > 0:
            days_cov = curr / ads
            if curr < 1.0 or days_cov < 0.5:
                so_count += 1
            elif days_cov < 3.0:
                crit_count += 1
        elif curr < 1.0:
            so_count += 1
    return so_count / denom, crit_count / denom


def inventory_risk(products: Sequence[dict]) -> float:
    """Inventory-only store risk 0..1 (pure).

    Stockout is weighted worse than critical: ``so*1.5 + crit*0.5`` capped at 1.
    """
    so_ratio, crit_ratio = stockout_ratios(products)
    return min(1.0, (so_ratio * 1.5) + (crit_ratio * 0.5))


def blend_risk(gnn_score: float, inv_risk: float,
               ratio: float = DEFAULT_BLEND_RATIO,
               gnn_uniform: bool = False) -> float:
    """Sigmoidal-brake blend of GNN and inventory risk (pure).

    The brake reduces the GNN's weight as inventory risk rises, so a clear
    on-the-ground stockout always dominates a model opinion. When the GNN is
    uniform (untrained / OOD) its weight is zero and we return inventory risk.
    """
    inv_risk = max(0.0, min(1.0, float(inv_risk)))
    if gnn_uniform:
        alpha = 0.0
    else:
        alpha = float(ratio) * (1.0 - (inv_risk ** 2))
    return (float(gnn_score) * alpha) + (inv_risk * (1.0 - alpha))


# =============================================================================
# Guarded model loader (lazy torch)
# =============================================================================

# Memoized (model, sim, status). status in {trained, untrained, unavailable}.
_MODEL_CACHE: dict = {}


def _network_path() -> str:
    return os.path.join(os.getcwd(), "stores_network.json")


def _checkpoint_path() -> str:
    return os.path.join(os.getcwd(), "st_gat_v2.pt")


def _try_load() -> Tuple[object, object, str]:
    """Load (model, sim, status) with graceful degradation. No exceptions escape."""
    try:
        import torch  # lazy: consoles without torch never pay this
    except Exception:
        return None, None, "unavailable"

    net = _network_path()
    if not os.path.exists(net):
        return None, None, "unavailable"

    try:
        from network_simulation import NetworkSimulator
        from models.store_gnn import StoreGraphNetwork

        sim = NetworkSimulator(net)
        f_in = sim.get_feature_matrix().shape[1]
        model = StoreGraphNetwork(in_features=f_in)

        ckpt = _checkpoint_path()
        if not os.path.exists(ckpt):
            return model, sim, "untrained"  # usable graph, random weights

        sd = torch.load(ckpt, map_location="cpu")
        state_dict = sd.get("model_state_dict", sd)

        # Same dimension guard as st_gat / the Command Center: a strict=False
        # load would silently drop conv1 on a mismatch, so verify the input
        # layer before declaring the model trained.
        ck_in = state_dict.get("conv1.weight")
        if ck_in is not None and ck_in.shape[0] == f_in:
            missing, _ = model.load_state_dict(state_dict, strict=False)
            core_missing = [k for k in missing if k.startswith(("conv1", "conv2"))]
            status = "untrained" if core_missing else "trained"
        else:
            status = "untrained"
        model.eval()
        return model, sim, status
    except Exception:
        return None, None, "unavailable"


def _load_model(force: bool = False) -> Tuple[object, object, str]:
    """Cached guarded loader. Pass force=True to invalidate (mainly for tests)."""
    if force or "result" not in _MODEL_CACHE:
        _MODEL_CACHE["result"] = _try_load()
    return _MODEL_CACHE["result"]


def model_status() -> str:
    """``trained`` | ``untrained`` | ``unavailable`` — drives the honest banner."""
    return _load_model()[2]


# =============================================================================
# Public API
# =============================================================================

def store_risk(stock_by_org: Dict[str, Sequence[dict]],
               ratio: float = DEFAULT_BLEND_RATIO) -> Dict[str, float]:
    """Per-store blended risk 0..1.

    GNN-enhanced when a trained model is available, inventory-only otherwise.
    The org keys of the returned dict are exactly the keys of ``stock_by_org``;
    stores the GNN graph doesn't know about still get an inventory-only score.
    """
    stock_by_org = stock_by_org or {}
    inv = {org: inventory_risk(prods) for org, prods in stock_by_org.items()}

    model, sim, status = _load_model()
    if status != "trained" or model is None or sim is None:
        return dict(inv)  # graceful: inventory-only

    try:
        import torch

        x_t = sim.get_feature_matrix()
        order = []
        for i, src in enumerate(getattr(sim, "stores_data", [])):
            org_cd = str(src.get("store_id", "")).replace("CFP-", "ORG")
            order.append(org_cd)
            so_ratio, crit_ratio = stockout_ratios(stock_by_org.get(org_cd, []))
            x_t[i, _COL_STOCKOUT] = so_ratio
            x_t[i, _COL_CRITICAL] = crit_ratio

        with torch.no_grad():
            out = model(x_t, sim.edge_index)
        risks = out["risk"].squeeze().tolist()
        if not isinstance(risks, list):
            risks = [risks]

        gnn_uniform = (len(risks) <= 1) or ((max(risks) - min(risks)) < _UNIFORM_EPS)

        result = dict(inv)
        for i, org_cd in enumerate(order):
            if i >= len(risks):
                break
            # Only score stores the caller actually asked about; a store in the
            # GNN graph but absent from stock_by_org keeps no entry (contract:
            # result keys == stock_by_org keys).
            if org_cd not in inv:
                continue
            so = float(x_t[i, _COL_STOCKOUT])
            crit = float(x_t[i, _COL_CRITICAL])
            inv_r = min(1.0, (so * 1.5) + (crit * 0.5))
            result[org_cd] = blend_risk(risks[i], inv_r, ratio, gnn_uniform)
        return result
    except Exception:
        return dict(inv)  # any inference failure -> inventory-only
