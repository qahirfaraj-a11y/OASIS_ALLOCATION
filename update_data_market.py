import re

with open("oasis/desktop/data.py", "r", encoding="utf-8") as f:
    content = f.read()

new_functions = """

def store_intelligence(org_cd: str, root: Optional[str] = None) -> Dict[str, Any]:
    try:
        from oasis.logic.gnn_service import get_gnn_resources
        model, sim = get_gnn_resources()
        if not model or not sim:
            return {"error": "GNN resources unavailable"}
        
        if not sim.is_hydrated:
            sim.hydrate_simulators()
            
        all_skus = []
        if org_cd == "ALL":
            for s_sim in sim.simulators.values():
                for sku in s_sim.skus.values():
                    all_skus.append({
                        "Product": sku.product_name,
                        "Category": sku.department,
                        "Units": sku.total_sales,
                        "Revenue": sku.total_sales * sku.unit_price,
                        "Stockouts": sku.stockout_days
                    })
        else:
            s_sim = sim.simulators.get(org_cd)
            if s_sim:
                for sku in s_sim.skus.values():
                    all_skus.append({
                        "Product": sku.product_name,
                        "Category": sku.department,
                        "Units": sku.total_sales,
                        "Revenue": sku.total_sales * sku.unit_price,
                        "Stockouts": sku.stockout_days
                    })
        
        if not all_skus:
            return {"top_qty": [], "top_rev": [], "categories": [], "error": None}
            
        import pandas as pd
        df = pd.DataFrame(all_skus)
        if org_cd == "ALL":
            df = df.groupby(["Product", "Category"]).sum().reset_index()
            
        top_qty = df.sort_values("Units", ascending=False).head(15).to_dict("records")
        top_rev = df.sort_values("Revenue", ascending=False).head(15).to_dict("records")
        cat_stats = df.groupby("Category")[["Revenue", "Units"]].sum().reset_index().to_dict("records")
        
        return {"top_qty": top_qty, "top_rev": top_rev, "categories": cat_stats, "error": None}
    except Exception as e:
        return {"error": str(e)[:200]}

def cluster_analysis(root: Optional[str] = None) -> Dict[str, Any]:
    try:
        from oasis.logic.gnn_service import get_gnn_resources
        from oasis.logic.simulation_bridge import get_all_store_risks
        
        gnn_model, gnn_sim = get_gnn_resources()
        if not gnn_model or not gnn_sim:
            return {"error": "GNN resources unavailable"}
            
        import torch
        from sklearn.decomposition import PCA
        from sklearn.cluster import KMeans
        
        x_t = gnn_sim.get_feature_matrix()
        X_np = x_t.cpu().numpy()
        
        pca = PCA(n_components=2)
        components = pca.fit_transform(X_np)
        
        kmeans = KMeans(n_clusters=4, random_state=42)
        clusters = kmeans.fit_predict(X_np)
        
        stores = gnn_sim.stores_data
        risk_scores_map = get_all_store_risks(12)
        
        res = []
        for i, s in enumerate(stores):
            sid = s['store_id']
            risk = risk_scores_map.get(sid, 0.0)
            res.append({
                "Store": sid,
                "Region": s.get('region', 'Unknown'),
                "Cluster": f"Group {clusters[i]}",
                "Risk": round(risk, 2)
            })
            
        return {"clusters": res, "error": None}
    except Exception as e:
        return {"error": str(e)[:200]}

"""

if "def store_intelligence" not in content:
    with open("oasis/desktop/data.py", "a", encoding="utf-8") as f:
        f.write(new_functions)

