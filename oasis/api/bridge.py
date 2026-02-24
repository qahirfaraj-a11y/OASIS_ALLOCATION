from fastapi import FastAPI, HTTPException, Body
from typing import List
import logging
import os
import json
from datetime import datetime

# Import Logic
from ..logic.db_connector import UniversalConnector
from ..logic.alert_monitor import AlertMonitor
from ..logic.online_sales import OnlineSalesIngestor
from .schemas import (
    DatabaseConnectionRequest, 
    SalesTransaction, 
    OrderRecommendation, 
    Alert, 
    OnlineSalesStats
)

# Setup Logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OasisBridge")

app = FastAPI(title="Oasis Manager Bridge", version="2.0")

# --- DATA DIRECTORY ---
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))

# --- GLOBAL STATE ---
active_connector = None
alert_monitor = AlertMonitor(spike_threshold_pct=300.0)
online_ingestor = OnlineSalesIngestor()

# --- LOAD REAL INTELLIGENCE DATA ---
# v2.0: Replace hardcoded mock data with actual intelligence JSON
historical_stats: dict = {}
_sales_intel_path = os.path.join(DATA_DIR, "sales_intelligence_2025.json")
if os.path.exists(_sales_intel_path):
    try:
        with open(_sales_intel_path, 'r', encoding='utf-8') as f:
            _raw_intel = json.load(f)
        # Normalize into AlertMonitor format: {product_id: {avg_daily_sales, product_name}}
        for key, data in _raw_intel.items():
            if isinstance(data, dict):
                historical_stats[key] = {
                    'avg_daily_sales': data.get('avg_daily_sales', data.get('total_qty', 0) / 90),
                    'product_name': data.get('product_name', key),
                }
        logger.info(f"Loaded {len(historical_stats)} items from sales intelligence for alert monitoring.")
    except Exception as e:
        logger.warning(f"Failed to load sales intelligence: {e}. Alert monitor will use empty history.")
else:
    logger.warning(f"Sales intelligence file not found at {_sales_intel_path}. Alert monitor will use empty history.")

# In-memory state
in_memory_alerts = []

# v2.0: Pending orders loaded from last engine run (if available)
pending_orders = []
_last_results_path = os.path.join(DATA_DIR, "outputs")
if os.path.exists(_last_results_path):
    # Try to find the most recent analysis output
    try:
        result_files = [f for f in os.listdir(_last_results_path) if f.endswith('.json')]
        if result_files:
            latest = max(result_files, key=lambda f: os.path.getmtime(os.path.join(_last_results_path, f)))
            with open(os.path.join(_last_results_path, latest), 'r', encoding='utf-8') as f:
                raw_orders = json.load(f)
            pending_orders = [
                {
                    'sku': o.get('item_code', o.get('sku', '')),
                    'product_name': o.get('product_name', 'Unknown'),
                    'current_stock': o.get('current_stock', 0),
                    'recommended_qty': o.get('recommended_quantity', 0),
                    'reasoning': o.get('reasoning', ''),
                    'priority': 'high' if o.get('recommended_quantity', 0) > 0 else 'low',
                }
                for o in raw_orders if o.get('recommended_quantity', 0) > 0
            ]
            logger.info(f"Loaded {len(pending_orders)} pending orders from last engine run ({latest}).")
    except Exception as e:
        logger.warning(f"Could not load previous engine results: {e}")


@app.get("/")
def health_check():
    return {"status": "Bridge Online", "time": datetime.now(), "intelligence_loaded": len(historical_stats)}

# --- 1. ERP CONNECTIVITY ---

@app.post("/connect", response_model=dict)
def connect_database(creds: DatabaseConnectionRequest):
    """
    Attempts to connect to an external ERP/POS Database.
    """
    global active_connector
    try:
        uri = f"{creds.driver}://{creds.username}:{creds.password}@{creds.host}:{creds.port}/{creds.database}"
        connector = UniversalConnector(uri)
        if connector.test_connection():
            active_connector = connector
            return {"status": "success", "message": f"Connected to {creds.database}"}
        else:
            raise HTTPException(status_code=400, detail="Connection Failed")
    except Exception as e:
        logger.error(f"Connect Logic Failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- 2. MANAGER WORKFLOW (Orders) ---

@app.get("/orders/review", response_model=List[OrderRecommendation])
def get_orders_for_review():
    """
    Returns list of orders waiting for Manager approval.
    v2.0: Now populated from actual engine output instead of hardcoded mock data.
    """
    return pending_orders

@app.post("/orders/approve/{sku}")
def approve_order(sku: str):
    """
    Simulates approval. In real life, writes to ERP.
    """
    global pending_orders
    order = next((o for o in pending_orders if o['sku'] == sku), None)
    if order:
        pending_orders = [o for o in pending_orders if o['sku'] != sku]
        # TODO: active_connector.push_purchase_order(order)
        return {"status": "approved", "sku": sku}
    raise HTTPException(status_code=404, detail="Order not found")

# --- 3. ALERTS & MONITORING ---

@app.post("/ingest/sales")
def ingest_sales_stream(batch: List[dict]):
    """
    Receives real-time sales transactions from POS.
    Checks for Velocity Spikes using real historical intelligence.
    v2.0: Uses loaded sales_intelligence_2025.json instead of hardcoded mock.
    """
    global in_memory_alerts
    
    new_alerts = alert_monitor.check_velocity_spikes(batch, historical_stats)
    in_memory_alerts.extend(new_alerts)
    
    return {"status": "processed", "alerts_generated": len(new_alerts), "history_size": len(historical_stats)}

@app.get("/alerts", response_model=List[dict])
def get_active_alerts():
    return in_memory_alerts

@app.delete("/alerts")
def clear_alerts():
    """Clear all resolved alerts."""
    global in_memory_alerts
    count = len(in_memory_alerts)
    in_memory_alerts = []
    return {"status": "cleared", "cleared_count": count}

# --- 4. ONLINE SALES ---

@app.get("/analysis/online-mix", response_model=OnlineSalesStats)
def get_channel_mix():
    """
    Returns comparison of Online vs In-Store performance.
    """
    # Dummy Data for Visualization
    online_transactions = [{"product_name": "Premium Coffee", "qty": 10, "price": 500}]
    store_transactions = [{"product_name": "Premium Coffee", "qty": 50, "price": 500}]
    
    report = online_ingestor.analyze_channel_performance(online_transactions, store_transactions)
    return report

# --- 5. POS/ERP INTEGRATION ---

@app.post("/erp/sync/{org_cd}")
def sync_from_erp(org_cd: str):
    """
    Pull latest data from POS/ERP database and load into OrderEngine.
    Works with both SQLite mock and live SQL Server.
    """
    if not active_connector:
        raise HTTPException(status_code=400, detail="No active database connection. Call /connect first.")
    
    try:
        from ..logic.order_engine import OrderEngine
        engine = OrderEngine(DATA_DIR)
        engine.load_from_erp(active_connector, org_cd)
        products = engine.load_erp_products(org_cd)
        
        return {
            "status": "synced",
            "org_cd": org_cd,
            "products_loaded": len(products),
            "databases_loaded": [k for k, v in engine.databases.items() if v],
        }
    except Exception as e:
        logger.error(f"ERP sync failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/erp/stock/{org_cd}")
def erp_stock_check(org_cd: str):
    """Real-time stock levels from POS/ERP."""
    if not active_connector:
        raise HTTPException(status_code=400, detail="No active database connection.")
    
    try:
        from ..logic.pos_erp_adapter import PosErpAdapter
        adapter = PosErpAdapter(active_connector)
        snapshot = adapter.fetch_stock_snapshot(org_cd)
        
        # Add summary stats
        total_items = len(snapshot)
        stockout_items = sum(1 for s in snapshot if float(s.get("current_stocks", 0)) <= 0)
        
        return {
            "org_cd": org_cd,
            "total_items": total_items,
            "stockout_count": stockout_items,
            "stockout_pct": round(stockout_items / total_items * 100, 1) if total_items > 0 else 0,
            "items": snapshot[:50],  # Return first 50 (sorted by lowest stock)
        }
    except Exception as e:
        logger.error(f"Stock check failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/erp/sales/{org_cd}")
def erp_sales_history(org_cd: str, days: int = 30):
    """Sales history from POS/ERP for the last N days."""
    if not active_connector:
        raise HTTPException(status_code=400, detail="No active database connection.")
    
    try:
        from ..logic.pos_erp_adapter import PosErpAdapter
        adapter = PosErpAdapter(active_connector)
        df = adapter.fetch_sales_history(org_cd, days=days)
        
        # Summary instead of raw dump
        summary = {
            "org_cd": org_cd,
            "days": days,
            "total_transactions": len(df),
            "total_revenue": round(float(df["net_amt"].sum()), 2) if not df.empty else 0,
            "total_qty": round(float(df["qty"].sum()), 1) if not df.empty else 0,
            "unique_products": int(df["itm_cd"].nunique()) if not df.empty else 0,
        }
        
        # Top 10 products by qty
        if not df.empty:
            top10 = df.groupby(["itm_cd", "item_name"]).agg(
                total_qty=("qty", "sum"),
                total_revenue=("net_amt", "sum"),
            ).sort_values("total_qty", ascending=False).head(10).reset_index()
            summary["top_products"] = top10.to_dict(orient="records")
        
        return summary
    except Exception as e:
        logger.error(f"Sales history failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/erp/push-po/{org_cd}")
def push_purchase_order_to_erp(org_cd: str, recommendations: List[dict] = Body(...)):
    """Push OASIS-generated purchase orders back to the POS/ERP."""
    if not active_connector:
        raise HTTPException(status_code=400, detail="No active database connection.")
    
    try:
        from ..logic.pos_erp_adapter import PosErpAdapter
        adapter = PosErpAdapter(active_connector)
        lines_written = adapter.push_purchase_order(org_cd, recommendations)
        
        return {
            "status": "pushed",
            "org_cd": org_cd,
            "po_lines_written": lines_written,
        }
    except Exception as e:
        logger.error(f"PO push failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/erp/orgs")
def list_erp_organizations():
    """List all organizations in the POS/ERP system."""
    if not active_connector:
        raise HTTPException(status_code=400, detail="No active database connection.")
    
    try:
        from ..logic.pos_erp_adapter import PosErpAdapter
        adapter = PosErpAdapter(active_connector)
        orgs = adapter.fetch_all_organizations()
        return {"organizations": orgs, "count": len(orgs)}
    except Exception as e:
        logger.error(f"Org list failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

