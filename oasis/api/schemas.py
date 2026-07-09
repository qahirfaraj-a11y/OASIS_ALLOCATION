from pydantic import BaseModel
from typing import List, Dict, Any
from datetime import datetime

class DatabaseConnectionRequest(BaseModel):
    driver: str  # postgresql, mssql, etc.
    host: str
    port: int
    database: str
    username: str
    password: str
    
class SalesTransaction(BaseModel):
    transaction_id: str
    timestamp: datetime
    items: List[Dict[str, Any]]
    store_id: str

class OrderRecommendation(BaseModel):
    sku: str
    product_name: str
    current_stock: float
    recommended_qty: float
    reasoning: str
    priority: str = "normal"

class Alert(BaseModel):
    type: str # VELOCITY_SPIKE, STOCKOUT_RISK
    severity: str # high, medium, low
    product_name: str
    message: str
    timestamp: datetime
    recommended_action: str

class OnlineSalesStats(BaseModel):
    online_total_revenue: float
    store_total_revenue: float
    channel_mix_pct: float
    online_top_items: Dict[str, float]
