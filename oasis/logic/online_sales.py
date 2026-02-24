import logging
import pandas as pd
from typing import List, Dict, Any

logger = logging.getLogger("OasisOnlineSales")

class OnlineSalesIngestor:
    """
    Handles ingestion and normalization of Online Sales data (Shopify, WooCommerce, etc.).
    """
    def __init__(self):
        pass

    def ingest_shopify_csv(self, file_path: str) -> List[Dict[str, Any]]:
        """
        Parses Shopify Order Export CSV.
        """
        try:
            df = pd.read_csv(file_path)
            # Normalize to Oasis Standard
            sales = []
            for _, row in df.iterrows():
                # Filter out null lines or financial transactions without items
                if pd.isna(row.get('Lineitem quantity')):
                    continue
                    
                sale = {
                    "source": "SHOPIFY",
                    "order_id": row.get('Name'),
                    "timestamp": row.get('Created at'),
                    "sku": row.get('Lineitem sku'),
                    "product_name": row.get('Lineitem name'),
                    "qty": row.get('Lineitem quantity'),
                    "price": row.get('Lineitem price'),
                    "customer_id": row.get('Email')
                }
                sales.append(sale)
            logger.info(f"Ingested {len(sales)} online transactions.")
            return sales
        except Exception as e:
            logger.error(f"Shopify Ingestion Failed: {e}")
            return []

    def analyze_channel_performance(self, online_sales: List[Dict], store_sales: List[Dict]) -> Dict[str, Any]:
        """
        Compares Online vs Physical performance.
        Returns aggregate stats.
        """
        online_df = pd.DataFrame(online_sales)
        store_df = pd.DataFrame(store_sales)
        
        report = {
            "online_total_revenue": 0,
            "store_total_revenue": 0,
            "online_top_items": [],
            "channel_mix_pct": 0
        }
        
        if not online_df.empty:
            report["online_total_revenue"] = (online_df['qty'] * online_df['price']).sum()
            top = online_df.groupby('product_name')['qty'].sum().sort_values(ascending=False).head(5)
            report["online_top_items"] = top.to_dict()
            
        if not store_df.empty:
             report["store_total_revenue"] = (store_df['qty'] * store_df['price']).sum()
             
        total = report["online_total_revenue"] + report["store_total_revenue"]
        if total > 0:
            report["channel_mix_pct"] = round((report["online_total_revenue"] / total) * 100, 1)
            
        return report
