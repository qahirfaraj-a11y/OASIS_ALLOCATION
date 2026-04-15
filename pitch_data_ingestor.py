import pandas as pd
import logging
import os
import re

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PitchIngestor")

class ProspectDataIngestor:
    def __init__(self, file_path: str):
        self.file_path = file_path
        self.raw_data = None
        self.clean_data = []

    def load(self):
        logger.info(f"Loading prospect data from {self.file_path}")
        self.raw_data = pd.read_csv(self.file_path)
        logger.info(f"Loaded {len(self.raw_data)} rows.")

    def _normalize_string(self, text: str) -> str:
        if pd.isna(text): return "UNKNOWN"
        # Basic cleanup: uppercase, strip, remove double spaces
        clean = str(text).upper().strip()
        clean = re.sub(r'\s+', ' ', clean)
        return clean

    def sanitize(self, days_in_period=30):
        if self.raw_data is None:
            self.load()

        logger.info("Sanitizing and normalizing data to O.A.S.I.S. standards...")
        for _, row in self.raw_data.iterrows():
            # Assume strict column mappings for now (could be dynamic later)
            raw_name = row.get('Item_Description', 'Unknown Item')
            raw_dept = row.get('Category', 'General')
            raw_supp = row.get('Supplier_Vendor', 'Unknown')
            
            p_name = self._normalize_string(raw_name)
            p_dept = self._normalize_string(raw_dept)
            
            # Remove silly trailing things the mock added
            p_name = p_name.replace(" - OLD", "")
            p_dept = p_dept.replace(" (LOCAL)", "")

            cost = float(row.get('Cost_Price_KES', 0.0))
            price = float(row.get('Retail_Price_KES', cost * 1.2)) # Fallback margin
            soh = int(row.get('Stock_On_Hand_Qty', 0))
            qty_sold = int(row.get('Qty_Sold_Last_30_Days', 0))

            ads = qty_sold / float(days_in_period)

            # Reconstruct into OASIS format
            self.clean_data.append({
                'product_name': p_name,
                'department': p_dept,
                'supplier_name': raw_supp,
                'unit_cost': cost,
                'unit_price': price,
                'current_stock': soh,
                'avg_daily_sales': ads,
                'qty_sold_period': qty_sold,
                'barcode': str(row.get('Barcode', ''))
            })
            
        logger.info(f"Successfully sanitized {len(self.clean_data)} items.")
        return self.clean_data

    def run_diagnostic_audit(self):
        """Phase 2: Sweep the normalized data for dead stock and stockouts."""
        if not self.clean_data:
            logger.warning("No clean data. Run sanitize() first.")
            return {}

        total_capital_tied = 0.0
        dead_stock_value = 0.0
        dead_stock_items = []
        
        lost_revenue_30d = 0.0
        stockout_items = []
        
        total_historical_revenue = 0.0

        for item in self.clean_data:
            capital_in_item = item['current_stock'] * item['unit_cost']
            total_capital_tied += capital_in_item
            total_historical_revenue += item['qty_sold_period'] * item['unit_price']

            # 1. Dead Stock (AMIT Target)
            # Low velocity (< 0.1 / day) but significant capital tied up
            if item['avg_daily_sales'] < 0.1 and item['current_stock'] > 5:
                dead_stock_value += capital_in_item
                dead_stock_items.append(item)

            # 2. Stockouts (DHARAM Ghost Demand Target)
            # High velocity (> 0.5 / day) but 0 stock on hand
            if item['current_stock'] == 0 and item['avg_daily_sales'] >= 0.5:
                # Estimate lost revenue for 14 days (typical out of stock duration without system)
                lost_rev = item['avg_daily_sales'] * 14 * item['unit_price']
                lost_revenue_30d += lost_rev
                stockout_items.append(item)

        diagnostic = {
            'total_items': len(self.clean_data),
            'total_capital_tied': total_capital_tied,
            'historical_revenue_30d': total_historical_revenue,
            'dead_stock_value': dead_stock_value,
            'dead_stock_count': len(dead_stock_items),
            'lost_revenue_opportunity': lost_revenue_30d,
            'stockout_count': len(stockout_items),
            'dead_stock_list': sorted(dead_stock_items, key=lambda x: x['current_stock']*x['unit_cost'], reverse=True),
            'stockout_list': sorted(stockout_items, key=lambda x: x['avg_daily_sales']*14*x['unit_price'], reverse=True)
        }
        
        logger.info("--- Diagnostic Audit Complete ---")
        logger.info(f"Total Capital Tied: ${total_capital_tied:,.2f}")
        logger.info(f"Dead Stock Value: ${dead_stock_value:,.2f} ({len(dead_stock_items)} items)")
        logger.info(f"Lost Revenue (est): ${lost_revenue_30d:,.2f} ({len(stockout_items)} fast-movers out of stock)")
        return diagnostic

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, "raw_prospect_data.csv")
    
    ingestor = ProspectDataIngestor(file_path)
    clean_data = ingestor.sanitize()
    results = ingestor.run_diagnostic_audit()
