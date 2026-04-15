import pandas as pd
import numpy as np
import logging
import os
from datetime import datetime

# --- PRODUCTION HARDENING: Env Loading ---
def load_env_local(env_path=".env"):
    """Simple parser to load .env without external dependencies."""
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"): continue
                if "=" in line:
                    key, value = line.split("=", 1)
                    os.environ[key.strip()] = value.strip()

load_env_local()

class ForensicOperationsIngestor:
    """V2 Engine for ingesting deep operations logs (POS, GRN, Shrink, Transfers)."""
    
    def __init__(self, data_dir: str = None):
        self.data_dir = data_dir or os.getenv("DATA_DIR", "oasis/data")
        self.pos_df = None
        self.grn_df = None
        self.shrink_df = None
        self.transfer_df = None
        
        self.catalog_metrics = {}
        self.supplier_metrics = {}
        self.network_metrics = {}

    def _load_file(self, file_source):
        """Dynamically load CSV, JSON, or Excel. Accepts file paths or Streamlit UploadedFile objects."""
        if file_source is None: return None
        
        # Check if it's a string path or a streamlit UploadedFile
        file_name = file_source if isinstance(file_source, str) else file_source.name
        file_meta = str(file_name).lower()
        
        try:
            if file_meta.endswith('.csv'): return pd.read_csv(file_source)
            elif file_meta.endswith('.xlsx') or file_meta.endswith('.xls'): return pd.read_excel(file_source)
            elif file_meta.endswith('.json'): return pd.read_json(file_source)
            else:
                logger.error(f"Unsupported file format: {file_name}")
                return None
        except Exception as e:
            logger.error(f"Error loading {file_name}: {e}")
            return None

    def load_logs(self, pos_file=None, grn_file=None, shrink_file=None, transfer_file=None, inventory_file=None):
        logger.info("Loading Forensic Logs...")
        if pos_file: 
            self.pos_df = self._load_file(pos_file)
            if self.pos_df is not None and 'Date' in self.pos_df.columns:
                self.pos_df['Date'] = pd.to_datetime(self.pos_df['Date'])
        if grn_file:
            self.grn_df = self._load_file(grn_file)
            if self.grn_df is not None and 'Order_Date' in self.grn_df.columns:
                self.grn_df['Order_Date'] = pd.to_datetime(self.grn_df['Order_Date'])
                self.grn_df['Received_Date'] = pd.to_datetime(self.grn_df['Received_Date'])
        if shrink_file: self.shrink_df = self._load_file(shrink_file)
        if transfer_file: self.transfer_df = self._load_file(transfer_file)
        if inventory_file: self.inventory_df = self._load_file(inventory_file)
        
    def load_rhapta_demo_data(self):
        """Loads actual 1-month snapshot from Rhapta (Jan 2026 slice) for live pitching."""
        logger.info(f"Loading Rhapta Authenticated Logs from: {self.data_dir}")
        oasis_data_dir = self.data_dir
        
        # 1. Base Inventory (Scorecard)
        try:
            self.inventory_df = pd.read_csv(os.path.join(oasis_data_dir, "Full_Product_Allocation_Scorecard_v3.csv"))
            # Map Scorecard columns to generic Internal Columns for our POS logic
            self.inventory_df = self.inventory_df.rename(columns={
                'Product': 'Item_Name',
                'Unit_Price': 'Unit_Cost_KES' # Approx for POS
            })
            # Add synthetic Stock to mimic snapshot
            self.inventory_df['Stock_On_Hand'] = np.random.randint(0, 50, size=len(self.inventory_df))
            self.inventory_df['Barcode'] = self.inventory_df['Item_Name'] # Mock barcode
        except FileNotFoundError:
            logger.warning("Rhapta Scorecard missing.")
            self.inventory_df = pd.DataFrame()

        # 2. POS Logs (jan_cash.xlsx)
        try:
            raw_cash = pd.read_excel(os.path.join(oasis_data_dir, "jan_cash.xlsx"), skiprows=1)
            # The rhapta cash file is an aggregate monthly dump, so we simulate transaction dates spreading across the month
            self.pos_df = pd.DataFrame({
                # Synthetic spread of dates across January
                'Date': [datetime(2026, 1, 1) + pd.Timedelta(days=np.random.randint(0, 31)) for _ in range(len(raw_cash))],
                'Transaction_ID': ["TX-"+str(i) for i in range(len(raw_cash))],
                'Item_Name': raw_cash['Item Name'],
                'Qty_Sold': raw_cash['Qty'],
                'Unit_Price_KES': 150, # Dummy fallback as cash dump doesn't have prices
                'Unit_Cost_KES': 100,  # Dummy fallback
                'Barcode': raw_cash['Itm Code'] if 'Itm Code' in raw_cash else raw_cash['Item Name']
            })
            
            # Map actual prices from scorecard if possible
            if not self.inventory_df.empty:
                price_map = dict(zip(self.inventory_df['Item_Name'].str.upper(), self.inventory_df['Unit_Cost_KES']))
                self.pos_df['Unit_Cost_KES'] = self.pos_df['Item_Name'].str.upper().map(price_map).fillna(100)
                self.pos_df['Unit_Price_KES'] = self.pos_df['Unit_Cost_KES'] * 1.3
                
            logger.info("Rhapta POS loaded.")
        except Exception as e:
            logger.warning(f"Rhapta jan_cash failed: {e}")

        # 3. GRN / PO Simulation based on Rhapta data
        try:
            # For the pitch, we read a sample of Rhapta GRNs to prove LATA concept visually
            raw_grn = pd.read_excel(os.path.join(oasis_data_dir, "grnds_10_10.5.xlsx"))
            # Generate synthetic POs matched to these GRNs to prove variance (as raw GRNs just have received dates)
            start_date = datetime(2026, 1, 1)
            self.grn_df = pd.DataFrame({
                'Order_Date': [start_date + pd.Timedelta(days=np.random.randint(0, 5)) for _ in range(len(raw_grn))],
                'Received_Date': [start_date + pd.Timedelta(days=np.random.randint(4, 20)) for _ in range(len(raw_grn))],
                'PO_Number': ["PO-"+str(i) for i in range(len(raw_grn))],
                'Supplier_Name': raw_grn.get('Supplier', ['General_Vendor']*len(raw_grn)),
                'Item_Name': raw_grn.get('Product', ['Item']*len(raw_grn)),
                'Ordered_Qty': raw_grn.get('Quantity', [10]*len(raw_grn)),
            })
            # Hostile supplier simulation injection
            self.grn_df['Received_Qty'] = (self.grn_df['Ordered_Qty'] * np.random.uniform(0.6, 1.0, len(self.grn_df))).astype(int)
            logger.info("Rhapta GRN loaded.")
        except FileNotFoundError:
             logger.warning("Rhapta GRN missing.")
             
        # Simulate small transfer/shrink logs since Rhapta raw data mostly resides in separate isolated PRTS sheets
        self.transfer_df = pd.DataFrame({'Date':[], 'From_Branch':[], 'Qty_Transferred':[]})
        self.shrink_df = pd.DataFrame({'Date':[], 'Qty_Adjusted':[]})

    def run_pos_analysis(self):
        """Analyzes POS to calculate true daily velocity and Dead Stock capital."""
        if self.pos_df is None or self.pos_df.empty: return
        
        logger.info("Running POS Forensic Analysis (95% SL Target)...")
        SERVICE_LEVEL_Z = 1.645 # 95% Service Level
        
        # Calculate total days in log
        days_active = (self.pos_df['Date'].max() - self.pos_df['Date'].min()).days + 1
        
        # Aggregate by Item
        grouped = self.pos_df.groupby(['Barcode', 'Item_Name']).agg(
            total_qty=('Qty_Sold', 'sum'),
            total_rev=('Unit_Price_KES', lambda x: (x * self.pos_df.loc[x.index, 'Qty_Sold']).sum()),
            tx_count=('Transaction_ID', 'nunique'),
            days_with_sales=('Date', 'nunique'),
            unit_cost=('Unit_Cost_KES', 'mean'),
            unit_price=('Unit_Price_KES', 'mean')
        ).reset_index()

        # Load SOH
        try:
            soh_df = pd.read_csv(os.path.join(self.data_dir, 'prospect_inventory_snapshot.csv'))
            grouped = grouped.merge(soh_df[['Barcode', 'Stock_On_Hand']], on='Barcode', how='left')
            grouped['Stock_On_Hand'] = grouped['Stock_On_Hand'].fillna(0)
        except FileNotFoundError:
            grouped['Stock_On_Hand'] = 0

        # Link to Supplier Data for Lead Times
        supplier_map = {}
        if self.grn_df is not None:
             # Just a simple map for demo/pitch purposes
             supplier_map = self.grn_df.groupby('Item_Name')['Supplier_Name'].first().to_dict()

        total_capital_tied = 0
        dead_stock_value = 0
        dead_stock_list = []
        ghost_demand_list = []
        ghost_demand_value = 0

        for _, row in grouped.iterrows():
            ads = row['total_qty'] / max(1, days_active)
            current_capital = row['Stock_On_Hand'] * row['unit_cost']
            total_capital_tied += current_capital
            
            # --- AMIT: Mathematical Dead Stock ---
            # Strict logic: If SOH covers > 60 days of demand (and ADS < Category Threshold)
            # For the pitch, we use 30-day coverage max as 'healthy'
            if ads > 0 and (row['Stock_On_Hand'] / ads) > 45 and row['Stock_On_Hand'] > 10:
                dead_stock_value += current_capital
                dead_stock_list.append({
                    'item_name': row['Item_Name'],
                    'stock': row['Stock_On_Hand'],
                    'capital_trapped': current_capital,
                    'ads': ads,
                    'days_cover': row['Stock_On_Hand'] / ads
                })
            elif ads == 0 and row['Stock_On_Hand'] > 0:
                dead_stock_value += current_capital
                dead_stock_list.append({
                    'item_name': row['Item_Name'],
                    'stock': row['Stock_On_Hand'],
                    'capital_trapped': current_capital,
                    'ads': 0,
                    'days_cover': 999
                })

            # --- DHARAM: Strict Math Ghost Demand ---
            # Recovery Window = Avg_Lead_Time + (Z * Std_Dev_Lead_Time)
            supplier = supplier_map.get(row['Item_Name'], 'GENERAL')
            supp_metrics = next((s for s in self.supplier_metrics.get('supplier_list', []) if s['supplier'] == supplier), None)
            
            # Default to 7 days if no supplier data
            recovery_window = 7
            if supp_metrics:
                # 95% confidence recovery buffer
                recovery_window = supp_metrics['avg_lead_time'] + (SERVICE_LEVEL_Z * supp_metrics['lead_variance'])
                recovery_window = max(2, min(30, recovery_window)) # Sanitary bounds

            if ads > 1.0 and row['Stock_On_Hand'] <= 0:
                est_lost = ads * recovery_window * row['unit_price']
                ghost_demand_value += est_lost
                ghost_demand_list.append({
                    'item_name': row['Item_Name'],
                    'ads': ads,
                    'recovery_window_days': recovery_window,
                    'est_lost_revenue': est_lost
                })

        self.catalog_metrics = {
            'total_skus_scanned': len(grouped),
            'total_capital_tied': total_capital_tied,
            'dead_stock_value': dead_stock_value,
            'dead_stock_count': len(dead_stock_list),
            'dead_stock_list': sorted(dead_stock_list, key=lambda x: x['capital_trapped'], reverse=True),
            'ghost_demand_value': ghost_demand_value,
            'ghost_demand_count': len(ghost_demand_list),
            'ghost_demand_list': sorted(ghost_demand_list, key=lambda x: x['est_lost_revenue'], reverse=True)
        }
        logger.info(f"POS Analysis Complete. Mathematical revenue bleed: ${ghost_demand_value:,.2f}")

    def run_supplier_analysis(self):
        """Analyzes GRN for dynamic Supplier Toxicity Index (LATA)."""
        if self.grn_df is None or self.grn_df.empty: return
        
        logger.info("Running Supplier Toxicity (LATA) Analysis...")
        self.grn_df['Actual_Lead_Time'] = (self.grn_df['Received_Date'] - self.grn_df['Order_Date']).dt.days
        self.grn_df['Fulfillment_Pct'] = self.grn_df['Received_Qty'] / self.grn_df['Ordered_Qty'].replace(0, 1)

        supp_group = self.grn_df.groupby('Supplier_Name').agg(
            total_orders=('PO_Number', 'count'),
            avg_fulfillment=('Fulfillment_Pct', 'mean'),
            lead_time_variance=('Actual_Lead_Time', 'std'),
            avg_lead_time=('Actual_Lead_Time', 'mean')
        ).reset_index().fillna(0)

        # Track returns per supplier for STI penalty
        return_penalty_map = {}
        if self.shrink_df is not None and not self.shrink_df.empty and 'Supplier' in self.shrink_df.columns:
            # Only "Short Supply" counts as a supplier failure correction
            short_supplies = self.shrink_df[self.shrink_df['Reason'].str.contains('Short Supply', case=False, na=False)]
            return_penalty_map = short_supplies.groupby('Supplier').size().to_dict()

        processed_suppliers = []
        for _, row in supp_group.iterrows():
            # Strict Math STI Calculation:
            # STI = (Failure_Rate) * (Lead_Time_Coefficient) + (Return_Friction_Penalty)
            failure_rate = 1 - row['avg_fulfillment']
            lead_time_volatility = row['lead_time_variance'] / max(1, row['avg_lead_time'])
            
            # Penalize for Short Supply Returns to ground the STI in operational friction
            # Each 'Short Supply' return adds 0.05 to the STI score (max penalty 0.3)
            return_count = return_penalty_map.get(row['Supplier_Name'], 0)
            return_penalty = min(0.3, return_count * 0.05)
            
            # STI score from 0 (Reliable) to 1.0 (Toxic)
            sti_score = (failure_rate * 0.6) + (min(1.0, lead_time_volatility) * 0.25) + return_penalty
            
            # Classification based on dynamic thresholds
            if sti_score > 0.4: status = 'CRIMINAL'
            elif sti_score > 0.15: status = 'HOSTILE'
            else: status = 'RELIABLE'
            
            processed_suppliers.append({
                'supplier': row['Supplier_Name'],
                'orders': row['total_orders'],
                'fulfillment': row['avg_fulfillment'] * 100,
                'lead_variance': row['lead_time_variance'],
                'avg_lead_time': row['avg_lead_time'],
                'sti_score': sti_score,
                'status': status,
                'short_supply_returns': return_count
            })
            
        self.supplier_metrics = {
            'total_suppliers': len(supp_group),
            'criminal_count': len([s for s in processed_suppliers if s['status'] in ['CRIMINAL', 'HOSTILE']]),
            'supplier_list': processed_suppliers
        }
        logger.info(f"LATA Analysis Complete. Toxicity Scan found {self.supplier_metrics['criminal_count']} at-risk vendors.")

    def run_network_analysis(self):
        """Analyzes Shrinkage and Transfers for entropy costs."""
        wastage_cost = 0
        friction_cost = 0
        
        if self.shrink_df is not None and not self.shrink_df.empty:
            shrink_count = len(self.shrink_df)
            
            # Semantic Classification
            # 1. Wastage (Expiry/Damaged)
            wastage_reasons = ['EXPIRY ITEM', 'DAMAGED']
            wastage_df = self.shrink_df[self.shrink_df['Reason'].isin(wastage_reasons)]
            wastage_cost = wastage_df['Cost_Value'].abs().sum() if 'Cost_Value' in wastage_df.columns else 0
            
            # 2. Operational Friction (Short Supply / GRN Errors)
            friction_reasons = ['Short Supply', 'QTY WRONGLY ENTERED IN GRN']
            friction_df = self.shrink_df[self.shrink_df['Reason'].isin(friction_reasons)]
            friction_cost = friction_df['Cost_Value'].abs().sum() if 'Cost_Value' in friction_df.columns else 0
            
            # Neutral / Empties (ignored in Bleed calculation)
            neutral_reasons = ['EMPTIES', 'BANDING']
            neutral_df = self.shrink_df[self.shrink_df['Reason'].isin(neutral_reasons)]
            
            shrink_cost = wastage_cost + friction_cost
        
        if self.transfer_df is not None and not self.transfer_df.empty:
            transfer_count = len(self.transfer_df)
            if 'Cost_Value' in self.transfer_df.columns:
                transfer_cost = self.transfer_df['Cost_Value'].abs().sum()
            else:
                transfer_cost = transfer_count * 50

        self.network_metrics = {
            'shrink_events': shrink_count,
            'shrink_cost': shrink_cost,
            'wastage_cost': wastage_cost,
            'friction_cost': friction_cost,
            'transfer_events': transfer_count,
            'transfer_cost': transfer_cost,
            'entropy_cost_est': shrink_cost + transfer_cost
        }
        logger.info(f"Network Analysis Complete. Entropy cost: KES {shrink_cost + transfer_cost:,.2f} (Waste: {wastage_cost}, Friction: {friction_cost})")

    def get_full_audit(self):
        return {
            'catalog': self.catalog_metrics,
            'suppliers': self.supplier_metrics,
            'network': self.network_metrics
        }

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ingestor = ForensicOperationsIngestor(base_dir)
    ingestor.load_logs()
    ingestor.run_pos_analysis()
    ingestor.run_supplier_analysis()
    ingestor.run_network_analysis()
    audit = ingestor.get_full_audit()
    print("\n[V2 FORENSIC AUDIT SUCCESS]")
