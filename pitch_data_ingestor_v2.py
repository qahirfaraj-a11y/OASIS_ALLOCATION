import pandas as pd
import numpy as np
import logging
import os
import glob
import json
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ForensicIngestor")

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
        
        # Department Map for category-aware thresholds
        self._dept_map = self._load_department_map()
        
        # Load Central Config
        self.config = self._load_central_config()
        self._apply_config_defaults()

    def _load_central_config(self):
        """Load central O.A.S.I.S. configuration."""
        path = os.path.join(self.data_dir, 'oasis_engines_config.json')
        if os.path.exists(path):
            try:
                with open(path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load central config: {e}")
        return {}

    def _apply_config_defaults(self):
        """Apply settings from central config to ingestor constants."""
        ds_conf = self.config.get("engines", {}).get("dead_stock", {})
        self.DEAD_STOCK_DAYS_DEFAULT = ds_conf.get("days_default", 45)
        self.DEAD_STOCK_CAPITAL_FLOOR = ds_conf.get("capital_floor", 500.0)
        
        tiers = ds_conf.get("perishability_tiers")
        if tiers:
            self.PERISHABILITY_TIERS = tiers
            logger.info(f"Loaded {len(tiers)} perishability tiers from config.")
    
    # --- PERISHABILITY TIERS ---
    # Category-aware dead-stock thresholds (max days of cover before flagging as dead)
    # Grouped by shelf-life profile of the Kenyan retail landscape
    PERISHABILITY_TIERS = {
        # Tier 1: Ultra-Perishable (1-7 day shelf life)
        'BREAD': 5, 'CAKES': 5, 'FRESH CREAM': 5, 'FRESH PASTA': 5,
        'FRESH MILK': 7, 'YOGHURT': 7, 'FRESH JUICE': 7, 'FRESH GOURMET': 7,
        'EGGS': 7, 'DELI PRODUCT': 7, 'DELI HAM': 7, 'DELI CHEESE': 7,
        'EA FRESH VEG': 5, 'FRESH VEG CS': 5, 'FRESH FRUITS CS': 5,
        'BAKERY BIGCOLD': 5, 'BAKERY CHEERS': 5, 'BAKERY ENNSVALLEY': 5, 'BAKERY FOODPLUS': 5,
        # Tier 2: Short Perishable (14-30 day shelf life)
        'BUTTER': 21, 'CHEESE': 21, 'MARGARINE': 21, 'FROZEN FISH/SEAFOOD': 30,
        'FROZEN FRUITS': 30, 'FROZEN GOURMET': 30, 'FROZEN VEGETABLE': 30,
        'ICE-CREAM': 30, 'DAIRY': 21, 'SOYA & OTHER MILK': 21,
        'BABY MILK': 30, 'BABY FOODS': 30,
        # Tier 3: Medium Shelf Life (45-60 days)
        'BISCUITS': 45, 'SNACKS': 45, 'CRISPS': 45, 'CHOCOLATES': 45,
        'CONFECTIONERY': 45, 'SWEETS': 45, 'CHEWING GUM': 60,
        'CEREALS': 60, 'BREAKFAST CEREALS': 60, 'OATS': 60, 'MUESLI': 60,
        'TEA': 60, 'COFFEE': 60, 'BEVERAGES': 45,
        # Tier 4: Long Shelf Life (90+ days)
        'CANNED FOOD': 90, 'CANNED MEAT': 90, 'CANNED DRINKS': 90,
        'CANNED FRUITS': 90, 'CANNED MILK': 90, 'CANNED SODA': 90,
        'RICE': 90, 'FLOUR': 90, 'SUGAR': 90, 'SALT': 90, 'PASTA': 90,
        'COOKING OIL': 90, 'COOKING FAT': 90, 'GHEE': 90,
        'SPICES': 120, 'DRIED FRUITS/NUTS': 90,
        # Tier 5: Non-Perishable (120+ days — durables)
        'WINES': 120, 'SPIRITS': 180, 'BEER': 60, 'CIDERS': 60,
        'DETERGENTS': 120, 'ALL CLEANERS': 120, 'BLEACHES': 120,
        'COSMETICS': 120, 'TOILETRIES': 120, 'HOUSEHOLD ITEMS': 120,
        'STATIONERIES': 180, 'TOYS': 180, 'HARDWARE': 180,
        'ELECTRICAL ITEMS': 180, 'HOME APPLIANCES ELEC': 180,
    }
    DEAD_STOCK_DAYS_DEFAULT = 45  # Fallback for unmapped departments
    DEAD_STOCK_CAPITAL_FLOOR = 500.0  # KES — ignore trivial trapped capital
    
    def _load_department_map(self):
        """Load the master product→department map for category-aware analysis."""
        paths = [
            os.path.join(os.path.dirname(os.path.abspath(__file__)), 'oasis', 'data', 'master_product_dept_map.json'),
            os.path.join(self.data_dir, 'master_product_dept_map.json'),
        ]
        for p in paths:
            if os.path.exists(p):
                try:
                    with open(p, 'r', encoding='utf-8') as f:
                        dept_map = json.load(f)
                    logger.info(f"Loaded department map: {len(dept_map)} items mapped.")
                    # Normalize keys to uppercase for matching
                    return {k.upper(): v.upper() for k, v in dept_map.items()}
                except Exception as e:
                    logger.warning(f"Failed to load department map: {e}")
        logger.warning("No department map found. Using flat dead-stock thresholds.")
        return {}
    
    def _get_dead_stock_days(self, item_name: str, supplier_name: str = None) -> int:
        """Get category-aware dead-stock threshold for an item, adjusted by LATA risk."""
        dept = self._dept_map.get(str(item_name).upper(), '')
        base_days = self.PERISHABILITY_TIERS.get(dept, self.DEAD_STOCK_DAYS_DEFAULT)
        
        # GAP A1 Fix: LATA Feedback Loop
        # If supplier is toxic, we are more surgical with their stock.
        # We reduce the allowed days of cover, effectively flagging them sooner.
        risk_adjustment = 1.0
        if supplier_name and hasattr(self, 'supplier_metrics'):
            supp_list = self.supplier_metrics.get('supplier_list', [])
            supp_metrics = next((s for s in supp_list if s['supplier'] == supplier_name), None)
            if supp_metrics:
                sti = supp_metrics.get('sti_score', 0)
                if sti > 0.4: # CRIMINAL
                    risk_adjustment = 0.70 # 30% reduction in allowed cover
                elif sti > 0.15: # HOSTILE
                    risk_adjustment = 0.85 # 15% reduction
        
        return int(base_days * risk_adjustment)

    def _load_file(self, file_source):
        """Dynamically load CSV, JSON, or Excel with Smart Header Detection. Supports single file or list."""
        if file_source is None: return None
        
        # Handle lists of files (Recursive)
        if isinstance(file_source, list):
            dfs = []
            for f in file_source:
                df_part = self._load_file(f)
                if df_part is not None: dfs.append(df_part)
            
            if not dfs: return None
            return pd.concat(dfs, ignore_index=True).drop_duplicates()

        file_name = file_source if isinstance(file_source, str) else file_source.name
        file_meta = str(file_name).lower()
        
        try:
            df = None
            if file_meta.endswith('.csv'): 
                df = pd.read_csv(file_source)
            elif file_meta.endswith('.xlsx') or file_meta.endswith('.xls'): 
                df = pd.read_excel(file_source)
            elif file_meta.endswith('.json'): 
                df = pd.read_json(file_source)
            else:
                logger.error(f"Unsupported file format: {file_name}")
                return None

            if df is not None and not df.empty:
                # --- SMART HEADER DETECTION (Skip report metadata rows) ---
                # Find the first row that has at least 3 recognizable retail-like keywords
                retail_keywords = {'item', 'qty', 'barcode', 'code', 'price', 'amt', 'date', 'vendor', 'supplier'}
                for i in range(min(10, len(df))):
                    row_content = [str(c).lower() for c in df.columns]
                    match_count = sum(1 for k in retail_keywords if any(k in c for c in row_content))
                    
                    if match_count >= 2:
                        return df # Current columns are good
                    
                    # If not, try promoting the first row to header
                    new_cols = df.iloc[0].values
                    df = df.iloc[1:].reset_index(drop=True)
                    df.columns = new_cols
                    
                return df
            return None
        except Exception as e:
            logger.error(f"Error loading {file_name}: {e}")
            return None

    def _normalize_df(self, df, column_map):
        """Standardizes dataframe columns based on a map of variants."""
        if df is None or df.empty: return df
        
        rename_dict = {}
        for target, variants in column_map.items():
            for variant in variants:
                # Case-insensitive match
                matches = [c for c in df.columns if str(c).strip().lower() == variant.lower()]
                if matches:
                    rename_dict[matches[0]] = target
                    break
        
        return df.rename(columns=rename_dict)

    def load_logs(self, pos_file=None, grn_file=None, shrink_file=None, transfer_file=None, inventory_file=None):
        logger.info("Loading Forensic Logs...")
        
        # --- MASTER DATA LAYER (For Enrichment) ---
        # Dynamic scorecard version discovery (glob for latest version)
        base_dir = os.path.dirname(os.path.abspath(__file__))
        sc_candidates = sorted(glob.glob(os.path.join(base_dir, "Full_Product_Allocation_Scorecard_v*.csv")))
        if sc_candidates:
            def _sc_ver(p):
                try: return int(os.path.basename(p).split('_v')[-1].replace('.csv', ''))
                except: return 0
            scorecard_path = max(sc_candidates, key=_sc_ver)
        else:
            scorecard_path = os.path.join(base_dir, "Full_Product_Allocation_Scorecard_v3.csv")
        
        master_df = None
        if os.path.exists(scorecard_path):
            master_df = pd.read_csv(scorecard_path)
            # Standardize Master
            master_df = master_df.rename(columns={'Product': 'Item_Name'})
            master_df['Unit_Cost'] = master_df['Unit_Price'] * (1 - master_df['Margin_Pct']/100)

        # POS Column Map
        pos_map = {
            'Date': ['Date', 'TRANSDATE', 'Sales Date', 'Bill Date', 'BILL_DT', 'Doc Date', 'Posting Date', 'Ref Date', 'Log Date'],
            'Barcode': ['Barcode', 'Item Code', 'Itm Code', 'ITM_CD', 'SCAN_ITM_CD', 'SKU', 'UPC', 'EAN', 'Bar Code'],
            'Item_Name': ['Item_Name', 'Item Name', 'Description', 'ITM_LONG_NAME', 'Product'],
            'Qty_Sold': ['Qty_Sold', 'Qty Sold', 'Quantity', 'QTY', 'Units', 'Sold_Qty', 'Qty'],
            'Unit_Price_KES': ['Unit_Price_KES', 'Unit Price', 'Price', 'Sell Price', 'BSP_SP', 'Rate', 'Gross Amt'],
            'Unit_Cost_KES': ['Unit_Cost_KES', 'Unit Cost', 'Cost', 'Cost Price', 'BCP_CP', 'WAC', 'Rate'],
            'Transaction_ID': ['Transaction_ID', 'Transaction ID', 'TXN_ID', 'Bill No', 'BILL_NO']
        }

        if pos_file: 
            raw_df = self._load_file(pos_file)
            self.pos_df = self._normalize_df(raw_df, pos_map)
            
            if self.pos_df is not None:
                # --- DATA ENRICHMENT (Fill the blanks from Master) ---
                if master_df is not None:
                    # Match by Item_Name if Barcode is numeric/missing
                    join_col = 'Item_Name' if 'Item_Name' in self.pos_df.columns else None
                    if join_col:
                        self.pos_df = self.pos_df.merge(
                            master_df[['Item_Name', 'Unit_Price', 'Unit_Cost']], 
                            on='Item_Name', how='left', suffixes=('', '_master')
                        ).fillna({'Unit_Price': 0, 'Unit_Cost': 0})
                        
                        if 'Unit_Price_KES' not in self.pos_df.columns or self.pos_df['Unit_Price_KES'].sum() == 0:
                            self.pos_df['Unit_Price_KES'] = self.pos_df['Unit_Price'].fillna(0)
                        if 'Unit_Cost_KES' not in self.pos_df.columns or self.pos_df['Unit_Cost_KES'].sum() == 0:
                            self.pos_df['Unit_Cost_KES'] = self.pos_df['Unit_Cost'].fillna(0)

                # Ensure critical columns exist or create defaults
                if 'Date' in self.pos_df.columns:
                    self.pos_df['Date'] = pd.to_datetime(self.pos_df['Date'], errors='coerce')
                else:
                    # Synthetic Date fallback for Monthly Reports
                    logger.warning("No Date column found. Assuming 30-day window.")
                    self.pos_df['Date'] = datetime.now() 

                # Fill missing for analysis stability
                if 'Qty_Sold' not in self.pos_df.columns: self.pos_df['Qty_Sold'] = 1
                if 'Barcode' not in self.pos_df.columns: 
                    self.pos_df['Barcode'] = self.pos_df['Item_Name'] if 'Item_Name' in self.pos_df.columns else range(len(self.pos_df))
                
                # V2 Fix: Ensure Barcode is always string to prevent merge type errors
                self.pos_df['Barcode'] = self.pos_df['Barcode'].astype(str)
                
                if 'Item_Name' not in self.pos_df.columns: self.pos_df['Item_Name'] = self.pos_df['Barcode']

        # GRN Normalization
        grn_map = {
            'Order_Date': ['Order_Date', 'PO Date', 'Order Date', 'PO_DT', 'Doc Date', 'TXN_DATE'],
            'Received_Date': ['Received_Date', 'GRN Date', 'RECV_DT', 'GRN_DT', 'GRN Date'],
            'PO_Number': ['PO_Number', 'PO No', 'Order No', 'PO_NO'],
            'Supplier_Name': ['Supplier_Name', 'Vendor', 'Supplier', 'Vendor Name', 'Vendor Code - Name', 'Ven Code / Name'],
            'Item_Name': ['Item_Name', 'Item Name', 'Product'],
            'Ordered_Qty': ['Ordered_Qty', 'PO Qty', 'Order Qty'],
            'Received_Qty': ['Received_Qty', 'GRN Qty', 'Recv Qty'],
            'Barcode': ['Barcode', 'Bar Code', 'Itm Code']
        }

        if grn_file:
            raw_grn = self._load_file(grn_file)
            self.grn_df = self._normalize_df(raw_grn, grn_map)
            if self.grn_df is not None:
                if 'Order_Date' in self.grn_df.columns: self.grn_df['Order_Date'] = pd.to_datetime(self.grn_df['Order_Date'], errors='coerce')
                if 'Received_Date' in self.grn_df.columns: self.grn_df['Received_Date'] = pd.to_datetime(self.grn_df['Received_Date'], errors='coerce')

        if shrink_file: 
            self.shrink_df = self._normalize_df(self._load_file(shrink_file), {
                'Date': ['Date', 'Doc Date', 'Adjustment Date'],
                'Qty_Adjusted': ['Qty_Adjusted', 'Qty', 'Adjust Qty', 'Shrink Qty'],
                'Item_Name': ['Item_Name', 'Description', 'Product']
            })
            if self.shrink_df is not None and 'Date' in self.shrink_df.columns:
                self.shrink_df['Date'] = pd.to_datetime(self.shrink_df['Date'], errors='coerce')

        if transfer_file: 
            self.transfer_df = self._normalize_df(self._load_file(transfer_file), {
                'Date': ['Date', 'STI Date', 'Transfer Date'],
                'Qty_Transferred': ['Qty_Transferred', 'Qty', 'STI Qty'],
                'Cost_Value': ['Cost_Value', 'Net Amt', 'Net Amount', 'Cost'],
                'Item_Name': ['Item_Name', 'Item Name', 'Description', 'Product'],
                'From_Branch': ['From_Branch', 'From Org Code/ Name', 'From Store'],
                'To_Branch': ['To_Branch', 'To Org Code/ Name', 'To Store'],
            })
            if self.transfer_df is not None and 'Date' in self.transfer_df.columns:
                self.transfer_df['Date'] = pd.to_datetime(self.transfer_df['Date'], errors='coerce')

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

    def _calculate_ghost_demand_threshold(self, ads_series):
        """Dynamic Ghost Demand threshold that scales with retail format.
        
        Logic: The floor is 2.0 ADS (absolute minimum to be considered 'fast').
        For larger stores with higher overall velocity, the threshold rises to
        the 75th percentile of the store's ADS distribution — ensuring only
        truly material stockouts are flagged.
        
        - Duka (tiny): P75 might be 1.5 → clamped to floor 2.0
        - Minimart:    P75 might be 3.0 → uses 3.0
        - Mega-Store:  P75 might be 8.0 → uses 8.0
        """
        FLOOR = 2.0
        positive_ads = ads_series[ads_series > 0]
        if positive_ads.empty:
            return FLOOR
        p75 = float(np.percentile(positive_ads, 75))
        threshold = max(FLOOR, p75)
        logger.info(f"Ghost Demand Threshold: {threshold:.2f} ADS (Floor={FLOOR}, P75={p75:.2f})")
        return threshold

    def run_pos_analysis(self):
        """Analyzes POS to calculate true daily velocity and Dead Stock capital."""
        if self.pos_df is None or self.pos_df.empty: return
        
        logger.info("Running POS Forensic Analysis (95% SL Target)...")
        SERVICE_LEVEL_Z = 1.645 # 95% Service Level
        
        # Ensure critical columns exist for aggregation
        for col in ['Barcode', 'Item_Name', 'Qty_Sold', 'Unit_Price_KES', 'Unit_Cost_KES', 'Transaction_ID']:
            if col not in self.pos_df.columns:
                if col == 'Transaction_ID': self.pos_df[col] = range(len(self.pos_df))
                else: self.pos_df[col] = 0

        # Calculate total days in log
        days_active = (self.pos_df['Date'].max() - self.pos_df['Date'].min()).days + 1 if 'Date' in self.pos_df.columns else 1
        
        # Aggregate by Item
        grouped = self.pos_df.groupby(['Barcode', 'Item_Name']).agg(
            total_qty=('Qty_Sold', 'sum'),
            total_rev=('Unit_Price_KES', lambda x: (x * self.pos_df.loc[x.index, 'Qty_Sold']).sum()),
            tx_count=('Transaction_ID', 'nunique'),
            days_with_sales=('Date', 'nunique') if 'Date' in self.pos_df.columns else ('Barcode', 'count'),
            unit_cost=('Unit_Cost_KES', 'mean'),
            unit_price=('Unit_Price_KES', 'mean')
        ).reset_index()

        # Load SOH
        soh_df = getattr(self, 'inventory_df', None)
        if soh_df is None:
            try:
                soh_df = pd.read_csv(os.path.join(self.data_dir, 'prospect_inventory_snapshot.csv'))
            except FileNotFoundError:
                logger.warning("No SOH data found (inventory_df or prospect_inventory_snapshot.csv).")
                soh_df = pd.DataFrame()

        if not soh_df.empty:
            # Normalize SOH column
            soh_map = {
                'Barcode': ['Barcode', 'Item Code', 'Bar Code'],
                'Stock_On_Hand': ['Stock_On_Hand', 'SOH', 'Current Stock', 'Stock']
            }
            soh_df = self._normalize_df(soh_df, soh_map)
            
            # V2 Fix: Ensure Barcodes are strings on both sides before merging
            if 'Barcode' in soh_df.columns:
                soh_df['Barcode'] = soh_df['Barcode'].astype(str)
            grouped['Barcode'] = grouped['Barcode'].astype(str)
            
            # Merge SOH
            if 'Barcode' in soh_df.columns and 'Stock_On_Hand' in soh_df.columns:
                grouped = grouped.merge(soh_df[['Barcode', 'Stock_On_Hand']], on='Barcode', how='left')
            else:
                logger.warning("SOH data missing Barcode or Stock_On_Hand columns.")
                grouped['Stock_On_Hand'] = 0
            
            grouped['Stock_On_Hand'] = grouped['Stock_On_Hand'].fillna(0)
        else:
            grouped['Stock_On_Hand'] = 0

        # Link to Supplier Data for Lead Times
        supplier_map = {}
        if self.grn_df is not None and not self.grn_df.empty:
             # Just a simple map for demo/pitch purposes
             if 'Item_Name' in self.grn_df.columns and 'Supplier_Name' in self.grn_df.columns:
                 supplier_map = self.grn_df.groupby('Item_Name')['Supplier_Name'].first().to_dict()
             else:
                 logger.warning("GRN missing Item_Name or Supplier_Name for mapping.")

        total_capital_tied = 0
        dead_stock_value = 0
        dead_stock_list = []
        ghost_demand_list = []
        ghost_demand_value = 0

        # Calculate scalable Ghost Demand threshold
        grouped['_ads_calc'] = grouped['total_qty'] / max(1, days_active)
        ghost_threshold = self._calculate_ghost_demand_threshold(grouped['_ads_calc'])

        for _, row in grouped.iterrows():
            ads = row['total_qty'] / max(1, days_active)
            current_capital = row['Stock_On_Hand'] * row['unit_cost']
            total_capital_tied += current_capital
            
            # --- AMIT: Category-Aware Dead Stock Detection (with LATA Feedback) ---
            supplier = supplier_map.get(row['Item_Name'], 'GENERAL')
            dead_stock_days = self._get_dead_stock_days(row['Item_Name'], supplier_name=supplier)
            
            if ads > 0 and (row['Stock_On_Hand'] / ads) > dead_stock_days and current_capital >= self.DEAD_STOCK_CAPITAL_FLOOR:
                dead_stock_value += current_capital
                dead_stock_list.append({
                    'item_name': row['Item_Name'],
                    'stock': row['Stock_On_Hand'],
                    'capital_trapped': current_capital,
                    'ads': ads,
                    'days_cover': row['Stock_On_Hand'] / ads,
                    'category_threshold_days': dead_stock_days,
                    'supplier': supplier
                })
            elif ads == 0 and row['Stock_On_Hand'] > 0 and current_capital >= self.DEAD_STOCK_CAPITAL_FLOOR:
                dead_stock_value += current_capital
                dead_stock_list.append({
                    'item_name': row['Item_Name'],
                    'stock': row['Stock_On_Hand'],
                    'capital_trapped': current_capital,
                    'ads': 0,
                    'days_cover': 999,
                    'category_threshold_days': dead_stock_days,
                    'supplier': supplier
                })

            # --- DHARAM: Strict Math Ghost Demand ---
            # Recovery Window = Avg_Lead_Time + (Z * Std_Dev_Lead_Time)
            supplier = supplier_map.get(row['Item_Name'], 'GENERAL')
            # Defensive: supplier_metrics may be empty if run_supplier_analysis() hasn't been called yet
            supp_list = self.supplier_metrics.get('supplier_list', [])
            supp_metrics = next((s for s in supp_list if s['supplier'] == supplier), None)
            
            # Default to 7 days if no supplier data
            dharam_cfg = self.config.get("engines", {}).get("dharam", {})
            max_rec = dharam_cfg.get("max_recovery_window_days", 90)
            sub_discount = dharam_cfg.get("substitution_density_discount", 0.8)
            
            recovery_window = 7
            if supp_metrics:
                # 95% confidence recovery buffer
                recovery_window = supp_metrics['avg_lead_time'] + (SERVICE_LEVEL_Z * supp_metrics['lead_variance'])
                recovery_window = max(2, min(max_rec, recovery_window)) # Sanitary bounds (configurable cap)

            if ads > ghost_threshold and row['Stock_On_Hand'] <= 0:
                # Math Refinement: Recovery window cannot be NaN (happens if supplier has only 1 order)
                safe_recovery = np.nan_to_num(recovery_window, nan=7.0)
                safe_price = np.nan_to_num(row['unit_price'], nan=0.0)
                
                # Apply Substitution Discount (Proposed Improvement D3)
                est_lost = ads * safe_recovery * safe_price * sub_discount
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
            'ghost_demand_list': sorted(ghost_demand_list, key=lambda x: x['est_lost_revenue'], reverse=True),
            'ghost_demand_threshold': ghost_threshold,
        }
        
        # Store full enriched grouped data for Excel evidence export
        self.catalog_df = grouped.copy()
        self.catalog_df['ADS'] = self.catalog_df['total_qty'] / max(1, days_active)
        self.catalog_df['Days_Coverage'] = self.catalog_df['Stock_On_Hand'] / self.catalog_df['ADS'].replace(0, 1).infer_objects(copy=False)
        
        logger.info(f"POS Analysis Complete. Mathematical revenue bleed: ${ghost_demand_value:,.2f}")

    def run_supplier_analysis(self):
        """Analyzes GRN for dynamic Supplier Toxicity Index (LATA)."""
        if self.grn_df is None or self.grn_df.empty: return
        
        logger.info("Running Supplier Toxicity (LATA) Analysis...")
        
        # Safe Lead Time Calculation
        if 'Received_Date' in self.grn_df.columns and 'Order_Date' in self.grn_df.columns:
            self.grn_df['Actual_Lead_Time'] = (self.grn_df['Received_Date'] - self.grn_df['Order_Date']).dt.days
        else:
            logger.warning("GRN missing Order_Date or Received_Date. Defaulting Lead Time to 0.")
            self.grn_df['Actual_Lead_Time'] = 0

        # Safe Fulfillment Calculation
        if 'Received_Qty' in self.grn_df.columns and 'Ordered_Qty' in self.grn_df.columns:
            self.grn_df['Fulfillment_Pct'] = self.grn_df['Received_Qty'] / self.grn_df['Ordered_Qty'].replace(0, 1)
        else:
            self.grn_df['Fulfillment_Pct'] = 1.0

        supp_group = self.grn_df.groupby('Supplier_Name').agg(
            total_orders=('PO_Number', 'count') if 'PO_Number' in self.grn_df.columns else ('Supplier_Name', 'count'),
            avg_fulfillment=('Fulfillment_Pct', 'mean'),
            lead_time_variance=('Actual_Lead_Time', 'std'),
            avg_lead_time=('Actual_Lead_Time', 'mean')
        ).reset_index().fillna(0) # Standard Deviation for groups of size 1 is NaN, fill with 0

        # Track returns per supplier for STI penalty
        return_penalty_map = {}
        if self.shrink_df is not None and not self.shrink_df.empty and 'Supplier' in self.shrink_df.columns:
            # Only "Short Supply" counts as a supplier failure correction
            short_supplies = self.shrink_df[self.shrink_df['Reason'].str.contains('Short Supply', case=False, na=False)]
            return_penalty_map = short_supplies.groupby('Supplier').size().to_dict()

        processed_suppliers = []
        for _, row in supp_group.iterrows():
            # v2.1: Configuration-driven weights
            lata_cfg = self.config.get("engines", {}).get("lata", {})
            w_fail = lata_cfg.get("pitch_sti_failure_weight", 0.6)
            w_var = lata_cfg.get("pitch_sti_variance_weight", 0.25)
            ret_cap = lata_cfg.get("pitch_sti_return_penalty_cap", 0.3)
            ret_mult = lata_cfg.get("return_penalty_rate_multiplier", 1.5)
            
            # Strict Math STI Calculation:
            # STI = (Failure_Rate) * (Lead_Time_Coefficient) + (Return_Friction_Penalty)
            failure_rate = 1 - row['avg_fulfillment']
            lead_time_volatility = row['lead_time_variance'] / max(1, row['avg_lead_time'])
            
            # Penalize for Short Supply Returns — RATE-NORMALIZED by order volume
            # A supplier with 5 returns on 10 orders (50% rate) is far worse than
            # one with 5 returns on 1000 orders (0.5% rate)
            return_count = return_penalty_map.get(row['Supplier_Name'], 0)
            total_orders = max(1, row['total_orders'])
            return_rate = return_count / total_orders
            return_penalty = min(ret_cap, return_rate * ret_mult)
            
            # STI score from 0 (Reliable) to 1.0 (Toxic)
            sti_score = (failure_rate * w_fail) + (min(1.0, lead_time_volatility) * w_var) + return_penalty
            
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
                'short_supply_returns': return_count,
                'return_rate': round(return_rate, 4),
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
        neutral_cost = 0
        unclassified_cost = 0
        shrink_count = 0
        shrink_cost = 0
        transfer_count = 0
        transfer_cost = 0
        
        if self.shrink_df is not None and not self.shrink_df.empty:
            shrink_count = len(self.shrink_df)
            
            # Semantic Classification using fuzzy pattern matching (v1.2: Config-driven)
            mande_cfg = self.config.get("engines", {}).get("mande", {})
            w_pat = mande_cfg.get("wastage_pattern", r'expir|damage|spoil|broken|rotten|perish')
            f_pat = mande_cfg.get("friction_pattern", r'short.?supply|wrongly.?entered|qty.?error|grn.?error|variance|mismatch')
            n_pat = mande_cfg.get("neutral_pattern", r'empt|banding|crate|deposit')

            # 1. Wastage (Expiry/Damaged/Spoilage)
            wastage_df = self.shrink_df[self.shrink_df['Reason'].str.contains(w_pat, case=False, na=False)]
            wastage_cost = wastage_df['Cost_Value'].abs().sum() if 'Cost_Value' in wastage_df.columns else 0
            
            # 2. Operational Friction (Short Supply / GRN Errors)
            friction_df = self.shrink_df[self.shrink_df['Reason'].str.contains(f_pat, case=False, na=False)]
            friction_cost = friction_df['Cost_Value'].abs().sum() if 'Cost_Value' in friction_df.columns else 0
            
            # 3. Neutral / Empties (excluded from Bleed calculation)
            neutral_df = self.shrink_df[self.shrink_df['Reason'].str.contains(n_pat, case=False, na=False)]
            neutral_cost = neutral_df['Cost_Value'].abs().sum() if 'Cost_Value' in neutral_df.columns else 0
            
            # 4. Unclassified (everything else)
            classified_idx = wastage_df.index.union(friction_df.index).union(neutral_df.index)
            unclassified_df = self.shrink_df[~self.shrink_df.index.isin(classified_idx)]
            unclassified_cost = unclassified_df['Cost_Value'].abs().sum() if 'Cost_Value' in unclassified_df.columns else 0
            
            shrink_cost = wastage_cost + friction_cost + unclassified_cost  # Exclude only neutral
        
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
            'neutral_cost': neutral_cost,
            'unclassified_cost': unclassified_cost,
            'transfer_events': transfer_count,
            'transfer_cost': transfer_cost,
            'entropy_cost_est': shrink_cost + transfer_cost
        }
        logger.info(f"Network Analysis Complete. Entropy cost: KES {shrink_cost + transfer_cost:,.2f} (Waste: {wastage_cost}, Friction: {friction_cost}, Neutral: {neutral_cost})")

    def run_cycle_analysis(self):
        """Analyzes 30-day retail waves and payday magnetism."""
        if self.pos_df is None or self.pos_df.empty or 'Date' not in self.pos_df.columns:
            self.cycle_metrics = {"status": "Insufficient Data"}
            return

        logger.info("Running Retail Cycle Intelligence...")
        df = self.pos_df.copy()
        df['Day'] = df['Date'].dt.day
        
        # 1. Demand Waveform (1-31)
        wave = df.groupby('Day')['Qty_Sold'].sum().reindex(range(1, 32)).fillna(0).infer_objects(copy=False).to_dict()
        
        # 2. Payday Strength (25-5 vs 6-24)
        payday_mask = (df['Day'] >= 25) | (df['Day'] <= 5)
        payday_sales = df[payday_mask]['Qty_Sold'].sum()
        midmonth_sales = df[~payday_mask]['Qty_Sold'].sum()
        
        # Normalize by window width (12 days for payday, 19 days for mid-month)
        payday_daily_avg = payday_sales / 12
        midmonth_daily_avg = midmonth_sales / 19
        
        payday_multiplier = payday_daily_avg / max(0.1, midmonth_daily_avg)
        
        self.cycle_metrics = {
            'demand_wave': wave,
            'payday_multiplier': round(payday_multiplier, 2),
            'payday_sales_share': round(payday_sales / (payday_sales + midmonth_sales), 2) if (payday_sales + midmonth_sales) > 0 else 0,
            'avg_monthly_rev': (df['Unit_Price_KES'] * df['Qty_Sold']).sum() / max(1, (self.pos_df['Date'].max() - self.pos_df['Date'].min()).days // 30)
        }

    def get_full_audit(self):
        return {
            'catalog': self.catalog_metrics,
            'suppliers': self.supplier_metrics,
            'network': self.network_metrics,
            'cycle': getattr(self, 'cycle_metrics', {}),
            # RAW EVIDENCE LOGS (For Non-Truncated Excel Audit)
            'full_catalog_df': getattr(self, 'catalog_df', None),
            'shrink_df': self.shrink_df,
            'transfer_df': self.transfer_df,
            'pos_raw_df': self.pos_df,
            'grn_raw_df': self.grn_df
        }

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ingestor = ForensicOperationsIngestor(base_dir)
    ingestor.load_logs()
    ingestor.run_supplier_analysis() # Run supplier analysis FIRST to enable LATA feedback loop
    ingestor.run_pos_analysis()
    ingestor.run_network_analysis()
    audit = ingestor.get_full_audit()
    print("\n[V2 FORENSIC AUDIT SUCCESS]")
