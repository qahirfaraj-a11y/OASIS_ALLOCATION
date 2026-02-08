
import os
import json
import pandas as pd
import logging
from typing import Dict, Any

logger = logging.getLogger("SimulationDataLoader")

class HistoricalDataLoader:
    """
    Ingests user's historical data assets to power the simulation.
    1. Monthly Cash Files -> Seasonality Indices.
    2. Forecasting JSON -> Item Trends.
    """
    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        
    def load_seasonality_indices(self) -> Dict[str, float]:
        """
        Reads jan_cash.xlsx ... oct_cash.xlsx to calculate relative volume.
        Returns: {'JAN': 0.9, 'DEC': 1.2, ...}
        """
        months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct']
        monthly_totals = {}
        
        logger.info("Loading monthly cash files for seasonality...")
        
        for m in months:
            fname = f"{m}_cash.xlsx"
            fpath = os.path.join(self.data_dir, fname)
            
            if not os.path.exists(fpath):
                logger.warning(f"Missing monthly file: {fname}")
                continue
                
            try:
                # Robust Load: Force convert all to numeric and pick the champion
                df = pd.read_excel(fpath)
                
                # Drop fully empty cols/rows
                df.dropna(how='all', inplace=True)
                
                max_sum = 0.0
                winner_found = False
                
                for col in df.columns:
                    # Force numeric
                    try:
                        s = pd.to_numeric(df[col], errors='coerce').fillna(0)
                        col_sum = s.sum()
                        if col_sum > max_sum:
                            max_sum = col_sum
                            winner_found = True
                    except:
                        pass
                
                if winner_found and max_sum > 0:
                    monthly_totals[m.upper()] = max_sum
                    # logger.info(f"Loaded {m}: Total={max_sum:,.0f}")
                else:
                    logger.warning(f"No numeric data found in {fname}")
                    
            except Exception as e:
                logger.error(f"Failed to read {fname}: {e}")
                
        if not monthly_totals:
            return {m.upper(): 1.0 for m in months}
            
        # Filter out noise (e.g., sum < 100)
        valid_totals = [v for v in monthly_totals.values() if v > 100.0]
        if not valid_totals:
             avg_vol = 1.0 # Avoid div by zero
        else:
             avg_vol = sum(valid_totals) / len(valid_totals)
        
        # Calculate Indices
        indices = {}
        for m in months:
             vol = monthly_totals.get(m.upper(), 0.0)
             if vol > 100.0 and avg_vol > 0:
                 indices[m.upper()] = vol / avg_vol
             else:
                 indices[m.upper()] = 1.0 # Default to Average if missing or noise
        
        # Default missing months to 1.0 (Nov, Dec)
        indices['NOV'] = 1.0
        indices['DEC'] = 1.0 
        
        logger.info(f"Calculated Seasonality Indices: {json.dumps({k: round(v,2) for k,v in indices.items()})}")
        return indices

    def load_item_trends(self) -> Dict[str, float]:
        """
        Reads sales_forecasting_2025.json.
        Returns: {'ITEM_NAME': 1.25} (Multiplier based on trend)
        """
        # Search for file
        matches = [f for f in os.listdir(self.data_dir) if "sales_forecasting" in f and f.endswith(".json")]
        if not matches:
            logger.warning("No sales forecasting JSON found.")
            return {}
            
        fpath = os.path.join(self.data_dir, matches[0])
        trends = {}
        
        try:
            with open(fpath, 'r', encoding='utf-8') as f:
                data = json.load(f)
                
            for p_name, p_data in data.items():
                # trend_pct can be +60.0 or -20.0 (percentage growth)
                pct = p_data.get('trend_pct', 0.0)
                
                # Convert to multiplier. 
                # +10% -> 1.10
                # -10% -> 0.90
                multiplier = 1.0 + (pct / 100.0)
                
                # Safety Caps (0.5x to 2.0x)
                multiplier = max(0.5, min(2.0, multiplier))
                
                trends[p_name.strip().upper()] = multiplier
                
            logger.info(f"Loaded trends for {len(trends)} items.")
            return trends
            
        except Exception as e:
            logger.error(f"Failed to load forecasts: {e}")
            return {}

    def load_monthly_demand(self, target_month: str) -> Dict[str, float]:
        """
        Reads {month}_cash.xlsx to build a specific demand map for that month.
        Used to guide the Initial Allocation (Hybrid Approach).
        Returns: {'ITEM_NAME': TotalQty, ...}
        """
        fname = f"{target_month.lower()}_cash.xlsx"
        fpath = os.path.join(self.data_dir, fname)
        
        if not os.path.exists(fpath):
            logger.warning(f"No cash file found for {target_month} ({fname}). Using static demand only.")
            return {}
            
        logger.info(f"Loading specific demand from {fname}...")
        try:
            # Read first few rows to find header
            df_preview = pd.read_excel(fpath, nrows=20, header=None)
            
            # Find row with "Item Name" or "Qty"
            header_row_idx = -1
            name_col_idx = -1
            qty_col_idx = -1
            
            for r_idx, row in df_preview.iterrows():
                row_str = [str(x).lower() for x in row.values]
                if "item name" in row_str and "qty" in row_str:
                    header_row_idx = r_idx
                    # Find column indices
                    for c_idx, cell in enumerate(row_str):
                        if "item name" in cell: name_col_idx = c_idx
                        if "qty" in cell: qty_col_idx = c_idx
                    break
            
            if header_row_idx == -1:
                logger.warning(f"Could not find 'Item Name'/'Qty' headers in {fname}.")
                return {}
                
            # Read full file with correct header
            df = pd.read_excel(fpath, header=header_row_idx)
            
            # Extract and Aggregate
            # map column names using the indices we found (or by name if standard)
            # The read_excel(header=i) should match the names.
            
            # Normalize column names
            df.columns = [str(c).strip().lower() for c in df.columns]
            
            # Identify columns again in the dataframe
            df_name_col = next((c for c in df.columns if 'item name' in c), None)
            df_qty_col = next((c for c in df.columns if 'qty' in c), None)
            
            if not df_name_col or not df_qty_col:
                return {}
                
            demand_map = {}
            for _, row in df.iterrows():
                try:
                    p_name = str(row[df_name_col]).strip().upper()
                    qty = float(row[df_qty_col])
                    if qty > 0:
                        demand_map[p_name] = demand_map.get(p_name, 0) + qty
                except:
                    continue
                    
            logger.info(f"Loaded {len(demand_map)} items from {fname} for Hybrid Allocation.")
            return demand_map
            
        except Exception as e:
            logger.error(f"Failed to load monthly demand: {e}")
            return {}
