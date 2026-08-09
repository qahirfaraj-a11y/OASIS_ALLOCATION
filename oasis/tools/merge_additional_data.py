import os
import json
# O.A.S.I.S. Data Integration Tool - v1.0.1 (Sync Heartbeat)
import pandas as pd
from datetime import datetime
import shutil
import logging

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MergeTool")

DATA_DIR = os.getenv(
    "OASIS_DATA_DIR",
    os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
)
SALES_INTEL_PATH = os.path.join(DATA_DIR, "sales_profitability_intelligence_2025_updated.json")
GRN_FREQ_PATH = os.path.join(DATA_DIR, "sku_grn_frequency.json")

SL_FILES = [
    os.path.join(DATA_DIR, "1_31_sl.xlsx"),
    os.path.join(DATA_DIR, "2_31_sl.xlsx")
]

GRN_FILES = [
    os.path.join(DATA_DIR, "2_29_grnds.xlsx"),
    os.path.join(DATA_DIR, "3_23_grnds.xlsx"),
    os.path.join(DATA_DIR, "1_15_grnds.xlsx"),
    os.path.join(DATA_DIR, "1_31grnds.xlsx"),
    os.path.join(DATA_DIR, "2_15_grnds.xlsx")
]

def backup_file(path):
    if os.path.exists(path):
        backup_path = path + ".bak_" + datetime.now().strftime("%Y%m%d_%H%M%S")
        shutil.copy2(path, backup_path)
        logger.info(f"Backed up {path} to {backup_path}")

def load_json(path):
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_json(path, data):
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4)
    logger.info(f"Saved {path}")

def process_sales(intel_data):
    logger.info("Processing Sales data...")
    updates = 0
    new_items = 0
    for f in SL_FILES:
        if not os.path.exists(f):
            logger.warning(f"File not found: {f}")
            continue
        
        logger.info(f"Reading {f}")
        df = pd.read_excel(f)
        # Column header comes from the CLIENT's own workbook.
        store_col = '027 - Rhapta Road'
        if store_col not in df.columns:
            logger.error(f"Column '{store_col}' not found in {f}")
            continue
            
        for _, row in df.iterrows():
            name = str(row['Item Name']).strip().upper()
            qty = row[store_col]
            if pd.isna(qty) or qty <= 0:
                continue
            
            if name not in intel_data:
                intel_data[name] = {
                    "total_qty_sold": 0,
                    "revenue": 0.0,
                    "margin_pct": 0.0,
                    "gross_profit": 0.0,
                    "sales_rank": 9999,
                    "category": str(row['Department']).strip().upper()
                }
                new_items += 1
            
            intel_data[name]["total_qty_sold"] += qty
            # Revenue calculation: use SP from grnds or existing if possible. 
            # For now, let's keep revenue as is or just add qty.
            updates += 1
            
    logger.info(f"Sales processing complete. Updated {updates} records, added {new_items} new items.")

def process_grns(freq_data):
    logger.info("Processing GRN data...")
    sku_dates = {} # SKU -> set of dates
    
    updates = 0
    for f in GRN_FILES:
        if not os.path.exists(f):
            logger.warning(f"File not found: {f}")
            continue
            
        logger.info(f"Reading {f}")
        df = pd.read_excel(f)
        df.columns = [str(c).strip() for c in df.columns]
        
        logger.info(f"Columns found: {df.columns.tolist()}")
        logger.info(f"Total rows: {len(df)}")
        if len(df) > 0:
            logger.info(f"First row GRN Date: {df.iloc[0].get('GRN Date')}, Type: {type(df.iloc[0].get('GRN Date'))}")

        date_col = 'GRN Date'
        name_col = 'Item Name'
        qty_col = 'GRN Qty'
        
        if date_col not in df.columns or name_col not in df.columns:
            logger.error(f"Required columns not found in {f}")
            continue
            
        # Convert date column to datetime
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        for _, row in df.iterrows():
            name = str(row[name_col]).strip().upper()
            date_val = row[date_col]
            qty = row[qty_col]
            
            if pd.isna(qty) or qty <= 0:
                continue
                
            if pd.isna(date_val):
                continue
            
            date_obj = date_val.date()
                
            if name not in sku_dates:
                sku_dates[name] = set()
            sku_dates[name].add(date_obj)
            updates += 1

    # Update frequency map
    # Since we don't have the full history, we'll just set frequency based on the new data 
    # OR merge if the SKU exists.
    # If the SKU exists, we could average the frequencies? 
    # Or just use the new data if it's substantial.
    
    item_added = 0
    for name, dates in sku_dates.items():
        if len(dates) > 1:
            sorted_dates = sorted(list(dates))
            gaps = [(sorted_dates[i] - sorted_dates[i-1]).days for i in range(1, len(sorted_dates)) if (sorted_dates[i] - sorted_dates[i-1]).days > 0]
            if gaps:
                avg_gap = sum(gaps) / len(gaps)
                new_freq = 1.0 / avg_gap
                
                if name in freq_data:
                    # Blend old and new frequency (simple average for now)
                    freq_data[name] = (freq_data[name] + new_freq) / 2
                else:
                    freq_data[name] = new_freq
                    item_added += 1
        elif len(dates) == 1:
            # Single occurrence: use a default low frequency if new, or keep existing
            if name not in freq_data:
                freq_data[name] = 0.05 # Baseline for "rarely received"
                item_added += 1
                
    logger.info(f"GRN processing complete. Processed {updates} records, added/updated frequencies for {len(sku_dates)} SKUs.")

def main():
    logger.info("Starting data merge...")
    
    # Load
    intel_data = load_json(SALES_INTEL_PATH)
    freq_data = load_json(GRN_FREQ_PATH)
    
    # Backups
    backup_file(SALES_INTEL_PATH)
    backup_file(GRN_FREQ_PATH)
    
    # Process
    process_sales(intel_data)
    process_grns(freq_data)
    
    # Save
    save_json(SALES_INTEL_PATH, intel_data)
    save_json(GRN_FREQ_PATH, freq_data)
    
    logger.info("Data merge completed successfully.")

if __name__ == "__main__":
    main()
