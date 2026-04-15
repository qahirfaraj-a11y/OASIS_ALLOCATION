"""
O.A.S.I.S. File Watcher Daemon
Handles 'Pathway 2' data integrations directly from ERP file dumps.

Watches a folder for inbound data dumps, enforces strict column validation,
and automatically triggers the daily pipeline upon successful ingestion.
"""

import time
import os
import shutil
import logging
import pandas as pd
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Fix paths for import
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from oasis.logic.daily_pipeline import DailyPipeline

logger = logging.getLogger("OASIS.FileWatcher")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

class ERPExtractionHandler(FileSystemEventHandler):
    """Event handler for new files in the drops directory."""

    def __init__(self, watch_dir: str, root_dir: str):
        self.watch_dir = watch_dir
        self.root_dir = root_dir
        self.archive_dir = os.path.join(watch_dir, "archive")
        os.makedirs(self.archive_dir, exist_ok=True)
        
        # We enforce EXACT matching to prevent algorithmic collapse from bad data
        self.required_columns = ['Item_Name', 'SOH', 'ADS', 'Unit_Cost']
        
        # Where the daily pipeline expects the final unified scorecard
        self.target_scorecard_path = os.path.join(self.root_dir, 'Full_Product_Allocation_Scorecard_v7.csv')

    def on_created(self, event):
        if event.is_directory or not event.src_path.endswith('.csv'):
            return
            
        # Give the filesystem a moment to finish writing the file
        time.sleep(2)
        
        filename = os.path.basename(event.src_path)
        logger.info(f"New data dump detected: {filename}")
        
        try:
            self.process_dump(event.src_path)
        except Exception as e:
            logger.error(f"Failed to process data dump {filename}: {e}")

    def process_dump(self, file_path: str):
        """Validates the CSV and triggers the pipeline."""
        df = pd.read_csv(file_path)
        
        # 1. Strict Validation
        missing_cols = [c for c in self.required_columns if c not in df.columns]
        if missing_cols:
            logger.error(f"Validation FAILED! Missing REQUIRED headers: {missing_cols}")
            logger.error(f"Available headers were: {list(df.columns)}")
            logger.error("Client IT must normalize their export headers. Rejecting file.")
            self._archive_file(file_path, "REJECTED")
            return

        # Replace nulls aggressively before saving for safe math
        df['SOH'] = pd.to_numeric(df['SOH'], errors='coerce').fillna(0)
        df['ADS'] = pd.to_numeric(df['ADS'], errors='coerce').fillna(0)
        df['Unit_Cost'] = pd.to_numeric(df['Unit_Cost'], errors='coerce').fillna(100)
        
        if 'Lead_Time' not in df.columns: df['Lead_Time'] = 7
        if 'Supplier' not in df.columns: df['Supplier'] = 'GENERAL'
        if 'Department' not in df.columns: df['Department'] = 'GENERAL'
        if 'Safety_Factor' not in df.columns: df['Safety_Factor'] = 1.3
        
        # 2. Save as Master Scorecard
        df.to_csv(self.target_scorecard_path, index=False)
        logger.info(f"Data validated. Overwritten master scorecard at {self.target_scorecard_path}")
        
        # 3. Archive Original Dump
        self._archive_file(file_path, "PROCESSED")
        
        # 4. Trigger Daily Pipeline
        logger.info("Automatically triggering O.A.S.I.S. Daily Pipeline...")
        config = {
            'data_dir': self.root_dir,
            'scorecard_path': self.target_scorecard_path,
            'shadow_mode': True,  # Default to shadow until explicit live mode
            'revenue_core_only': False,
            'amit_enabled': True,
            'lata_enabled': True,
            'dharam_enabled': True,
        }
        
        pipeline = DailyPipeline(config)
        pipeline.run_daily_cycle()
        logger.info("Pipeline execution triggered by file watcher is complete.")

    def _archive_file(self, file_path: str, prefix: str):
        filename = os.path.basename(file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"{prefix}_{timestamp}_{filename}"
        dest_path = os.path.join(self.archive_dir, new_name)
        
        shutil.move(file_path, dest_path)
        logger.info(f"Archived drop to {dest_path}")


def start_watcher(root_dir: str):
    """Starts the endless loop watching for file drops."""
    watch_dir = os.path.join(root_dir, "inbound_drops")
    os.makedirs(watch_dir, exist_ok=True)
    
    event_handler = ERPExtractionHandler(watch_dir, root_dir)
    observer = Observer()
    observer.schedule(event_handler, watch_dir, recursive=False)
    observer.start()
    
    logger.info(f"O.A.S.I.S. Watcher listening actively on: {watch_dir}")
    logger.info("Awaiting authorized data dumps. Press Ctrl+C to terminate.")
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()

if __name__ == "__main__":
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    start_watcher(base_dir)
