import os
import sys
import time
import shutil
import logging
import argparse
import json
import re

import pandas as pd
from datetime import datetime, time as dtime, timedelta
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from logging.handlers import RotatingFileHandler

sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from oasis.logic.shadow_mode import ShadowModeEngine
from oasis.logic.simulation_pipeline import SimulationEngine
from shadow_report_generator import generate_shadow_report

# Create logs directory if not exists
if not os.path.exists("shadow_logs"):
    os.makedirs("shadow_logs")

logger = logging.getLogger("OASIS.ShadowMonitor")
logger.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

# 10MB per log, keep 5 backups
file_handler = RotatingFileHandler("shadow_logs/shadow_daemon.log", maxBytes=10*1024*1024, backupCount=5)
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

class ShadowDropHandler(FileSystemEventHandler):
    """Watches for ERP scorecard or PO drops in monitoring/inbound."""

    def __init__(self, engine, root_dir, output_dir, archive_dir):
        self.engine = engine
        self.root_dir = root_dir
        self.output_dir = output_dir
        self.archive_dir = archive_dir
        
        # Buffers keyed by Store ID to handle multiple branch drops
        self.buffers = {} # {store_id: {'scorecard': path, 'human_po': path, 'last_event': timestamp}}

    # (S1 fix: removed duplicate on_created stub that was shadowed by the real implementation below)

    def wait_for_file_lock(self, path, timeout=30):
        """Wait until a file is available and not being written to by another process."""
        start_time = time.time()
        last_size = -1
        while time.time() - start_time < timeout:
            try:
                # Try to open the file exclusively
                # On Windows, this will fail if another process is writing
                with open(path, 'rb') as f:
                    current_size = os.path.getsize(path)
                    if current_size == last_size and current_size > 0:
                        return True
                    last_size = current_size
            except (IOError, OSError):
                # File is locked or not yet available
                pass
            time.sleep(1)
        return False

    def on_created(self, event):
        if event.is_directory: return
        filename = os.path.basename(event.src_path)
        ext = os.path.splitext(filename)[1].lower()
        if ext not in ['.csv', '.xlsx', '.xls']: return

        # 1. Atomic Write Detection (Production Hardened)
        logger.info(f"Lock Check: Waiting for {filename} to stabilize...")
        if not self.wait_for_file_lock(event.src_path):
            logger.warning(f"Timeout: {filename} remained locked too long. Skipping.")
            return
        
        # 2. Extract Store ID from filename (Pattern: StoreID_Scorecard...)
        # Fallback to 101 if not found
        store_match = re.search(r'Store(\d+)', filename, re.IGNORECASE)
        store_id = store_match.group(1) if store_match else "101"
        
        if store_id not in self.buffers:
            self.buffers[store_id] = {'scorecard': None, 'human_po': None}

        fn_lower = filename.lower()
        if 'scorecard' in fn_lower or 'inventory' in fn_lower:
            self.buffers[store_id]['scorecard'] = event.src_path
            logger.info(f"[Store {store_id}] Queued Scorecard: {filename}")
        elif 'po' in fn_lower or 'order' in fn_lower:
            self.buffers[store_id]['human_po'] = event.src_path
            logger.info(f"[Store {store_id}] Queued Human PO: {filename}")

        if self.buffers[store_id]['scorecard']:
            self.process_shadow_run(store_id)

    def process_shadow_run(self, store_id):
        try:
            logger.info(f"Starting Shadow Cycle for Store {store_id}...")
            buf = self.buffers[store_id]
            
            # 1. Run Shadow Logic
            self.engine.run_shadow_cycle(buf['scorecard'])
            
            # 2. Ingest Human PO if available
            if buf['human_po']:
                self.engine.ingest_human_orders(buf['human_po'])
            
            # 3. Compare and Stats
            self.engine.generate_comparison()
            stats = self.engine.get_summary_stats()
            
            # 4. Auto-Simulation (7-Day Backtest Context)
            logger.info(f"Triggering Auto-Simulation for Store {store_id} (7-Day Backtest)...")
            try:
                # Calculate dates for the last 7 days
                end_date = datetime.now()
                start_date = end_date - timedelta(days=7)
                
                sim_config = {
                    'data_dir': self.root_dir, # Or adjust if needed
                    'store_id': store_id,
                    'shadow_mode': True
                }
                sim_engine = SimulationEngine(sim_config)
                sim_report = sim_engine.run_simulation(start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d"))
                
                # Add sim stats to the main stats for the report generator
                stats['simulation_summary'] = sim_report.get('summary', {})
                logger.info(f"Auto-Simulation Complete: KES {stats['simulation_summary'].get('cumulative_holding_risk', 0):,.2f} savings opportunity identified.")
            except Exception as sim_e:
                logger.error(f"Auto-Simulation failed: {sim_e}")

            # 5. Generate Report
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_name = f"Shadow_Report_Store{store_id}_{timestamp}.docx"
            report_path = os.path.join(self.output_dir, report_name)
            
            # Note: generate_shadow_report might need an update to handle stats['simulation_summary']
            report_data = generate_shadow_report(self.engine.comparison, stats)
            with open(report_path, "wb") as f:
                f.write(report_data.getbuffer())
            
            logger.info(f"SUCCESS [Store {store_id}]: Report saved to {report_path}")
            
            # 5. Archive and Cleanup
            self._archive(buf['scorecard'])
            if buf['human_po']:
                self._archive(buf['human_po'])
            
            # Reset buffer for this store
            self.buffers[store_id] = {'scorecard': None, 'human_po': None}
            
        except Exception as e:
            logger.error(f"Error during shadow processing: {e}", exc_info=True)

    def _archive(self, file_path):
        if not file_path or not os.path.exists(file_path):
            return
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        dest = os.path.join(self.archive_dir, f"{ts}_{os.path.basename(file_path)}")
        shutil.move(file_path, dest)
        logger.info(f"Archived {os.path.basename(file_path)}")
        
        # Trigger 30-day purge (Step 2.3)
        self.purge_old_archives(days=30)

    def purge_old_archives(self, days=30):
        """Cleanup files older than N days to save disk space."""
        try:
            cutoff = datetime.now() - timedelta(days=days)
            purged_count = 0
            for filename in os.listdir(self.archive_dir):
                fpath = os.path.join(self.archive_dir, filename)
                if os.path.isfile(fpath):
                    mtime = datetime.fromtimestamp(os.path.getmtime(fpath))
                    if mtime < cutoff:
                        os.remove(fpath)
                        purged_count += 1
            if purged_count > 0:
                logger.info(f"Retention Policy: Purged {purged_count} archived files older than {days} days.")
        except Exception as e:
            logger.error(f"Archive Purge Error: {e}")


def run_sql_mode(engine, output_dir):
    """Automated nightly SQL baseline pull using iRetailBridge."""
    config_path = "db_config.json"
    if not os.path.exists(config_path):
        logger.warning(f"SQL Mode Skipped: {config_path} not found.")
        return

    try:
        from oasis.logic.iretail_integration import IRetailBridge
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        bridge = IRetailBridge(
            server=config['server'],
            database=config['database'],
            username=config.get('username'),
            password=config.get('password'),
            trusted_connection=config.get('trusted_connection', True)
        )
        
        if bridge.connect():
            for store_id in config.get('store_ids', [101]):
                logger.info(f"O.A.S.I.S. Automated Push: Syncing Store {store_id}...")
                
                # 1. Pull live stock snapshot
                products = bridge.sync_stock_snapshot(store_id=store_id)
                if not products: continue
                
                # 2. Save temporary scorecard for the engine
                temp_scorecard = os.path.join(output_dir, f"StoreID_{store_id}_Automated_Sync.csv")
                pd.DataFrame(products).to_csv(temp_scorecard, index=False)
                
                # 3. Run Shadow Logic
                engine.run_shadow_cycle(temp_scorecard)
                engine.generate_comparison() # No human PO for automated SQL baseline
                stats = engine.get_summary_stats()
                
                # 4. Generate Report
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                report_path = os.path.join(output_dir, f"Shadow_Report_Store{store_id}_{ts}.docx")
                report_data = generate_shadow_report(engine.comparison, stats)
                with open(report_path, "wb") as f:
                    f.write(report_data.getbuffer())
                
                logger.info(f"Automated Nightly Baseline Complete for Store {store_id}")
            
            bridge.close()
    except Exception as e:
        logger.error(f"SQL Automated Baseline Failed: {e}", exc_info=True)

def main():
    parser = argparse.ArgumentParser(description="O.A.S.I.S. Shadow Mode Background Monitor")
    parser.add_argument("--mode", choices=['file', 'sql'], default='file', help="Operation mode (watcher or scheduled SQL)")
    parser.add_argument("--store-id", default="101", help="Store identifier for report naming")
    parser.add_argument("--root", default=".", help="Workspace root directory")
    args = parser.parse_args()

    # Paths
    root = os.path.abspath(args.root)
    inbound_dir = os.path.join(root, "monitoring", "inbound")
    archive_dir = os.path.join(root, "monitoring", "archive")
    reports_dir = os.path.join(root, "monitoring", "reports")
    
    os.makedirs(inbound_dir, exist_ok=True)
    os.makedirs(archive_dir, exist_ok=True)
    os.makedirs(reports_dir, exist_ok=True)

    engine = ShadowModeEngine(root)
    
    logger.info("Initializing O.A.S.I.S. Shadow Monitor...")
    logger.info(f"Mode: {args.mode.upper()}")
    logger.info(f"Store ID: {args.store_id}")
    
    if args.mode == 'file':
        handler = ShadowDropHandler(engine, root, reports_dir, archive_dir)
        observer = Observer()
        observer.schedule(handler, inbound_dir, recursive=False)
        observer.start()
        logger.info(f"Active Monitoring on {inbound_dir}")
        
        health_file = os.path.join(root, "shadow_health.json")
        db_path = os.path.join(root, "oasis", "data", "mock_pos_erp.db")
        
        try:
            while True:
                # 1. Load existing health data to preserve last_pulse across iterations
                health_data = {
                    "status": "Healthy",
                    "mode": "File Watcher",
                    "last_heartbeat": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "monitoring_path": inbound_dir,
                    "active_buffers": len(handler.buffers)
                }
                if os.path.exists(health_file):
                    try:
                        with open(health_file, 'r') as f:
                            saved_health = json.load(f)
                        # Carry forward last_pulse from persisted state
                        if "last_pulse" in saved_health:
                            health_data["last_pulse"] = saved_health["last_pulse"]
                    except (json.JSONDecodeError, IOError):
                        pass

                # 2. Dynamic Schedule Fetch
                pulse_time_str = "19:00"
                if os.path.exists(db_path):
                    try:
                        import sqlite3
                        with sqlite3.connect(db_path) as conn:
                            res = conn.execute("SELECT CONFIG_VALUE FROM OASIS_SYSTEM_CONFIG WHERE CONFIG_KEY='shadow_audit_pulse_time'").fetchone()
                            if res: pulse_time_str = res[0]
                    except Exception: pass
                
                try:
                    pulse_h, pulse_m = map(int, pulse_time_str.split(':'))
                except Exception:
                    pulse_h, pulse_m = 19, 0

                with open(health_file, 'w') as f:
                    json.dump(health_data, f, indent=4)

                now = datetime.now()
                # 3. Dynamic Audit Pulse (fires ONCE per day)
                current_day = now.strftime("%Y-%m-%d")
                if now.hour == pulse_h and now.minute >= pulse_m and health_data.get("last_pulse") != current_day:
                    logger.info(f"O.A.S.I.S. Audit Pulse [{pulse_time_str}]: Running Daily SQL shadow routines...")
                    run_sql_mode(engine, reports_dir)
                    health_data["last_pulse"] = current_day
                    # Persist the pulse marker
                    with open(health_file, 'w') as f:
                        json.dump(health_data, f, indent=4)
                    
                time.sleep(10)
        except KeyboardInterrupt:
            observer.stop()
        observer.join()
        
    elif args.mode == 'sql':
        # SQL mode waits for 7 PM to run
        logger.info("SQL Mode Active. Waiting for 19:00 (7 PM) execution window...")
        try:
            executed_today = False
            while True:
                now = datetime.now()
                pulse_time_str = "19:00"
                db_path = os.path.join(root, "oasis", "data", "mock_pos_erp.db")
                if os.path.exists(db_path):
                    try:
                        import sqlite3
                        with sqlite3.connect(db_path) as conn:
                            res = conn.execute("SELECT CONFIG_VALUE FROM OASIS_SYSTEM_CONFIG WHERE CONFIG_KEY='shadow_audit_pulse_time'").fetchone()
                            if res: pulse_time_str = res[0]
                    except: pass
                
                try:
                    pulse_h, pulse_m = map(int, pulse_time_str.split(':'))
                except:
                    pulse_h, pulse_m = 19, 0

                if now.hour == pulse_h and now.minute == pulse_m and not executed_today:
                    logger.info(f"{pulse_time_str} Triggered: Running Daily SQL Shadow Comparison...")
                    run_sql_mode(engine, reports_dir)
                    executed_today = True
                if now.hour == (pulse_h + 1) % 24: 
                    executed_today = False # Reset for next day
                time.sleep(30)
        except KeyboardInterrupt:
            logger.info("Terminating Shadow Monitor.")

if __name__ == "__main__":
    main()
