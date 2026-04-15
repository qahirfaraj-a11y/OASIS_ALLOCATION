"""
O.A.S.I.S. Daily Pipeline Orchestrator
Phase 6 of the Client Implementation Playbook.

Master chain: Data Pull -> AMIT Pre-flight -> LATA Pre-flight -> Order Engine -> Approval Queue.
"""

import os
import json
import logging
from datetime import datetime
from typing import Dict, Any, Optional

logger = logging.getLogger("OASIS.Pipeline")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


class DailyPipeline:
    """
    Orchestrates the full daily O.A.S.I.S. cycle.
    Each step produces artifacts consumed by the next.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        config should contain:
        - data_dir: Path to oasis/data
        - scorecard_path: Path to the product scorecard CSV
        - nn_path: Path to the neural network export (optional)
        - shadow_mode: bool (True = shadow, False = live)
        - revenue_core_only: bool (True = top 20% only)
        - amit_enabled: bool
        - lata_enabled: bool
        - dharam_enabled: bool
        """
        self.config = config
        self.data_dir = config.get('data_dir', '.')
        self.pipeline_log_dir = os.path.join(self.data_dir, 'pipeline_logs')
        os.makedirs(self.pipeline_log_dir, exist_ok=True)

        self.run_log = {
            'run_id': datetime.now().strftime('%Y%m%d_%H%M%S'),
            'start_time': None,
            'steps': [],
            'status': 'NOT_STARTED',
        }

    def _log_step(self, step_name: str, status: str, detail: str = ''):
        entry = {
            'step': step_name,
            'status': status,
            'timestamp': datetime.now().isoformat(),
            'detail': detail,
        }
        self.run_log['steps'].append(entry)
        logger.info(f"[{step_name}] {status}: {detail}")

    def run_daily_cycle(self):
        """Executes the full daily pipeline."""
        self.run_log['start_time'] = datetime.now().isoformat()
        self.run_log['status'] = 'RUNNING'
        logger.info(f"===== DAILY PIPELINE START: {self.run_log['run_id']} =====")

        # Step 0: Live SQL Extraction
        extraction_mode = self.config.get('extraction_mode', 'file')
        if extraction_mode == 'sql':
            self._log_step('DATA_EXTRACTION', 'RUNNING', 'Pulling live SQL integration...')
            try:
                import pandas as pd
                from .iretail_integration import IRetailBridge
                bridge = IRetailBridge(
                    server=self.config.get('sql_server', 'localhost'),
                    database=self.config.get('sql_db', 'iRetailDB'),
                    username=self.config.get('sql_user', ''),
                    password=self.config.get('sql_pass', ''),
                    trusted_connection=self.config.get('sql_trusted', True)
                )
                if not bridge.connect():
                    raise ConnectionError("Could not connect to SQL ERP database.")

                store_id = self.config.get('store_id')
                stock_list = bridge.sync_stock_snapshot(store_id=store_id)
                sales_df = bridge.sync_sales_history(days=30, store_id=store_id)
                bridge.close()

                if not stock_list:
                    raise ValueError("Empty stock snapshot from ERP.")

                stock_df = pd.DataFrame(stock_list)
                # Map internal SQL columns to strict pipeline required headers
                stock_df['Item_Name'] = stock_df['product_name']
                stock_df['SOH'] = stock_df['current_stocks']
                stock_df['Unit_Cost'] = stock_df.get('cost_price', 100)
                stock_df['Supplier'] = stock_df.get('supplier_name', 'GENERAL')
                stock_df['Department'] = stock_df.get('department', 'GENERAL')

                if not sales_df.empty:
                    sales_df['Item_Name'] = sales_df['product_name']
                    sales_df['ADS'] = sales_df['avg_daily_sales']
                    scorecard = pd.merge(stock_df, sales_df[['Item_Name', 'ADS']], on='Item_Name', how='left')
                else:
                    scorecard = stock_df
                    scorecard['ADS'] = 0.0

                scorecard['ADS'] = scorecard['ADS'].fillna(0.0)

                # Overwrite scorecard path for subsequent steps
                sc_path = os.path.join(self.pipeline_log_dir, f'live_scorecard_{self.run_log["run_id"]}.csv')
                scorecard.to_csv(sc_path, index=False)
                self.config['scorecard_path'] = sc_path

                self._log_step('DATA_EXTRACTION', 'OK', f'Extracted {len(scorecard)} active items.')

            except Exception as e:
                self._log_step('DATA_EXTRACTION', 'FAILED', str(e))
                self.run_log['status'] = 'FAILED'
                self._save_log()
                return self.run_log
        else:
            self._log_step('DATA_EXTRACTION', 'SKIPPED', 'Executing from static file dump')

        # Step 1: Data Validation
        try:
            scorecard_path = self.config.get('scorecard_path')
            if not scorecard_path or not os.path.exists(scorecard_path):
                raise FileNotFoundError(f"Scorecard not found: {scorecard_path}")
            self._log_step('DATA_VALIDATION', 'OK', f'Scorecard: {scorecard_path}')
        except Exception as e:
            self._log_step('DATA_VALIDATION', 'FAILED', str(e))
            self.run_log['status'] = 'FAILED'
            self._save_log()
            return self.run_log

        # Step 2: AMIT Pre-Flight (if enabled)
        amit_blocked = []
        if self.config.get('amit_enabled', True):
            try:
                from .amit_governance import AMITGovernance
                amit = AMITGovernance(self.data_dir)
                neg_list = amit.generate_negative_list(scorecard_path)
                amit.activate_purchase_block()
                amit_blocked = neg_list['Item_Name'].tolist() if not neg_list.empty else []
                self._log_step('AMIT_PREFLIGHT', 'OK', f'{len(amit_blocked)} items blocked')
            except Exception as e:
                self._log_step('AMIT_PREFLIGHT', 'WARNING', f'AMIT failed: {e}. Proceeding without blocks.')
        else:
            self._log_step('AMIT_PREFLIGHT', 'SKIPPED', 'AMIT disabled in config')

        # Step 3: LATA Pre-Flight (if enabled)
        if self.config.get('lata_enabled', True):
            try:
                from .lata_shield import run_lata
                lata_result = run_lata(self.data_dir, self.config.get('nn_path'))
                self._log_step('LATA_PREFLIGHT', 'OK', f'Supplier risk scores updated')
            except Exception as e:
                self._log_step('LATA_PREFLIGHT', 'WARNING', f'LATA failed: {e}. Using default safety factors.')
        else:
            self._log_step('LATA_PREFLIGHT', 'SKIPPED', 'LATA disabled in config')

        # Step 4: Shadow or Live PO Generation
        shadow_mode = self.config.get('shadow_mode', True)
        try:
            from .shadow_mode import ShadowModeEngine
            engine = ShadowModeEngine(self.data_dir)
            shadow_po = engine.run_shadow_cycle(scorecard_path)

            # Filter out AMIT-blocked items
            if amit_blocked:
                before = len(shadow_po)
                shadow_po = shadow_po[~shadow_po['Item_Name'].isin(amit_blocked)]
                filtered = before - len(shadow_po)
                self._log_step('AMIT_FILTER', 'OK', f'{filtered} dead stock items removed from PO')

            # Revenue Core filter (Phase 4 — top 20% only)
            if self.config.get('revenue_core_only', False):
                top_n = max(1, int(len(shadow_po) * 0.2))
                shadow_po = shadow_po.nlargest(top_n, 'ADS')
                self._log_step('REVENUE_CORE_FILTER', 'OK', f'Limited to top {top_n} items by velocity')

            mode_label = 'SHADOW' if shadow_mode else 'LIVE'
            self._log_step('PO_GENERATION', 'OK',
                           f'{mode_label}: {len(shadow_po)} items, KES {shadow_po["Shadow_Order_Value"].sum():,.2f}')

            # Save the PO to approval queue with StoreID tagging
            store_id = self.config.get('store_id', 'GLOBAL')
            shadow_po['Store_ID'] = store_id
            po_filename = f'daily_po_{store_id}_{self.run_log["run_id"]}.csv'
            po_path = os.path.join(self.pipeline_log_dir, po_filename)
            shadow_po.to_csv(po_path, index=False)
            self._log_step('APPROVAL_QUEUE', 'OK', f'PO saved to {po_path}')

        except Exception as e:
            self._log_step('PO_GENERATION', 'FAILED', str(e))
            self.run_log['status'] = 'FAILED'
            self._save_log()
            return self.run_log

        # Step 5: Completion
        self.run_log['status'] = 'COMPLETED'
        self.run_log['end_time'] = datetime.now().isoformat()
        self._save_log()
        logger.info(f"===== DAILY PIPELINE COMPLETE: {self.run_log['run_id']} =====")
        return self.run_log

    def _save_log(self):
        log_path = os.path.join(self.pipeline_log_dir, f'pipeline_run_{self.run_log["run_id"]}.json')
        with open(log_path, 'w') as f:
            json.dump(self.run_log, f, indent=2)
