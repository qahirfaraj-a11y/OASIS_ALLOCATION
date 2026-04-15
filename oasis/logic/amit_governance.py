"""
O.A.S.I.S. AMIT Governance Wrapper
Phase 3 of the Client Implementation Playbook.

Provides formal governance features around the AMIT dead stock detection:
- Exportable Negative List (for client sign-off)
- System-level purchase block activation
- Weekly Capital Recovery tracking
"""

import os
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List, Any

logger = logging.getLogger("OASIS.AMITGov")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


class AMITGovernance:
    """Governance wrapper around the AMIT dead stock detection engine."""

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.governance_dir = os.path.join(data_dir, 'amit_governance')
        os.makedirs(self.governance_dir, exist_ok=True)
        self.negative_list = pd.DataFrame()

    def generate_negative_list(self, scorecard_path: str, ads_threshold: float = 0.2, soh_threshold: int = 15):
        """
        Scans the scorecard and generates the formal AMIT Negative List.
        Items with ADS < threshold AND SOH > threshold are classified as dead stock.
        """
        logger.info("Generating AMIT Negative List...")
        scorecard = pd.read_csv(scorecard_path)

        # Normalize columns
        col_map = {}
        for c in scorecard.columns:
            cl = c.lower().strip()
            if 'product' in cl or 'item' in cl: col_map[c] = 'Item_Name'
            elif 'ads' in cl or 'avg_daily' in cl or 'velocity' in cl: col_map[c] = 'ADS'
            elif 'soh' in cl or 'stock_on_hand' in cl or 'stock' in cl: col_map[c] = 'SOH'
            elif 'cost' in cl or 'unit_cost' in cl or 'unit_price' in cl: col_map[c] = 'Unit_Cost'
            elif 'supplier' in cl or 'vendor' in cl: col_map[c] = 'Supplier'
            elif 'department' in cl or 'dept' in cl: col_map[c] = 'Department'
            elif 'barcode' in cl: col_map[c] = 'Barcode'
        scorecard = scorecard.rename(columns=col_map)

        if 'ADS' not in scorecard.columns: scorecard['ADS'] = 0
        if 'SOH' not in scorecard.columns: scorecard['SOH'] = 0
        if 'Unit_Cost' not in scorecard.columns: scorecard['Unit_Cost'] = 100

        scorecard['ADS'] = pd.to_numeric(scorecard['ADS'], errors='coerce').fillna(0)
        scorecard['SOH'] = pd.to_numeric(scorecard['SOH'], errors='coerce').fillna(0)
        scorecard['Unit_Cost'] = pd.to_numeric(scorecard['Unit_Cost'], errors='coerce').fillna(100)

        # Apply AMIT dead stock detection
        dead = scorecard[(scorecard['ADS'] < ads_threshold) & (scorecard['SOH'] > soh_threshold)].copy()
        dead['Capital_Trapped'] = dead['SOH'] * dead['Unit_Cost']
        dead['Days_Of_Stock'] = (dead['SOH'] / dead['ADS'].replace(0, 0.01)).astype(int)
        dead['Classification'] = 'DEAD_STOCK'
        dead['Date_Flagged'] = datetime.now().strftime('%Y-%m-%d')

        self.negative_list = dead.sort_values('Capital_Trapped', ascending=False)

        # Save
        neg_path = os.path.join(self.governance_dir, 'amit_negative_list.csv')
        self.negative_list.to_csv(neg_path, index=False)

        total_trapped = dead['Capital_Trapped'].sum()
        logger.info(f"Negative List: {len(dead)} items, KES {total_trapped:,.2f} trapped capital. Saved to {neg_path}")
        return self.negative_list

    def export_negative_list_excel(self):
        """Exports the Negative List as a formatted Excel file for client sign-off."""
        if self.negative_list.empty:
            logger.error("No Negative List generated. Run generate_negative_list() first.")
            return None

        import io
        output = io.BytesIO()
        cols = ['Item_Name', 'Department', 'Supplier', 'SOH', 'ADS', 'Unit_Cost',
                'Capital_Trapped', 'Days_Of_Stock', 'Classification', 'Date_Flagged']
        export_cols = [c for c in cols if c in self.negative_list.columns]

        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            workbook = writer.book
            header_fmt = workbook.add_format({'bold': True, 'bg_color': '#C0392B', 'font_color': 'white', 'border': 1})
            money_fmt = workbook.add_format({'num_format': '#,##0.00'})

            self.negative_list[export_cols].to_excel(writer, sheet_name='AMIT Negative List', index=False)
            ws = writer.sheets['AMIT Negative List']
            ws.set_column('A:A', 40)
            ws.set_column('B:J', 18, money_fmt)
            for i, col in enumerate(export_cols):
                ws.write(0, i, col, header_fmt)

            # Add sign-off sheet
            sign_df = pd.DataFrame({
                'Field': ['Client Name', 'Authorized By', 'Date', 'Signature', 'Notes'],
                'Value': ['', '', '', '', '']
            })
            sign_df.to_excel(writer, sheet_name='Client Sign-Off', index=False)
            ws2 = writer.sheets['Client Sign-Off']
            ws2.set_column('A:A', 25)
            ws2.set_column('B:B', 50)

        output.seek(0)
        return output

    def activate_purchase_block(self, config_path: str = None):
        """
        Writes the Negative List SKUs into the O.A.S.I.S. config JSON
        so the ordering engine hard-skips them.
        """
        if self.negative_list.empty:
            logger.error("No Negative List loaded.")
            return

        blocked_items = self.negative_list['Item_Name'].tolist()
        block_config = {
            'amit_blocked_skus': blocked_items,
            'block_activated': datetime.now().isoformat(),
            'total_blocked': len(blocked_items),
        }

        block_path = config_path or os.path.join(self.governance_dir, 'amit_purchase_blocks.json')
        with open(block_path, 'w') as f:
            json.dump(block_config, f, indent=2)

        logger.info(f"Purchase block activated for {len(blocked_items)} items. Config: {block_path}")
        return block_config

    def track_capital_recovery(self, current_soh_path: str):
        """
        Compares current SOH against the original Negative List to calculate
        how much capital has been freed since the flush began.
        """
        if self.negative_list.empty:
            logger.error("No Negative List loaded.")
            return {}

        current = pd.read_csv(current_soh_path)

        # Normalize
        col_map = {}
        for c in current.columns:
            cl = c.lower().strip()
            if 'product' in cl or 'item' in cl: col_map[c] = 'Item_Name'
            elif 'soh' in cl or 'stock' in cl or 'qty' in cl: col_map[c] = 'Current_SOH'
        current = current.rename(columns=col_map)

        original = self.negative_list[['Item_Name', 'SOH', 'Unit_Cost', 'Capital_Trapped']].copy()
        original = original.rename(columns={'SOH': 'Original_SOH'})

        merged = original.merge(current[['Item_Name', 'Current_SOH']], on='Item_Name', how='left')
        merged['Current_SOH'] = pd.to_numeric(merged['Current_SOH'], errors='coerce').fillna(merged['Original_SOH'])
        merged['Units_Recovered'] = (merged['Original_SOH'] - merged['Current_SOH']).clip(lower=0)
        merged['Capital_Recovered'] = merged['Units_Recovered'] * merged['Unit_Cost']

        total_original = merged['Capital_Trapped'].sum()
        total_recovered = merged['Capital_Recovered'].sum()
        recovery_pct = (total_recovered / max(total_original, 1)) * 100

        report = {
            'date': datetime.now().strftime('%Y-%m-%d'),
            'items_on_negative_list': len(merged),
            'original_trapped_capital': total_original,
            'capital_recovered': total_recovered,
            'recovery_percentage': recovery_pct,
            'remaining_trapped': total_original - total_recovered,
        }

        # Save weekly report
        report_path = os.path.join(self.governance_dir, f'capital_recovery_{datetime.now().strftime("%Y%m%d")}.json')
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)

        logger.info(f"Capital Recovery: KES {total_recovered:,.2f} recovered ({recovery_pct:.1f}%). Saved to {report_path}")
        return report
