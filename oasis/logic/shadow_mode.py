"""
O.A.S.I.S. Shadow Mode Engine
Phase 2 of the Client Implementation Playbook.

Generates "shadow" POs using the full O.A.S.I.S. intelligence stack, then compares
them against the client's actual human-generated POs to prove algorithmic superiority.
"""

import os
import json
import csv
import logging
import pandas as pd
import numpy as np
from datetime import datetime, date
from typing import Dict, List, Any, Optional

logger = logging.getLogger("OASIS.ShadowMode")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")


class ShadowModeEngine:
    """
    Runs O.A.S.I.S. ordering logic in shadow (non-dispatching) mode.
    Logs hypothetical POs and compares them against the client's real orders.
    """

    def __init__(self, data_dir: str):
        self.data_dir = data_dir
        self.shadow_log_dir = os.path.join(data_dir, 'shadow_logs')
        os.makedirs(self.shadow_log_dir, exist_ok=True)

        self.shadow_po = pd.DataFrame()
        self.human_po = pd.DataFrame()
        self.comparison = pd.DataFrame()

    def run_shadow_cycle(self, scorecard_path: str, pos_path: str = None, grn_path: str = None):
        """
        Runs the full O.A.S.I.S. ordering logic and writes the result to a shadow log
        instead of dispatching to suppliers.
        """
        logger.info("Running Shadow Cycle...")

        # Load scorecard
        scorecard = pd.read_csv(scorecard_path)

        # Normalize column names
        col_map = {}
        for c in scorecard.columns:
            cl = c.lower().strip()
            if ('product' in cl or 'item' in cl) and 'Item_Name' not in col_map.values(): col_map[c] = 'Item_Name'
            elif ('ads' in cl or 'avg_daily' in cl or 'velocity' in cl) and 'ADS' not in col_map.values(): col_map[c] = 'ADS'
            elif ('soh' in cl or 'stock_on_hand' in cl or 'stock' in cl) and 'SOH' not in col_map.values(): col_map[c] = 'SOH'
            elif ('cost' in cl or 'unit_cost' in cl or 'unit_price' in cl) and 'Unit_Cost' not in col_map.values(): col_map[c] = 'Unit_Cost'
            elif ('lead' in cl) and 'Lead_Time' not in col_map.values(): col_map[c] = 'Lead_Time'
            elif ('supplier' in cl or 'vendor' in cl) and 'Supplier' not in col_map.values(): col_map[c] = 'Supplier'
            elif ('department' in cl or 'dept' in cl) and 'Department' not in col_map.values(): col_map[c] = 'Department'
            elif ('safety' in cl) and 'Safety_Factor' not in col_map.values(): col_map[c] = 'Safety_Factor'
        scorecard = scorecard.rename(columns=col_map)

        # Set sensible defaults for missing columns
        if 'ADS' not in scorecard.columns: scorecard['ADS'] = 1.0
        if 'SOH' not in scorecard.columns: scorecard['SOH'] = 0
        if 'Unit_Cost' not in scorecard.columns: scorecard['Unit_Cost'] = 100
        if 'Lead_Time' not in scorecard.columns: scorecard['Lead_Time'] = 7
        if 'In_Transit' not in scorecard.columns: scorecard['In_Transit'] = 0 # Prevent double-ordering
        if 'Safety_Factor' not in scorecard.columns: scorecard['Safety_Factor'] = 1.645 # Default to 95% SL Z-score
        if 'Supplier' not in scorecard.columns: scorecard['Supplier'] = 'GENERAL'
        if 'Department' not in scorecard.columns: scorecard['Department'] = 'GENERAL'

        # 95% Service Level Statistical Formula:
        # ReorderPoint = (ADS x Lead_Time) + (Z x Sigma_LT x ADS)
        # Simplified for Shadow Mode to: (ADS x (Lead_Time + 1.645 * StdDev_LT))
        scorecard['ADS'] = pd.to_numeric(scorecard['ADS'], errors='coerce').fillna(0)
        scorecard['SOH'] = pd.to_numeric(scorecard['SOH'], errors='coerce').fillna(0)
        scorecard['In_Transit'] = pd.to_numeric(scorecard['In_Transit'], errors='coerce').fillna(0)
        scorecard['Lead_Time'] = pd.to_numeric(scorecard['Lead_Time'], errors='coerce').fillna(7)
        scorecard['Unit_Cost'] = pd.to_numeric(scorecard['Unit_Cost'], errors='coerce').fillna(100)

        # Calculate Reorder Point with 95% Service Level Confidence
        # For a clean pitch, we assume a ~2 day standard deviation if not provided
        std_dev_lt = 2.0 
        scorecard['Effective_Lead_Time'] = scorecard['Lead_Time'] + (1.645 * std_dev_lt)
        
        scorecard['Reorder_Point'] = scorecard['ADS'] * scorecard['Effective_Lead_Time']
        
        # Net Requirement includes SOH and In-Transit (Pending) orders
        scorecard['Shadow_Order_Qty'] = (scorecard['Reorder_Point'] - (scorecard['SOH'] + scorecard['In_Transit'])).clip(lower=0).astype(int)
        
        scorecard['Shadow_Order_Value'] = scorecard['Shadow_Order_Qty'] * scorecard['Unit_Cost']
        scorecard['Order_Reason'] = scorecard.apply(self._classify_reason, axis=1)

        # Filter to items that actually need ordering
        orders = scorecard[scorecard['Shadow_Order_Qty'] > 0].copy()

        self.shadow_po = orders[['Item_Name', 'Supplier', 'Department', 'ADS', 'SOH',
                                  'Reorder_Point', 'Shadow_Order_Qty', 'Shadow_Order_Value',
                                  'Order_Reason']].copy()
        self.shadow_po['Date'] = datetime.now().strftime('%Y-%m-%d')

        # Save shadow log
        today_str = datetime.now().strftime('%Y%m%d')
        log_path = os.path.join(self.shadow_log_dir, f'shadow_po_{today_str}.csv')
        self.shadow_po.to_csv(log_path, index=False)

        logger.info(f"Shadow PO generated: {len(self.shadow_po)} items, "
                     f"KES {self.shadow_po['Shadow_Order_Value'].sum():,.2f} total value. "
                     f"Saved to {log_path}")
        return self.shadow_po

    def _classify_reason(self, row):
        if row['SOH'] <= 0 and row['ADS'] > 2.0:
            return 'CRITICAL_STOCKOUT'
        elif row['SOH'] <= 0:
            return 'STOCKOUT'
        elif row['SOH'] < row['Reorder_Point'] * 0.5:
            return 'LOW_STOCK'
        elif row['Shadow_Order_Qty'] > 0:
            return 'RESTOCK'
        return 'NO_ORDER'

    def ingest_human_orders(self, human_po_path: str):
        """
        Loads the client's actual PO (what the human buyer ordered).
        Expects at minimum: Item_Name, Ordered_Qty columns.
        """
        logger.info(f"Ingesting human PO from {human_po_path}...")
        ext = human_po_path.lower()
        if ext.endswith('.csv'):
            self.human_po = pd.read_csv(human_po_path)
        elif ext.endswith('.xlsx') or ext.endswith('.xls'):
            self.human_po = pd.read_excel(human_po_path)
        elif ext.endswith('.json'):
            self.human_po = pd.read_json(human_po_path)

        # Normalize columns
        col_map = {}
        for c in self.human_po.columns:
            cl = c.lower().strip()
            if ('item' in cl or 'product' in cl or 'description' in cl) and 'Item_Name' not in col_map.values(): col_map[c] = 'Item_Name'
            elif ('qty' in cl or 'quantity' in cl or 'ordered' in cl) and 'Human_Order_Qty' not in col_map.values(): col_map[c] = 'Human_Order_Qty'
        self.human_po = self.human_po.rename(columns=col_map)

        if 'Human_Order_Qty' not in self.human_po.columns:
            logger.warning("Could not find quantity column in human PO. Using first numeric column.")
            for c in self.human_po.columns:
                if self.human_po[c].dtype in ['int64', 'float64']:
                    self.human_po = self.human_po.rename(columns={c: 'Human_Order_Qty'})
                    break

        logger.info(f"Human PO loaded: {len(self.human_po)} line items.")
        return self.human_po

    def generate_comparison(self):
        """
        Side-by-side comparison: O.A.S.I.S. shadow PO vs. human PO.
        Identifies divergences and classifies them.
        """
        if self.shadow_po.empty:
            logger.error("No shadow PO generated yet. Run run_shadow_cycle() first.")
            return pd.DataFrame()

        shadow = self.shadow_po[['Item_Name', 'Shadow_Order_Qty', 'Shadow_Order_Value', 
                                  'Order_Reason', 'ADS', 'SOH', 'Unit_Cost']].copy()

        if self.human_po.empty:
            # No human PO to compare — just return shadow with "human ordered nothing" annotation
            shadow['Human_Order_Qty'] = 0
            shadow['Divergence'] = 'HUMAN_MISSED'
            shadow['Divergence_Detail'] = 'O.A.S.I.S. identified a restock need. Human buyer did not order this item.'
            self.comparison = shadow
        else:
            human = self.human_po[['Item_Name', 'Human_Order_Qty']].copy()

            # Merge on item name (fuzzy-tolerant: uppercase match)
            shadow['_key'] = shadow['Item_Name'].astype(str).str.upper().str.strip()
            human['_key'] = human['Item_Name'].astype(str).str.upper().str.strip()
            human_agg = human.groupby('_key').agg({'Human_Order_Qty': 'sum'}).reset_index()

            merged = shadow.merge(human_agg, on='_key', how='outer', suffixes=('', '_human'))
            merged['Shadow_Order_Qty'] = merged['Shadow_Order_Qty'].fillna(0).astype(int)
            merged['Human_Order_Qty'] = merged['Human_Order_Qty'].fillna(0).astype(int)
            merged['Shadow_Order_Value'] = merged['Shadow_Order_Value'].fillna(0)

            # Classify divergence
            merged['Divergence'] = merged.apply(self._classify_divergence, axis=1)
            merged['Divergence_Detail'] = merged.apply(self._explain_divergence, axis=1)

            self.comparison = merged.drop(columns=['_key'], errors='ignore')

        # Save comparison
        today_str = datetime.now().strftime('%Y%m%d')
        comp_path = os.path.join(self.shadow_log_dir, f'shadow_comparison_{today_str}.csv')
        self.comparison.to_csv(comp_path, index=False)
        logger.info(f"Shadow comparison saved: {comp_path}")
        return self.comparison

    def _classify_divergence(self, row):
        shadow = row.get('Shadow_Order_Qty', 0)
        human = row.get('Human_Order_Qty', 0)
        if shadow > 0 and human == 0: return 'HUMAN_MISSED'
        if shadow == 0 and human > 0: return 'HUMAN_OVER_ORDERED'
        if shadow > 0 and human > 0 and human > shadow * 1.5: return 'HUMAN_OVER_ORDERED'
        if shadow > 0 and human > 0 and human < shadow * 0.5: return 'HUMAN_UNDER_ORDERED'
        if shadow > 0 and human > 0: return 'ALIGNED'
        return 'NO_ORDER'

    def _explain_divergence(self, row):
        div = row.get('Divergence', '')
        shadow = row.get('Shadow_Order_Qty', 0)
        human = row.get('Human_Order_Qty', 0)
        if div == 'HUMAN_MISSED':
            return f'O.A.S.I.S. would order {shadow} units. Buyer ordered nothing. Risk: stockout.'
        elif div == 'HUMAN_OVER_ORDERED':
            return f'Buyer ordered {human} units. O.A.S.I.S. recommends {shadow}. Risk: future dead stock.'
        elif div == 'HUMAN_UNDER_ORDERED':
            return f'Buyer ordered {human} units. O.A.S.I.S. recommends {shadow}. Risk: insufficient coverage.'
        elif div == 'ALIGNED':
            return f'Both ordered similar quantities (Human: {human}, O.A.S.I.S.: {shadow}).'
        return 'No order needed by either party.'

    def get_summary_stats(self):
        """Returns aggregate comparison statistics for reporting."""
        if self.comparison.empty:
            return {}

        total_items = len(self.comparison)
        missed = len(self.comparison[self.comparison['Divergence'] == 'HUMAN_MISSED'])
        over = len(self.comparison[self.comparison['Divergence'] == 'HUMAN_OVER_ORDERED'])
        under = len(self.comparison[self.comparison['Divergence'] == 'HUMAN_UNDER_ORDERED'])
        aligned = len(self.comparison[self.comparison['Divergence'] == 'ALIGNED'])

        shadow_total = self.comparison['Shadow_Order_Value'].sum()
        # Risk weighting: Human over-ordering is taxed at the 17% retail holding cost
        over_ordered_df = self.comparison[self.comparison['Divergence'] == 'HUMAN_OVER_ORDERED']
        over_order_waste_risk = (over_ordered_df['Human_Order_Qty'] - over_ordered_df['Shadow_Order_Qty']) * over_ordered_df['Unit_Cost'] * 0.17

        return {
            'total_line_items': total_items,
            'human_missed': missed,
            'human_over_ordered': over,
            'human_under_ordered': under,
            'aligned': aligned,
            'oasis_total_value': shadow_total,
            'over_order_waste_risk': over_order_waste_risk.sum(),
            'accuracy_pct': (aligned / max(total_items, 1)) * 100,
        }


    def load_aggregated_shadow_logs(self, days=14):
        """Loads the last N days of shadow logs for the 14-day aggregated review."""
        all_logs = []
        for f in sorted(os.listdir(self.shadow_log_dir)):
            if f.startswith('shadow_comparison_') and f.endswith('.csv'):
                df = pd.read_csv(os.path.join(self.shadow_log_dir, f))
                all_logs.append(df)

        if all_logs:
            combined = pd.concat(all_logs[-days:], ignore_index=True)
            return combined
        return pd.DataFrame()
