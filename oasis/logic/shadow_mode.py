"""
O.A.S.I.S. Shadow Mode Engine
Phase 2 of the Client Implementation Playbook.

Generates "shadow" POs using the full O.A.S.I.S. intelligence stack, then compares
them against the client's actual human-generated POs to prove algorithmic superiority.
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Dict, Any

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

    def _get_grn_db(self) -> Dict[str, Any]:
        """Loads and caches the GRN intelligence DB to avoid duplicate reads."""
        if hasattr(self, '_cached_grn_db'):
            return self._cached_grn_db
        grn_cache_path = os.path.join(self.data_dir, "grn_intelligence_cache.json")
        self._cached_grn_db = {}
        if os.path.exists(grn_cache_path):
            try:
                with open(grn_cache_path, 'r') as f:
                    self._cached_grn_db = json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load GRN intelligence cache: {e}")
        return self._cached_grn_db

    def run_shadow_cycle(self, scorecard_path: str, pos_path: str = None, grn_path: str = None, shadow_budget: float = 250000000.0):
        """
        Runs the full O.A.S.I.S. ordering logic and writes the result to a shadow log
        instead of dispatching to suppliers.
        """
        logger.info(f"Running Shadow Cycle with budget: {shadow_budget}")

        import asyncio
        from .order_engine import OrderEngine

        # Define a high-speed Shadow Mixin to prevent Streamlit UI timeouts
        # For a massive 40,000 product baseline, deep string fuzzy-matching results in O(N^2) operations.
        # This overrides it to use only primary alias tracking, keeping Golden logic untouched.
        class ShadowFastOrderEngine(OrderEngine):
            def find_best_match(self, code, barcode, name, database):
                # Only trust exact alias cache hits during shadow bulk simulation
                norm_name = self.normalize_product_name(str(name))
                if getattr(self, '_sales_index_cache', None) and norm_name in self._sales_index_cache:
                    return database.get(self._sales_index_cache[norm_name])
                return None

        # BUG 1 FIX: Replace simplistic statistical formula with full OrderEngine intelligence.
        engine = ShadowFastOrderEngine(self.data_dir)
        engine.load_local_databases() # Pre-load sync
        
        # We must run the async intelligence layer:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        # The AI's mathematical intent can use the provided shadow_budget (default 250M)
        
        recommendations = loop.run_until_complete(
            engine.run_intelligent_analysis(
                file_path=scorecard_path,
                output_path="",
                allocation_mode="replenishment",
                total_budget=shadow_budget
            )
        )
        loop.close()
        
        if not recommendations:
            logger.warning("OrderEngine returned empty recommendations.")
            self.shadow_po = pd.DataFrame()
            return self.shadow_po
            
        # Convert engine output to shadow PO format
        df = pd.DataFrame(recommendations)
        
        # Rename output fields to match shadow PO expected schema
        col_map = {
            'product_name': 'Item_Name',
            'supplier_name': 'Supplier',
            'department': 'Department',
            'avg_daily_sales': 'ADS',
            'current_stocks': 'SOH',
            'cost_price': 'Unit_Cost',
            'recommended_quantity': 'Shadow_Order_Qty',
            'reasoning': 'Order_Reason'
        }
        df = df.rename(columns=col_map)
        
        # Ensure critical columns exist
        if 'Unit_Cost' not in df.columns: df['Unit_Cost'] = 100.0
        if 'Shadow_Order_Qty' not in df.columns: df['Shadow_Order_Qty'] = 0
        
        # Calculate values
        df['Shadow_Order_Qty'] = pd.to_numeric(df['Shadow_Order_Qty'], errors='coerce').fillna(0).astype(int)
        
        # FINANCIAL ENRICHMENT (GRN CACHE)
        grn_db = self._get_grn_db()

        def enrich_initial_cost(row):
            cost = pd.to_numeric(row.get('Unit_Cost'), errors='coerce')
            if pd.notnull(cost) and cost > 0 and cost != 100.0:
                return cost
            
            # Lookup in GRN DB
            item_name = str(row.get('Item_Name', '')).strip().upper()
            grn_stat = grn_db.get(item_name)
            if grn_stat and isinstance(grn_stat, dict):
                grn_cost = float(grn_stat.get('avg_cost', 0.0))
                if grn_cost > 0: return grn_cost
            return 100.0

        df['Unit_Cost'] = df.apply(enrich_initial_cost, axis=1)
        df['Shadow_Order_Value'] = df['Shadow_Order_Qty'] * df['Unit_Cost']
        df['Reorder_Point'] = 0  # Replaced by OASIS complex guards
        df['Date'] = datetime.now().strftime('%Y-%m-%d')
        
        # Filter to actual orders
        orders = df[df['Shadow_Order_Qty'] > 0].copy()
            
        cols_to_keep = ['Item_Name', 'Supplier', 'Department', 'ADS', 'SOH', 'Unit_Cost',
                         'Reorder_Point', 'Shadow_Order_Qty', 'Shadow_Order_Value', 'Order_Reason', 'Date']
        
        # Keep only overlapping columns
        available_cols = [c for c in cols_to_keep if c in orders.columns]
        self.shadow_po = orders[available_cols].copy()

        # Save shadow log
        today_str = datetime.now().strftime('%Y%m%d')
        # BUG 9 FIX: Add intra-day timestamp to prevent overwrites
        time_str = datetime.now().strftime('%H%M%S')
        log_path = os.path.join(self.shadow_log_dir, f'shadow_po_{today_str}_{time_str}.csv')
        tmp_path = log_path + '.tmp'
        self.shadow_po.to_csv(tmp_path, index=False)
        os.replace(tmp_path, log_path)

        logger.info(f"Shadow PO generated via OrderEngine: {len(self.shadow_po)} items, "
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

    def ingest_human_orders(self, human_po_path):
        """
        Loads the client's actual PO (what the human buyer ordered).
        Accepts a single path or a list of paths.
        Expects at minimum: Item_Name, Ordered_Qty columns.
        """
        paths = [human_po_path] if isinstance(human_po_path, str) else human_po_path
        dfs = []
        for path in paths:
            logger.info(f"Ingesting human PO from {path}...")
            ext = path.lower()
            try:
                if ext.endswith('.csv'):
                    df = pd.read_csv(path)
                elif ext.endswith('.xlsx') or ext.endswith('.xls'):
                    df = pd.read_excel(path)
                elif ext.endswith('.json'):
                    df = pd.read_json(path)
                else:
                    continue
                dfs.append(df)
            except Exception as e:
                logger.error(f"Failed to load {path}: {e}")
                
        if not dfs:
            logger.error("No valid human PO data loaded.")
            self.human_po = pd.DataFrame()
            return self.human_po
            
        self.human_po = pd.concat(dfs, ignore_index=True)

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

        cols_to_keep = ['Item_Name', 'Shadow_Order_Qty', 'Shadow_Order_Value', 
                        'Order_Reason', 'ADS', 'SOH', 'Unit_Cost']
        # Filter to columns that actually exist in the dataframe
        available_cols = [c for c in cols_to_keep if c in self.shadow_po.columns]
        shadow = self.shadow_po[available_cols].copy()
        
        # Fill missing columns with defaults to prevent downstream crashes
        if 'Shadow_Order_Qty' not in shadow.columns: shadow['Shadow_Order_Qty'] = 0
        if 'Shadow_Order_Value' not in shadow.columns: shadow['Shadow_Order_Value'] = 0.0
        if 'Unit_Cost' not in shadow.columns: shadow['Unit_Cost'] = 100.0
        if 'ADS' not in shadow.columns: shadow['ADS'] = 0.0
        if 'SOH' not in shadow.columns: shadow['SOH'] = 0.0
        if 'Order_Reason' not in shadow.columns: shadow['Order_Reason'] = 'UNKNOWN'

        if self.human_po.empty:
            # No human PO to compare — just return shadow with "human ordered nothing" annotation
            shadow['Human_Order_Qty'] = 0
            shadow['Divergence'] = 'HUMAN_MISSED'
            shadow['Divergence_Detail'] = 'O.A.S.I.S. identified a restock need. Human buyer did not order this item.'
            self.comparison = shadow
        else:
            # Robust column check
            if 'Item_Name' not in self.human_po.columns or 'Human_Order_Qty' not in self.human_po.columns:
                logger.error(f"Human PO ingestion failed: Missing required columns. Found: {self.human_po.columns.tolist()}")
                # Fallback to returning shadow-only comparison
                shadow['Human_Order_Qty'] = 0
                shadow['Divergence'] = 'HUMAN_MISSED'
                shadow['Divergence_Detail'] = 'Human PO data invalid or missing item/qty columns.'
                self.comparison = shadow
                return self.comparison

            human = self.human_po[['Item_Name', 'Human_Order_Qty']].copy()

            # Merge on item name (fuzzy-tolerant: uppercase match)
            shadow['_key'] = shadow['Item_Name'].astype(str).str.upper().str.strip()
            human['_key'] = human['Item_Name'].astype(str).str.upper().str.strip()
            human_agg = human.groupby('_key').agg({'Human_Order_Qty': 'sum'}).reset_index()

            merged = shadow.merge(human_agg, on='_key', how='outer', suffixes=('', '_human'))
            
            # BUG 7 FIX: Items that were only in the human PO will have NaN Item_Name from the shadow df.
            # Fill them from the _key used for the outer merge.
            merged['Item_Name'] = merged['Item_Name'].fillna(merged['_key'])
            
            merged['Shadow_Order_Qty'] = merged['Shadow_Order_Qty'].fillna(0).astype(int)
            merged['Human_Order_Qty'] = merged['Human_Order_Qty'].fillna(0).astype(int)
            merged['Shadow_Order_Value'] = merged['Shadow_Order_Value'].fillna(0)
            merged['Unit_Cost'] = merged['Unit_Cost'].fillna(0.0)
            merged['ADS'] = merged['ADS'].fillna(0.0)
            merged['SOH'] = merged['SOH'].fillna(0.0)

            # MAP TO GRN COST PRICES
            grn_db = self._get_grn_db()
                    
            def get_enriched_cost(row):
                if row['Unit_Cost'] > 0 and row['Unit_Cost'] != 100.0:
                    return row['Unit_Cost']
                
                item_name = str(row['Item_Name']).strip().upper()
                grn_stat = grn_db.get(item_name)
                if grn_stat and isinstance(grn_stat, dict):
                    cost = float(grn_stat.get('avg_cost', 0.0))
                    if cost > 0:
                        return cost
                return 100.0
                
            merged['Unit_Cost'] = merged.apply(get_enriched_cost, axis=1)
            merged['Human_Order_Value'] = merged['Human_Order_Qty'] * merged['Unit_Cost']
            
            # Recalculate Shadow Order Value to ensure consistency with newly mapped GRN prices
            merged['Shadow_Order_Value'] = merged['Shadow_Order_Qty'] * merged['Unit_Cost']

            # Classify divergence
            merged['Divergence'] = merged.apply(self._classify_divergence, axis=1)
            merged['Divergence_Detail'] = merged.apply(self._explain_divergence, axis=1)

            self.comparison = merged.drop(columns=['_key'], errors='ignore')

        # Save comparison atomically
        today_str = datetime.now().strftime('%Y%m%d')
        comp_path = os.path.join(self.shadow_log_dir, f'shadow_comparison_{today_str}.csv')
        tmp_comp_path = comp_path + '.tmp'
        self.comparison.to_csv(tmp_comp_path, index=False)
        os.replace(tmp_comp_path, comp_path)
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
        human_total = self.comparison['Human_Order_Value'].sum() if 'Human_Order_Value' in self.comparison.columns else 0.0

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
            'human_total_value': human_total,
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
