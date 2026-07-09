"""
Data Validator — Validates and sanitizes data flowing into OASIS from external sources.

Catches malformed/missing data at the boundary before it reaches the OrderEngine,
preventing silent corruption of calculations downstream.
"""

import logging
import pandas as pd
from typing import List, Dict, Any

logger = logging.getLogger("DataValidator")


class DataValidator:
    """
    Validates DataFrames and product dicts entering the OASIS pipeline.

    Enforces:
    - Required columns present
    - Correct types (numeric fields aren't strings)
    - Sane ranges (no negative stock, no prices > 1M, etc.)
    - Missing-field defaults
    """

    # ------------------------------------------------------------------
    # Schema Definitions
    # ------------------------------------------------------------------

    STOCK_REQUIRED = ['product_name', 'current_stocks']
    STOCK_DEFAULTS = {
        'current_stocks': 0,
        'barcode': '',
        'department': 'UNKNOWN',
        'pack_size': 1,
        'selling_price': 0.0,
        'cost_price': 0.0,
        'supplier_name': 'Unknown',
    }

    SALES_REQUIRED = ['product_name', 'total_qty_sold']
    SALES_DEFAULTS = {
        'total_qty_sold': 0,
        'days_with_sales': 0,
        'total_revenue': 0.0,
        'avg_daily_sales': 0.0,
    }

    # ------------------------------------------------------------------
    # DataFrame Validation
    # ------------------------------------------------------------------

    def validate_stock_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and sanitize a stock DataFrame from iRetail."""
        df = self._check_required_columns(df, self.STOCK_REQUIRED, "Stock")
        df = self._apply_defaults(df, self.STOCK_DEFAULTS)

        # Type coercion
        df['current_stocks'] = pd.to_numeric(df['current_stocks'], errors='coerce').fillna(0).astype(int)
        df['pack_size'] = pd.to_numeric(df['pack_size'], errors='coerce').fillna(1).astype(int)
        df['selling_price'] = pd.to_numeric(df['selling_price'], errors='coerce').fillna(0.0)
        df['cost_price'] = pd.to_numeric(df['cost_price'], errors='coerce').fillna(0.0)

        # Range checks
        df['current_stocks'] = df['current_stocks'].clip(lower=0)
        df['selling_price'] = df['selling_price'].clip(lower=0, upper=1_000_000)
        df['cost_price'] = df['cost_price'].clip(lower=0, upper=1_000_000)
        df['pack_size'] = df['pack_size'].clip(lower=1, upper=10_000)

        # Drop rows with empty product names
        before = len(df)
        df = df[df['product_name'].str.strip().str.len() > 0]
        dropped = before - len(df)
        if dropped:
            logger.warning(f"Dropped {dropped} rows with empty product names.")

        return df.reset_index(drop=True)

    def validate_sales_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and sanitize a sales DataFrame from iRetail."""
        df = self._check_required_columns(df, self.SALES_REQUIRED, "Sales")
        df = self._apply_defaults(df, self.SALES_DEFAULTS)

        # Type coercion
        df['total_qty_sold'] = pd.to_numeric(df['total_qty_sold'], errors='coerce').fillna(0)
        df['avg_daily_sales'] = pd.to_numeric(df['avg_daily_sales'], errors='coerce').fillna(0.0)
        df['total_revenue'] = pd.to_numeric(df['total_revenue'], errors='coerce').fillna(0.0)

        # Range checks
        df['total_qty_sold'] = df['total_qty_sold'].clip(lower=0)
        df['avg_daily_sales'] = df['avg_daily_sales'].clip(lower=0.0)

        return df.reset_index(drop=True)

    def validate_product_list(self, products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Validate a list of product dicts before they enter the OrderEngine.
        Fixes common issues: missing fields, wrong types, out-of-range values.
        """
        validated = []
        for i, p in enumerate(products):
            name = str(p.get('product_name', '')).strip()
            if not name:
                logger.debug(f"Skipping product at index {i}: empty name.")
                continue

            validated.append({
                'product_name': name.upper(),
                'current_stocks': max(0, int(self._safe_numeric(p.get('current_stocks', 0)))),
                'barcode': str(p.get('barcode', '')).strip(),
                'department': str(p.get('department', 'UNKNOWN')).upper().strip(),
                'pack_size': max(1, int(self._safe_numeric(p.get('pack_size', 1)))),
                'selling_price': max(0.0, float(self._safe_numeric(p.get('selling_price', 0)))),
                'cost_price': max(0.0, float(self._safe_numeric(p.get('cost_price', 0)))),
                'supplier_name': str(p.get('supplier_name', 'Unknown')).strip(),
                # Pass through any extra fields
                **{k: v for k, v in p.items() if k not in self.STOCK_DEFAULTS and k != 'product_name'},
            })

        logger.info(f"Validated {len(validated)}/{len(products)} products.")
        return validated

    # ------------------------------------------------------------------
    # Internal Helpers
    # ------------------------------------------------------------------

    def _check_required_columns(self, df: pd.DataFrame, required: List[str], context: str) -> pd.DataFrame:
        """Check that required columns are present. Raise if critical ones missing."""
        missing = [col for col in required if col not in df.columns]
        if missing:
            raise ValueError(f"{context} data missing required columns: {missing}. Got: {list(df.columns)}")
        return df

    def _apply_defaults(self, df: pd.DataFrame, defaults: Dict[str, Any]) -> pd.DataFrame:
        """Fill missing columns with default values."""
        for col, default in defaults.items():
            if col not in df.columns:
                df[col] = default
        return df

    @staticmethod
    def _safe_numeric(val, default=0) -> float:
        """Convert a value to float, returning default on failure."""
        try:
            return float(val) if val is not None else default
        except (ValueError, TypeError):
            return default
