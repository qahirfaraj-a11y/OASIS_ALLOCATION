"""
iRetail Integration Facade — Single entry-point for all iRetail ↔ OASIS operations.

Combines MssqlConnector (SQL queries) and BcpWrapper (bulk transfer) into a
unified interface that the OrderEngine and API layer can call directly.
"""

import os
import logging
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, List, Any

from .mssql_connector import MssqlConnector
from .bcp_wrapper import BcpWrapper
from .data_validator import DataValidator

logger = logging.getLogger("iRetailIntegration")


class IRetailBridge:
    """
    Facade for iRetail data operations.

    Usage::

        bridge = IRetailBridge(server="RETAILSRV\\IRETAIL", database="iRetailDB")
        bridge.connect()

        # Pull stock into OASIS format
        products = bridge.sync_stock_snapshot(store_id=101)

        # Pull sales history
        sales_df = bridge.sync_sales_history(days=90, store_id=101)

        # Push purchase order back
        bridge.push_purchase_order(order_rows)
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        trusted_connection: bool = True,
        bcp_path: Optional[str] = None,
        data_dir: str = "oasis/data",
    ):
        self.sql = MssqlConnector(
            server=server,
            database=database,
            username=username,
            password=password,
            trusted_connection=trusted_connection,
        )
        self.bcp = BcpWrapper(
            server=server,
            database=database,
            username=username,
            password=password,
            trusted_connection=trusted_connection,
            bcp_path=bcp_path,
        )
        self.validator = DataValidator()
        self.data_dir = data_dir
        self._connected = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def connect(self) -> bool:
        """Establish connections (SQL + verify BCP)."""
        try:
            self.sql.connect()
            bcp_ok = self.bcp.verify_connection()
            self._connected = True
            if not bcp_ok:
                logger.warning("SQL connected but BCP verification failed. Bulk ops may not work.")
            return True
        except Exception as e:
            logger.error(f"iRetail connection failed: {e}")
            return False

    def close(self):
        self.sql.close()
        self._connected = False

    # ------------------------------------------------------------------
    # Data Pull (iRetail → OASIS)
    # ------------------------------------------------------------------

    def sync_stock_snapshot(self, store_id: Optional[int] = None) -> List[Dict[str, Any]]:
        """
        Pull live stock from iRetail and transform into OASIS product dicts.

        Returns:
            List of product dicts compatible with OrderEngine.enrich_product_data()
        """
        logger.info(f"Syncing stock snapshot (store={store_id or 'all'})...")
        df = self.sql.fetch_stock(store_id=store_id)

        if df.empty:
            logger.warning("No stock data returned from iRetail.")
            return []

        # Validate and sanitize
        df = self.validator.validate_stock_dataframe(df)

        # Transform to OASIS format
        products = []
        for _, row in df.iterrows():
            products.append({
                'product_name': str(row['product_name']).strip().upper(),
                'current_stocks': int(row.get('current_stocks', 0)),
                'barcode': str(row.get('barcode', '')),
                'department': str(row.get('department', 'UNKNOWN')).upper(),
                'pack_size': int(row.get('pack_size', 1)) or 1,
                'selling_price': float(row.get('selling_price', 0)),
                'cost_price': float(row.get('cost_price', 0)),
                'supplier_name': str(row.get('supplier_name', 'Unknown')).strip(),
            })

        logger.info(f"Synced {len(products)} products from iRetail.")
        return products

    def sync_sales_history(
        self, days: int = 90, store_id: Optional[int] = None
    ) -> pd.DataFrame:
        """
        Pull POS sales from iRetail, calculate velocity metrics.

        Returns:
            DataFrame with columns: product_name, barcode, total_qty_sold,
            days_with_sales, total_revenue, avg_daily_sales
        """
        logger.info(f"Syncing {days}-day sales history (store={store_id or 'all'})...")
        df = self.sql.fetch_sales(days=days, store_id=store_id)

        if df.empty:
            logger.warning("No sales data returned from iRetail.")
            return df

        # Derived metric: avg daily sales
        df['avg_daily_sales'] = (df['total_qty_sold'] / days).round(2)

        # Validate
        df = self.validator.validate_sales_dataframe(df)

        logger.info(f"Synced sales for {len(df)} products.")
        return df

    def sync_grn_history(self, days: int = 180) -> pd.DataFrame:
        """Pull GRN data for supplier pattern analysis."""
        logger.info(f"Syncing {days}-day GRN history...")
        return self.sql.fetch_grn_history(days=days)

    # ------------------------------------------------------------------
    # Data Push (OASIS → iRetail)
    # ------------------------------------------------------------------

    def push_purchase_order(
        self,
        order_rows: List[Dict[str, Any]],
        staging_table: str = "dbo.PurchaseOrderStaging",
    ) -> int:
        """
        Write OASIS-generated purchase order to iRetail staging table.

        Args:
            order_rows: List of dicts with keys:
                product_name, barcode, recommended_quantity, supplier_name,
                estimated_cost, reasoning
            staging_table: Target staging table in iRetail

        Returns:
            Number of rows inserted
        """
        if not order_rows:
            logger.warning("No order rows to push.")
            return 0

        df = pd.DataFrame(order_rows)
        df['created_at'] = datetime.now().isoformat()
        df['source'] = 'OASIS'

        # Write to CSV for BCP bulk load
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = os.path.join(self.data_dir, f"po_staging_{timestamp}.csv")
        df.to_csv(csv_path, index=False, encoding='utf-8')

        try:
            self.bcp.import_file(csv_path, staging_table)
            logger.info(f"Pushed {len(order_rows)} order lines to {staging_table}.")
            return len(order_rows)
        except Exception as e:
            logger.error(f"BCP import failed: {e}")
            # Fallback: try row-by-row SQL INSERT
            logger.info("Falling back to SQL INSERT...")
            return self._fallback_sql_insert(df, staging_table)

    def _fallback_sql_insert(self, df: pd.DataFrame, table: str) -> int:
        """Row-by-row INSERT fallback when BCP fails."""
        try:
            df.to_sql(
                table.split('.')[-1],  # Strip schema prefix
                self.sql.engine,
                schema=table.split('.')[0] if '.' in table else 'dbo',
                if_exists='append',
                index=False,
                chunksize=500,
            )
            return len(df)
        except Exception as e:
            logger.error(f"Fallback SQL INSERT also failed: {e}")
            return 0

    # ------------------------------------------------------------------
    # Bulk Export (High-Performance)
    # ------------------------------------------------------------------

    def bulk_export_sales(
        self,
        output_file: Optional[str] = None,
        days: int = 365,
    ) -> str:
        """
        High-performance BCP export of full sales history.
        Use for initial data load or analytics refresh.

        Returns:
            Path to the exported CSV file.
        """
        if output_file is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(self.data_dir, f"iretail_sales_export_{timestamp}.csv")

        query = f"""
            SELECT
                t.TransDate, i.ItemName, i.Barcode, i.DeptCode,
                t.Qty, t.SellingPrice, t.CostPrice,
                sup.SupplierName
            FROM Transactions t
            INNER JOIN Items i ON t.ItemCode = i.ItemCode
            LEFT JOIN Supplier sup ON i.SupplierCode = sup.SupplierCode
            WHERE t.TransDate >= DATEADD(day, -{days}, GETDATE())
        """
        return self.bcp.export_query(query, output_file)

    # ------------------------------------------------------------------
    # Health Check
    # ------------------------------------------------------------------

    def health_check(self) -> Dict[str, Any]:
        """Return a status summary for the iRetail connection."""
        status = {
            'sql_connected': False,
            'bcp_available': False,
            'server': self.sql.server,
            'database': self.sql.database,
        }
        try:
            self.sql.fetch_dataframe("SELECT 1 AS ping")
            status['sql_connected'] = True
        except Exception:
            pass

        try:
            status['bcp_available'] = self.bcp.verify_connection()
        except Exception:
            pass

        return status
