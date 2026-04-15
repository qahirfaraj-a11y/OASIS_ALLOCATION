"""
MSSQL Connector — Specialized wrapper around UniversalConnector for iRetail's SQL Server backend.

Adds:
- Windows Auth (Trusted Connection) support
- Named instance handling (e.g. SERVER\\INSTANCE)
- iRetail-specific query helpers for stock, sales and POS data
- Connection string builder with ODBC driver auto-detection
"""

import logging
import pandas as pd
from typing import Optional, Dict, Any, List

logger = logging.getLogger("MssqlConnector")

# ---------------------------------------------------------------------------
# ODBC Driver Detection
# ---------------------------------------------------------------------------

def detect_odbc_driver() -> str:
    """Auto-detect the best available MSSQL ODBC driver on this machine."""
    try:
        import pyodbc
        drivers = [d for d in pyodbc.drivers() if 'SQL Server' in d]
        # Prefer newest driver (e.g. "ODBC Driver 18 for SQL Server")
        drivers.sort(reverse=True)
        if drivers:
            return drivers[0]
    except ImportError:
        pass
    # Fallback to the legacy driver that ships with Windows
    return "SQL Server"


class MssqlConnector:
    """
    Thin wrapper for connecting to iRetail's MS SQL Server.

    Usage::

        conn = MssqlConnector(server="RETAILSRV\\IRETAIL", database="iRetailDB")
        conn.connect()
        df = conn.fetch_stock(store_id=101)
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        trusted_connection: bool = True,
        port: int = 1433,
        driver: Optional[str] = None,
    ):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted = trusted_connection
        self.port = port
        self.driver = driver or detect_odbc_driver()
        self._engine = None

    # ------------------------------------------------------------------
    # Connection Management
    # ------------------------------------------------------------------

    def build_connection_string(self) -> str:
        """Build a SQLAlchemy-compatible connection URL."""
        from urllib.parse import quote_plus
        if self.trusted:
            params = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                "Trusted_Connection=yes;"
                "TrustServerCertificate=yes;"
            )
        else:
            params = (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server},{self.port};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password};"
                "TrustServerCertificate=yes;"
            )
        return f"mssql+pyodbc:///?odbc_connect={quote_plus(params)}"

    def connect(self):
        """Create a SQLAlchemy engine with connection pooling."""
        from sqlalchemy import create_engine
        url = self.build_connection_string()
        self._engine = create_engine(
            url,
            pool_size=5,
            max_overflow=10,
            pool_pre_ping=True,    # Auto-reconnect on stale connections
            pool_recycle=3600,     # Recycle connections every hour
        )
        # Quick connectivity check
        with self._engine.connect() as conn:
            conn.execute("SELECT 1")
        logger.info(f"Connected to {self.server}/{self.database}")

    @property
    def engine(self):
        if self._engine is None:
            raise RuntimeError("Not connected. Call connect() first.")
        return self._engine

    def close(self):
        if self._engine:
            self._engine.dispose()
            self._engine = None
            logger.info("Connection pool closed.")

    # ------------------------------------------------------------------
    # iRetail Query Helpers
    # ------------------------------------------------------------------

    def fetch_dataframe(self, query: str, params: Optional[dict] = None) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""
        return pd.read_sql(query, self.engine, params=params)

    def fetch_stock(self, store_id: Optional[int] = None) -> pd.DataFrame:
        """
        Pull current stock levels from iRetail.
        Maps to OASIS fields: product_name, current_stocks, barcode, department.
        """
        query = """
            SELECT
                i.ItemName          AS product_name,
                s.OnHandQty         AS current_stocks,
                i.Barcode           AS barcode,
                d.DeptName          AS department,
                i.PackSize          AS pack_size,
                s.SellingPrice      AS selling_price,
                s.CostPrice         AS cost_price,
                sup.SupplierName    AS supplier_name
            FROM StockOnHand s
            INNER JOIN Items i      ON s.ItemCode = i.ItemCode
            LEFT JOIN Department d  ON i.DeptCode = d.DeptCode
            LEFT JOIN Supplier sup  ON i.SupplierCode = sup.SupplierCode
            WHERE 1=1
        """
        params = []
        if store_id:
            query += " AND s.StoreID = ?"
            params.append(store_id)
        return self.fetch_dataframe(query, params=params)

    def fetch_sales(self, days: int = 90, store_id: Optional[int] = None) -> pd.DataFrame:
        """
        Pull POS sales history for the last N days.
        Used for demand forecasting and velocity calculations.
        """
        query = """
            SELECT
                i.ItemName          AS product_name,
                i.Barcode           AS barcode,
                SUM(t.Qty)          AS total_qty_sold,
                COUNT(DISTINCT t.TransDate) AS days_with_sales,
                SUM(t.Qty * t.SellingPrice) AS total_revenue
            FROM Transactions t
            INNER JOIN Items i ON t.ItemCode = i.ItemCode
            WHERE t.TransDate >= DATEADD(day, -?, GETDATE())
        """
        params = [days]
        if store_id:
            query += " AND t.StoreID = ?"
            params.append(store_id)
        query += " GROUP BY i.ItemName, i.Barcode"
        return self.fetch_dataframe(query, params=params)

    def fetch_grn_history(self, days: int = 180) -> pd.DataFrame:
        """Pull GRN (Goods Received Notes) for supplier pattern analysis."""
        query = """
            SELECT
                g.GRNDate,
                i.ItemName          AS product_name,
                i.Barcode,
                g.ReceivedQty,
                sup.SupplierName    AS supplier_name,
                g.CostPrice
            FROM GRNDetail g
            INNER JOIN Items i      ON g.ItemCode = i.ItemCode
            LEFT JOIN Supplier sup  ON g.SupplierCode = sup.SupplierCode
            WHERE g.GRNDate >= DATEADD(day, -?, GETDATE())
            ORDER BY g.GRNDate DESC
        """
        params = [days]
        return self.fetch_dataframe(query, params=params)

    def execute(self, query: str, params: Optional[dict] = None) -> int:
        """Execute a non-SELECT statement (INSERT/UPDATE/DELETE). Returns rowcount."""
        with self.engine.begin() as conn:
            result = conn.execute(query, params or {})
            return result.rowcount
