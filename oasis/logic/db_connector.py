import logging
from typing import List, Dict, Any, Optional
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure Logging
logger = logging.getLogger("OasisDBConnector")

class SchemaMapper:
    """
    Maps external ERP/POS column names to Oasis internal standard format.
    Functions as a translation layer.
    """
    def __init__(self, mapping_config: Dict[str, str] = None):
        # Default Mapping (Fallbacks)
        self.mapping = {
            "sku": "item_code",
            "stock_qty": "current_stock",
            "description": "product_name",
            "retail_price": "selling_price",
            "cost": "cost_price",
            "barcode": "barcode",
            "supplier": "supplier_name"
        }
        if mapping_config:
            self.mapping.update(mapping_config)

    @classmethod
    def for_pos_erp(cls) -> "SchemaMapper":
        """Pre-configured mapper for the RXL POS/ERP schema."""
        return cls({
            # Item Master
            "ITM_CD": "item_code",
            "ITM_LONG_NAME": "product_name",
            "ITM_SHORT_NAME": "product_short_name",
            "SCAN_ITM_CD": "barcode",
            "HSN_CD": "hsn_code",
            "UOM_CD": "uom",
            "UOM_DESC": "uom_desc",
            "CATEGORY": "category",
            "DEPARTMENT": "department",
            # Stock Master
            "SM_QTY": "current_stocks",
            "SM_WAC": "wac",
            "SM_LAST_RECV_DT": "last_received_date",
            # Pricing
            "BSP_SP": "selling_price",
            "BSP_MRP": "mrp",
            "BCP_CP": "cost_price",
            # Organization
            "ORG_CD": "org_code",
            "ORG_NAME": "store_name",
            # Sales
            "QTY": "qty_sold",
            "NET_AMT": "net_amount",
            "TOTAL_VALUE": "total_value",
            "TAX_AMT": "tax_amount",
            # Supplier
            "SUPPLIER_CD": "supplier_cd",
            "SUPPLIER_NAME": "supplier_name",
        })

    def map_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """Translates a single record's keys."""
        new_record = {}
        for k, v in record.items():
            # Check if key is in mapping values (already mapped)
            if k in self.mapping.values():
                new_record[k] = v
                continue
            
            # Check if key is in mapping keys (needs mapping)
            found = False
            for map_k, map_v in self.mapping.items():
                if k.lower() == map_k.lower():
                    new_record[map_v] = v
                    found = True
                    break
            
            if not found:
                new_record[k] = v # Keep original if no map
        return new_record

    def map_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Renames DataFrame columns based on mapping."""
        rename_map = {}
        for col in df.columns:
            for map_k, map_v in self.mapping.items():
                if col.lower() == map_k.lower():
                    rename_map[col] = map_v
                    break
        return df.rename(columns=rename_map)


class UniversalConnector:
    """
    Generic Database Adapter using SQLAlchemy.
    Supports: Postgres, MySQL, MSSQL, Oracle, SQLite.
    """
    def __init__(self, connection_string: str, schema_mapper: SchemaMapper = None):
        self.connection_string = connection_string
        self.mapper = schema_mapper if schema_mapper else SchemaMapper()
        self.engine = None
        self._connect()

    def _connect(self):
        try:
            # Create connection pool
            self.engine = create_engine(self.connection_string, pool_pre_ping=True)
            logger.info("Database Engine created successfully.")
        except Exception as e:
            logger.error(f"Failed to create database engine: {e}")
            raise

    def test_connection(self) -> bool:
        """Ping the database."""
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            logger.error(f"Connection Test Failed: {e}")
            return False

    def fetch_stock_snapshot(self, query: str) -> List[Dict[str, Any]]:
        """
        Executes a query to fetch inventory snapshot.
        Returns a list of mapped dictionaries.
        """
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(query))
                # Convert to dict
                columns = result.keys()
                rows = [dict(zip(columns, row)) for row in result.fetchall()]
                
                # Setup Mapping
                mapped_rows = [self.mapper.map_record(r) for r in rows]
                return mapped_rows
        except Exception as e:
            logger.error(f"Error fetching stock snapshot: {e}")
            return []

    def fetch_sales_history(self, query: str) -> pd.DataFrame:
        """
        Fetches sales history as a Pandas DataFrame for analysis.
        """
        try:
            df = pd.read_sql(query, self.engine)
            return self.mapper.map_dataframe(df)
        except Exception as e:
            logger.error(f"Error fetching sales history: {e}")
            return pd.DataFrame()

    def push_purchase_order(self, po_data: Dict[str, Any], table_name: str = "integration_purchase_orders"):
        """
        Writes a generated PO back to the ERP integration table.
        """
        try:
            df = pd.DataFrame([po_data])
            # Append to table
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            logger.info(f"Pushed PO to {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error pushing PO: {e}")
            return False
