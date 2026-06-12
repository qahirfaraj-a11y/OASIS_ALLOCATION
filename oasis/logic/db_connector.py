import logging
import time
import sqlite3
from typing import List, Dict, Any, Optional
from functools import wraps
import pandas as pd
from sqlalchemy import create_engine, text, inspect, event
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Configure Logging
logger = logging.getLogger("OasisDBConnector")

# ---------------------------------------------------------------------------
# Centralized SQLite Helpers & Listeners (v10.3 Auth/WAL Hardening)
# ---------------------------------------------------------------------------

@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    """Activates high-concurrency WAL mode and optimal PRAGMAS on SQLAlchemy engines."""
    if type(dbapi_connection).__module__ == "sqlite3":
        try:
            cursor = dbapi_connection.cursor()
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=30000")
            cursor.close()
        except Exception as e:
            logger.debug(f"Could not config SQLite PRAGMAs: {e}")

def get_sqlite_conn(db_path: str = None, timeout: float = 30.0) -> sqlite3.Connection:
    """
    Returns a raw database connection configured for high concurrency.

    If db_path is given, opens that SQLite file directly.
    If db_path is None, delegates to the centralized oasis.logic.db module
    which reads OASIS_DB_URL (supports both SQLite and PostgreSQL).
    """
    if db_path:
        conn = sqlite3.connect(db_path, timeout=timeout)
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn
    from . import db as oasis_db
    return oasis_db.get_raw_connection()


# ---------------------------------------------------------------------------
# Retry Decorator
# ---------------------------------------------------------------------------

def with_retry(max_retries: int = 3, base_delay: float = 1.0, backoff_factor: float = 2.0):
    """
    Decorator that retries a method on failure with exponential backoff.
    
    Args:
        max_retries:    Maximum number of retry attempts
        base_delay:     Initial delay in seconds
        backoff_factor: Multiplier for each subsequent retry
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries:
                        delay = base_delay * (backoff_factor ** attempt)
                        logger.warning(
                            f"[Retry {attempt+1}/{max_retries}] {func.__name__} failed: {e}. "
                            f"Retrying in {delay:.1f}s..."
                        )
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} retries: {e}"
                        )
            if last_exception is not None:
                raise last_exception
            raise RuntimeError(f"{func.__name__} failed without an exception")
        return wrapper
    return decorator


class SchemaMapper:
    """
    Maps external ERP/POS column names to Oasis internal standard format.
    Functions as a translation layer.
    """
    def __init__(self, mapping_config: Optional[Dict[str, str]] = None):
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
    
    Features:
    - Automatic retry with exponential backoff on transient failures
    - Health check endpoint for monitoring
    - Auto-reconnect on connection loss
    """
    def __init__(self, connection_string: str, schema_mapper: Optional[SchemaMapper] = None,
                 max_retries: int = 3, retry_delay: float = 1.0):
        self.connection_string = connection_string
        self.mapper = schema_mapper if schema_mapper else SchemaMapper()
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.engine = None
        self._connect()

    def _connect(self):
        try:
            # Production-grade engine options
            connect_args = {}
            engine_kwargs = {
                "connect_args": connect_args,
                "pool_pre_ping": True,
                "pool_recycle": 3600
            }
            
            if self.connection_string.startswith("sqlite"):
                # Essential for Streamlit/Multi-threaded use of SQLite
                connect_args["check_same_thread"] = False
                connect_args["timeout"] = 30
            else:
                # Add pooling for remote DBs (Postgres/MySQL)
                engine_kwargs["pool_size"] = 10
                engine_kwargs["max_overflow"] = 20
                
            self.engine = create_engine(self.connection_string, **engine_kwargs)
            logger.info(f"Database Engine initialized (Back-end: {'SQLite' if 'sqlite' in self.connection_string else 'Remote'}).")
        except Exception as e:
            logger.error(f"CRITICAL: Failed to initialize database engine: {e}")
            raise

    def _ensure_connection(self):
        """Re-establish connection if engine is None or broken."""
        if self.engine is None:
            self._connect()
        
        if self.engine is not None:
            try:
                with self.engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
            except Exception:
                logger.warning("Connection lost, attempting to reconnect...")
                self._connect()
        else:
            logger.error("Engine could not be initialized.")

    def test_connection(self) -> bool:
        """Ping the database."""
        if self.engine is None:
            logger.error("Connection Test Failed: Engine is None")
            return False
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except SQLAlchemyError as e:
            logger.error(f"Connection Test Failed: {e}")
            return False

    def health_check(self) -> Dict[str, Any]:
        """
        Comprehensive health check returning connection status, latency, 
        and table availability.
        
        Returns:
            {
                "connected": bool,
                "latency_ms": float,
                "tables_found": int,
                "status": "healthy" | "degraded" | "down"
            }
        """
        result = {
            "connected": False,
            "latency_ms": 0.0,
            "tables_found": 0,
            "status": "down",
        }
        
        try:
            start = time.time()
            if self.engine is None:
                result["status"] = "down"
                return result

            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            result["latency_ms"] = float(round((time.time() - start) * 1000, 1))
            result["connected"] = True
            
            # Check core tables exist
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            result["tables_found"] = len(tables)
            
            core_tables = {"ITEM_MST", "STOCK_MASTER", "POS_SALES_DTL", "ORGANIZATION_MST"}
            missing = core_tables - set(tables)
            
            if not missing:
                result["status"] = "healthy"
            else:
                result["status"] = "degraded"
                logger.warning(f"Missing tables: {missing}")
                
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            result["status"] = "down"
        
        return result

    @with_retry(max_retries=3, base_delay=0.5)
    def fetch_stock_snapshot(self, query: str) -> List[Dict[str, Any]]:
        """
        Executes a query to fetch inventory snapshot.
        Returns a list of mapped dictionaries.
        """
        self._ensure_connection()
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
            raise

    @with_retry(max_retries=3, base_delay=0.5)
    def fetch_sales_history(self, query: str) -> pd.DataFrame:
        """
        Fetches sales history as a Pandas DataFrame for analysis.
        """
        self._ensure_connection()
        try:
            df = pd.read_sql(query, self.engine)
            return self.mapper.map_dataframe(df)
        except Exception as e:
            logger.error(f"Error fetching sales history: {e}")
            raise

    @with_retry(max_retries=3, base_delay=0.5)
    def push_purchase_order(self, po_data: Dict[str, Any], table_name: str = "integration_purchase_orders"):
        """
        Writes a generated PO back to the ERP integration table.
        """
        self._ensure_connection()
        try:
            df = pd.DataFrame([po_data])
            # Append to table
            df.to_sql(table_name, self.engine, if_exists='append', index=False)
            logger.info(f"Pushed PO to {table_name}")
            return True
        except Exception as e:
            logger.error(f"Error pushing PO: {e}")
            raise


# ---------------------------------------------------------------------------
# System Config Helpers 
# ---------------------------------------------------------------------------

def load_system_config(db_path: str) -> Dict[str, str]:
    """Load all system config as a key-value dict."""
    try:
        conn = get_sqlite_conn(db_path)
        rows = conn.execute(
            "SELECT CONFIG_KEY, CONFIG_VALUE FROM OASIS_SYSTEM_CONFIG"
        ).fetchall()
        conn.close()
        return {row[0]: row[1] for row in rows}
    except Exception as e:
        logger.error(f"Failed to load system config: {e}")
        return {}


def load_system_config_full(db_path: str) -> List[Dict[str, str]]:
    """Load all system config with metadata (group, description, etc)."""
    try:
        conn = get_sqlite_conn(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM OASIS_SYSTEM_CONFIG ORDER BY CONFIG_GROUP, CONFIG_KEY"
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error(f"Failed to load system config: {e}")
        return []


def save_system_config(db_path: str, key: str, value: str, updated_by: str = "system"):
    """Save a single config key-value pair."""
    from datetime import datetime
    try:
        conn = get_sqlite_conn(db_path)
        conn.execute(
            """INSERT INTO OASIS_SYSTEM_CONFIG (CONFIG_KEY, CONFIG_VALUE, UPDATED_BY, UPDATED_DT)
               VALUES (?, ?, ?, ?)
               ON CONFLICT(CONFIG_KEY) DO UPDATE SET 
                   CONFIG_VALUE = excluded.CONFIG_VALUE,
                   UPDATED_BY = excluded.UPDATED_BY,
                   UPDATED_DT = excluded.UPDATED_DT""",
            (key, value, updated_by, datetime.now().isoformat())
        )
        conn.commit()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"Failed to save config {key}: {e}")
        return False


def ensure_oasis_tables(db_path: str):
    """
    Ensure OASIS-specific tables exist in the database.
    Called on startup to migrate existing DBs without requiring a full rebuild.
    """
    import sqlite3
    migration_sql = """
    CREATE TABLE IF NOT EXISTS OASIS_USERS (
    TENANT_ID TEXT DEFAULT 'default_tenant',
        USER_ID       INTEGER PRIMARY KEY AUTOINCREMENT,
        USERNAME      TEXT UNIQUE NOT NULL,
        PASSWORD_HASH TEXT NOT NULL,
        DISPLAY_NAME  TEXT NOT NULL,
        ROLE          TEXT NOT NULL CHECK(ROLE IN ('branch_manager','regional_manager','ops_admin')),
        ASSIGNED_ORG  TEXT,
        EMAIL         TEXT,
        ACTIVE_FLAG   TEXT DEFAULT 'Y',
        CREATED_DT    TEXT,
        LAST_LOGIN_DT TEXT
    );

    CREATE TABLE IF NOT EXISTS OASIS_AUDIT_LOG (
    TENANT_ID TEXT DEFAULT 'default_tenant',
        LOG_ID        INTEGER PRIMARY KEY AUTOINCREMENT,
        USERNAME      TEXT NOT NULL,
        ACTION        TEXT NOT NULL,
        ENTITY_TYPE   TEXT,
        ENTITY_ID     TEXT,
        ORG_CD        TEXT,
        DETAILS       TEXT,
        CREATED_DT    TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS OASIS_SYSTEM_CONFIG (
    TENANT_ID TEXT DEFAULT 'default_tenant',
        CONFIG_KEY    TEXT PRIMARY KEY,
        CONFIG_VALUE  TEXT NOT NULL,
        CONFIG_GROUP  TEXT DEFAULT 'general',
        DESCRIPTION   TEXT,
        UPDATED_BY    TEXT,
        UPDATED_DT    TEXT
    );
    
    CREATE TABLE IF NOT EXISTS INTEGRATION_TRANSFER_ORDERS (
    TENANT_ID TEXT DEFAULT 'default_tenant',
        TRANSFER_ID   INTEGER PRIMARY KEY AUTOINCREMENT,
        FROM_ORG_CD   TEXT NOT NULL,
        TO_ORG_CD     TEXT NOT NULL,
        ITM_CD        TEXT NOT NULL,
        PRODUCT_NAME  TEXT,
        QUANTITY      REAL DEFAULT 0,
        VALUE_KES     REAL DEFAULT 0,
        STATUS        TEXT DEFAULT 'REQUESTED',
        URGENCY       TEXT DEFAULT 'NORMAL',
        REQUESTED_BY  TEXT,
        CREATED_DT    TEXT,
        COMPLETED_DT  TEXT
    );
    CREATE TABLE IF NOT EXISTS OASIS_SESSIONS (
    TENANT_ID TEXT DEFAULT 'default_tenant',
        SESSION_ID    TEXT PRIMARY KEY,
        USERNAME      TEXT NOT NULL,
        CREATED_DT    TEXT NOT NULL,
        EXPIRES_DT    TEXT NOT NULL,
        IS_REVOKED    INTEGER DEFAULT 0
    );
    """
    try:
        conn = get_sqlite_conn(db_path)
        conn.executescript(migration_sql)
        
        # Schema Migrations (v10.3 Auth Hardening)
        try:
            conn.execute("ALTER TABLE OASIS_USERS ADD COLUMN FAILED_ATTEMPTS INTEGER DEFAULT 0")
        except sqlite3.OperationalError:
            pass # Column exists
            
        try:
            conn.execute("ALTER TABLE OASIS_USERS ADD COLUMN LOCKOUT_UNTIL TEXT")
        except sqlite3.OperationalError:
            pass # Column exists
            
        conn.commit()
        
        # Always attempt to seed default users (INSERT OR IGNORE handles existing ones)
        from .auth_manager import seed_users
        seed_users(db_path)
            
        # Seed config if empty or missing new keys
        from datetime import datetime
        now = datetime.now().isoformat()
        configs = [
            ("spike_threshold_pct", "200.0", "alerting", "Velocity spike detection threshold (%)"),
            ("stockout_warning_hours", "8", "alerting", "Hours-to-stockout warning threshold"),
            ("safety_stock_days", "14", "ordering", "Default safety stock in days"),
            ("min_order_value_kes", "5000", "ordering", "Minimum order value (KES)"),
            ("max_supplier_concentration_pct", "40", "ordering", "Max % of budget from single supplier"),
            ("max_transfer_cost_kes", "500", "transfers", "Maximum cost per inter-branch transfer (KES)"),
            ("min_excess_ratio", "2.0", "transfers", "Minimum excess ratio before donating stock"),
            ("fresh_transfer_max_hours", "6", "transfers", "Max hours for fresh item transfers"),
            ("gnn_risk_blend_ratio", "0.5", "transfers", "Blend ratio for GNN risk vs Deterministic inventory risk (0.0 to 1.0)"),
            ("auto_approve_po_below_kes", "50000", "ordering", "Auto-approve POs below this value"),
            ("simulation_seed", "42", "general", "Random seed for simulation reproducibility"),
            ("shadow_audit_pulse_time", "19:00", "shadow_mode", "Daily forensic audit time (HH:MM)"),
        ]
        conn.executemany(
            """INSERT OR IGNORE INTO OASIS_SYSTEM_CONFIG
               (CONFIG_KEY, CONFIG_VALUE, CONFIG_GROUP, DESCRIPTION, UPDATED_BY, UPDATED_DT)
               VALUES (?,?,?,?,?,?)""",
            [(k, v, g, d, "system", now) for k, v, g, d in configs]
        )
        conn.commit()
        conn.close()
        logger.info("OASIS tables/users verified successfully")
    except Exception as e:
        logger.error(f"Failed to ensure OASIS tables: {e}")
