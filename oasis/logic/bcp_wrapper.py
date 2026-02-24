"""
BCP Wrapper — Python interface to Microsoft's Bulk Copy Program (bcp.exe).

Used for high-performance data transfer between OASIS and iRetail's MSSQL backend.
Supports both export (query → file) and import (file → table) operations.
"""

import os
import subprocess
import logging
import tempfile
from typing import Optional, List

logger = logging.getLogger("BcpWrapper")

# Common bcp.exe locations on Windows
_BCP_SEARCH_PATHS = [
    r"C:\Rxl\bcp.exe",
    r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\170\Tools\Binn\bcp.exe",
    r"C:\Program Files\Microsoft SQL Server\Client SDK\ODBC\180\Tools\Binn\bcp.exe",
    r"C:\Program Files\Microsoft SQL Server\110\Tools\Binn\bcp.exe",
    r"C:\Program Files\Microsoft SQL Server\150\Tools\Binn\bcp.exe",
]


def find_bcp() -> str:
    """Locate bcp.exe on the system. Checks known paths then falls back to PATH."""
    for path in _BCP_SEARCH_PATHS:
        if os.path.isfile(path):
            return path
    # Fallback: assume it's on PATH
    return "bcp"


class BcpWrapper:
    """
    Wraps bcp.exe for bulk data operations against SQL Server.

    Usage::

        bcp = BcpWrapper(server="RETAILSRV\\IRETAIL", database="iRetailDB")
        bcp.export_query("SELECT * FROM StockOnHand", "stock_snapshot.csv")
        bcp.import_file("staging_orders.csv", "dbo.PurchaseOrderStaging")
    """

    def __init__(
        self,
        server: str,
        database: str,
        username: Optional[str] = None,
        password: Optional[str] = None,
        trusted_connection: bool = True,
        bcp_path: Optional[str] = None,
    ):
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.trusted = trusted_connection
        self.bcp_path = bcp_path or find_bcp()

    # ------------------------------------------------------------------
    # Internal Helpers
    # ------------------------------------------------------------------

    def _auth_args(self) -> List[str]:
        """Build authentication arguments for bcp."""
        if self.trusted:
            return ["-T"]  # Trusted/Windows Auth
        return ["-U", self.username, "-P", self.password]

    def _run_bcp(self, args: List[str], description: str) -> subprocess.CompletedProcess:
        """Execute bcp with the given arguments. Raises on failure."""
        cmd = [self.bcp_path] + args
        logger.info(f"BCP {description}: {' '.join(cmd[:6])}...")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=600,  # 10 minute timeout for large transfers
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip() or result.stdout.strip()
            logger.error(f"BCP {description} FAILED (rc={result.returncode}): {error_msg}")
            raise RuntimeError(f"BCP {description} failed: {error_msg}")

        # Parse row count from stdout (e.g. "1234 rows copied.")
        rows_copied = 0
        for line in result.stdout.split('\n'):
            if 'rows copied' in line.lower():
                try:
                    rows_copied = int(line.strip().split()[0])
                except (ValueError, IndexError):
                    pass

        logger.info(f"BCP {description}: {rows_copied} rows transferred.")
        return result

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def export_query(
        self,
        query: str,
        output_file: str,
        field_terminator: str = ",",
        row_terminator: str = "\\n",
        code_page: str = "65001",  # UTF-8
    ) -> str:
        """
        Export a SQL query result to a flat file using BCP queryout.

        Args:
            query: SQL SELECT statement
            output_file: Path for the output file
            field_terminator: Column delimiter (default comma for CSV)
            row_terminator: Row delimiter
            code_page: Character encoding (65001 = UTF-8)

        Returns:
            Path to the output file.
        """
        args = [
            query,
            "queryout", output_file,
            "-S", self.server,
            "-d", self.database,
            "-c",  # Character mode
            "-t", field_terminator,
            "-r", row_terminator,
            "-C", code_page,
        ] + self._auth_args()

        self._run_bcp(args, f"EXPORT → {os.path.basename(output_file)}")
        return output_file

    def export_table(
        self,
        table_name: str,
        output_file: str,
        field_terminator: str = ",",
    ) -> str:
        """
        Export an entire table to a flat file using BCP out.

        Args:
            table_name: Fully qualified table name (e.g. dbo.StockOnHand)
            output_file: Path for the output file

        Returns:
            Path to the output file.
        """
        args = [
            f"{self.database}.{table_name}",
            "out", output_file,
            "-S", self.server,
            "-c",
            "-t", field_terminator,
            "-C", "65001",
        ] + self._auth_args()

        self._run_bcp(args, f"TABLE EXPORT {table_name}")
        return output_file

    def import_file(
        self,
        input_file: str,
        table_name: str,
        field_terminator: str = ",",
        row_terminator: str = "\\n",
        first_row: int = 2,  # Skip header row
        batch_size: int = 10000,
        format_file: Optional[str] = None,
    ) -> None:
        """
        Bulk-load a flat file into an MSSQL table using BCP in.

        Args:
            input_file: Path to the data file
            table_name: Target table (e.g. dbo.PurchaseOrderStaging)
            first_row: First data row (2 = skip header)
            batch_size: Commit interval
            format_file: Optional BCP format file for complex mappings
        """
        if not os.path.isfile(input_file):
            raise FileNotFoundError(f"Input file not found: {input_file}")

        args = [
            f"{self.database}.{table_name}",
            "in", input_file,
            "-S", self.server,
            "-c",
            "-t", field_terminator,
            "-r", row_terminator,
            "-F", str(first_row),
            "-b", str(batch_size),
            "-C", "65001",
        ]

        if format_file:
            args.extend(["-f", format_file])

        args += self._auth_args()
        self._run_bcp(args, f"IMPORT {os.path.basename(input_file)} → {table_name}")

    def verify_connection(self) -> bool:
        """Quick connectivity test: export a trivial query."""
        try:
            tmp = os.path.join(tempfile.gettempdir(), "bcp_test.txt")
            self.export_query("SELECT 1 AS test", tmp)
            os.remove(tmp)
            logger.info("BCP connectivity verified.")
            return True
        except Exception as e:
            logger.error(f"BCP connectivity test failed: {e}")
            return False
