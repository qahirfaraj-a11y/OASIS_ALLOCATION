"""The driver auto-detection must actually prefer the newest driver.

It did not. `sorted(reverse=True)` is alphabetical: "SQL Server" beats
"ODBC Driver 18 for SQL Server" on the first character, so a machine with
Driver 18 installed was handed the in-box legacy driver — the one that cannot
reflect a schema without the HY104 setinputsizes workaround. Measured on this
machine after installing Driver 18: detect_odbc_driver() returned "SQL Server".
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import mssql_connector as MC


def _drivers(monkeypatch, names):
    """Stand in for pyodbc.drivers() without needing pyodbc installed."""
    import types
    fake = types.ModuleType("pyodbc")
    fake.drivers = lambda: list(names)
    monkeypatch.setitem(sys.modules, "pyodbc", fake)


class TestDriverChoice:
    def test_the_newest_numbered_driver_wins(self, monkeypatch):
        _drivers(monkeypatch, ["SQL Server", "ODBC Driver 17 for SQL Server",
                               "ODBC Driver 18 for SQL Server"])
        assert MC.detect_odbc_driver() == "ODBC Driver 18 for SQL Server"

    def test_the_legacy_driver_never_outranks_a_numbered_one(self, monkeypatch):
        """The original defect, in one line: 'S' > 'O'."""
        _drivers(monkeypatch, ["SQL Server", "ODBC Driver 18 for SQL Server"])
        assert MC.detect_odbc_driver() != "SQL Server"

    def test_versions_compare_as_numbers_not_text(self, monkeypatch):
        """Alphabetically '9' > '18'."""
        _drivers(monkeypatch, ["ODBC Driver 9 for SQL Server",
                               "ODBC Driver 18 for SQL Server"])
        assert MC.detect_odbc_driver() == "ODBC Driver 18 for SQL Server"

    def test_order_reported_by_the_system_does_not_matter(self, monkeypatch):
        for order in ([" ODBC Driver 18 for SQL Server", "SQL Server"],
                      ["SQL Server", "ODBC Driver 18 for SQL Server "]):
            _drivers(monkeypatch, order)
            assert MC.detect_odbc_driver().strip() == \
                "ODBC Driver 18 for SQL Server"

    def test_only_the_in_box_driver_installed(self, monkeypatch):
        """A client with nothing else must still be given a working answer."""
        _drivers(monkeypatch, ["SQL Server"])
        assert MC.detect_odbc_driver() == "SQL Server"

    def test_native_client_is_not_mistaken_for_a_numbered_driver(self, monkeypatch):
        """'SQL Server Native Client 11.0' carries a number that is not an
        ODBC Driver version. It must not outrank Driver 18."""
        _drivers(monkeypatch, ["SQL Server Native Client 11.0",
                               "ODBC Driver 18 for SQL Server"])
        assert MC.detect_odbc_driver() == "ODBC Driver 18 for SQL Server"

    def test_no_sql_server_driver_at_all(self, monkeypatch):
        _drivers(monkeypatch, ["Microsoft Access Driver (*.mdb, *.accdb)"])
        assert MC.detect_odbc_driver() == "SQL Server"

    def test_pyodbc_missing_does_not_raise(self, monkeypatch):
        """Detection runs on machines where the driver stack is absent."""
        monkeypatch.setitem(sys.modules, "pyodbc", None)
        import builtins
        real = builtins.__import__

        def _fail(name, *a, **kw):
            if name == "pyodbc":
                raise ImportError("no pyodbc")
            return real(name, *a, **kw)

        monkeypatch.setattr(builtins, "__import__", _fail)
        assert MC.detect_odbc_driver() == "SQL Server"


class TestTheBuiltStringStaysUsable:
    """Driver 18 defaults to Encrypt=yes. The builder must keep saying what to
    do about the certificate, or every string it produces breaks on upgrade."""

    def test_trusted_connection_string_trusts_the_certificate(self, monkeypatch):
        _drivers(monkeypatch, ["ODBC Driver 18 for SQL Server"])
        c = MC.MssqlConnector(server="host", database="db")
        s = c.build_connection_string()
        assert "TrustServerCertificate%3Dyes" in s or \
            "TrustServerCertificate=yes" in s

    def test_sql_auth_string_trusts_the_certificate(self, monkeypatch):
        _drivers(monkeypatch, ["ODBC Driver 18 for SQL Server"])
        c = MC.MssqlConnector(server="host", database="db", username="u",
                              password="p", trusted_connection=False)
        s = c.build_connection_string()
        assert "TrustServerCertificate%3Dyes" in s or \
            "TrustServerCertificate=yes" in s

    def test_an_explicit_driver_is_respected(self):
        """Detection is a default, not a policy. An operator naming a driver
        has a reason — usually that it is the only one installed."""
        c = MC.MssqlConnector(server="h", database="d",
                              driver="ODBC Driver 17 for SQL Server")
        assert c.driver == "ODBC Driver 17 for SQL Server"
