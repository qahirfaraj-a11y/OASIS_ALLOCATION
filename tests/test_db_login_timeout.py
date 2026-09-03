"""A database that is not there should say so quickly.

Without a login timeout the legacy "SQL Server" ODBC driver waits on its own
network layer before returning "SQL Server does not exist or access denied".
Measured against a closed port: 17.8 s without, 6.4 s with.

That is not a rare path. Every surface touching the POS paid it whenever the
server was down or its container had not come up yet — one map redraw cost 17
seconds for nothing, and the operator saw a spinner rather than an outage.
"""

import importlib
import os
import sys
from urllib.parse import quote_plus

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import db_connector as DC

_MSSQL = "mssql+pyodbc://u:p@host:1433/db?driver=" + quote_plus("SQL Server")


class TestTheLoginTimeout:
    def test_odbc_gets_one(self):
        assert DC.engine_options(_MSSQL)["connect_args"]["timeout"] == \
            DC.LOGIN_TIMEOUT_S

    def test_it_is_short_enough_to_be_worth_having(self):
        """The driver's own default is effectively 15-17 s. A timeout at or
        above that buys nothing."""
        assert 1 <= DC.LOGIN_TIMEOUT_S <= 10

    def test_sqlite_keeps_its_lock_wait(self):
        """SQLite's connect_args timeout is the LOCK wait, a different thing
        entirely, and must not be shortened to the login value."""
        args = DC.engine_options("sqlite:///x.db")["connect_args"]
        assert args["timeout"] == 30
        assert args["check_same_thread"] is False

    def test_a_non_odbc_backend_is_left_alone(self):
        """pyodbc's connect(timeout=) is what carries this. Passing the same
        keyword to a driver that reads it differently would be a guess."""
        assert DC.engine_options("postgresql://u:p@h/d")["connect_args"] == {}

    def test_the_environment_can_raise_it(self, monkeypatch):
        """A genuinely slow link should not be forced to fail."""
        monkeypatch.setenv("OASIS_DB_LOGIN_TIMEOUT", "30")
        reloaded = importlib.reload(DC)
        try:
            assert reloaded.LOGIN_TIMEOUT_S == 30
            assert reloaded.engine_options(_MSSQL)["connect_args"]["timeout"] == 30
        finally:
            monkeypatch.delenv("OASIS_DB_LOGIN_TIMEOUT", raising=False)
            importlib.reload(DC)

    def test_a_nonsense_value_does_not_break_startup(self, monkeypatch):
        monkeypatch.setenv("OASIS_DB_LOGIN_TIMEOUT", "not-a-number")
        reloaded = importlib.reload(DC)
        try:
            assert reloaded.LOGIN_TIMEOUT_S == 5
        finally:
            monkeypatch.delenv("OASIS_DB_LOGIN_TIMEOUT", raising=False)
            importlib.reload(DC)


class TestTheRestOfTheOptionsSurvived:
    """engine_options was extracted out of _connect so it could be tested at
    all. The extraction must not have dropped anything on the way."""

    def test_the_legacy_driver_workaround_is_still_applied(self):
        """The in-box 'SQL Server' driver dies with HY104 on setinputsizes
        before reading a single table."""
        assert DC.engine_options(_MSSQL)["use_setinputsizes"] is False

    @pytest.mark.parametrize("driver", ["ODBC Driver 17 for SQL Server",
                                        "ODBC Driver 18 for SQL Server"])
    def test_a_modern_driver_is_not_crippled(self, driver):
        opts = DC.engine_options(
            "mssql+pyodbc://u:p@h:1433/db?driver=" + quote_plus(driver))
        assert "use_setinputsizes" not in opts

    def test_driver_18_still_gets_the_login_timeout(self):
        """The workaround is keyed on the driver name; the timeout is keyed on
        pyodbc. Switching driver must not quietly drop the timeout with it."""
        url = ("mssql+pyodbc://u:p@h:1433/db?driver=" +
               quote_plus("ODBC Driver 18 for SQL Server") +
               "&TrustServerCertificate=yes")
        opts = DC.engine_options(url)
        assert opts["connect_args"]["timeout"] == DC.LOGIN_TIMEOUT_S
        assert opts["pool_pre_ping"] is True

    def test_trust_server_certificate_is_never_injected(self):
        """Driver 18 defaults to Encrypt=yes, so a URL without this fails on a
        self-signed certificate. Tempting to add it here — but that would turn
        off certificate verification for every client without their say. The
        URL is the operator's to write; .env.example documents it."""
        url = ("mssql+pyodbc://u:p@h:1433/db?driver=" +
               quote_plus("ODBC Driver 18 for SQL Server"))
        opts = DC.engine_options(url)
        assert "TrustServerCertificate" not in str(opts)

    def test_remote_backends_still_pool(self):
        opts = DC.engine_options(_MSSQL)
        assert opts["pool_size"] == 10 and opts["max_overflow"] == 20
        assert opts["pool_pre_ping"] is True
        assert opts["pool_recycle"] == 3600

    def test_sqlite_does_not_pool(self):
        """A file-backed database has nothing to pool, and QueuePool over one
        is how SQLite locking problems get invented."""
        opts = DC.engine_options("sqlite:///x.db")
        assert "pool_size" not in opts and "max_overflow" not in opts
