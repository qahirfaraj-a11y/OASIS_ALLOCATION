"""Alert thresholds must actually be read, not silently defaulted.

_load_thresholds passed engine.url -- a SQLAlchemy URL object -- to
load_system_config, which takes a file path and hands it to sqlite3.connect.
Every call raised "expected str, bytes or os.PathLike object, not URL", was
swallowed into {}, and every threshold fell back to its hardcoded default. The
only visible symptom was one ERROR line per refresh, and on a console with
10-second auto-refresh that reads as log noise rather than a broken feature.
"""
import json
import os
import sqlite3

import pytest

from oasis.logic import notification_service as ns


class _FakeEngine:
    def __init__(self, url):
        self.url = url        # a real engine exposes a URL object, not a str


class _FakeConnector:
    def __init__(self, url):
        self.engine = _FakeEngine(url)


class _URLLike:
    """Stands in for sqlalchemy.engine.URL: str()-able, not a path."""

    def __init__(self, s):
        self._s = s

    def __str__(self):
        return self._s

    def __fspath__(self):
        raise TypeError("URL is not a path")


def _svc(url):
    svc = ns.NotificationService.__new__(ns.NotificationService)
    svc.db_connector = _FakeConnector(url)
    return svc


@pytest.fixture
def store(tmp_path):
    p = tmp_path / "store.db"
    c = sqlite3.connect(str(p))
    c.execute("CREATE TABLE OASIS_SYSTEM_CONFIG ("
              "CONFIG_KEY TEXT, CONFIG_VALUE TEXT)")
    c.execute("INSERT INTO OASIS_SYSTEM_CONFIG VALUES (?,?)",
              ("alert_stockout_threshold", "7"))
    c.commit()
    c.close()
    return p


def test_a_sqlite_store_yields_its_configured_thresholds(store):
    svc = _svc(_URLLike(f"sqlite:///{store}"))
    got = svc._load_thresholds()
    assert got.get("alert_stockout_threshold") == "7", (
        "the tuned threshold did not reach the notification service")


def test_a_url_object_does_not_raise_its_way_to_an_empty_dict(store):
    """The regression: a URL that str()s fine but is not a path."""
    svc = _svc(_URLLike(f"sqlite:///{store}"))
    assert svc._load_thresholds() != {}


def test_a_remote_store_is_empty_but_quiet():
    """No local file to open, so {} -- the old result, minus the exception."""
    svc = _svc(_URLLike("mssql+pyodbc://host/db?driver=ODBC+Driver+18"))
    assert svc._load_thresholds() == {}


def test_a_missing_file_is_still_survivable(tmp_path):
    svc = _svc(_URLLike(f"sqlite:///{tmp_path / 'nope.db'}"))
    assert svc._load_thresholds() == {}
