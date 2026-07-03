"""Tests for the OASIS Home launcher helpers."""

import os
import socket
import sqlite3
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.ui.home import (
    CONSOLES, console_cards, latest_file, port_live, system_snapshot,
)


class TestConsoleCards:
    def test_covers_all_consoles_with_urls(self):
        cards = console_cards(check=lambda p: p == 8500)   # only ops "live"
        assert {c["key"] for c in cards} == {c["key"] for c in CONSOLES}
        ops = next(c for c in cards if c["key"] == "ops")
        intel = next(c for c in cards if c["key"] == "intel")
        assert ops["live"] is True and ops["url"] == "http://localhost:8500"
        assert intel["live"] is False

    def test_ports_are_distinct(self):
        ports = [c["port"] for c in CONSOLES]
        assert len(ports) == len(set(ports))


class TestPortLive:
    def test_detects_listening_socket(self):
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        port = srv.getsockname()[1]
        try:
            assert port_live(port) is True
        finally:
            srv.close()

    def test_closed_port_is_down(self):
        s = socket.socket()
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
        s.close()                      # nothing listening now
        assert port_live(port) is False


class TestSystemSnapshot:
    def test_reads_store_stats(self, tmp_path):
        db = str(tmp_path / "s.db")
        c = sqlite3.connect(db)
        c.executescript("""
            CREATE TABLE POS_SALES_HDR (BILL_DT TEXT);
            CREATE TABLE STOCK_MASTER (SM_QTY REAL);
            INSERT INTO POS_SALES_HDR VALUES (date('now','localtime'));
            INSERT INTO STOCK_MASTER VALUES (5);
            INSERT INTO STOCK_MASTER VALUES (0);
        """)
        c.commit()
        c.close()
        snap = system_snapshot(db)
        assert snap["db_exists"] and snap["bills_today"] == 1
        assert snap["skus"] == 2 and snap["stockouts"] == 1

    def test_missing_db_fail_soft(self):
        snap = system_snapshot("/no/such.db")
        assert snap["db_exists"] is False and snap["bills_today"] == 0


class TestLatestFile:
    def test_newest_wins(self, tmp_path):
        a = tmp_path / "old.md"
        b = tmp_path / "new.md"
        a.write_text("a")
        time.sleep(0.02)
        b.write_text("b")
        os.utime(b, None)
        assert latest_file(str(tmp_path), ".md") == str(b)
        assert latest_file(str(tmp_path), ".zip") is None

    def test_missing_dir(self):
        assert latest_file("/no/such/dir") is None
