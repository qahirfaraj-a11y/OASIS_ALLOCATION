"""Tests for the centralized logging configuration (oasis.logging_config)."""

import json
import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logging_config import configure_logging, JSONFormatter


def _make_record(msg="hello", **extra):
    record = logging.LogRecord(
        name="TestLogger", level=logging.INFO, pathname=__file__,
        lineno=1, msg=msg, args=(), exc_info=None,
    )
    for k, v in extra.items():
        setattr(record, k, v)
    return record


class TestJSONFormatter:
    def test_basic_fields(self):
        out = json.loads(JSONFormatter().format(_make_record("hello world")))
        assert out["message"] == "hello world"
        assert out["level"] == "INFO"
        assert out["logger"] == "TestLogger"
        assert "ts" in out

    def test_extra_fields_included(self):
        out = json.loads(
            JSONFormatter().format(_make_record("po created", po_id=42, org="NBO01"))
        )
        assert out["po_id"] == 42
        assert out["org"] == "NBO01"

    def test_non_serializable_extra_becomes_repr(self):
        out = json.loads(
            JSONFormatter().format(_make_record("x", weird=object()))
        )
        assert out["weird"].startswith("<object object")

    def test_single_line_output(self):
        formatted = JSONFormatter().format(_make_record("multi\nline"))
        assert "\n" not in formatted


class TestConfigureLogging:
    def test_replaces_existing_handlers(self):
        logging.basicConfig(level=logging.DEBUG)
        configure_logging(level="WARNING", fmt="text")
        root = logging.getLogger()
        assert root.level == logging.WARNING
        assert len(root.handlers) == 1

    def test_json_format_selected(self):
        configure_logging(level="INFO", fmt="json")
        root = logging.getLogger()
        assert isinstance(root.handlers[0].formatter, JSONFormatter)

    def test_env_var_level(self, monkeypatch):
        monkeypatch.setenv("OASIS_LOG_LEVEL", "ERROR")
        monkeypatch.delenv("OASIS_LOG_FORMAT", raising=False)
        monkeypatch.delenv("OASIS_LOG_FILE", raising=False)
        configure_logging()
        assert logging.getLogger().level == logging.ERROR

    def test_log_file_handler_added(self, tmp_path, monkeypatch):
        monkeypatch.delenv("OASIS_LOG_LEVEL", raising=False)
        monkeypatch.delenv("OASIS_LOG_FORMAT", raising=False)
        log_file = tmp_path / "oasis.log"
        configure_logging(log_file=str(log_file))
        logging.getLogger("FileTest").info("write me")
        for h in logging.getLogger().handlers:
            h.flush()
        assert log_file.exists()
        assert "write me" in log_file.read_text(encoding="utf-8")
        # Reset to plain config so later tests aren't holding the file open
        configure_logging(level="INFO", fmt="text", log_file=None)
