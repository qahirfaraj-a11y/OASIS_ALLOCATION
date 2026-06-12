"""
OASIS Centralized Logging Configuration
=======================================
Single place to configure logging for all OASIS processes (APIs, dashboards,
pipelines, schedulers). Call ``configure_logging()`` once at process startup;
it takes over the root logger so the scattered module-level
``logging.basicConfig`` calls become harmless no-ops.

Environment variables:
    OASIS_LOG_LEVEL   DEBUG | INFO | WARNING | ERROR  (default: INFO)
    OASIS_LOG_FORMAT  text | json                     (default: text)
    OASIS_LOG_FILE    Optional path; if set, logs also go to this file
                      with rotation (5 MB x 3 backups).
"""

import json
import logging
import logging.handlers
import os
import sys
from datetime import datetime, timezone

_TEXT_FORMAT = "%(asctime)s [%(name)s] %(levelname)s: %(message)s"


class JSONFormatter(logging.Formatter):
    """Render log records as single-line JSON for log aggregators."""

    def format(self, record: logging.LogRecord) -> str:
        entry = {
            "ts": datetime.fromtimestamp(
                record.created, tz=timezone.utc
            ).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            entry["exc_info"] = self.formatException(record.exc_info)
        # Extra fields passed via logger.info("...", extra={...})
        for key, value in record.__dict__.items():
            if key in ("args", "msg", "exc_info", "exc_text", "stack_info",
                       "created", "msecs", "relativeCreated", "levelname",
                       "levelno", "name", "pathname", "filename", "module",
                       "funcName", "lineno", "thread", "threadName",
                       "process", "processName", "taskName"):
                continue
            try:
                json.dumps(value)
                entry[key] = value
            except (TypeError, ValueError):
                entry[key] = repr(value)
        return json.dumps(entry, ensure_ascii=False)


def configure_logging(
    level: str = None,
    fmt: str = None,
    log_file: str = None,
) -> None:
    """
    Configure the root logger for an OASIS process. Idempotent: replaces
    any handlers installed by earlier basicConfig calls (force=True), so
    it is safe regardless of import order.

    Args:
        level:    Override OASIS_LOG_LEVEL (DEBUG/INFO/WARNING/ERROR)
        fmt:      Override OASIS_LOG_FORMAT ("text" or "json")
        log_file: Override OASIS_LOG_FILE (path for rotating file handler)
    """
    level = (level or os.getenv("OASIS_LOG_LEVEL", "INFO")).upper()
    fmt = (fmt or os.getenv("OASIS_LOG_FORMAT", "text")).lower()
    log_file = log_file or os.getenv("OASIS_LOG_FILE")

    if fmt == "json":
        formatter: logging.Formatter = JSONFormatter()
    else:
        formatter = logging.Formatter(_TEXT_FORMAT)

    handlers: list = []
    console = logging.StreamHandler(sys.stderr)
    console.setFormatter(formatter)
    handlers.append(console)

    if log_file:
        os.makedirs(os.path.dirname(os.path.abspath(log_file)), exist_ok=True)
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=5 * 1024 * 1024, backupCount=3,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        handlers.append(file_handler)

    logging.basicConfig(
        level=getattr(logging, level, logging.INFO),
        handlers=handlers,
        force=True,
    )

    # Quieten noisy third-party loggers unless explicitly in DEBUG mode.
    if level != "DEBUG":
        for noisy in ("urllib3", "httpx", "apscheduler", "watchdog",
                      "sqlalchemy.engine"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
