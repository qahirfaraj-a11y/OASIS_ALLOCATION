"""
O.A.S.I.S. Windows Service Wrapper
=====================================
Runs the Scheduler, FileWatcher, and Heartbeat as a Windows Service
that starts on boot and runs invisibly in the background.

Install (Admin required):
    python oasis_service.py install
    python oasis_service.py start

Run directly (no service registration, useful for testing):
    python oasis_service.py --run-direct

Uninstall:
    python oasis_service.py stop
    python oasis_service.py remove
"""

import os
import sys
import time
import logging
import threading
import argparse
from pathlib import Path

# Ensure project root is in sys.path
PROJECT_ROOT = str(Path(__file__).parent.resolve())
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# ── Logging Setup ─────────────────────────────────────────────────────
LOG_DIR = os.path.join(PROJECT_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "oasis_service.log"), encoding="utf-8"),
    ]
)
logger = logging.getLogger("OASIS.Service")


# ── Core Service Logic ────────────────────────────────────────────────

class OasisServiceCore:
    """
    The actual business logic that runs regardless of whether
    we're in a Windows Service or running directly.
    """

    def __init__(self):
        self._shutdown = threading.Event()
        self._scheduler = None
        self._heartbeat = None
        self._watcher_thread = None

    def start(self):
        """Start all background services."""
        logger.info("O.A.S.I.S. Service starting...")
        os.environ["PYTHONUTF8"] = "1"

        # Load client config
        config_path = os.environ.get(
            "OASIS_CLIENT_CONFIG",
            os.path.join(PROJECT_ROOT, "oasis_client_config.json")
        )

        from oasis.logic.data_gateway import load_client_config
        config = load_client_config(config_path)
        client_id = config.get("client", {}).get("client_id", "UNKNOWN")
        logger.info(f"Client: {client_id}")

        # 1. Ensure DB
        db_path = config.get("paths", {}).get("db_path",
                    os.path.join(PROJECT_ROOT, "oasis.db"))
        try:
            from oasis.logic.db_connector import UniversalConnector
            connector = UniversalConnector(db_path)
            connector.ensure_oasis_tables()
            logger.info(f"Database OK: {db_path}")
        except Exception as e:
            logger.error(f"DB init failed: {e}")

        # 2. Start Scheduler
        try:
            from oasis.logic.scheduler_service import OasisScheduler
            self._scheduler = OasisScheduler(db_path)
            cycle = config.get("scheduler", {}).get("cycle", "24_HOUR")
            self._scheduler.apply_cycle(cycle)
            self._scheduler.start()
            logger.info(f"Scheduler started: cycle={cycle}")
        except Exception as e:
            logger.error(f"Scheduler failed: {e}")

        # 3. Start FileWatcher (for file-dump clients)
        pathway = config.get("data_pathway", "file")
        if pathway in ("file", "hybrid"):
            try:
                from oasis.logic.file_watcher import start_watcher
                data_dir = config.get("paths", {}).get("data_dir",
                            os.path.join(PROJECT_ROOT, "oasis", "data"))
                self._watcher_thread = threading.Thread(
                    target=start_watcher, args=(data_dir,), daemon=True
                )
                self._watcher_thread.start()
                logger.info("FileWatcher started.")
            except Exception as e:
                logger.error(f"FileWatcher failed: {e}")

        # 4. Start Heartbeat
        try:
            from oasis.logic.heartbeat import HeartbeatService
            self._heartbeat = HeartbeatService(config)
            self._heartbeat.start()
        except Exception as e:
            logger.error(f"Heartbeat failed: {e}")

        logger.info("All services started. O.A.S.I.S. is operational.")

    def stop(self):
        """Stop all background services."""
        logger.info("O.A.S.I.S. Service stopping...")
        self._shutdown.set()

        if self._scheduler:
            self._scheduler.stop()
        if self._heartbeat:
            self._heartbeat.stop()

        logger.info("O.A.S.I.S. Service stopped.")

    def wait(self):
        """Block until shutdown signal."""
        self._shutdown.wait()


# ── Windows Service (pywin32) ─────────────────────────────────────────

def _try_windows_service():
    """Attempt to use pywin32 for Windows Service registration."""
    try:
        import win32serviceutil
        import win32service
        import win32event
        import servicemanager

        class OasisWinService(win32serviceutil.ServiceFramework):
            _svc_name_ = "OASISService"
            _svc_display_name_ = "O.A.S.I.S. Autonomous Supply Intelligence"
            _svc_description_ = (
                "Runs the O.A.S.I.S. procurement intelligence engine: "
                "scheduler, file watcher, and telemetry heartbeat."
            )

            def __init__(self, args):
                win32serviceutil.ServiceFramework.__init__(self, args)
                self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)
                self._core = OasisServiceCore()

            def SvcStop(self):
                self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
                self._core.stop()
                win32event.SetEvent(self.hWaitStop)

            def SvcDoRun(self):
                servicemanager.LogMsg(
                    servicemanager.EVENTLOG_INFORMATION_TYPE,
                    servicemanager.PYS_SERVICE_STARTED,
                    (self._svc_name_, "")
                )
                self._core.start()
                win32event.WaitForSingleObject(self.hWaitStop, win32event.INFINITE)

        return OasisWinService

    except ImportError:
        return None


# ── Direct Run Mode ───────────────────────────────────────────────────

def run_direct():
    """Run the service directly in the foreground (no Windows Service)."""
    import signal

    core = OasisServiceCore()

    def _signal_handler(signum, frame):
        logger.info(f"Signal {signum} received. Shutting down...")
        core.stop()

    signal.signal(signal.SIGINT, _signal_handler)
    signal.signal(signal.SIGTERM, _signal_handler)

    core.start()

    logger.info("Running in direct mode. Press Ctrl+C to stop.")
    try:
        core.wait()
    except KeyboardInterrupt:
        core.stop()


# ── Main ──────────────────────────────────────────────────────────────

def main():
    # Check for --run-direct flag first (before pywin32 takes over argv)
    if "--run-direct" in sys.argv:
        run_direct()
        return

    # Try Windows Service mode
    WinService = _try_windows_service()

    if WinService is not None:
        import win32serviceutil
        if len(sys.argv) == 1:
            # No arguments — show help
            print("O.A.S.I.S. Windows Service")
            print()
            print("Usage:")
            print(f"  {sys.argv[0]} install    — Register the service")
            print(f"  {sys.argv[0]} start      — Start the service")
            print(f"  {sys.argv[0]} stop       — Stop the service")
            print(f"  {sys.argv[0]} remove     — Unregister the service")
            print(f"  {sys.argv[0]} --run-direct  — Run in foreground (no service)")
            print()
            return

        win32serviceutil.HandleCommandLine(WinService)
    else:
        # pywin32 not installed — fall back to direct mode
        logger.info("pywin32 not available. Running in direct mode.")
        logger.info("Install pywin32 for Windows Service support: pip install pywin32")
        run_direct()


if __name__ == "__main__":
    main()
