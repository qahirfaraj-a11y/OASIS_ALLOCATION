"""
Console process launcher — start / stop / health-check OASIS surfaces.

Used by the Home app to turn the status board into a real launcher.
Pure logic (no Streamlit); the UI calls these helpers and tracks PIDs
in session_state.

Each console is launched as a background subprocess running the appropriate
``entrypoint.py --mode ...`` command, inheriting ``OASIS_DB_PATH`` from the
caller so every surface points at the same store.
"""

from __future__ import annotations

import logging
import os
import signal
import socket
import subprocess
import sys
from dataclasses import dataclass
from typing import Dict, List, Optional

logger = logging.getLogger("OASIS.Launcher")

# ── Console definitions ──────────────────────────────────────────────────

@dataclass(frozen=True)
class ConsoleDef:
    key: str
    mode: str               # entrypoint.py --mode <mode>
    extra_args: tuple = ()   # extra CLI args after --mode
    port: int = 0
    title: str = ""

# Maps each console key to its entrypoint invocation.
CONSOLE_DEFS: Dict[str, ConsoleDef] = {
    "ops": ConsoleDef("ops", "shell", port=8500, title="Operations Console"),
    "command": ConsoleDef("command", "dashboard", extra_args=("--dashboard", "command"),
                          port=8501, title="Command Center"),
    "intel": ConsoleDef("intel", "intel", port=8510, title="Intelligence Console"),
    "stgat": ConsoleDef("stgat", "dashboard", extra_args=("--dashboard", "stgat"),
                        port=8505, title="Market Intelligence"),
    "hub": ConsoleDef("hub", "hub", port=8700, title="Cloud Hub"),
    "pitch": ConsoleDef("pitch", "dashboard", extra_args=("--dashboard", "pitch"),
                        port=8504, title="Diagnostic Tool"),
}


def _project_root() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _python() -> str:
    """Resolve the Python executable — prefer the venv if it exists."""
    root = _project_root()
    venv = os.path.join(root, ".oasis_venv", "Scripts", "python.exe")
    if os.path.exists(venv):
        return venv
    return sys.executable


def _entrypoint() -> str:
    return os.path.join(_project_root(), "entrypoint.py")


# ── Launch / Stop / Health ───────────────────────────────────────────────

def port_live(port: int, host: str = "127.0.0.1", timeout: float = 0.4) -> bool:
    """True if something is listening on the port (console up)."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def start_console(key: str, db_path: Optional[str] = None,
                  port: Optional[int] = None, return_proc: bool = False):
    """Launch a console as a detached subprocess. Returns PID or None on error.

    The child inherits the current env with ``OASIS_DB_PATH`` overridden to
    ``db_path`` (if given), ensuring every console reads the same store.
    """
    cdef = CONSOLE_DEFS.get(key)
    if cdef is None:
        logger.error("Unknown console key: %s", key)
        return None

    actual_port = port or cdef.port
    if port_live(actual_port):
        logger.info("%s already running on :%d — skipping", cdef.title, actual_port)
        return None  # already up

    env = os.environ.copy()
    if db_path:
        env["OASIS_DB_PATH"] = db_path
    if key == "command":
        env["OASIS_LIVE_MODE"] = "true"

    cmd: List[str] = [
        _python(), _entrypoint(),
        "--mode", cdef.mode,
        *cdef.extra_args,
    ]
    if actual_port:
        cmd.extend(["--port", str(actual_port)])

    logger.info("Launching %s → %s (port %d)", cdef.title, " ".join(cmd), actual_port)
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=_project_root(),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        logger.info("%s started (PID %d)", cdef.title, proc.pid)
        return proc if return_proc else proc.pid
    except Exception as e:
        logger.error("Failed to launch %s: %s", cdef.title, e)
        return None


def stop_console(pid: int) -> bool:
    """Terminate a console process by PID. Returns True if signal sent."""
    try:
        os.kill(pid, signal.SIGTERM)
        logger.info("Sent SIGTERM to PID %d", pid)
        return True
    except (OSError, ProcessLookupError):
        return False


def is_alive(pid: int) -> bool:
    """Check if a process is still running (best-effort, cross-platform)."""
    try:
        os.kill(pid, 0)
        return True
    except (OSError, ProcessLookupError):
        return False


def start_all(db_path: Optional[str] = None) -> Dict[str, Optional[int]]:
    """Launch every console. Returns {key: pid_or_None}."""
    return {key: start_console(key, db_path=db_path) for key in CONSOLE_DEFS}


def start_service(entrypoint_mode: str = "serve",
                  db_path: Optional[str] = None) -> Optional[int]:
    """Launch a long-running background service (e.g. the supervisor).

    ``serve`` is a routine operation that used to be CLI-only (reachable only
    via ``serve.bat``) — this gives Home (and anything else) a programmatic
    affordance (deep-analysis finding S9). Returns the child PID, or None if
    launch failed.
    """
    env = os.environ.copy()
    if db_path:
        env["OASIS_DB_PATH"] = db_path
    cmd = [_python(), _entrypoint(), "--mode", entrypoint_mode]
    logger.info("Starting service → %s", " ".join(cmd))
    try:
        proc = subprocess.Popen(
            cmd,
            cwd=_project_root(),
            env=env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )
        logger.info("Service started (PID %d)", proc.pid)
        return proc.pid
    except Exception as e:
        logger.error("Failed to start service: %s", e)
        return None


def stop_all(pids: Dict[str, int]) -> int:
    """Send SIGTERM to all tracked PIDs. Returns count of signals sent."""
    count = 0
    for key, pid in pids.items():
        if pid and stop_console(pid):
            count += 1
    return count
