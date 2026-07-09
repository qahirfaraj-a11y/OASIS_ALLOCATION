"""
Machine fingerprint for OASIS trial stamp integrity.

Generates a stable, non-reversible machine identifier used to bind the trial
stamp to a specific machine. This prevents copying the trial state file from
one machine to another to extend evaluations.

The fingerprint is a SHA-256 of machine-specific identifiers (hostname,
OS install ID, volume serial number). It is NOT personally identifiable —
it's a one-way hash. No network calls, no admin rights required.

    from oasis.logic.machine_fingerprint import machine_id
    mid = machine_id()   # -> "a3f8c1..." (64-char hex string, stable per machine)
"""

from __future__ import annotations

import hashlib
import logging
import os
import platform
import socket
import subprocess

logger = logging.getLogger("OASIS.MachineFingerprint")

# Sentinel returned when we genuinely cannot identify the machine.
# The trial stamp will still work — it just won't be machine-bound.
UNKNOWN_MACHINE = "UNKNOWN_MACHINE"


def _windows_machine_guid() -> str:
    """Read the MachineGuid from the Windows registry (unique per OS install)."""
    try:
        import winreg
        key = winreg.OpenKey(
            winreg.HKEY_LOCAL_MACHINE,
            r"SOFTWARE\Microsoft\Cryptography",
            0,
            winreg.KEY_READ | winreg.KEY_WOW64_64KEY,
        )
        value, _ = winreg.QueryValueEx(key, "MachineGuid")
        winreg.CloseKey(key)
        return str(value)
    except Exception as e:
        logger.debug("Could not read MachineGuid: %s", e)
        return ""


def _volume_serial() -> str:
    """Get the C: drive volume serial number (Windows-specific)."""
    try:
        result = subprocess.run(
            ["cmd", "/c", "vol", "C:"],
            capture_output=True, text=True, timeout=5,
        )
        # Output: " Volume Serial Number is XXXX-XXXX"
        for line in result.stdout.splitlines():
            if "Serial Number" in line:
                return line.split("is")[-1].strip()
    except Exception as e:
        logger.debug("Could not read volume serial: %s", e)
    return ""


def _linux_machine_id() -> str:
    """Read /etc/machine-id on Linux systems."""
    for path in ("/etc/machine-id", "/var/lib/dbus/machine-id"):
        try:
            with open(path, "r") as f:
                return f.read().strip()
        except (OSError, IOError):
            continue
    return ""


def _collect_signals() -> list[str]:
    """Gather machine-identifying signals (best-effort, no failures)."""
    signals = []

    # 1. Hostname (stable across reboots, changes if user renames machine)
    try:
        signals.append(f"host:{socket.gethostname()}")
    except Exception:
        pass

    # 2. Platform-specific machine ID
    system = platform.system().lower()
    if system == "windows":
        guid = _windows_machine_guid()
        if guid:
            signals.append(f"win_guid:{guid}")
        serial = _volume_serial()
        if serial:
            signals.append(f"vol_serial:{serial}")
    elif system == "linux":
        mid = _linux_machine_id()
        if mid:
            signals.append(f"linux_mid:{mid}")
    elif system == "darwin":
        try:
            result = subprocess.run(
                ["ioreg", "-rd1", "-c", "IOPlatformExpertDevice"],
                capture_output=True, text=True, timeout=5,
            )
            for line in result.stdout.splitlines():
                if "IOPlatformUUID" in line:
                    uuid = line.split('"')[-2]
                    signals.append(f"mac_uuid:{uuid}")
                    break
        except Exception:
            pass

    # 3. Username as a weak tiebreaker
    try:
        signals.append(f"user:{os.getlogin()}")
    except Exception:
        pass

    return signals


def machine_id() -> str:
    """Return a stable, non-reversible machine identifier (64-char hex).

    Returns UNKNOWN_MACHINE if no signals could be collected. The caller
    should handle this gracefully (e.g., skip machine-binding).
    """
    signals = _collect_signals()
    if not signals:
        logger.warning("Could not collect any machine signals for fingerprint")
        return UNKNOWN_MACHINE

    raw = "|".join(sorted(signals))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()
