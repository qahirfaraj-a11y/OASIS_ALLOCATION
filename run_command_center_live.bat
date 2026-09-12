@echo off
setlocal
title O.A.S.I.S. Command Center (Rhapta live)
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)
REM Point this console at the real Rhapta snapshot the mock POS streams into.
set "OASIS_DB_PATH=%~dp0oasis\data\rhapta_pos.db"
REM OASIS_DB_PATH only redirects OASIS's OWN store. The POS *source* is chosen
REM separately by OASIS_POS_DB_URL, so a machine-level live-POS URL survived
REM into this console and every product/org read went to MSSQL: offline, the
REM first load_orgs() call died on a 258 login timeout before any tab drew.
REM Cleared inside setlocal, so the machine variable is untouched.
set "OASIS_POS_DB_URL="
set "OASIS_DB_URL="
REM Live run: time-of-day auto-accrues with the POS stream (no manual slider).
set "OASIS_LIVE_MODE=true"
if not exist "%OASIS_DB_PATH%" (
    echo [setup] Building Rhapta snapshot from catalog...
    "%PYTHON_EXEC%" entrypoint.py --mode build-pos-db
)
"%PYTHON_EXEC%" entrypoint.py --mode dashboard --dashboard command %*
pause
