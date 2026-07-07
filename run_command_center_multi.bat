@echo off
setlocal
title O.A.S.I.S. Command Center (5-Store Network)
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)
REM Point the Command Center at the 5-store network DB the multi POS streams
REM into - this is where transfers/allocation across stores become demo-able.
set "OASIS_DB_PATH=%~dp0oasis\data\rhapta_multi_store.db"
set "OASIS_LIVE_MODE=true"
if not exist "%OASIS_DB_PATH%" (
    echo [setup] Building 5-store network from the real catalog...
    "%PYTHON_EXEC%" entrypoint.py --mode build-multi-store-db
    echo [setup] Seeding per-store demand history...
    "%PYTHON_EXEC%" entrypoint.py --mode seed-multi-history
)
"%PYTHON_EXEC%" entrypoint.py --mode dashboard --dashboard command %*
pause
