@echo off
setlocal
title O.A.S.I.S. Command Center (Multi-Store Live)
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)

REM ── Multi-Store DB (5 stores, separate from the single-store rhapta_pos.db) ─
set "OASIS_DB_PATH=%~dp0oasis\data\rhapta_multi_store.db"

REM Live run: time-of-day auto-accrues with the multi-store POS stream.
set "OASIS_LIVE_MODE=true"

if not exist "%OASIS_DB_PATH%" (
    echo.
    echo  ==============================================================
    echo  [  Multi-Store first-run setup                               ]
    echo  ==============================================================
    echo  [  Run  run_multi_store_pos.bat  first to build the          ]
    echo  [  5-store database and seed demand history.                 ]
    echo  ==============================================================
    echo.
    echo [setup] Building multi-store DB from catalog...
    "%PYTHON_EXEC%" entrypoint.py --mode build-prior
    "%PYTHON_EXEC%" entrypoint.py --mode build-multi-store-db
    "%PYTHON_EXEC%" entrypoint.py --mode seed-multi-history
)

echo.
echo  Starting Command Center against Multi-Store DB (5 stores)...
echo  (Run  run_multi_store_pos.bat  in a separate window for live POS)
echo.

"%PYTHON_EXEC%" entrypoint.py --mode dashboard --dashboard command %*
pause
