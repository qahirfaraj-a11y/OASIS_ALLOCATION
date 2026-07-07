@echo off
setlocal
title O.A.S.I.S. Multi-Store POS - 5 Tills Streaming
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)

REM Five differentiated stores share one network DB. Sales only; no restocking.
set "OASIS_DB_PATH=%~dp0oasis\data\rhapta_multi_store.db"

REM First run: build the 5-store network then seed each store's demand history.
if not exist "%OASIS_DB_PATH%" (
    echo [setup] Building 5-store network from the real catalog...
    "%PYTHON_EXEC%" entrypoint.py --mode build-multi-store-db
    echo [setup] Seeding per-store demand history...
    "%PYTHON_EXEC%" entrypoint.py --mode seed-multi-history
)

REM Stream all five tills until you close the window / Ctrl-C.
REM Pass args to override, e.g.  run_multi_pos.bat --batches 100
"%PYTHON_EXEC%" entrypoint.py --mode multi-pos-stream --batches 0 %*
pause
