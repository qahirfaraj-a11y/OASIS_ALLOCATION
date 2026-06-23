@echo off
setlocal
title O.A.S.I.S. Mock POS - Real-Time Sales Stream (Rhapta)
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)

REM Stream against the real Rhapta catalog snapshot so the three consoles reflect it.
set OASIS_DB_PATH=%~dp0oasis\data\rhapta_pos.db

REM First run: prior -> clean snapshot (no simulated history) -> REAL demand from
REM the monthly *_cash.xlsx (OASIS_CASH_DIR, default Desktop\Projects). ADS is the
REM real Rhapta daily demand; stock stays full and TODAY is empty (start of day).
if not exist "%OASIS_DB_PATH%" (
    echo [setup] Building department halo prior from the vault...
    "%PYTHON_EXEC%" entrypoint.py --mode build-prior
    echo [setup] Building Rhapta snapshot (stock only, no simulated demand)...
    set "OASIS_HISTORY_DAYS=0"
    "%PYTHON_EXEC%" entrypoint.py --mode build-pos-db
    echo [setup] Deriving REAL demand baseline from monthly cash files...
    "%PYTHON_EXEC%" entrypoint.py --mode seed-real-demand
)

REM Real-time stream: one receipt every 2s until you close the window (Ctrl-C).
REM Pass args to override, e.g.  run_mock_pos.bat --interval 1 --batches 200
"%PYTHON_EXEC%" entrypoint.py --mode pos-stream --interval 2 --batches 0 %*
pause
