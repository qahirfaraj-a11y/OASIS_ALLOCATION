@echo off
setlocal
title O.A.S.I.S. Mock POS - Real-Time Sales Stream (Rhapta)
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)

REM Stream against the real Rhapta catalog snapshot so the three consoles reflect it.
set OASIS_DB_PATH=%~dp0oasis\data\rhapta_pos.db

REM First run: build the clean snapshot + department halo prior from the catalog.
if not exist "%OASIS_DB_PATH%" (
    echo [setup] Building clean Rhapta POS snapshot from catalog...
    "%PYTHON_EXEC%" entrypoint.py --mode build-pos-db
    echo [setup] Building department halo prior from the vault...
    "%PYTHON_EXEC%" entrypoint.py --mode build-prior
)

REM Real-time stream: one receipt every 2s until you close the window (Ctrl-C).
REM Pass args to override, e.g.  run_mock_pos.bat --interval 1 --batches 200
"%PYTHON_EXEC%" entrypoint.py --mode pos-stream --interval 2 --batches 0 %*
pause
