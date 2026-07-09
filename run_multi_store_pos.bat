@echo off
setlocal
title O.A.S.I.S. Multi-Store POS Stream (5 Stores)
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)

REM ── Multi-Store DB path (single file, 5 ORGs) ──────────────────────
set "OASIS_DB_PATH=%~dp0oasis\data\rhapta_multi_store.db"

REM ── First run: build the multi-store snapshot + seed history ────────
if not exist "%OASIS_DB_PATH%" (
    echo.
    echo  ==============================================================
    echo  [  O.A.S.I.S. Multi-Store Setup  - one-time -                ]
    echo  ==============================================================
    echo  [  Building 5 differentiated stores from Rhapta catalog...   ]
    echo  ==============================================================
    echo.

    echo [1/3] Building department halo prior from the vault...
    "%PYTHON_EXEC%" entrypoint.py --mode build-prior

    echo [2/3] Building multi-store database - 5 stores, different profiles...
    "%PYTHON_EXEC%" entrypoint.py --mode build-multi-store-db

    echo [3/3] Seeding demand history per store - 30 days each...
    "%PYTHON_EXEC%" entrypoint.py --mode seed-multi-history
)

echo.
echo  ==============================================================
echo  [  Starting 5-Store Real-Time POS Stream                     ]
echo  [  Each store streams at its own cadence (press Ctrl-C stop) ]
echo  ==============================================================
echo.

REM ── Stream all 5 stores concurrently ────────────────────────────────
REM    Pass --batches N to cap receipts per store, or 0 for infinite.
set PYTHONUNBUFFERED=1
"%PYTHON_EXEC%" entrypoint.py --mode multi-pos-stream --batches 200 %*
pause
