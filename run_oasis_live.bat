@echo off
setlocal
title O.A.S.I.S. Operations Console (Rhapta live)
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)
REM Point this console at the real Rhapta snapshot the mock POS streams into.
set "OASIS_DB_PATH=%~dp0oasis\data\rhapta_pos.db"
if not exist "%OASIS_DB_PATH%" (
    echo [setup] Building Rhapta snapshot from catalog...
    "%PYTHON_EXEC%" entrypoint.py --mode build-pos-db
)
"%PYTHON_EXEC%" entrypoint.py --mode shell %*
pause
