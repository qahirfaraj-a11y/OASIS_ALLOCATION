@echo off
setlocal
title O.A.S.I.S. Home - Suite Launcher
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)
REM Point the snapshot panel at the live Rhapta store DB.
set "OASIS_DB_PATH=%~dp0oasis\data\rhapta_pos.db"
"%PYTHON_EXEC%" entrypoint.py --mode home %*
pause
