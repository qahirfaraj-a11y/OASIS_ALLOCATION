@echo off
setlocal
title O.A.S.I.S. — Intelligence Console
cd /d "%~dp0"

:: Venv detection
if exist .oasis_venv\Scripts\python.exe (
    set PYTHON_EXEC=.oasis_venv\Scripts\python.exe
) else (
    set PYTHON_EXEC=python
)

:: Monitoring / intelligence front door — dashboards, velocity alerts,
:: stock review, network intelligence. Runs alongside the Operations
:: Console (run_oasis.bat) on a separate port (8510 by default).
"%PYTHON_EXEC%" entrypoint.py --mode intel %*
pause
