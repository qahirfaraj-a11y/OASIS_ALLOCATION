@echo off
setlocal
title O.A.S.I.S. Intelligence Console (DEMO)
cd /d "%~dp0"
set OASIS_DEMO_MODE=true
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)
"%PYTHON_EXEC%" entrypoint.py --mode intel %*
pause
