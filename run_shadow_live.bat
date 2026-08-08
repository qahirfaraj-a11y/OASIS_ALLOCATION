@echo off
setlocal
title O.A.S.I.S. Shadow Engine Console
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)
"%PYTHON_EXEC%" entrypoint.py --mode dashboard --dashboard shadow %*
pause
