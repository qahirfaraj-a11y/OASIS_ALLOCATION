@echo off
setlocal
title O.A.S.I.S. Shadow Auditor
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)
:: --pathway file|sql controls whether the daemon watches filesystem or polls ERP
"%PYTHON_EXEC%" entrypoint.py --mode shadow --pathway %1 %2 %3
pause
