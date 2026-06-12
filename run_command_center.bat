@echo off
setlocal
title O.A.S.I.S. Command Center
cd /d "%~dp0"

:: Venv detection
if exist .oasis_venv\Scripts\python.exe (
    set PYTHON_EXEC=.oasis_venv\Scripts\python.exe
) else (
    set PYTHON_EXEC=python
)

"%PYTHON_EXEC%" entrypoint.py --mode dashboard --dashboard command %*
pause
