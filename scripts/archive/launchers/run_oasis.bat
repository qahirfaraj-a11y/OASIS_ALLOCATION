@echo off
setlocal
title O.A.S.I.S. — Unified Shell
cd /d "%~dp0"

:: Venv detection
if exist .oasis_venv\Scripts\python.exe (
    set PYTHON_EXEC=.oasis_venv\Scripts\python.exe
) else (
    set PYTHON_EXEC=python
)

:: Single front door — one login, journey-driven navigation to every function.
"%PYTHON_EXEC%" entrypoint.py --mode shell %*
pause
