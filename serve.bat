@echo off
cd /d "%~dp0"
.oasis_venv\Scripts\python.exe entrypoint.py --mode serve
