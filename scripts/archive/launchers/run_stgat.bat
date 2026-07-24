@echo off
setlocal
title O.A.S.I.S. ST-GAT Market Pulse
cd /d "%~dp0"
if exist .oasis_venv\Scripts\python.exe (set PYTHON_EXEC=.oasis_venv\Scripts\python.exe) else (set PYTHON_EXEC=python)
:: Route through Streamlit via the entrypoint (bare `python st_gat_dashboard.py`
:: does not start a Streamlit server and renders nothing).
"%PYTHON_EXEC%" entrypoint.py --mode dashboard --dashboard stgat %*
pause
