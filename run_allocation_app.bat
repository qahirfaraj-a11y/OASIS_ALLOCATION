@echo off
setlocal enabledelayedexpansion
title O.A.S.I.S. Allocation Engine
cd /d "%~dp0"
echo Starting O.A.S.I.S. Allocation Engine

REM 1. Check for Virtual Environment
if not exist ".oasis_venv\Scripts\python.exe" (
    echo Creating isolated Virtual Environment
    python -m venv .oasis_venv
    echo Installing dependencies...
    call .oasis_venv\Scripts\activate.bat
    python -m pip install --upgrade pip
    if exist "requirements.txt" (
        python -m pip install -r requirements.txt
    )
) else (
    REM 2. Activate VENV (skip pip on normal startup)
    echo Activating Environment...
    call .oasis_venv\Scripts\activate.bat
)

REM 3. Port Hunting (avoid conflict with Command Center on 8501)
set PORT=8502
:findport
netstat -ano | findstr :!PORT! >nul
if !errorlevel! equ 0 (
    echo [PORT] !PORT! is busy. Hunting for next open port...
    set /a PORT+=1
    goto findport
)

REM 4. Run the App
echo Launching Allocation Engine at http://localhost:!PORT!
python -m streamlit run allocation_app.py --server.port !PORT! --server.headless true

if errorlevel 1 (
    echo ERROR: App failed to start.
    pause
)

endlocal
