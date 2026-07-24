@echo off
setlocal EnableDelayedExpansion

:: O.A.S.I.S. Minimal Installer
:: ===========================

echo [INFO] Starting O.A.S.I.S. Installer...
:: pause removed for test

set "INSTALL_DIR=%~dp0"
set "VENV_DIR=%INSTALL_DIR%.oasis_venv"
set "CONFIG_FILE=%INSTALL_DIR%oasis_client_config.json"
set "CONFIG_TEMPLATE=%INSTALL_DIR%oasis_client_config.template.json"
set "DB_PATH=%INSTALL_DIR%oasis.db"

echo [1/4] Checking Python...
set "PYTHON_CMD="
for %%P in (python python3 py) do (
    where %%P >nul 2>&1
    if !errorlevel! equ 0 (
        set "PYTHON_CMD=%%P"
    )
)

if not defined PYTHON_CMD (
    echo [ERROR] Python not found. Please install Python 3.10+ and add to PATH.
    pause
    exit /b 1
)
echo [OK] Found Python.

echo [2/4] Setting up Virtual Environment...
if not exist "%VENV_DIR%\Scripts\python.exe" (
    echo Creating venv...
    %PYTHON_CMD% -m venv "%VENV_DIR%"
)
set "PYTHON=%VENV_DIR%\Scripts\python.exe"
set "PIP=%VENV_DIR%\Scripts\pip.exe"
echo [OK] Venv ready.

echo [3/4] Installing requirements...
"%PIP%" install -r "%INSTALL_DIR%requirements.txt" --quiet
echo [OK] Dependencies installed.

echo [4/4] Initializing Config ^& DB...
if not exist "%CONFIG_FILE%" (
    if exist "%CONFIG_TEMPLATE%" (
        copy "%CONFIG_TEMPLATE%" "%CONFIG_FILE%" >nul
        echo Created config from template.
    ) else (
        echo {> "%CONFIG_FILE%"
        echo   "client": {"client_id": "new_client"},>> "%CONFIG_FILE%"
        echo   "paths": {"data_dir": "oasis/data", "db_path": "oasis.db"}>> "%CONFIG_FILE%"
        echo }>> "%CONFIG_FILE%"
        echo Created minimal config.
    )
)

:: Initialize DB
echo Initializing database...
"%PYTHON%" -c "import sys; sys.path.insert(0, '%INSTALL_DIR:\=/%'); from oasis.logic.db_connector import ensure_oasis_tables; ensure_oasis_tables('%DB_PATH:\=/%'); print('Database initialized.')"

echo.
echo ========================================
echo INSTALLATION COMPLETE
echo ========================================
echo.
:: pause removed for test
exit /b 0
