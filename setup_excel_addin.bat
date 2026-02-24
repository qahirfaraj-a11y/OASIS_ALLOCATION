
@echo off
echo ===================================================
echo   OASIS Excel Add-in Setup
echo ===================================================
echo.
echo Installing Python dependencies...
pip install xlwings pandas openpyxl
echo.
echo Installing Excel Add-in...
xlwings addin install
echo.
echo Configuring Add-in...
echo Please open Excel. You should see an 'xlwings' tab.
echo.
echo To inspect the code, look at: oasis_excel.py
echo.
echo Usage:
echo 1. Open Excel.
echo 2. Go to xlwings tab -> Import Functions.
echo 3. Or use the provided 'oasis_template.xlsm' (if you generated one).
echo.
echo NOTE: For a persistent ribbon button, we usually deploy a manifest.
echo For this prototype:
echo 1. Open Excel -> xlwings tab.
echo 2. Set 'PYTHON PATH' to your python executable.
echo 3. Set 'UDF MODULES' to: oasis_excel
echo 4. Click 'Import Functions'.
echo 5. You can then attach 'generate_optimization' to a button.
echo.
pause
