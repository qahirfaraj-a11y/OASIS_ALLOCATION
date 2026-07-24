@echo off
echo ===========================================
echo   O.A.S.I.S. State Hard Reset
echo ===========================================
echo.
echo WARNING: This will delete the mock database and transfer registry.
echo Please ensure the Command Center terminal is CLOSED before proceeding.
echo.
pause

echo Deleting transfer registry...
del /q "oasis\data\transfers_registry.json" 2>nul
echo Deleting mock database...
del /q "oasis\data\mock_pos_erp.db" 2>nul
del /q "oasis\data\mock_pos_erp.db-journal" 2>nul

echo.
echo Reset Complete! You can now launch run_command_center.bat again.
pause
