@echo off
echo ==============================================
echo Starting OASIS Online Terminal...
echo ==============================================
echo.
echo Installing requirements (pyngrok, flet)...
pip install pyngrok flet openpyxl pandas >nul 2>&1

echo.
echo Launching Web Server and Secure Tunnel...
echo Keep this window open. 
echo A secure ngrok.io link will appear below shortly.
echo.
python -m oasis.main_online

pause
