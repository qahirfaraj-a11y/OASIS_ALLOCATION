# run_servers.ps1

Write-Host "Starting VJ Canvas Data Pipeline..."

# Start Backend Server
Start-Process powershell -ArgumentList "-NoExit -Command `"cd backend; .\venv\Scripts\Activate.ps1; uvicorn main:app --host 0.0.0.0 --port 8000 --reload`"" -WindowStyle Normal

# Start Frontend Server
Start-Process powershell -ArgumentList "-NoExit -Command `"cd frontend; python -m http.server 3000`"" -WindowStyle Normal

Write-Host "Backend API is running on http://localhost:8000"
Write-Host "Frontend App is running on http://localhost:3000"
Write-Host "Please open http://localhost:3000 in your browser to test VJ Canvas."
