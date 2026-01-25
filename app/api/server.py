import os
import shutil
import logging
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, FileResponse
from pydantic import BaseModel
from typing import List, Optional
import asyncio
import json
from dotenv import load_dotenv

# Load env vars from .env file
load_dotenv()

# Import the existing engine
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from app.logic.order_engine import OrderEngine

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OASIS-API")

app = FastAPI(title="OASIS Mobile API", version="1.0")

from fastapi.staticfiles import StaticFiles

# CORS - Allow all for local dev (User's phone on wifi)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve Static Files (Frontend)
STATIC_DIR = os.path.join(os.path.dirname(__file__), 'static')
os.makedirs(STATIC_DIR, exist_ok=True)
app.mount("/app", StaticFiles(directory=STATIC_DIR, html=True), name="static")

# Global State (Simple in-memory for single-user local app)
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
UPLOAD_DIR = os.path.join(DATA_DIR, 'uploads')
OUTPUT_DIR = os.path.join(DATA_DIR, 'outputs')

os.makedirs(UPLOAD_DIR, exist_ok=True)
os.makedirs(OUTPUT_DIR, exist_ok=True)

class AnalysisStatus(BaseModel):
    state: str  # 'idle', 'processing', 'completed', 'error'
    message: str
    progress: int

current_status = AnalysisStatus(state="idle", message="Ready", progress=0)
last_results = []

def run_analysis_task(file_path: str, original_filename: str):
    global current_status, last_results
    try:
        current_status = AnalysisStatus(state="processing", message="Initializing Engine...", progress=10)
        
        # AUTH CHECK
        if not os.environ.get("ANTHROPIC_API_KEY"):
            error_msg = "Missing API Key. Please set ANTHROPIC_API_KEY environment variable."
            logger.error(error_msg)
            current_status = AnalysisStatus(state="error", message=error_msg, progress=0)
            return

        # Initialize Engine
        engine = OrderEngine(DATA_DIR)
        
        # Async load standard databases
        # Since we are in a sync wrapper here, we might need a mini event loop or just call the sync parts if possible
        # Or better, make this entire function async and run it correctly.
        # However, OrderEngine.run_intelligent_analysis is async.
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        current_status.message = "Loading Intelligence..."
        current_status.progress = 20
        
        # Output file path
        output_filename = f"Analyzed_{original_filename}"
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        
        # Run Analysis
        current_status.message = "AI Analyzing Inventory..."
        current_status.progress = 40
        
        # Execute the engine
        recommendations = loop.run_until_complete(engine.run_intelligent_analysis(file_path, output_path))
        
        last_results = recommendations
        current_status = AnalysisStatus(state="completed", message="Analysis Complete", progress=100)
        logger.info(f"Analysis complete. {len(recommendations)} items recommended.")
        
        loop.close()

    except Exception as e:
        logger.error(f"Analysis Failed: {e}")
        current_status = AnalysisStatus(state="error", message=str(e), progress=0)

@app.get("/")
def read_root():
    return {"status": "OASIS Mobile backend is running"}

@app.get("/status")
def get_status():
    return current_status

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), background_tasks: BackgroundTasks = None):
    global current_status
    
    if current_status.state == 'processing':
        raise HTTPException(status_code=400, detail="Analysis already in progress")

    try:
        file_location = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_location, "wb+") as file_object:
            shutil.copyfileobj(file.file, file_object)
            
        # Start Analysis in Background
        current_status = AnalysisStatus(state="uploading", message="File uploaded. Starting analysis...", progress=5)
        background_tasks.add_task(run_analysis_task, file_location, file.filename)
        
        return {"filename": file.filename, "message": "File uploaded successfully, analysis started."}
        
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/results")
def get_results():
    if not last_results:
        return {"results": []}
    return {"results": last_results}

@app.get("/download")
def download_results():
    # Find latest file in output dir
    try:
        files = os.listdir(OUTPUT_DIR)
        paths = [os.path.join(OUTPUT_DIR, basename) for basename in files]
        latest_file = max(paths, key=os.path.getctime)
        return FileResponse(path=latest_file, filename=os.path.basename(latest_file), media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
    except Exception as e:
         raise HTTPException(status_code=404, detail="No output file found")

if __name__ == "__main__":
    import uvicorn
    # Host 0.0.0.0 is crucial for local network access
    uvicorn.run(app, host="0.0.0.0", port=8000)
