import os
import shutil
import logging
import threading
from fastapi import FastAPI, UploadFile, File, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel
import asyncio
from dotenv import load_dotenv

# Load env vars from .env file
load_dotenv()

# Import the existing engine
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from oasis.logic.order_engine import OrderEngine
from oasis.api.security import require_auth, allowed_origins
from oasis.api.observability import attach_observability

# Configure Logging (centralized — honors OASIS_LOG_LEVEL/FORMAT/FILE)
from oasis.logging_config import configure_logging
configure_logging()
logger = logging.getLogger("OASIS-API")

app = FastAPI(title="OASIS Mobile API", version="1.0")
attach_observability(app, service_name="oasis-mobile-api")

from fastapi.staticfiles import StaticFiles

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins(),
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
# Guards current_status / last_results — mutated from background tasks while
# request handlers read them concurrently.
_state_lock = threading.Lock()


def _set_status(status: AnalysisStatus):
    global current_status
    with _state_lock:
        current_status = status


def run_analysis_task(file_path: str, original_filename: str):
    global current_status, last_results
    try:
        _set_status(AnalysisStatus(state="processing", message="Initializing Engine...", progress=10))
        
        # AUTH CHECK
        if not os.environ.get("ANTHROPIC_API_KEY"):
            error_msg = "Missing API Key. Please set ANTHROPIC_API_KEY environment variable."
            logger.error(error_msg)
            _set_status(AnalysisStatus(state="error", message=error_msg, progress=0))
            return

        # Initialize Engine
        engine = OrderEngine(DATA_DIR)
        
        # Async load standard databases
        # Since we are in a sync wrapper here, we might need a mini event loop or just call the sync parts if possible
        # Or better, make this entire function async and run it correctly.
        # However, OrderEngine.run_intelligent_analysis is async.
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        _set_status(AnalysisStatus(state="processing", message="Loading Intelligence...", progress=20))

        # Output file path
        output_filename = f"Analyzed_{original_filename}"
        output_path = os.path.join(OUTPUT_DIR, output_filename)

        # Run Analysis
        _set_status(AnalysisStatus(state="processing", message="AI Analyzing Inventory...", progress=40))

        # Execute the engine
        recommendations = loop.run_until_complete(engine.run_intelligent_analysis(file_path, output_path))

        with _state_lock:
            last_results = recommendations
            current_status = AnalysisStatus(state="completed", message="Analysis Complete", progress=100)
        logger.info(f"Analysis complete. {len(recommendations)} items recommended.")

        loop.close()

    except Exception as e:
        logger.error(f"Analysis Failed: {e}")
        _set_status(AnalysisStatus(state="error", message=str(e), progress=0))

@app.get("/")
def read_root():
    return {"status": "OASIS Mobile backend is running"}

@app.get("/status")
def get_status(identity: dict = Depends(require_auth)):
    with _state_lock:
        return current_status

@app.post("/upload")
async def upload_file(file: UploadFile = File(...), background_tasks: BackgroundTasks = None,
                      identity: dict = Depends(require_auth)):
    global current_status

    with _state_lock:
        if current_status.state == 'processing':
            raise HTTPException(status_code=400, detail="Analysis already in progress")

    try:
        # Strip any client-supplied directory components before writing to disk
        safe_name = os.path.basename(file.filename or "upload.xlsx")
        file_location = os.path.join(UPLOAD_DIR, safe_name)
        with open(file_location, "wb+") as file_object:
            shutil.copyfileobj(file.file, file_object)

        # Start Analysis in Background
        _set_status(AnalysisStatus(state="uploading", message="File uploaded. Starting analysis...", progress=5))
        background_tasks.add_task(run_analysis_task, file_location, safe_name)

        return {"filename": safe_name, "message": "File uploaded successfully, analysis started."}

    except Exception as e:
        logger.error(f"Upload failed: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/results")
def get_results(identity: dict = Depends(require_auth)):
    with _state_lock:
        if not last_results:
            return {"results": []}
        return {"results": last_results}

@app.get("/download")
def download_results(identity: dict = Depends(require_auth)):
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
