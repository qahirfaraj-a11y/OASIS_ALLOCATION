import os
import shutil
import uuid
import subprocess
from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse, FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pathlib import Path

app = FastAPI()

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Allows all origins
    allow_credentials=True,
    allow_methods=["*"],  # Allows all methods
    allow_headers=["*"],  # Allows all headers
)

UPLOAD_DIR = Path("uploads")
OUTPUT_DIR = Path("separated")

UPLOAD_DIR.mkdir(exist_ok=True)
OUTPUT_DIR.mkdir(exist_ok=True)

def separate_audio(input_file: Path, output_dir: Path, job_id: str):
    """
    Runs Spleeter to separate the audio file.
    Since Spleeter's python API can sometimes be tricky with multiprocessing in FastAPI,
    we'll use a subprocess call to the spleeter CLI for robustness.
    """
    try:
        # We'll use 4stems model: vocals, drums, bass, other
        command = [
            "spleeter", "separate",
            "-p", "spleeter:4stems",
            "-o", str(output_dir),
            str(input_file)
        ]
        
        # Run Spleeter
        result = subprocess.run(command, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"Error running spleeter: {result.stderr}")
            # Mark as failed (you'd typically use a DB or redis for state here)
            with open(output_dir / job_id / "status.txt", "w") as f:
                f.write("FAILED")
        else:
            # Mark as done
            with open(output_dir / job_id / "status.txt", "w") as f:
                f.write("DONE")
             
    except Exception as e:
        print(f"Exception during separation: {e}")
        # Make sure directory exists to write status
        (output_dir / job_id).mkdir(exist_ok=True)
        with open(output_dir / job_id / "status.txt", "w") as f:
            f.write("FAILED")
    finally:
         # Clean up the original uploaded file to save space
         if input_file.exists():
             os.remove(input_file)

@app.post("/upload")
async def upload_file(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename.endswith(('.mp3', '.wav', '.ogg', '.flac')):
        raise HTTPException(status_code=400, detail="Unsupported file format")

    job_id = str(uuid.uuid4())
    
    # Save the uploaded file
    file_ext = Path(file.filename).suffix
    save_path = UPLOAD_DIR / f"{job_id}{file_ext}"
    
    with open(save_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
        
    # Setup job output directory and write initial status
    job_dir = OUTPUT_DIR / job_id
    job_dir.mkdir(exist_ok=True)
    with open(job_dir / "status.txt", "w") as f:
        f.write("PROCESSING")

    # Start separation in background
    background_tasks.add_task(separate_audio, save_path, OUTPUT_DIR, job_id)

    return JSONResponse(content={
        "job_id": job_id,
        "message": "File uploaded successfully. Separation started in the background."
    })

@app.get("/status/{job_id}")
async def get_status(job_id: str):
    status_file = OUTPUT_DIR / job_id / "status.txt"
    if not status_file.exists():
        raise HTTPException(status_code=404, detail="Job not found")
        
    with open(status_file, "r") as f:
        status = f.read().strip()
        
    if status == "DONE":
        # Spleeter creates a folder with the name of the input file minus extension
        # We need to find the stems
        folder_contents = os.listdir(OUTPUT_DIR / job_id)
        # Spleeter usually creates a subfolder inside our output dir
        # Let's list the available stems
        stems = {}
        target_folder = OUTPUT_DIR / job_id
        
        # If output was a subfolder
        subfolders = [f for f in target_folder.iterdir() if f.is_dir()]
        if subfolders:
             stem_dir = subfolders[0]
             for stem_file in stem_dir.glob("*.wav"):
                 stems[stem_file.stem] = f"/download/{job_id}/{stem_dir.name}/{stem_file.name}"
        
        return {"status": status, "stems": stems}
        
    return {"status": status}

@app.get("/download/{job_id}/{folder_name}/{filename}")
async def download_stem(job_id: str, folder_name:str, filename: str):
    file_path = OUTPUT_DIR / job_id / folder_name / filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail="File not found")
        
    return FileResponse(path=file_path, media_type='audio/wav', filename=filename)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
