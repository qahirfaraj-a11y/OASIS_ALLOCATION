"""
OASIS Cloud Hub — FastAPI application.

Run it with:  python entrypoint.py --mode hub   (or uvicorn oasis_hub.app:app)

Wires the three routers, ensures the schema exists on startup, and applies a
restrictive CORS policy (the supplier portal front-end origins only — never '*').
"""

import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .db import init_db
from .routers import admin, ingest, portal

try:
    from oasis.logging_config import configure_logging
    configure_logging()
except Exception:                      # hub can run standalone without oasis logging
    logging.basicConfig(level=logging.INFO)

logger = logging.getLogger("OASIS.Hub")


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("OASIS Cloud Hub ready.")
    yield


app = FastAPI(
    title="OASIS Cloud Hub",
    version="0.1.0",
    description="Licensing, opt-in ingestion, and the Retail Central "
                "Intelligence supplier portal.",
    lifespan=lifespan,
)


def _allowed_origins():
    raw = os.getenv("OASIS_HUB_ALLOWED_ORIGINS", "")
    if raw.strip():
        return [o.strip() for o in raw.split(",") if o.strip()]
    return ["http://localhost:8600", "http://127.0.0.1:8600"]


app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins(),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(admin.router)
app.include_router(ingest.router)
app.include_router(portal.router)


@app.get("/health")
def health():
    return {"status": "ok", "service": "oasis-hub", "version": app.version}


@app.get("/")
def root():
    return {"service": "OASIS Cloud Hub", "docs": "/docs"}
