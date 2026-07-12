# ============================================================
# OASIS Cloud Hub — slim image
# ============================================================
# The hub is a lightweight FastAPI service (licensing + ingestion + supplier
# portal). It needs NONE of the client app's heavy deps (torch, scipy, ODBC) —
# just the web stack + the minimal oasis modules the licensing issuer imports.
# Build context is the REPO ROOT:  docker build -f oasis_hub/Dockerfile .
# ============================================================
FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    "fastapi==0.135.1" "uvicorn==0.42.0" "sqlalchemy==2.0.48" \
    "pydantic==2.12.5" "bcrypt==5.0.0"

# The hub package (includes portal_web/ static app)
COPY oasis_hub/ /app/oasis_hub/

# Minimal oasis surface the licensing issuer imports (all stdlib-only modules):
#   licensing -> oasis.logic.license_manager -> oasis.logic.machine_fingerprint
#   app       -> oasis.logging_config
COPY oasis/__init__.py /app/oasis/__init__.py
COPY oasis/logging_config.py /app/oasis/logging_config.py
COPY oasis/logic/__init__.py /app/oasis/logic/__init__.py
COPY oasis/logic/license_manager.py /app/oasis/logic/license_manager.py
COPY oasis/logic/machine_fingerprint.py /app/oasis/logic/machine_fingerprint.py

ENV PYTHONPATH=/app PYTHONUNBUFFERED=1 PYTHONUTF8=1
EXPOSE 8700

HEALTHCHECK --interval=30s --timeout=5s --retries=5 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8700/health').getcode()==200 else 1)"

CMD ["uvicorn", "oasis_hub.app:app", "--host", "0.0.0.0", "--port", "8700"]
