# ============================================================
# O.A.S.I.S. — Multi-stage Docker Build
# ============================================================
# Stage 1: Build dependencies (cached layer)
# Stage 2: Production runtime (minimal footprint)
# ============================================================

# ── Stage 1: Builder ─────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# System dependencies for pyodbc, scipy, torch
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ \
    unixodbc-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install MSSQL ODBC Driver 18 (for Pathway 1: SQL clients)
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg \
    && echo "deb [signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first for Docker layer caching
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ── Stage 2: Production Runtime ──────────────────────────────
FROM python:3.11-slim AS runtime

LABEL maintainer="iLink Technologies"
LABEL description="O.A.S.I.S. — Autonomous Supply Intelligence System"
LABEL version="2.1.0"

WORKDIR /app

# Runtime system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    unixodbc \
    libgomp1 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy ODBC driver from builder
COPY --from=builder /opt/microsoft /opt/microsoft
COPY --from=builder /etc/odbcinst.ini /etc/odbcinst.ini

# Copy Python packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY oasis/ /app/oasis/
COPY models/ /app/models/
COPY neutral_network_export/ /app/neutral_network_export/
COPY oasis_engines_config.json /app/
COPY requirements.txt /app/

# Copy database migrations (entrypoint --mode migrate)
COPY alembic.ini /app/
COPY alembic/ /app/alembic/

# Copy dashboard files
COPY ops_dashboard.py /app/dashboards/
COPY approval_dashboard.py /app/dashboards/
COPY shadow_dashboard.py /app/dashboards/
COPY kuber_terminal.py /app/dashboards/
COPY st_gat_dashboard.py /app/dashboards/
COPY command_center.py /app/dashboards/
COPY allocation_app.py /app/dashboards/

# Copy Flet app
COPY integrated_app.py /app/flet_app/

# Copy helper modules that dashboards import.
# (Shared UI components now live in oasis/ui/, already copied via `COPY oasis/`.
#  The old root-level ui_components.py no longer exists — removed to unbreak the build.)
COPY shadow_monitor.py /app/dashboards/

# Copy entrypoint
COPY entrypoint.py /app/

# Create required directories
RUN mkdir -p /data/inbound_drops/bootstrap \
             /data/inbound_drops/archive \
             /app/logs \
             /app/oasis/data/pipeline_logs \
             /app/oasis/data/approved_pos \
             /app/shadow_logs

# Set environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONUTF8=1
ENV PYTHONPATH=/app
ENV OASIS_CLIENT_CONFIG=/data/oasis_client_config.json

# Expose dashboard + API ports
EXPOSE 8501 8502 8503 8504 8505 8550 8600

# Health check
HEALTHCHECK --interval=60s --timeout=10s --retries=3 \
    CMD python -c "from oasis.logic.data_gateway import DataGateway; gw = DataGateway(); print(gw.health_check())" || exit 1

# Default entrypoint
ENTRYPOINT ["python", "/app/entrypoint.py"]
CMD ["--mode", "full"]
