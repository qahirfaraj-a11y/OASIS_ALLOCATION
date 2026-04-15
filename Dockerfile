# ─── Stage 1: Build dependencies ───
FROM python:3.10-slim AS builder

WORKDIR /app

# System deps for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ─── Stage 2: Runtime ───
FROM python:3.10-slim

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application code
COPY oasis/ ./oasis/
COPY ops_dashboard.py .
COPY allocation_app.py .
COPY integrated_app.py .
COPY st_gat_dashboard.py .
COPY intraday_sim.py .
COPY retail_simulator.py .
COPY network_simulation.py .
COPY store_network_generator.py .
COPY generate_showcase_scenario.py .

# Copy models and config
COPY models/ ./models/
COPY store_coords.json .
COPY staple_products.json .
COPY barcode_department_map.json .
COPY product_department_map.json .

# Copy scorecards (latest version)
COPY Full_Product_Allocation_Scorecard_v*.csv ./

# Copy supplier calendar
COPY Supplier_Order_Calendar_2026.xlsx .

# Create data directory for persistent DB
RUN mkdir -p oasis/data

# Expose Streamlit port
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8501/_stcore/health')" || exit 1

# Streamlit configuration
ENV STREAMLIT_SERVER_PORT=8501
ENV STREAMLIT_SERVER_HEADLESS=true
ENV STREAMLIT_BROWSER_GATHER_USAGE_STATS=false
ENV STREAMLIT_SERVER_FILE_WATCHER_TYPE=none

ENTRYPOINT ["streamlit", "run", "ops_dashboard.py", \
            "--server.port=8501", \
            "--server.headless=true", \
            "--browser.gatherUsageStats=false"]
