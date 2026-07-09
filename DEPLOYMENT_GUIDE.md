# OASIS Retail Manager — Deployment Guide

> Production deployment instructions for Docker and bare-metal environments.

---

## Prerequisites

| Requirement | Version | Notes |
|-------------|---------|-------|
| Python | 3.10+ | For bare-metal deployment |
| Docker | 20.10+ | For containerized deployment |
| Docker Compose | 2.0+ | Optional, simplifies multi-service setup |
| RAM | 2 GB+ | 4 GB recommended with GNN inference |
| Disk | 500 MB+ | Plus database and model files |

---

## Option 1: Docker Deployment (Recommended)

### Quick Start
```bash
cd C:\Users\iLink\.gemini\antigravity\scratch

# Build and start
docker compose up -d

# Check status
docker compose ps
docker compose logs -f oasis-app

# Stop
docker compose down
```

### Custom Port
```bash
# Edit docker-compose.yml or override:
docker compose up -d -e STREAMLIT_SERVER_PORT=9000
```

### Persistent Data
Data is stored in Docker named volumes:
- `oasis-data` → SQLite database (`oasis/data/`)
- `oasis-uploads` → User uploads

To back up:
```bash
docker run --rm -v oasis-data:/data -v $(pwd):/backup alpine tar czf /backup/oasis-data-backup.tar.gz /data
```

### Production MSSQL Connection
Set environment variables in `docker-compose.yml`:
```yaml
environment:
  - OASIS_DB_TYPE=mssql
  - OASIS_DB_HOST=your-db-server.example.com
  - OASIS_DB_NAME=OASIS_PROD
  - OASIS_DB_USER=oasis_user
  - OASIS_DB_PASS=your-secure-password
```

---

## Option 2: Bare-Metal (Local Development)

### Setup
```bash
cd C:\Users\iLink\.gemini\antigravity\scratch

# Install dependencies
pip install -r requirements.txt

# Build mock database (first time only)
python -m oasis.logic.mock_pos_erp

# Launch
streamlit run ops_dashboard.py --server.headless true --browser.gatherUsageStats false
```

### Using the Batch File
```bash
run_command_center.bat
```
This handles Python checks, dependency installation, database creation, and launching.

---

## Scheduler Configuration

The scheduler runs background jobs inside the Streamlit process.

### Available Jobs

| Job | Default Schedule | Description |
|-----|-----------------|-------------|
| Morning PO | `0 6 * * *` (6:00 AM) | Scans all stores for PO recommendations |
| Hourly Monitor | `0 * * * *` (Every hour) | Checks stock levels, flags critical items |
| Evening Summary | `0 20 * * *` (8:00 PM) | Generates end-of-day KPI summary |

### Managing via UI
1. Login as `ops_admin`
2. Navigate to ⚙️ Settings → Scheduler section
3. Click **▶️ Start Scheduler** to activate
4. Toggle individual jobs on/off
5. Use **▶️ Run Now** for manual triggers

### Cron Format
```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of month (1-31)
│ │ │ ┌───────────── month (1-12)
│ │ │ │ ┌───────────── day of week (0-6, Sun=0)
│ │ │ │ │
* * * * *
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `STREAMLIT_SERVER_PORT` | `8501` | Dashboard port |
| `STREAMLIT_SERVER_HEADLESS` | `true` | Run without browser auto-open |
| `OASIS_DB_TYPE` | `sqlite` | Database type (`sqlite` or `mssql`) |
| `ANTHROPIC_API_KEY` | — | Required for Stage 6 AI Strategic Analysis |
| `DATABASE_DIR` | `./oasis/data` | Directory for all JSON/CSV intelligence databases |
| `SHOPIFY_SYNC` | `true` | Enable Phase 1 online demand blending |

---

## Standalone Apps (Mosaic Architecture)

These standalone apps remain available for independent testing:

| App | Launcher | Purpose |
|-----|----------|---------|
| Allocation Engine | `run_allocation_app.bat` | Budget-constrained order generation |
| OASIS Desktop | `run_app.bat` | Offline file processor (Flet) |
| Integrated App | `run_integrated_app.bat` | Allocation → Simulation → Supplier lifecycle |
| ST-GAT Dashboard | `run_st_gat_dashboard.bat` | GNN network visualization |
| **Command Center** | `run_command_center.bat` | **Unified dashboard (all features)** |

---

## Final Pre-Flight Checklist

Before switching the system to LIVE production, verify the following:

1. **Database Sync**: Ensure `oasis/data/mock_pos_erp.db` is populated. Run `python -m oasis.logic.mock_pos_erp` if missing.
2. **Environment Mode**: Check `.env` contains `production_mode=true`.
3. **API Connectivity**: If using AI Strategic Analysis, ensure `ANTHROPIC_API_KEY` is set.
4. **Port Clearance**: Ensure port `8501` is not blocked by a previous instance.
5. **Intelligence Cache**: Verify `oasis/data/oasis_engines_config.json` is present and engines are `enabled: true`.

---

## Troubleshooting

### "Mock DB not found"
```bash
python -m oasis.logic.mock_pos_erp
```

### "Streamlit not installed"
```bash
pip install -r requirements.txt
```

### Docker build fails
```bash
# Check Docker is running
docker info

# Build with verbose output
docker compose build --no-cache --progress=plain
```

### Port already in use
```bash
# Find and kill the process using port 8501
netstat -ano | findstr :8501
taskkill /PID <PID> /F
```

### GNN model not loading
Ensure `store_gnn_checkpoint.pt` and `stores_network.json` are in the project root or models/ folder.
These files are intentionally excluded from `.dockerignore` — uncomment the exclusions if you don't need GNN features.
