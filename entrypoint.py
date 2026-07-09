"""
O.A.S.I.S. Container Entrypoint
=================================
Orchestrates which services run inside the Docker container.

Modes:
    --mode full      : Engine + all dashboards (single-container deployment)
    --mode engine    : Scheduler + FileWatcher + Heartbeat only
    --mode dashboard : Single Streamlit dashboard (--dashboard ops|shadow|approval|stgat)
    --mode api       : Mobile API (FastAPI/uvicorn, default port 8550)
    --mode bridge    : Manager Bridge API (FastAPI/uvicorn, default port 8600)
    --mode migrate   : Run Alembic migrations (alembic upgrade head) and exit
    --mode bootstrap : Run Day-0 initialization and exit

Usage:
    python entrypoint.py --mode full
    python entrypoint.py --mode dashboard --dashboard ops
    python entrypoint.py --mode api --port 8550
    python entrypoint.py --mode migrate
    python entrypoint.py --mode bootstrap
"""

import argparse
import logging
import os
import signal
import subprocess
import sys
import time
import threading

LOG_DIR = os.environ.get("OASIS_LOG_DIR", os.path.join(os.path.dirname(__file__), "logs"))
os.makedirs(LOG_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(LOG_DIR, "entrypoint.log"),
                            encoding="utf-8"),
    ]
)
logger = logging.getLogger("OASIS.Entrypoint")

# ── Dashboard Map ─────────────────────────────────────────────────────
DASHBOARD_MAP = {
    "ops":        "ops_dashboard.py",
    "shadow":     "shadow_dashboard.py",
    "approval":   "approval_dashboard.py",
    "stgat":      "st_gat_dashboard.py",
    "command":    "ops_dashboard.py",
    "allocation": "allocation_app.py",
    "integrated": "integrated_app.py",
    "pitch":      "pitch_app_v2.py",
}

DEFAULT_PORTS = {
    "ops": 8501, "command": 8501, "allocation": 8502, "stgat": 8502,
    "integrated": 8503, "pitch": 8504, "shadow": 8506, "approval": 8507,
}

# ── Graceful Shutdown ─────────────────────────────────────────────────
_shutdown = threading.Event()


def _handle_signal(signum, frame):
    logger.info(f"Received signal {signum}. Shutting down gracefully...")
    _shutdown.set()


signal.signal(signal.SIGTERM, _handle_signal)
signal.signal(signal.SIGINT, _handle_signal)


# ── Mode: Engine ──────────────────────────────────────────────────────

def run_engine():
    """Start the core engine: Scheduler + FileWatcher + Heartbeat."""
    logger.info("Starting O.A.S.I.S. Engine...")

    from oasis.logic.data_gateway import load_client_config
    config = load_client_config()

    # 1. Ensure DB is initialized
    db_path = config.get("paths", {}).get("db_path", "/data/oasis.db")
    try:
        from oasis.logic.db_connector import UniversalConnector
        connector = UniversalConnector(db_path)
        connector.ensure_oasis_tables()
        logger.info(f"Database initialized: {db_path}")
    except Exception as e:
        logger.error(f"DB initialization failed: {e}")

    # 2. Start Scheduler with cycle preset
    scheduler = None
    try:
        from oasis.logic.scheduler_service import OasisScheduler
        scheduler = OasisScheduler(db_path)

        cycle = config.get("scheduler", {}).get("cycle", "24_HOUR")
        scheduler.apply_cycle(cycle)
        scheduler.start()
        logger.info(f"Scheduler started with cycle: {cycle}")
    except Exception as e:
        logger.error(f"Scheduler start failed: {e}")

    # 3. Start FileWatcher (for file-dump clients)
    watcher_thread = None
    pathway = config.get("data_pathway", "file")
    if pathway in ("file", "hybrid"):
        try:
            from oasis.logic.file_watcher import start_watcher
            data_dir = config.get("paths", {}).get("data_dir", "/app/oasis/data")
            watcher_thread = threading.Thread(
                target=start_watcher, args=(data_dir,), daemon=True
            )
            watcher_thread.start()
            logger.info("FileWatcher started.")
        except Exception as e:
            logger.error(f"FileWatcher start failed: {e}")

    # 4. Start Heartbeat
    heartbeat = None
    try:
        from oasis.logic.heartbeat import HeartbeatService
        heartbeat = HeartbeatService(config)
        heartbeat.start()
    except Exception as e:
        logger.error(f"Heartbeat start failed: {e}")

    # 5. Hold the process alive until shutdown signal
    logger.info("O.A.S.I.S. Engine running. Waiting for shutdown signal...")
    _shutdown.wait()

    # Cleanup
    logger.info("Shutting down engine services...")
    if scheduler:
        scheduler.stop()
    if heartbeat:
        heartbeat.stop()
    logger.info("Engine shutdown complete.")


# ── Mode: Dashboard ───────────────────────────────────────────────────

def run_dashboard(name: str, port: int = 8501):
    """Launch a single Streamlit dashboard."""
    script = DASHBOARD_MAP.get(name)
    if not script:
        logger.error(f"Unknown dashboard: {name}. Valid: {list(DASHBOARD_MAP.keys())}")
        sys.exit(1)

    if not os.path.exists(script):
        logger.error(f"Dashboard script not found: {script}")
        sys.exit(1)

    logger.info(f"Starting dashboard: {name} on port {port}")

    cmd = [
        sys.executable, "-m", "streamlit", "run", script,
        "--server.port", str(port),
        "--server.address", "0.0.0.0",
        "--server.headless", "true",
        "--browser.gatherUsageStats", "false",
    ]

    proc = subprocess.Popen(cmd)
    _shutdown.wait()
    proc.terminate()
    proc.wait(timeout=10)
    logger.info(f"Dashboard {name} stopped.")


# ── Mode: API / Bridge (FastAPI via uvicorn) ─────────────────────────

def run_api(port: int = 8550):
    """Start the Mobile API (oasis.api.server) under uvicorn."""
    logger.info(f"Starting O.A.S.I.S. Mobile API on port {port}...")
    cmd = [
        sys.executable, "-m", "uvicorn", "oasis.api.server:app",
        "--host", "0.0.0.0", "--port", str(port),
    ]
    proc = subprocess.Popen(cmd)
    _shutdown.wait()
    proc.terminate()
    proc.wait(timeout=10)
    logger.info("Mobile API stopped.")


def run_bridge(port: int = 8600):
    """Start the Manager Bridge API (oasis.api.bridge) under uvicorn."""
    logger.info(f"Starting O.A.S.I.S. Manager Bridge on port {port}...")
    cmd = [
        sys.executable, "-m", "uvicorn", "oasis.api.bridge:app",
        "--host", "0.0.0.0", "--port", str(port),
    ]
    proc = subprocess.Popen(cmd)
    _shutdown.wait()
    proc.terminate()
    proc.wait(timeout=10)
    logger.info("Manager Bridge stopped.")


# ── Mode: Migrate (Alembic) ──────────────────────────────────────────

def _require_module(module: str) -> None:
    """Gate a CLI mode on a licensed/trial module SKU (fail-closed)."""
    from oasis.logic.license_manager import MODULE_LABELS, allowed_modules
    if module not in allowed_modules():
        label = MODULE_LABELS.get(module, module)
        raise SystemExit(f"This command is part of the '{label}' module, which "
                         "is not licensed for this install. Contact iLink.")


def _read_version() -> str:
    """Platform version from the VERSION file (single source of truth)."""
    try:
        with open(os.path.join(os.path.dirname(os.path.abspath(__file__)),
                               "VERSION"), "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return "unknown"


def run_upgrade():
    """Safe on-prem upgrade: backup the store DB, apply migrations, preflight.

    Run this AFTER unpacking a new release over the install directory. If the
    preflight fails, restore the printed backup with --mode restore --file.
    """
    from oasis.logic.backup_util import backup_db
    root = os.path.dirname(os.path.abspath(__file__))
    db_path = os.getenv("OASIS_DB_PATH",
                        os.path.join(root, "oasis", "data", "rhapta_pos.db"))
    print(f"[upgrade] O.A.S.I.S. v{_read_version()}")
    if os.path.exists(db_path):
        res = backup_db(db_path, keep=int(os.getenv("OASIS_BACKUP_KEEP", "10")))
        print(f"[upgrade] 1/3 backup: {res['backup']} ({res['size_mb']} MB)")
    else:
        print(f"[upgrade] 1/3 backup: skipped (no DB at {db_path})")
    print("[upgrade] 2/3 migrations…")
    run_migrate()
    print("[upgrade] 3/3 preflight…")
    from oasis.logic.preflight import format_report, run_preflight
    report = run_preflight()
    print(format_report(report))
    if report["overall"] == "FAIL":
        print("[upgrade] PREFLIGHT FAILED — restore the backup above with "
              "--mode restore --file <backup> and contact iLink.")
        sys.exit(2)
    print("[upgrade] complete — relaunch the consoles.")


def run_migrate():
    """Apply Alembic migrations (upgrade head) against OASIS_DB_URL.

    Runs alembic in-process (`python -m alembic` is unsupported by the installed
    alembic). Databases that predate alembic (all current installs: schema
    exists but no alembic_version table) are STAMPED at head first — the
    baseline migration would otherwise try to re-create existing tables.
    Errors are printed (alembic's fileConfig silences our logger).
    """
    print("[migrate] applying Alembic migrations (upgrade head)…")
    root = os.path.dirname(os.path.abspath(__file__))
    cwd = os.getcwd()
    try:
        from alembic import command
        from alembic.config import Config
        from sqlalchemy import create_engine, inspect

        from oasis.logic import db as oasis_db
        os.chdir(root)   # alembic.ini + migrations/ live at the project root
        cfg = Config(os.path.join(root, "alembic.ini"))

        url = oasis_db.get_sqlalchemy_url()
        insp = inspect(create_engine(url))
        tables = set(insp.get_table_names())
        if "alembic_version" not in tables and "ORGANIZATION_MST" in tables:
            print("[migrate] existing pre-alembic schema detected — stamping head")
            command.stamp(cfg, "head")
        command.upgrade(cfg, "head")
        print("[migrate] migrations applied successfully.")
    except SystemExit as e:
        if e.code not in (0, None):
            print(f"[migrate] FAILED (exit {e.code})")
            sys.exit(int(e.code))
    except Exception as e:
        print(f"[migrate] FAILED: {e}")
        sys.exit(1)
    finally:
        os.chdir(cwd)


# ── Mode: Shell (unified single front door, U3) ──────────────────────

def _run_streamlit_console(script_name: str, port: int, label: str):
    logger.info(f"Starting {label} on port {port}...")
    script = os.path.join(os.path.dirname(os.path.abspath(__file__)), script_name)
    cmd = [
        sys.executable, "-m", "streamlit", "run", script,
        "--server.port", str(port),
        "--server.address", "0.0.0.0",
        "--server.headless", "true",
        "--browser.gatherUsageStats", "false",
    ]
    proc = subprocess.Popen(cmd)
    _shutdown.wait()
    proc.terminate()
    proc.wait(timeout=10)
    logger.info(f"{label} stopped.")


def run_shell(port: int = 8500):
    """Launch the Operations Console (app.py) — transactional surfaces."""
    _run_streamlit_console("app.py", port, "O.A.S.I.S. Operations Console")


def run_intel(port: int = 8510):
    """Launch the Intelligence Console (app_intel.py) — monitoring surfaces."""
    _run_streamlit_console("app_intel.py", port, "O.A.S.I.S. Intelligence Console")


def run_home(port: int = 8490):
    """Launch the suite Home launcher (home_app.py) — one front door."""
    _run_streamlit_console("home_app.py", port, "O.A.S.I.S. Home")


# ── Mode: Full ────────────────────────────────────────────────────────

def run_full():
    """Start engine + all dashboards in one container."""
    logger.info("Starting O.A.S.I.S. Full Stack...")

    # Start engine in background thread
    engine_thread = threading.Thread(target=run_engine, daemon=True)
    engine_thread.start()

    # Give engine 5 seconds to initialize DB
    time.sleep(5)

    # Start dashboards as subprocesses
    from oasis.logic.data_gateway import load_client_config
    config = load_client_config()
    ui_cfg = config.get("ui", {})

    dashboard_procs = []
    for name, script in DASHBOARD_MAP.items():
        if not os.path.exists(script):
            continue
        port_key = f"{name}_dashboard_port" if name != "command" else "command_center_port"
        port = ui_cfg.get(port_key, 8501 + len(dashboard_procs))

        cmd = [
            sys.executable, "-m", "streamlit", "run", script,
            "--server.port", str(port),
            "--server.address", "0.0.0.0",
            "--server.headless", "true",
            "--browser.gatherUsageStats", "false",
        ]
        proc = subprocess.Popen(cmd)
        dashboard_procs.append((name, proc))
        logger.info(f"Dashboard {name} started on port {port}")

    # Hold until shutdown
    _shutdown.wait()

    # Cleanup dashboards
    for name, proc in dashboard_procs:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        logger.info(f"Dashboard {name} stopped.")


# ── Mode: Showcase ────────────────────────────────────────────────────

def run_showcase(port: int = 8505):
    """Reset the narrative DB then launch the command center in showcase mode."""
    logger.info("Starting O.A.S.I.S. Showcase Mode...")
    os.environ["OASIS_SHOWCASE_MODE"] = "true"
    showcase_db = os.path.join("oasis", "data", "mock_pos_erp_showcase.db")
    os.environ.setdefault("OASIS_DB_PATH", showcase_db)

    # Regenerate demo data
    gen_script = os.path.join(os.path.dirname(__file__), "generate_showcase_scenario.py")
    if os.path.exists(gen_script):
        logger.info("Resetting showcase narrative...")
        subprocess.run([sys.executable, gen_script], check=False)

    run_dashboard("command", port)


# ── Mode: Shadow (with background daemon) ────────────────────────────

def run_shadow_full(port: int = 8506, pathway: str = "file"):
    """Launch the shadow daemon in background + dashboard in foreground."""
    logger.info("Starting O.A.S.I.S. Shadow Auditor...")

    # Ensure directories
    for d in ("monitoring/inbound", "monitoring/reports", "monitoring/archive", "shadow_logs"):
        os.makedirs(d, exist_ok=True)

    # Background daemon
    daemon_log = os.path.join("shadow_logs", "shadow_daemon_console.log")
    daemon_cmd = [sys.executable, "shadow_monitor.py", "--mode", pathway, "--root", "."]
    with open(daemon_log, "a") as f:
        daemon = subprocess.Popen(daemon_cmd, stdout=f, stderr=subprocess.STDOUT)
    logger.info(f"Shadow daemon PID {daemon.pid}")

    try:
        run_dashboard("shadow", port)
    finally:
        daemon.terminate()
        daemon.wait(timeout=10)
        logger.info("Shadow daemon stopped.")


# ── Mode: Simulation ─────────────────────────────────────────────────

def run_simulation(scenario: str = "Baseline", days: int = 30,
                   budget: int = 300000, month: str = "NOV"):
    """Run a simulation scenario via the CLI runner."""
    logger.info(f"Starting simulation: {scenario} ({days}d, {budget} KES, {month})")
    script = os.path.join(os.path.dirname(__file__), "run_simulation_scenario.py")
    if not os.path.exists(script):
        logger.error(f"Simulation runner not found: {script}")
        sys.exit(1)

    cmd = [sys.executable, script,
           "--scenario", scenario, "--days", str(days),
           "--budget", str(budget), "--month", month]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


# ── Mode: Desktop (Flet native) ──────────────────────────────────────

def run_desktop():
    """Launch the Flet native desktop app."""
    logger.info("Starting O.A.S.I.S. Desktop App...")
    cmd = [sys.executable, "-m", "oasis.main"]
    result = subprocess.run(cmd)
    sys.exit(result.returncode)


# ── Mode: Bootstrap ───────────────────────────────────────────────────

def run_bootstrap():
    """Execute Day-0 initialization and exit."""
    logger.info("Running Day-0 Bootstrap...")
    from oasis.logic.data_gateway import DataGateway
    gw = DataGateway()
    report = gw.bootstrap_retail_universe()

    # Print summary
    steps = report.get("steps", {})
    ok_count = sum(1 for v in steps.values() if isinstance(v, dict) and v.get("status") == "OK")
    fail_count = sum(1 for v in steps.values() if isinstance(v, dict) and v.get("status") == "FAILED")

    logger.info(f"Bootstrap complete: {ok_count} OK, {fail_count} FAILED")
    logger.info(f"Total products ingested: {steps.get('total_products', 0)}")

    if fail_count > 0:
        logger.warning("Some steps failed. Check bootstrap_report.json for details.")
        sys.exit(1)
    sys.exit(0)


# ── Main ──────────────────────────────────────────────────────────────

def _find_open_port(start: int) -> int:
    """Find the first open TCP port starting from `start`."""
    import socket
    port = start
    while port < start + 100:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            if s.connect_ex(("127.0.0.1", port)) != 0:
                return port
        port += 1
    return start


def main():
    parser = argparse.ArgumentParser(description="O.A.S.I.S. Unified Entrypoint")
    parser.add_argument("--mode",
                        choices=["full", "engine", "dashboard", "showcase",
                                 "shadow", "simulation", "desktop", "bootstrap",
                                 "api", "bridge", "migrate", "shell", "intel",
                                 "preflight", "build-views", "bootstrap-intel",
                                 "bootstrap-governance", "build-graph",
                                 "build-store-graph", "build-baskets", "build-prior",
                                 "build-pos-db", "seed-history", "seed-real-demand",
                                 "pos-sim", "pos-stream", "pos-inject",
                                 "build-multi-store-db", "seed-multi-history",
                                 "multi-pos-stream",
                                 "issue-license", "license-status",
                                 "backup", "restore", "set-password",
                                 "package-release", "set-branding", "show-branding",
                                 "value-report", "metering-report", "home",
                                 "version", "upgrade", "assess",
                                 "supplier-scorecard", "category-report",
                                 "inject-grn-costs", "sku-deepdive"],
                        default="full", help="Run mode")
    parser.add_argument("--dashboard", choices=list(DASHBOARD_MAP.keys()),
                        default="ops", help="Dashboard to launch (dashboard mode)")
    parser.add_argument("--port", type=int, default=0,
                        help="Port (0 = auto-detect from default + port-hunt)")
    # Simulation options
    parser.add_argument("--scenario", default="Baseline")
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--budget", type=int, default=300000)
    parser.add_argument("--month", default="NOV")
    # Shadow option
    parser.add_argument("--pathway", choices=["file", "sql"], default="file",
                        help="Data pathway for shadow mode")
    # Mock POS injector options
    parser.add_argument("--batches", type=int, default=10, help="pos-inject: number of bills")
    parser.add_argument("--interval", type=float, default=15.0, help="pos-inject: seconds between bills")
    parser.add_argument("--org", default=None, help="pos-inject: org to inject into (default: first active)")
    parser.add_argument("--sku-count", type=int, default=8, help="pos-inject: SKUs per bill")
    # Licensing (issue-license / license-status)
    parser.add_argument("--tenant", default=None, help="issue-license: tenant id")
    parser.add_argument("--modules", default=None,
                        help="issue-license: comma-separated module list "
                             "(core,ordering,network,revenue,api)")
    parser.add_argument("--bundle", default=None,
                        help="issue-license: bundle preset "
                             "(starter | pro | enterprise) — overrides --modules")
    parser.add_argument("--expiry", default=None, help="issue-license: YYYY-MM-DD")
    parser.add_argument("--out", default=None, help="issue-license: output key path")
    # Category report
    parser.add_argument("--category", default="alcohol",
                        help="category-report: named section (alcohol/dairy/beverages)")
    parser.add_argument("--departments", default=None,
                        help="category-report: comma-separated depts (overrides --category)")
    parser.add_argument("--pdf", action="store_true",
                        help="category-report/sku-deepdive: also render a PDF")
    parser.add_argument("--files", default=None,
                        help="sku-deepdive: comma-separated snapshot xlsx paths")
    parser.add_argument("--section", default="alcohol",
                        help="sku-deepdive: section label for the report")
    # Backup / restore / credentials
    parser.add_argument("--file", default=None, help="restore: backup file to restore")
    parser.add_argument("--username", default=None, help="set-password: user to update")
    parser.add_argument("--password", default=None, help="set-password: new password")
    # Whitelabel branding
    parser.add_argument("--tenant-name", default=None, help="set-branding: client display name")
    parser.add_argument("--product-name", default=None, help="set-branding: product name shown to users (default: OASIS)")
    parser.add_argument("--logo", default=None, help="set-branding: path to logo (png/jpg, embedded as data-uri)")
    parser.add_argument("--primary-color", default=None, help="set-branding: hex primary/accent")
    parser.add_argument("--accent-color", default=None, help="set-branding: hex hover/darker accent")
    parser.add_argument("--welcome-message", default=None, help="set-branding: Home page subtitle")
    # Release packaging
    parser.add_argument("--bundle-runtime", action="store_true",
                        help="package-release: include embedded Python + wheels for offline install")
    # Pre-flight diagnostics toggle
    parser.add_argument("--skip-diag", action="store_true",
                        help="Skip production_diagnostic.py preflight")
    args = parser.parse_args()

    logger.info(f"O.A.S.I.S. v{_read_version()} — Mode: {args.mode}")

    # Resolve port
    if args.port == 0:
        default = DEFAULT_PORTS.get(args.dashboard, 8501)
        port = _find_open_port(default)
    else:
        port = args.port

    # Optional pre-flight diagnostics
    if args.mode in ("dashboard", "showcase", "shadow") and not args.skip_diag:
        diag = os.path.join(os.path.dirname(__file__), "production_diagnostic.py")
        if os.path.exists(diag):
            logger.info("Running pre-flight diagnostics...")
            result = subprocess.run([sys.executable, diag])
            if result.returncode != 0:
                logger.error("Diagnostics failed — start aborted. Use --skip-diag to override.")
                sys.exit(1)

    if args.mode == "full":
        run_full()
    elif args.mode == "engine":
        run_engine()
    elif args.mode == "dashboard":
        run_dashboard(args.dashboard, port)
    elif args.mode == "showcase":
        run_showcase(port)
    elif args.mode == "shadow":
        run_shadow_full(port, args.pathway)
    elif args.mode == "simulation":
        run_simulation(args.scenario, args.days, args.budget, args.month)
    elif args.mode == "desktop":
        run_desktop()
    elif args.mode == "bootstrap":
        run_bootstrap()
    elif args.mode == "api":
        run_api(args.port if args.port else 8550)
    elif args.mode == "bridge":
        run_bridge(args.port if args.port else 8600)
    elif args.mode == "migrate":
        run_migrate()
    elif args.mode == "shell":
        run_shell(args.port if args.port else 8500)
    elif args.mode == "intel":
        run_intel(args.port if args.port else 8510)
    elif args.mode == "home":
        run_home(args.port if args.port else 8490)
    elif args.mode == "preflight":
        from oasis.logic.preflight import run_preflight, format_report
        report = run_preflight()
        print(format_report(report))
        sys.exit(0 if report["overall"] != "FAIL" else 2)
    elif args.mode == "build-views":
        from oasis.logic.client_schema import (
            load_profile, identity_profile, validate_profile,
            generate_view_ddl, create_clause_for,
        )
        profile_path = os.getenv("OASIS_SCHEMA_PROFILE")
        profile = load_profile(profile_path) if profile_path else identity_profile()
        v = validate_profile(profile)
        dialect = os.getenv("OASIS_DB_DIALECT", "ansi")
        print("\n".join(generate_view_ddl(profile, create_clause_for(dialect))))
        if not v["ok"]:
            sys.stderr.write(f"WARN: profile does not satisfy the required contract: {v}\n")
        sys.exit(0 if v["ok"] else 2)
    elif args.mode == "bootstrap-intel":
        from oasis.logic import db as oasis_db
        from oasis.logic.db_connector import SchemaMapper, UniversalConnector
        from oasis.logic.intel_bootstrap import bootstrap_intelligence
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        data_dir = os.getenv("OASIS_DATA_DIR",
                             os.path.join(os.path.dirname(__file__), "oasis", "data"))
        adapter = PosErpAdapter(UniversalConnector(
            oasis_db.get_pos_sqlalchemy_url(), SchemaMapper.for_pos_erp()))
        summary = bootstrap_intelligence(adapter, data_dir)
        print(f"Intelligence bootstrap complete: {summary}")
    elif args.mode == "bootstrap-governance":
        from oasis.logic.governance_bootstrap import run_governance
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        nn_path = os.getenv("OASIS_NN_PATH", os.path.join(root, "neutral_network_export"))
        summary = run_governance(data_dir, nn_path)
        print(f"Governance bootstrap: {summary}")
        sys.exit(0 if summary.get("overall") != "FAILED" else 2)
    elif args.mode == "build-graph":
        from oasis.logic.graph_export import build_graph_from_data
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        out_dir = os.getenv("OASIS_NN_OUT", os.path.join(root, "neutral_network_export"))
        k = int(os.getenv("OASIS_SUBSTITUTION_K", "8"))
        summary = build_graph_from_data(data_dir, out_dir, k=k)
        print(f"Graph export complete: {{'skus': {summary['skus']}, "
              f"'nodes': {summary['nodes']}, 'edges': {summary['edges']}, "
              f"'relations': {summary['relations']}}}")
    elif args.mode == "build-store-graph":
        from oasis.logic import db as oasis_db
        from oasis.logic.db_connector import SchemaMapper, UniversalConnector
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        from oasis.logic.store_graph_export import build_store_graph_from_adapter
        root = os.path.dirname(__file__)
        out_path = os.getenv("OASIS_STORE_GRAPH_OUT",
                             os.path.join(root, "stores_network.json"))
        adapter = PosErpAdapter(UniversalConnector(
            oasis_db.get_pos_sqlalchemy_url(), SchemaMapper.for_pos_erp()))
        summary = build_store_graph_from_adapter(adapter, out_path)
        print(f"Store-graph export complete: {summary}")
    elif args.mode == "build-baskets":
        from oasis.logic.basket_affinity import build_baskets_from_db
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "mock_pos_erp.db"))
        nn_dir = os.getenv("OASIS_NN_OUT", os.path.join(root, "neutral_network_export"))
        min_count = int(os.getenv("OASIS_BASKET_MIN_COUNT", "3"))
        min_lift = float(os.getenv("OASIS_BASKET_MIN_LIFT", "1.0"))
        min_item = int(os.getenv("OASIS_BASKET_MIN_ITEM_COUNT", "5"))
        summary = build_baskets_from_db(db_path, nn_dir, min_count=min_count,
                                        min_lift=min_lift, min_item_count=min_item)
        print(f"Basket affinity build complete: {summary}")
    elif args.mode == "build-prior":
        from oasis.logic.vault_prior import build_department_prior
        root = os.path.dirname(__file__)
        vault = os.getenv("OASIS_VAULT_DIR", os.path.join(root, "oasis_vault"))
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        out = os.getenv("OASIS_BASKET_PRIOR",
                        os.path.join(root, "neutral_network_export", "basket_prior.json"))
        print(f"Department halo prior built: {build_department_prior(vault, data_dir, out)}")
    elif args.mode == "build-pos-db":
        from oasis.logic.mock_pos_build import build_from_xlsx
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        summary = build_from_xlsx(data_dir, db_path)
        # Seed a prior-days demand history (for a normalised ADS baseline) unless
        # disabled. Stock is untouched; today is left empty (start of day).
        hist_days = int(os.getenv("OASIS_HISTORY_DAYS", "30"))
        if hist_days > 0:
            from oasis.logic.pos_simulator import seed_demand_history
            prior = os.getenv("OASIS_BASKET_PRIOR",
                              os.path.join(root, "neutral_network_export", "basket_prior.json"))
            bpd = int(os.getenv("OASIS_HISTORY_BILLS_PER_DAY", "400"))
            summary["history"] = seed_demand_history(db_path, prior_path=prior,
                                                     days=hist_days, bills_per_day=bpd)
        print(f"Clean POS DB built from catalog: {summary}")
    elif args.mode == "seed-real-demand":
        from oasis.logic.real_demand import seed_real_demand_from_files
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        cash_dir = os.getenv("OASIS_CASH_DIR",
                             os.path.join(os.path.expanduser("~"), "Desktop", "Projects"))
        print(f"Real demand seeded from {cash_dir}: "
              f"{seed_real_demand_from_files(db_path, cash_dir, org=args.org or 'ORG001')}")
    elif args.mode == "seed-history":
        from oasis.logic.pos_simulator import seed_demand_history
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        prior = os.getenv("OASIS_BASKET_PRIOR",
                          os.path.join(root, "neutral_network_export", "basket_prior.json"))
        days = int(os.getenv("OASIS_HISTORY_DAYS", "30"))
        bpd = int(os.getenv("OASIS_HISTORY_BILLS_PER_DAY", "400"))
        print(f"Demand history seeded: {seed_demand_history(db_path, prior_path=prior, days=days, bills_per_day=bpd)}")
    elif args.mode == "pos-sim":
        from oasis.logic.pos_simulator import run_simulator
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        prior = os.getenv("OASIS_BASKET_PRIOR",
                          os.path.join(root, "neutral_network_export", "basket_prior.json"))
        run_simulator(db_path, prior_path=prior, batches=args.batches,
                      interval=args.interval, org=args.org or "ORG001")
    elif args.mode == "pos-stream":
        from oasis.logic.pos_simulator import stream_realtime
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        prior = os.getenv("OASIS_BASKET_PRIOR",
                          os.path.join(root, "neutral_network_export", "basket_prior.json"))
        # real-time defaults: pace 2s, stream until Ctrl-C (--batches 0)
        interval = args.interval if args.interval != 15.0 else 2.0
        stream_realtime(db_path, prior_path=prior, org=args.org or "ORG001",
                        interval=interval, batches=args.batches if args.batches != 10 else 0)
    elif args.mode == "version":
        print(f"O.A.S.I.S. v{_read_version()}")
    elif args.mode == "upgrade":
        run_upgrade()
    elif args.mode == "assess":
        from oasis.logic.day0_assessment import write_assessment
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        cash_dir = os.getenv("OASIS_CASH_DIR",
                             os.path.join(os.path.expanduser("~"), "Desktop", "Projects"))
        out_dir = os.getenv("OASIS_REPORTS_DIR", os.path.join(root, "reports"))
        res = write_assessment(data_dir, cash_dir, out_dir, tenant=args.tenant or "")
        print(f"Day-0 assessment written: {res['markdown']}")
        print(f"  {res['skus']:,} SKUs | dead stock KES {res['dead_stock_value']:,.0f} "
              f"({res['dead_skus']:,} SKUs) | ghost sellers {res['ghost_sellers']:,} "
              f"| demand coverage {res['coverage_pct']}%")
    elif args.mode == "category-report":
        _require_module("revenue")
        from oasis.logic.category_report import write_category_report
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        cash_dir = os.getenv("OASIS_CASH_DIR",
                             os.path.join(os.path.expanduser("~"), "Desktop", "Projects"))
        out_dir = os.getenv("OASIS_REPORTS_DIR", os.path.join(root, "reports"))
        depts = ([d.strip() for d in args.departments.split(",")]
                 if args.departments else None)
        res = write_category_report(data_dir, cash_dir, args.category, out_dir,
                                    tenant=args.tenant or "", departments=depts,
                                    pdf=args.pdf)
        print(f"Category report written: {res['markdown']}")
        if res.get("pdf"):
            print(f"  PDF: {res['pdf']}")
        print(f"  {res['skus']:,} SKUs | revenue KES {res['revenue_period']:,.0f} | "
              f"gross profit KES {res['gross_profit_period']:,.0f}"
              + (f" ({res['avg_margin_pct']}% margin)" if res['avg_margin_pct'] is not None else "")
              + f" | dead KES {res['dead_value']:,.0f} | ghost {res['ghost_sellers']:,}")
    elif args.mode == "sku-deepdive":
        _require_module("revenue")
        from oasis.logic.sku_deepdive import write_sku_deepdive
        if not args.files:
            raise SystemExit("sku-deepdive requires --files <a.xlsx,b.xlsx,...>")
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        cash_dir = os.getenv("OASIS_CASH_DIR",
                             os.path.join(os.path.expanduser("~"), "Desktop", "Projects"))
        out_dir = os.getenv("OASIS_REPORTS_DIR", os.path.join(root, "reports"))
        files = [f.strip() for f in args.files.split(",") if f.strip()]
        res = write_sku_deepdive(files, cash_dir, data_dir, out_dir,
                                 tenant=args.tenant or "", section=args.section,
                                 pdf=args.pdf)
        print(f"SKU deep dive written: {res['markdown']}")
        if res.get("pdf"):
            print(f"  PDF: {res['pdf']}")
        print(f"  {res['n_skus']:,} SKUs | capital KES {res['total_capital']:,.0f} | "
              f"clear KES {res['clear_capital']:,.0f} | reduce KES {res['reduce_capital']:,.0f} | "
              f"lost KES {res['lost_rev_day']:,.0f}/day")
    elif args.mode == "inject-grn-costs":
        from oasis.logic.grn_cost import inject_from_files
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        res = inject_from_files(db_path, data_dir)
        print(f"GRN costs injected into {db_path}: {res}")
    elif args.mode == "supplier-scorecard":
        _require_module("ordering")
        from oasis.logic.supplier_scorecard import write_scorecard
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR", os.path.join(root, "oasis", "data"))
        out_dir = os.getenv("OASIS_REPORTS_DIR", os.path.join(root, "reports"))
        res = write_scorecard(data_dir, out_dir, tenant=args.tenant or "")
        print(f"Supplier scorecard written: {res['markdown']} "
              f"({res['suppliers']} suppliers, {res['unreliable']} unreliable)")
    elif args.mode == "value-report":
        from oasis.logic.value_report import write_value_report
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        out_dir = os.getenv("OASIS_REPORTS_DIR", os.path.join(root, "reports"))
        res = write_value_report(db_path, out_dir, period_days=args.days,
                                 tenant=args.tenant or "")
        print(f"Value report written: {res['markdown']}")
        print(f"  capital recovered KES {res['value_recovered']:,.0f} | "
              f"sales KES {res['sales_revenue']:,.0f} / {res['bills']:,} bills | "
              f"dead stock KES {res['dead_stock_value']:,.0f} "
              f"({res['dead_stock_pct_of_inventory']}%) | "
              f"stockouts {res['stockouts']:,} ({res['stockout_pct']}%)")
    elif args.mode == "metering-report":
        from datetime import datetime as _dt
        from datetime import timedelta as _td

        from oasis.logic.value_report import usage_summary
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        since = (_dt.now() - _td(days=args.days)).strftime("%Y-%m-%d")
        print(f"Usage since {since}: {usage_summary(db_path, since)}")
    elif args.mode == "set-branding":
        import base64
        import mimetypes

        from oasis.logic.branding import Branding, load_branding, save_branding
        cur = load_branding()
        updates = {}
        if args.tenant_name is not None: updates["tenant_name"] = args.tenant_name
        if args.product_name is not None: updates["product_name"] = args.product_name
        if args.primary_color is not None: updates["primary_color"] = args.primary_color
        if args.accent_color is not None: updates["accent_color"] = args.accent_color
        if args.welcome_message is not None: updates["welcome_message"] = args.welcome_message
        if args.logo:
            if not os.path.exists(args.logo):
                raise SystemExit(f"logo not found: {args.logo}")
            mime = mimetypes.guess_type(args.logo)[0] or "image/png"
            with open(args.logo, "rb") as f:
                updates["logo_data_uri"] = f"data:{mime};base64,{base64.b64encode(f.read()).decode()}"
            updates["logo"] = os.path.basename(args.logo)
        merged = {**{k: getattr(cur, k) for k in Branding.__dataclass_fields__ if k != "source"}, **updates}
        merged.pop("source", None)
        path = save_branding(Branding(**merged))
        print(f"Branding saved: {path}")
        print(f"  tenant     : {merged.get('tenant_name')}")
        print(f"  product    : {merged.get('product_name')}")
        print(f"  primary    : {merged.get('primary_color')} / accent {merged.get('accent_color')}")
        print(f"  logo       : {merged.get('logo') or '(none)'}")
    elif args.mode == "show-branding":
        from oasis.logic.branding import load_branding
        b = load_branding()
        print(f"Branding source: {b.source or '(defaults — no branding.json)'}")
        print(f"  tenant   : {b.tenant_name}")
        print(f"  product  : {b.product_name}")
        print(f"  primary  : {b.primary_color} / accent {b.accent_color}")
        print(f"  logo     : {b.logo or '(none)'}"
              + (" [+data-uri]" if b.logo_data_uri else ""))
        print(f"  welcome  : {b.welcome_message or '(default)'}")
    elif args.mode == "package-release":
        from oasis.logic.release_packager import build_release
        root = os.path.dirname(os.path.abspath(__file__))
        res = build_release(root, bundle_runtime=args.bundle_runtime)
        print(f"Release packaged: {res['zip']}")
        print(f"  v{res['version']} | {res['files_shipped']:,} files shipped, "
              f"{res['files_skipped']:,} excluded | {res['size_mb']} MB")
        if args.bundle_runtime:
            print(f"  Runtime: {res.get('runtime_files', 0)} files, "
                  f"{res.get('runtime_mb', 0)} MB (offline-ready)")
    elif args.mode == "backup":
        from oasis.logic.backup_util import backup_db
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        keep = int(os.getenv("OASIS_BACKUP_KEEP", "10"))
        print(f"Backup complete: {backup_db(db_path, keep=keep)}")
    elif args.mode == "restore":
        from oasis.logic.backup_util import restore_db
        if not args.file:
            raise SystemExit("restore requires --file <backup path>")
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        print(f"Restore complete: {restore_db(db_path, args.file)}")
    elif args.mode == "set-password":
        import getpass
        import sqlite3 as _sq
        from oasis.logic.auth_manager import hash_password
        if not args.username:
            raise SystemExit("set-password requires --username")
        pw = args.password or getpass.getpass(f"New password for {args.username}: ")
        if len(pw) < 8:
            raise SystemExit("Password must be at least 8 characters")
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_pos.db"))
        conn = _sq.connect(db_path)
        try:
            n = conn.execute("UPDATE OASIS_USERS SET PASSWORD_HASH=? WHERE USERNAME=?",
                             (hash_password(pw), args.username)).rowcount
            conn.commit()
        finally:
            conn.close()
        if n == 0:
            raise SystemExit(f"User '{args.username}' not found in {db_path}")
        print(f"Password updated for '{args.username}' ({db_path})")
    elif args.mode == "issue-license":
        from oasis.logic.license_manager import (
            BUNDLES, KNOWN_MODULES, OfflineLicenseManager,
        )
        if not args.tenant or not args.expiry:
            raise SystemExit("issue-license requires --tenant and --expiry YYYY-MM-DD")
        if args.bundle:
            if args.bundle not in BUNDLES:
                raise SystemExit(f"unknown bundle '{args.bundle}'; "
                                 f"choose from: {', '.join(BUNDLES)}")
            mods = list(BUNDLES[args.bundle])
        else:
            mods = [m.strip() for m in (args.modules or "core").split(",") if m.strip()]
        if "core" not in mods:
            raise SystemExit("every license must include 'core' (the mandatory base)")
        unknown = [m for m in mods if m not in KNOWN_MODULES]
        if unknown:
            logger.warning(f"Unknown module(s) {unknown}; known: {KNOWN_MODULES}")
        print(f"License issued: {OfflineLicenseManager().issue(args.tenant, mods, args.expiry, out_path=args.out)}")
    elif args.mode == "license-status":
        from oasis.logic.license_manager import (
            KNOWN_MODULES, MODULE_LABELS, OfflineLicenseManager,
        )
        mgr = OfflineLicenseManager()
        for m in KNOWN_MODULES:
            s = mgr.status(m)
            print(f"  {m:<10} {MODULE_LABELS.get(m, m):<34} {s['mode']:<11} {s['reason']}")
    elif args.mode == "pos-inject":
        from oasis.logic.pos_injector import run_injector
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "mock_pos_erp.db"))
        run_injector(db_path, batches=args.batches, interval=args.interval,
                     org=args.org, sku_count=args.sku_count)
    elif args.mode == "build-multi-store-db":
        from oasis.logic.multi_store_build import build_multi_store_from_xlsx
        root = os.path.dirname(__file__)
        data_dir = os.getenv("OASIS_DATA_DIR",
                             os.path.join(root, "oasis", "data"))
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_multi_store.db"))
        summary = build_multi_store_from_xlsx(data_dir, db_path)
        print(f"Multi-store DB built: {summary['stores']} stores, "
              f"{summary['catalog_skus']} catalog SKUs")
    elif args.mode == "seed-multi-history":
        from oasis.logic.multi_store_pos import seed_multi_store_history
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_multi_store.db"))
        prior = os.getenv("OASIS_BASKET_PRIOR",
                          os.path.join(root, "neutral_network_export", "basket_prior.json"))
        print(f"Multi-store history: {seed_multi_store_history(db_path, prior_path=prior)}")
    elif args.mode == "multi-pos-stream":
        from oasis.logic.multi_store_pos import stream_multi_store
        root = os.path.dirname(__file__)
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(root, "oasis", "data", "rhapta_multi_store.db"))
        prior = os.getenv("OASIS_BASKET_PRIOR",
                          os.path.join(root, "neutral_network_export", "basket_prior.json"))
        stream_multi_store(db_path, prior_path=prior,
                           batches=args.batches if args.batches != 10 else 0)


if __name__ == "__main__":
    main()
