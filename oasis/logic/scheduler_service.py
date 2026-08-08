"""
OASIS Scheduler Service
========================
APScheduler-based background task manager for automated operations.

Jobs:
    1. Morning PO Generation — runs SimulationBridge for all stores
    2. Hourly Stockout Monitor — checks stock levels vs ADS
    3. Evening Summary — computes day-end KPIs

Usage:
    from oasis.logic.scheduler_service import OasisScheduler
    scheduler = OasisScheduler(db_path)
    scheduler.start()
"""

import logging
import sqlite3
import json
from datetime import datetime
from typing import Dict, Any, Optional, List

logger = logging.getLogger("OasisScheduler")


class SchedulerJob:
    """Represents a scheduled job configuration."""
    def __init__(self, job_id: str, name: str, description: str,
                 default_cron: str, enabled: bool = True, handler: Any = None):
        self.job_id = job_id
        self.name = name
        self.description = description
        self.cron = default_cron
        self.enabled = enabled
        self.last_run: Optional[str] = None
        self.last_status: str = "NEVER_RUN"
        self.last_result: str = ""
        self.handler = handler # The actual function to call


# JOB HANDLERS (Proxy to instance methods)
# ---------------------------------------------------------------------------

# These proxy functions will be bound to the OasisScheduler instance at runtime
# when the scheduler is started. They are defined globally to be referenced
# in JOB_DEFINITIONS.
_scheduler_instance: Optional["OasisScheduler"] = None

def _set_scheduler_instance(instance: "OasisScheduler"):
    global _scheduler_instance
    _scheduler_instance = instance

def _run_morning_po_job():
    if _scheduler_instance:
        _scheduler_instance._run_morning_po()
    else:
        logger.error("Scheduler instance not set for morning PO job.")

def _run_hourly_monitor_job():
    if _scheduler_instance:
        _scheduler_instance._run_hourly_monitor()
    else:
        logger.error("Scheduler instance not set for hourly monitor job.")

def _run_evening_summary_job():
    if _scheduler_instance:
        _scheduler_instance._run_evening_summary()
    else:
        logger.error("Scheduler instance not set for evening summary job.")


# Pre-defined job configurations
JOB_DEFINITIONS = {
    "morning_po": SchedulerJob(
        "morning_po", "Morning PO Generation",
        "Generate replenishment orders for all stores",
        "0 8 * * *", True, _run_morning_po_job
    ),
    "hourly_monitor": SchedulerJob(
        "hourly_monitor", "Hourly Stock Monitor",
        "Check for critical stockouts and spikes",
        "0 * * * *", True, _run_hourly_monitor_job
    ),
    "evening_summary": SchedulerJob(
        "evening_summary", "Evening Sales Summary",
        "Aggregate daily sales and calculate ADS",
        "0 22 * * *", True, _run_evening_summary_job
    )
}

# ── Cycle Presets ─────────────────────────────────────────────────────
# Clients select one preset; the scheduler auto-configures all cron jobs.
CYCLE_PRESETS = {
    "LIVE": {
        "morning_po":      "0 8 * * *",
        "hourly_monitor":  "*/15 * * * *",    # Every 15 minutes for live clients
        "evening_summary": "0 22 * * *",
    },
    "8_HOUR": {
        "morning_po":      "0 6 * * *",
        "hourly_monitor":  "0 6,14,22 * * *", # 3x daily refresh
        "evening_summary": "0 22 * * *",
    },
    "24_HOUR": {
        "morning_po":      "0 6 * * *",
        "hourly_monitor":  "0 * * * *",       # Standard hourly
        "evening_summary": "0 22 * * *",
    },
}


class OasisScheduler:
    """
    Manages background scheduled tasks for the OASIS system.
    Uses APScheduler's BackgroundScheduler for non-blocking execution.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.scheduler = None
        self.jobs: Dict[str, SchedulerJob] = {}
        self._running = False

        # Set this instance as the global scheduler instance for job handlers
        _set_scheduler_instance(self)

        # Load job configs from DB or use defaults
        self._load_job_configs()

    def _load_job_configs(self):
        """Load job configurations from OASIS_SYSTEM_CONFIG or use defaults."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute(
                "SELECT CONFIG_VALUE FROM OASIS_SYSTEM_CONFIG WHERE CONFIG_KEY = 'scheduler_jobs'"
            )
            row = cursor.fetchone()
            conn.close()

            if row and row[0]:
                try:
                    saved = json.loads(row[0])
                    for job_id, job_def in JOB_DEFINITIONS.items():
                        # Use the handler from JOB_DEFINITIONS
                        job = SchedulerJob(
                            job_def.job_id, job_def.name, job_def.description,
                            job_def.cron, job_def.enabled, job_def.handler
                        )
                        if job_id in saved and isinstance(saved[job_id], dict):
                            job.enabled = saved[job_id].get("enabled", job.enabled)
                            job.cron = saved[job_id].get("cron", job.cron)
                            job.last_run = saved[job_id].get("last_run")
                            job.last_status = saved[job_id].get("last_status", "NEVER_RUN")
                            job.last_result = saved[job_id].get("last_result", "")
                        self.jobs[job_id] = job
                except json.JSONDecodeError as je:
                    logger.error(f"Failed to parse scheduler job configs: {je}")
                    self.jobs = {k: SchedulerJob(v.job_id, v.name, v.description, v.cron, v.enabled, v.handler)
                                 for k, v in JOB_DEFINITIONS.items()}
            else:
                self.jobs = {k: SchedulerJob(v.job_id, v.name, v.description, v.cron, v.enabled, v.handler)
                             for k, v in JOB_DEFINITIONS.items()}
        except Exception as e:
            logger.warning(f"Could not load scheduler config: {e}. Using defaults.")
            self.jobs = {k: SchedulerJob(v.job_id, v.name, v.description, v.cron, v.enabled, v.handler)
                         for k, v in JOB_DEFINITIONS.items()}

    def _save_job_configs(self):
        """Persist job configurations to OASIS_SYSTEM_CONFIG."""
        try:
            data = {}
            for job_id, job in self.jobs.items():
                data[job_id] = {
                    "enabled": job.enabled,
                    "cron": job.cron,
                    "last_run": job.last_run,
                    "last_status": job.last_status,
                    "last_result": job.last_result,
                }
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                """INSERT OR REPLACE INTO OASIS_SYSTEM_CONFIG
                   (CONFIG_KEY, CONFIG_VALUE, CONFIG_GROUP, DESCRIPTION, UPDATED_BY, UPDATED_DT)
                   VALUES (?, ?, 'scheduler', 'Scheduler job configurations', 'system', ?)""",
                ("scheduler_jobs", json.dumps(data), datetime.now().isoformat())
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"Failed to save scheduler config: {e}")

    def start(self):
        """Start the background scheduler."""
        if self._running:
            logger.info("Scheduler already running")
            return

        try:
            from apscheduler.schedulers.background import BackgroundScheduler
            from apscheduler.triggers.cron import CronTrigger

            sched = BackgroundScheduler(daemon=True) # Use local variable for scheduler
            self.scheduler = sched # Assign to instance variable

            for job_id, job in self.jobs.items():
                if job.enabled:
                    handler = job.handler # Use the handler directly from SchedulerJob
                    if handler:
                        parts = job.cron.split()
                        if len(parts) == 5:
                            trigger = CronTrigger(
                                minute=parts[0], hour=parts[1],
                                day=parts[2], month=parts[3],
                                day_of_week=parts[4]
                            )
                            sched.add_job( # Use local sched
                                handler, trigger, id=job_id,
                                name=job.name, replace_existing=True
                            )
                            logger.info(f"Scheduled job: {job.name} ({job.cron})")

            sched.start() # Use local sched
            self._running = True
            logger.info("OASIS Scheduler started")
        except ImportError:
            logger.warning("APScheduler not installed. Run: pip install apscheduler")
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")

    def stop(self):
        """Stop the background scheduler."""
        local_scheduler = self.scheduler # type: ignore
        if local_scheduler and self._running:
            local_scheduler.shutdown(wait=False)
            self._running = False
            logger.info("OASIS Scheduler stopped")

    def is_running(self) -> bool:
        return self._running

    def apply_cycle(self, cycle_name: str):
        """
        Apply a cycle preset (LIVE, 8_HOUR, 24_HOUR).
        Reconfigures all job crons from the preset and restarts if running.
        """
        cycle_name = cycle_name.upper()
        if cycle_name not in CYCLE_PRESETS:
            logger.error(f"Unknown cycle preset: {cycle_name}. Valid: {list(CYCLE_PRESETS.keys())}")
            return

        preset = CYCLE_PRESETS[cycle_name]
        was_running = self._running

        if was_running:
            self.stop()

        for job_id, new_cron in preset.items():
            if job_id in self.jobs:
                self.jobs[job_id].cron = new_cron
                logger.info(f"Cycle {cycle_name}: {job_id} → {new_cron}")

        self._save_job_configs()

        if was_running:
            self.start()

        logger.info(f"Cycle preset '{cycle_name}' applied successfully.")

    def get_job_status(self) -> List[Dict[str, Any]]:
        """Get status of all jobs."""
        statuses = []
        for job_id, job in self.jobs.items():
            next_run = None
            local_scheduler = self.scheduler # type: ignore
            if self._running and local_scheduler:
                sched_job = local_scheduler.get_job(job_id)
                if sched_job and sched_job.next_run_time:
                    next_run = sched_job.next_run_time.strftime("%Y-%m-%d %H:%M:%S")

            statuses.append({
                "job_id": job_id,
                "name": job.name,
                "description": job.description,
                "enabled": job.enabled,
                "cron": job.cron,
                "last_run": job.last_run or "Never",
                "last_status": job.last_status,
                "last_result": job.last_result,
                "next_run": next_run or ("Disabled" if not job.enabled else "Not scheduled"),
            })
        return statuses

    def toggle_job(self, job_id: str, enabled: bool):
        """Enable/disable a job."""
        if job_id in self.jobs:
            self.jobs[job_id].enabled = enabled
            local_scheduler = self.scheduler # type: ignore
            if self._running and local_scheduler:
                if enabled:
                    handler = self.jobs[job_id].handler # Use handler from job object
                    if handler:
                        from apscheduler.triggers.cron import CronTrigger
                        parts = self.jobs[job_id].cron.split()
                        if len(parts) == 5:
                            trigger = CronTrigger(
                                minute=parts[0], hour=parts[1],
                                day=parts[2], month=parts[3],
                                day_of_week=parts[4]
                            )
                            local_scheduler.add_job( # type: ignore
                                handler, trigger, id=job_id,
                                name=self.jobs[job_id].name, replace_existing=True
                            )
                else:
                    try:
                        local_scheduler.remove_job(job_id) # type: ignore
                    except Exception:
                        pass
            self._save_job_configs()

    def update_cron(self, job_id: str, new_cron: str):
        """Update the cron expression for a job."""
        if job_id in self.jobs:
            self.jobs[job_id].cron = new_cron
            if self._running and self.jobs[job_id].enabled:
                self.toggle_job(job_id, False)
                self.toggle_job(job_id, True)
            self._save_job_configs()

    def run_now(self, job_id: str) -> str:
        """Manually trigger a job immediately."""
        job = self.jobs.get(job_id)
        if job and job.handler:
            try:
                # Call the actual instance method via the proxy
                if job.handler == _run_morning_po_job:
                    result = self._run_morning_po()
                elif job.handler == _run_hourly_monitor_job:
                    result = self._run_hourly_monitor()
                elif job.handler == _run_evening_summary_job:
                    result = self._run_evening_summary()
                else:
                    return f"Unknown handler for job: {job_id}"

                return result or "Completed successfully"
            except Exception as e:
                return f"Error: {e}"
        return f"Unknown job: {job_id}"

    def _get_job_handler(self, job_id: str):
        """
        Get the function handler for a job.
        This method is now largely redundant as handlers are stored in SchedulerJob.
        It's kept for backward compatibility if needed, but direct access via job.handler is preferred.
        """
        job = self.jobs.get(job_id)
        return job.handler if job else None

    def _record_run(self, job_id: str, status: str, result: str):
        """Record a job run in the job config and audit log."""
        if job_id in self.jobs:
            self.jobs[job_id].last_run = datetime.now().isoformat()
            self.jobs[job_id].last_status = status
            self.jobs[job_id].last_result = result[:500]
            self._save_job_configs()

        # Also write to audit log
        try:
            from oasis.logic.audit_logger import log_action
            log_action(
                self.db_path, "SCHEDULER", f"JOB_{status}",
                "SCHEDULED_JOB", job_id, None,
                {"result": result[:200]}
            )
        except Exception as e:
            logger.error(f"Failed to log scheduler action: {e}")

    # ── Job Handlers ──────────────────────────────────────────────────

    def _run_morning_po(self) -> str:
        """Generate PO recommendations for all stores."""
        job_id = "morning_po"
        try:
            from oasis.logic import db as oasis_db
            from oasis.logic.pos_erp_adapter import PosErpAdapter
            from oasis.logic.db_connector import UniversalConnector

            connector = UniversalConnector(oasis_db.to_sqlalchemy_url(self.db_path))
            adapter = PosErpAdapter(connector)
            orgs = adapter.fetch_all_organizations()

            results = []
            for org in orgs:
                org_cd = org["ORG_CD"]
                org_name = org.get("ORG_NAME", org_cd)
                try:
                    products = adapter.fetch_enriched_products(org_cd)
                    n_products = len(products)
                    n_critical = sum(1 for p in products
                                     if float(p.get('current_stocks', 0)) <= 0) # type: ignore
                    results.append(f"{org_name}: {n_products} SKUs, {n_critical} stockouts")
                except Exception as e:
                    results.append(f"{org_name}: Error - {str(e)[:50]}")

            summary = f"Morning PO scan for {len(orgs)} stores. " + "; ".join(results[:5])
            self._record_run(job_id, "SUCCESS", summary)
            return summary

        except Exception as e:
            error_msg = f"Morning PO failed: {e}"
            self._record_run(job_id, "FAILED", error_msg)
            return error_msg

    def _run_hourly_monitor(self) -> str:
        """Check stock levels and flag critical items."""
        job_id = "hourly_monitor"
        try:
            from oasis.logic import db as oasis_db
            from oasis.logic.pos_erp_adapter import PosErpAdapter
            from oasis.logic.db_connector import UniversalConnector

            connector = UniversalConnector(oasis_db.to_sqlalchemy_url(self.db_path))
            adapter = PosErpAdapter(connector)
            orgs = adapter.fetch_all_organizations()

            total_stockouts: int = 0
            low_stock: int = 0
            total_items: int = 0

            for org in orgs:
                org_cd = org["ORG_CD"]
                try:
                    products = adapter.fetch_enriched_products(org_cd)
                    for p in products: # type: ignore
                        stock: float = float(p.get('current_stocks', 0.0)) # type: ignore
                        if stock <= 0:
                            total_stockouts += 1
                        elif stock <= 10:
                            low_stock += 1
                        total_items += 1
                except Exception:
                    continue

            summary = (f"Hourly check: {total_stockouts} stockouts, "
                       f"{low_stock} critical across {len(orgs)} stores")
            self._record_run(job_id, "SUCCESS", summary)
            return summary

        except Exception as e:
            error_msg = f"Hourly monitor failed: {e}"
            self._record_run(job_id, "FAILED", error_msg)
            return error_msg

    def _run_evening_summary(self) -> str:
        """Generate end-of-day KPI summary."""
        job_id = "evening_summary"
        try:
            from oasis.logic import db as oasis_db
            from oasis.logic.pos_erp_adapter import PosErpAdapter
            from oasis.logic.db_connector import UniversalConnector

            connector = UniversalConnector(oasis_db.to_sqlalchemy_url(self.db_path))
            adapter = PosErpAdapter(connector)
            orgs = adapter.fetch_all_organizations()

            total_revenue: float = 0.0
            total_units: int = 0

            for org in orgs:
                org_cd = org["ORG_CD"]
                try:
                    sales = adapter.fetch_sales_history(org_cd, days=1)
                    if not sales.empty:
                        total_revenue += float(sales["net_amt"].sum())
                        total_units += int(sales["qty"].sum())
                except Exception:
                    continue

            summary = (f"EOD Summary: KES {total_revenue:,.0f} revenue, "
                       f"{total_units:,} units across {len(orgs)} stores")
            self._record_run(job_id, "SUCCESS", summary)
            return summary

        except Exception as e:
            error_msg = f"Evening summary failed: {e}"
            self._record_run(job_id, "FAILED", error_msg)
            return error_msg
