"""Background jobs for the web console — because the engine is not a web request.

WHY
---
``generate_smart_orders`` runs the real engine. On Odoo's 345 products that is
about thirty seconds; on the local 39,728-SKU catalogue it does not return
within three minutes. A synchronous endpoint therefore either times out at the
proxy, or holds a worker for minutes while the browser shows a spinner that
cannot say anything useful. Neither is acceptable in front of an operator.

So: submit a job, poll it, render when it lands.

PROGRESS IS REAL, NOT DECORATIVE
--------------------------------
The engine already narrates itself through the logging module — "Phase 3:
Enriching 39728 products", "Loaded 12 bypass suppliers", and so on. A job
attaches a handler for the duration of its own run and surfaces those lines
verbatim. That is honest progress: it reports what the engine is actually
doing, and when something takes a long time the operator can see WHICH step.
A synthetic percentage bar would have to be invented, and would lie.

The handler filters on THREAD IDENTITY, so two jobs running at once do not
capture each other's output.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional

logger = logging.getLogger("OasisWeb.jobs")

#: Keep the registry bounded — this is a long-running server process and a
#: dict that only ever grows is a leak with extra steps.
MAX_JOBS = 60
#: A job that has not finished by this point is reported as failed rather than
#: left spinning forever in the browser.
JOB_TIMEOUT_S = 900


@dataclass
class Job:
    id: str
    kind: str
    key: str
    status: str = "running"          # running | done | error
    started: float = field(default_factory=time.time)
    finished: Optional[float] = None
    result: Any = None
    error: Optional[str] = None
    log: List[str] = field(default_factory=list)
    thread_id: Optional[int] = None

    @property
    def elapsed(self) -> float:
        return round((self.finished or time.time()) - self.started, 1)

    def public(self, with_result: bool = True) -> Dict[str, Any]:
        out: Dict[str, Any] = {
            "id": self.id, "kind": self.kind, "key": self.key,
            "status": self.status, "elapsed": self.elapsed,
            "step": self.log[-1] if self.log else None,
            "log": self.log[-8:],
            "error": self.error,
        }
        if with_result and self.status == "done":
            out["result"] = self.result
        return out


_JOBS: Dict[str, Job] = {}
_BY_KEY: Dict[str, str] = {}
_LOCK = threading.Lock()

#: Log lines worth showing an operator. The engine emits a great deal at INFO,
#: much of it about loading JSON files; these are the phases that actually take
#: time and therefore explain a wait.
_INTERESTING = ("Phase", "Enriching", "Fetched", "Fetching", "Loaded",
                "Calculating", "Scanning", "Network", "MOQ", "Odoo",
                "products", "Pushed", "recommendations")


class _JobLogHandler(logging.Handler):
    """Route log records back to the job that emitted them.

    Filtering on thread identity rather than logger name is what makes this
    safe with two jobs in flight: the engine's loggers are module-level and
    shared, so a name-based filter would cross the streams.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            tid = threading.get_ident()
            with _LOCK:
                job = next((j for j in _JOBS.values()
                            if j.thread_id == tid and j.status == "running"), None)
            if job is None:
                return
            msg = record.getMessage()
            if not any(k.lower() in msg.lower() for k in _INTERESTING):
                return
            if job.log and job.log[-1] == msg:
                return
            job.log.append(msg[:180])
            if len(job.log) > 60:
                del job.log[:-40]
        except Exception:
            pass                      # a logging handler must never raise


_handler = _JobLogHandler()
_handler.setLevel(logging.INFO)
_installed = False


def _install() -> None:
    global _installed
    if not _installed:
        root = logging.getLogger()
        root.addHandler(_handler)
        if root.level > logging.INFO:
            root.setLevel(logging.INFO)
        _installed = True


def _prune() -> None:
    if len(_JOBS) <= MAX_JOBS:
        return
    done = sorted((j for j in _JOBS.values() if j.status != "running"),
                  key=lambda j: j.finished or 0)
    for j in done[:len(_JOBS) - MAX_JOBS]:
        _JOBS.pop(j.id, None)
        if _BY_KEY.get(j.key) == j.id:
            _BY_KEY.pop(j.key, None)


def submit(kind: str, key: str, fn: Callable[[], Any]) -> Job:
    """Start `fn` in a thread, or return the job already doing that work.

    Single-flight on (kind, key): a browser that polls, reloads, or has the
    page open twice must not launch a second full engine run against the same
    store. The engine is the most expensive thing in the system.
    """
    _install()
    full_key = f"{kind}:{key}"
    with _LOCK:
        existing_id = _BY_KEY.get(full_key)
        existing = _JOBS.get(existing_id) if existing_id else None
        if existing and existing.status == "running":
            if existing.elapsed < JOB_TIMEOUT_S:
                return existing
            existing.status = "error"
            existing.error = f"Timed out after {JOB_TIMEOUT_S}s."
            existing.finished = time.time()

        job = Job(id=uuid.uuid4().hex[:12], kind=kind, key=key)
        _JOBS[job.id] = job
        _BY_KEY[full_key] = job.id
        _prune()

    def _run() -> None:
        job.thread_id = threading.get_ident()
        try:
            job.result = fn()
            job.status = "done"
        except Exception as e:
            job.status = "error"
            # Never surface the raw exception: an adapter error can carry a
            # connection URI with credentials in it.
            job.error = f"{type(e).__name__}: {str(e)[:180]}"
            logger.error("job %s (%s) failed: %s", job.id, full_key,
                         type(e).__name__)
        finally:
            job.finished = time.time()

    threading.Thread(target=_run, name=f"oasis-job-{job.id}", daemon=True).start()
    return job


def get(job_id: str) -> Optional[Job]:
    job = _JOBS.get(job_id)
    if job and job.status == "running" and job.elapsed > JOB_TIMEOUT_S:
        job.status = "error"
        job.error = f"Timed out after {JOB_TIMEOUT_S}s."
        job.finished = time.time()
    return job
