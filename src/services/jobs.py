"""
In-memory job registry for tracking background ingest/derive tasks.

Jobs are ephemeral (lost on API restart) — appropriate for this scale. A future
move to Redis/DB-backed jobs would slot in behind the same interface.
"""
import threading
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Dict, Literal, Optional, Any

from src.middleware.logging_config import get_logger

logger = get_logger(__name__)

JobStatus = Literal["running", "ok", "error"]


@dataclass
class Job:
    id: str
    kind: str
    status: JobStatus = "running"
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    finished_at: Optional[datetime] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        d = asdict(self)
        d["started_at"] = self.started_at.isoformat()
        d["finished_at"] = self.finished_at.isoformat() if self.finished_at else None
        return d


_JOBS: Dict[str, Job] = {}
_LOCK = threading.Lock()

# Cap the registry so long-running APIs don't leak. Oldest finished jobs evict first.
MAX_JOBS = 500


def create_job(kind: str, meta: Optional[Dict[str, Any]] = None) -> Job:
    job = Job(id=str(uuid.uuid4()), kind=kind, meta=meta or {})
    with _LOCK:
        _JOBS[job.id] = job
        _evict_if_needed()
    return job


def finish_job(job_id: str, result: Optional[Dict[str, Any]] = None, error: Optional[str] = None):
    with _LOCK:
        job = _JOBS.get(job_id)
        if job is None:
            return
        job.finished_at = datetime.now(timezone.utc)
        if error is not None:
            job.status = "error"
            job.error = error
        else:
            job.status = "ok"
            job.result = result or {}
    logger.info(
        "Job finished",
        extra={"job_id": job_id, "kind": job.kind, "status": job.status, "error": error},
    )


def get_job(job_id: str) -> Optional[Job]:
    with _LOCK:
        return _JOBS.get(job_id)


def list_jobs(limit: int = 50) -> list:
    with _LOCK:
        items = sorted(_JOBS.values(), key=lambda j: j.started_at, reverse=True)
        return [j.to_dict() for j in items[:limit]]


def _evict_if_needed():
    # Must be called while holding _LOCK.
    if len(_JOBS) <= MAX_JOBS:
        return
    finished = sorted(
        (j for j in _JOBS.values() if j.finished_at is not None),
        key=lambda j: j.finished_at,
    )
    for j in finished[: len(_JOBS) - MAX_JOBS]:
        _JOBS.pop(j.id, None)
