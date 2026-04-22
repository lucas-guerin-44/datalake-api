"""
Backup routes.

Design note: the parquet artifacts themselves are NEVER exposed over HTTP —
only the manifest. The manifest is enough to confirm a backup ran, with row
counts and paths. Actual artifact retrieval is an operator concern (rsync,
scp, S3 sync, etc.) and stays off the public surface.
"""
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Query

from src.middleware.logging_config import get_logger
from src.core.database import User
from src.services.backup import (
    export_catalog, latest_manifest, list_backups, prune_old_backups,
)
from src.services.jobs import create_job, finish_job
from src.auth.auth import ScopedAuth

logger = get_logger(__name__)
router = APIRouter(prefix="/backup", tags=["backup"])


def _run_backup_job(job_id: str, keep: Optional[int]):
    try:
        manifest = export_catalog()
        removed = prune_old_backups(keep=keep) if keep is not None else 0
        finish_job(job_id, result={
            "exported_at": manifest["exported_at"],
            "ohlc_rows": manifest["ohlc"]["row_count"],
            "tick_rows": manifest["ticks"]["row_count"],
            "pruned_old_backups": removed,
        })
    except Exception as e:
        logger.exception("Backup job failed", extra={"job_id": job_id})
        finish_job(job_id, error=str(e))


@router.post("/run")
def run_backup(
    background_tasks: BackgroundTasks,
    keep: Optional[int] = Query(8, ge=1, le=365, description="How many most-recent backups to retain. Older ones are pruned."),
    current_user: User = Depends(ScopedAuth("admin")),
):
    """
    Trigger a catalog export as a background job. Admin-only.

    Intended to be called by an external cron (cronjob.org, systemd timer).
    Returns a job id you can poll at /jobs/{id}.
    """
    job = create_job("backup", meta={"keep": keep})
    background_tasks.add_task(_run_backup_job, job.id, keep)
    return {"status": "queued", "job_id": job.id}


@router.get("/latest")
def get_latest_manifest(
    current_user: User = Depends(ScopedAuth("read")),
):
    """Return the manifest of the most recent backup. Parquet files are NOT exposed."""
    m = latest_manifest()
    if m is None:
        raise HTTPException(status_code=404, detail="No backups found")
    return m


@router.get("")
def list_all_backups(
    current_user: User = Depends(ScopedAuth("read")),
):
    """List all backup directories present on disk (names + timestamps, not contents)."""
    return {"backups": [{"name": b["name"]} for b in list_backups()]}
