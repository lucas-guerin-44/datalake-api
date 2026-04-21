"""Jobs routes — inspect the status of background tasks (e.g. derive after ingest)."""
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException

from src.config import ALLOW_PUBLIC_READS
from src.core.database import User
from src.auth.auth import ScopedAuth
from src.services.jobs import get_job, list_jobs

router = APIRouter()


@router.get("/jobs")
def list_jobs_api(
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """List recent jobs (newest first)."""
    return {"jobs": list_jobs()}


@router.get("/jobs/{job_id}")
def get_job_api(
    job_id: str,
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """Get status and result of a specific job."""
    job = get_job(job_id)
    if job is None:
        raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
    return job.to_dict()
