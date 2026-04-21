"""Catalog routes - view database statistics and data coverage."""
from pathlib import Path
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, Form, HTTPException

from src.middleware.logging_config import get_logger
from src.config import ALLOW_PUBLIC_READS
from src.core.database import User
from src.core.datalake import get_database_stats, get_tick_database_stats, list_instruments, list_timeframes, get_data_range, list_tick_instruments, get_tick_coverage, find_gaps
from src.services.validators import validate_instrument, validate_timeframe
from fastapi import Query
from src.services.backup import export_catalog, restore_catalog, DEFAULT_BACKUP_ROOT, MANIFEST_FILENAME
from src.services.jobs import create_job, finish_job
from src.auth.auth import ScopedAuth

logger = get_logger(__name__)
router = APIRouter()


@router.get("/catalog")
def get_catalog(
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS))
):
    """
    Return DuckDB database statistics including available instruments,
    timeframes, and data coverage per instrument/timeframe pair.
    """
    try:
        stats = get_database_stats()
        tick_stats = get_tick_database_stats()

        coverage = []
        for instrument in list_instruments():
            for timeframe in list_timeframes(instrument):
                data_range = get_data_range(instrument, timeframe)
                if data_range:
                    coverage.append({
                        "instrument": instrument,
                        "timeframe": timeframe,
                        "min_date": str(data_range["min_date"]) if data_range["min_date"] else None,
                        "max_date": str(data_range["max_date"]) if data_range["max_date"] else None,
                        "record_count": data_range["count"],
                    })

        # Include tick coverage
        for instrument in list_tick_instruments():
            tick_cov = get_tick_coverage(instrument)
            if tick_cov:
                coverage.append({
                    "instrument": instrument,
                    "timeframe": "TICK",
                    "min_date": str(tick_cov["min_date"]) if tick_cov["min_date"] else None,
                    "max_date": str(tick_cov["max_date"]) if tick_cov["max_date"] else None,
                    "record_count": tick_cov["count"],
                })

        return {"status": "ok", "database": stats, "tick_database": tick_stats, "coverage": coverage}
    except Exception as e:
        logger.error("Failed to retrieve catalog", exc_info=True)
        return {"status": "error", "message": str(e)}


@router.get("/catalog/stats")
def get_stats(
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS))
):
    """Return quick database statistics."""
    try:
        return get_database_stats()
    except Exception as e:
        logger.error("Failed to retrieve stats", exc_info=True)
        return {"status": "error", "message": str(e)}


def _run_export_job(job_id: str, output_dir: Optional[str]):
    try:
        manifest = export_catalog(Path(output_dir) if output_dir else None)
        finish_job(job_id, result=manifest)
    except Exception as e:
        logger.exception("Export job failed", extra={"job_id": job_id})
        finish_job(job_id, error=str(e))


def _run_restore_job(job_id: str, manifest_path: str):
    try:
        result = restore_catalog(Path(manifest_path))
        finish_job(job_id, result=result)
    except Exception as e:
        logger.exception("Restore job failed", extra={"job_id": job_id})
        finish_job(job_id, error=str(e))


@router.get("/catalog/gaps")
def get_catalog_gaps(
    instrument: str = Query(..., description="Instrument symbol"),
    timeframe: str = Query(..., description="Timeframe (M1, M5, H1, ...)"),
    start: Optional[str] = Query(None, description="Inclusive start timestamp (ISO-8601)"),
    end: Optional[str] = Query(None, description="Exclusive end timestamp (ISO-8601)"),
    min_gap_seconds: Optional[int] = Query(None, description="Only report gaps longer than this (default: 2× bar duration)"),
    limit: int = Query(100, ge=1, le=1000),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """
    Locate unusually-large gaps in an OHLC series. Each entry is flagged
    `is_weekend=true` if it matches the typical FX weekend closure pattern —
    filter those client-side if you only want real data holes.
    """
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)
    gaps = find_gaps(instrument, timeframe, start, end, min_gap_seconds, limit)
    return {
        "instrument": instrument,
        "timeframe": timeframe,
        "threshold_seconds": min_gap_seconds,
        "gap_count": len(gaps),
        "gaps": [
            {
                **g,
                "gap_start": g["gap_start"].isoformat() if g["gap_start"] else None,
                "gap_end": g["gap_end"].isoformat() if g["gap_end"] else None,
            }
            for g in gaps
        ],
    }


@router.post("/catalog/export")
def export_catalog_api(
    background_tasks: BackgroundTasks,
    output_dir: Optional[str] = Form(None, description=f"Target directory. Defaults to {DEFAULT_BACKUP_ROOT}/<timestamp>/"),
    background: bool = Form(False, description="Run as a background job and return a job id"),
    current_user: User = Depends(ScopedAuth("write")),
):
    """
    Export the entire datalake to partitioned Parquet plus a manifest.json.
    Safe to run while the API is live (uses DuckDB's MVCC snapshot). Requires write scope.
    """
    if background:
        job = create_job("catalog_export", meta={"output_dir": output_dir})
        background_tasks.add_task(_run_export_job, job.id, output_dir)
        return {"status": "ok", "job_id": job.id}

    try:
        manifest = export_catalog(Path(output_dir) if output_dir else None)
        return {"status": "ok", "manifest": manifest}
    except Exception as e:
        logger.exception("Export failed")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/catalog/restore")
def restore_catalog_api(
    background_tasks: BackgroundTasks,
    manifest_path: str = Form(..., description=f"Path to a manifest.json from a prior export (defaults to {MANIFEST_FILENAME} inside the export dir)"),
    background: bool = Form(False, description="Run as a background job and return a job id"),
    current_user: User = Depends(ScopedAuth("admin")),
):
    """
    Restore (merge) a previously exported catalog into the live datalake. Existing rows
    are overwritten by values from the backup; untouched rows stay. Requires admin scope.
    """
    if background:
        job = create_job("catalog_restore", meta={"manifest_path": manifest_path})
        background_tasks.add_task(_run_restore_job, job.id, manifest_path)
        return {"status": "ok", "job_id": job.id}

    try:
        return restore_catalog(Path(manifest_path))
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Restore failed")
        raise HTTPException(status_code=500, detail=str(e))
