"""Ingest routes - upload and batch ingest data files."""
from pathlib import Path

from fastapi import APIRouter, BackgroundTasks, Depends, Form, File, Request, UploadFile, HTTPException

from src.config import MAX_UPLOAD_SIZE_BYTES, MAX_UPLOAD_SIZE_MB
from src.middleware.logging_config import get_logger
from src.core.database import User
from src.core.datalake import derive_ohlc_timeframes, derive_ohlc_from_ticks, write_transaction
from src.services.pipeline import ingest_single_file, ingest_tick_file, parse_filename_meta, DEFAULT_STAGING
from src.services.validators import validate_instrument, validate_timeframe, sanitize_filename
from src.services.jobs import create_job, finish_job
from src.auth.auth import ScopedAuth

logger = get_logger(__name__)
router = APIRouter()


def _check_upload_size(request: Request):
    """Reject oversized uploads early via Content-Length."""
    cl = request.headers.get("content-length")
    if cl is not None:
        try:
            size = int(cl)
        except ValueError:
            return
        if size > MAX_UPLOAD_SIZE_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"Upload exceeds max size of {MAX_UPLOAD_SIZE_MB} MB",
            )


def _save_upload(file: UploadFile, raw_bytes: bytes) -> Path:
    safe_name = sanitize_filename(file.filename or "upload.csv")
    tmp_path = DEFAULT_STAGING / safe_name
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp_path, "wb") as f:
        f.write(raw_bytes)
    return tmp_path


async def _read_upload_capped(file: UploadFile) -> bytes:
    """Read the upload in chunks and enforce the size cap even if Content-Length lied."""
    buf = bytearray()
    while True:
        chunk = await file.read(1024 * 1024)
        if not chunk:
            break
        buf.extend(chunk)
        if len(buf) > MAX_UPLOAD_SIZE_BYTES:
            raise HTTPException(
                status_code=413,
                detail=f"Upload exceeds max size of {MAX_UPLOAD_SIZE_MB} MB",
            )
    return bytes(buf)


def _run_derive_ohlc_job(job_id: str, instrument: str, source_tf: str, start, end):
    try:
        with write_transaction():
            result = derive_ohlc_timeframes(instrument, source_tf, start, end)
        finish_job(job_id, result={"targets": result})
    except Exception as e:
        logger.exception("Derive OHLC job failed", extra={"job_id": job_id})
        finish_job(job_id, error=str(e))


def _run_derive_ticks_job(job_id: str, instrument: str, start, end):
    try:
        with write_transaction():
            result = derive_ohlc_from_ticks(instrument, start, end)
        finish_job(job_id, result={"targets": result})
    except Exception as e:
        logger.exception("Derive ticks job failed", extra={"job_id": job_id})
        finish_job(job_id, error=str(e))


@router.post("/ingest")
async def ingest_file_api(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    instrument: str = Form(...),
    timeframe: str = Form(...),
    derive: bool = Form(True, description="Auto-derive higher timeframes from the ingested window"),
    background: bool = Form(False, description="Run derivation as a background job and return a job id"),
    current_user: User = Depends(ScopedAuth("write")),
):
    """Ingest a single CSV/Excel file into DuckDB. Requires write scope."""
    _check_upload_size(request)
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    raw_bytes = await _read_upload_capped(file)
    tmp_path = _save_upload(file, raw_bytes)

    try:
        if background and derive:
            # Do the raw upsert synchronously (fast), queue derivation as a job.
            ingest_single_file(tmp_path, instrument, timeframe, derive=False)
            import pandas as pd  # local import keeps route import cheap
            from src.services.pipeline import _read_raw, _standardize
            df = _standardize(_read_raw(tmp_path))
            job = create_job(
                "derive_ohlc",
                meta={"instrument": instrument, "source_timeframe": timeframe},
            )
            background_tasks.add_task(
                _run_derive_ohlc_job,
                job.id, instrument, timeframe,
                df["timestamp"].min(), df["timestamp"].max(),
            )
            return {
                "status": "ok",
                "instrument": instrument,
                "timeframe": timeframe,
                "derive_job_id": job.id,
            }

        ingest_single_file(tmp_path, instrument, timeframe, derive=derive)
        return {"status": "ok", "instrument": instrument, "timeframe": timeframe, "derived": derive}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ingest-batch")
async def ingest_batch_api(
    folder: Path = Form(DEFAULT_STAGING),
    derive: bool = Form(True, description="Auto-derive higher timeframes from the ingested window"),
    current_user: User = Depends(ScopedAuth("write")),
):
    """Ingest all CSV/Excel files in a folder. Requires write scope."""
    files = sorted([p for p in folder.iterdir() if p.suffix.lower() in {".csv", ".xlsx", ".xls"}])
    if not files:
        return {"status": "empty", "message": "No files found"}

    results = []
    for f in files:
        try:
            instrument, timeframe = parse_filename_meta(f)
            ingest_single_file(f, instrument, timeframe, derive=derive)
            results.append({"file": f.name, "status": "ok"})
        except Exception as e:
            results.append({"file": f.name, "status": "error", "error": str(e)})

    return {"results": results}


@router.post("/ingest/ticks")
async def ingest_tick_file_api(
    request: Request,
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    instrument: str = Form(...),
    derive: bool = Form(True, description="Auto-derive OHLC bars (M1..D1) from the ingested ticks"),
    background: bool = Form(False, description="Run derivation as a background job and return a job id"),
    current_user: User = Depends(ScopedAuth("write")),
):
    """Ingest a single tick CSV file into DuckDB. Requires write scope."""
    _check_upload_size(request)
    instrument = validate_instrument(instrument)

    raw_bytes = await _read_upload_capped(file)
    tmp_path = _save_upload(file, raw_bytes)

    try:
        if background and derive:
            rows = ingest_tick_file(tmp_path, instrument, derive=False)
            from src.services.pipeline import _read_raw_tick, standardize_tick_csv
            df = standardize_tick_csv(_read_raw_tick(tmp_path))
            job = create_job("derive_ticks", meta={"instrument": instrument})
            background_tasks.add_task(
                _run_derive_ticks_job,
                job.id, instrument,
                df["timestamp"].min(), df["timestamp"].max(),
            )
            return {
                "status": "ok",
                "instrument": instrument,
                "rows_inserted": rows,
                "derive_job_id": job.id,
            }

        rows = ingest_tick_file(tmp_path, instrument, derive=derive)
        return {"status": "ok", "instrument": instrument, "rows_inserted": rows, "derived": derive}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ingest-batch/ticks")
async def ingest_tick_batch_api(
    folder: Path = Form(DEFAULT_STAGING),
    derive: bool = Form(True, description="Auto-derive OHLC bars (M1..D1) from the ingested ticks"),
    current_user: User = Depends(ScopedAuth("write")),
):
    """Ingest all tick CSV files matching {INSTRUMENT}_TICK_*.csv from a folder. Requires write scope."""
    files = sorted([p for p in folder.iterdir() if p.suffix.lower() == ".csv" and "_TICK_" in p.name.upper()])
    if not files:
        return {"status": "empty", "message": "No tick files found"}

    results = []
    for f in files:
        try:
            instrument, _ = parse_filename_meta(f)
            rows = ingest_tick_file(f, instrument, derive=derive)
            results.append({"file": f.name, "status": "ok", "rows_inserted": rows})
        except Exception as e:
            results.append({"file": f.name, "status": "error", "error": str(e)})

    return {"results": results}
