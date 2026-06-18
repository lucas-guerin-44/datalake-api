"""Ingest routes - upload and batch ingest data files."""
from pathlib import Path
from typing import Callable, List, Optional

from fastapi import APIRouter, Depends, Form, File, Request, UploadFile, HTTPException
from fastapi.concurrency import run_in_threadpool

from src.config import MAX_UPLOAD_SIZE_BYTES, MAX_UPLOAD_SIZE_MB
from src.core.concurrency import write_slot
from src.middleware.logging_config import get_logger
from src.core.database import User
from src.core import derivation_queue
from src.services.pipeline import (
    ingest_single_file, ingest_tick_file, ingest_single_file_queued,
    ingest_tick_file_queued, parse_filename_meta, DEFAULT_STAGING,
)
from src.services.validators import validate_instrument, validate_timeframe, sanitize_filename
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


async def _handle_file_ingest(
    request: Request,
    file: UploadFile,
    instrument: str,
    derive: bool,
    wait: bool,
    sync_ingest_fn: Callable,
    queued_ingest_fn: Callable,
    build_response: Callable,
    timeframe: Optional[str] = None,
):
    """Shared handler for single-file ingest (OHLC and tick)."""
    _check_upload_size(request)
    instrument = validate_instrument(instrument)
    if timeframe is not None:
        timeframe = validate_timeframe(timeframe)

    raw_bytes = await _read_upload_capped(file)
    tmp_path = _save_upload(file, raw_bytes)

    try:
        if not derive:
            result = await run_in_threadpool(sync_ingest_fn, tmp_path, instrument, timeframe, False) if timeframe is not None else await run_in_threadpool(sync_ingest_fn, tmp_path, instrument, False)
            return build_response(instrument, result, timeframe=timeframe, derived=False)

        res = await run_in_threadpool(queued_ingest_fn, tmp_path, instrument, timeframe) if timeframe is not None else await run_in_threadpool(queued_ingest_fn, tmp_path, instrument)
        if wait and res["queue_id"] is not None:
            final = await run_in_threadpool(derivation_queue.wait_for, res["queue_id"])
            return build_response(instrument, res, timeframe=timeframe, derived=final)
        return build_response(instrument, res, timeframe=timeframe, derived="queued")
    except HTTPException:
        raise
    except (ValueError, FileNotFoundError) as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Ingest failed unexpectedly")
        raise HTTPException(status_code=500, detail="Internal server error")


async def _handle_batch_ingest(
    file_filter: Callable[[Path], bool],
    parse_meta: Callable[[Path], tuple],
    sync_ingest_fn: Callable,
    derive: bool,
    empty_message: str,
):
    """Shared handler for batch ingest (OHLC and tick)."""
    files = sorted(p for p in DEFAULT_STAGING.iterdir() if file_filter(p))
    if not files:
        return {"status": "empty", "message": empty_message}

    results = []
    for f in files:
        try:
            meta = parse_meta(f)
            await run_in_threadpool(sync_ingest_fn, f, *meta, derive)
            results.append({"file": f.name, "status": "ok"})
        except Exception as e:
            results.append({"file": f.name, "status": "error", "error": str(e)})

    return {"results": results}


# --- OHLC endpoints -----------------------------------------------------------


def _ohlc_response(instrument, result, timeframe=None, derived=False):
    return {"status": "ok", "instrument": instrument, "timeframe": timeframe, "derived": derived}


@router.post("/ingest")
async def ingest_file_api(
    request: Request,
    file: UploadFile = File(...),
    instrument: str = Form(...),
    timeframe: str = Form(...),
    derive: bool = Form(True, description="Queue auto-derivation of higher timeframes from the ingested window"),
    wait: bool = Form(False, description="Block until derivation completes (read-after-write). Default: derive asynchronously off the request path."),
    current_user: User = Depends(ScopedAuth("write")),
    _wslot: None = Depends(write_slot),
):
    """Ingest a single CSV/Excel file into DuckDB. Requires write scope."""
    return await _handle_file_ingest(
        request, file, instrument, derive, wait,
        sync_ingest_fn=ingest_single_file,
        queued_ingest_fn=ingest_single_file_queued,
        build_response=_ohlc_response,
        timeframe=timeframe,
    )


@router.post("/ingest-batch")
async def ingest_batch_api(
    derive: bool = Form(True, description="Auto-derive higher timeframes from the ingested window"),
    current_user: User = Depends(ScopedAuth("write")),
):
    """Ingest all CSV/Excel files in the staging directory. Requires write scope."""
    return await _handle_batch_ingest(
        file_filter=lambda p: p.suffix.lower() in {".csv", ".xlsx", ".xls"},
        parse_meta=parse_filename_meta,
        sync_ingest_fn=ingest_single_file,
        derive=derive,
        empty_message="No files found",
    )


# --- Tick endpoints -----------------------------------------------------------


def _tick_response(instrument, result, timeframe=None, derived=False):
    return {"status": "ok", "instrument": instrument, "rows_inserted": result["rows_inserted"], "derived": derived, "queue_id": result.get("queue_id")}


@router.post("/ingest/ticks")
async def ingest_tick_file_api(
    request: Request,
    file: UploadFile = File(...),
    instrument: str = Form(...),
    derive: bool = Form(True, description="Queue auto-derivation of OHLC bars (M1..D1) from the ingested ticks"),
    wait: bool = Form(False, description="Block until derivation completes (read-after-write). Default: derive asynchronously."),
    current_user: User = Depends(ScopedAuth("write")),
    _wslot: None = Depends(write_slot),
):
    """Ingest a single tick CSV file into DuckDB. Requires write scope."""
    return await _handle_file_ingest(
        request, file, instrument, derive, wait,
        sync_ingest_fn=ingest_tick_file,
        queued_ingest_fn=ingest_tick_file_queued,
        build_response=_tick_response,
    )


@router.post("/ingest-batch/ticks")
async def ingest_tick_batch_api(
    derive: bool = Form(True, description="Auto-derive OHLC bars (M1..D1) from the ingested ticks"),
    current_user: User = Depends(ScopedAuth("write")),
):
    """Ingest all tick CSV files matching {INSTRUMENT}_TICK_*.csv from staging."""
    def _tick_meta(f):
        instrument, _ = parse_filename_meta(f)
        return (instrument,)

    return await _handle_batch_ingest(
        file_filter=lambda p: p.suffix.lower() == ".csv" and "_TICK_" in p.name.upper(),
        parse_meta=_tick_meta,
        sync_ingest_fn=ingest_tick_file,
        derive=derive,
        empty_message="No tick files found",
    )


# --- Queue stats --------------------------------------------------------------


@router.get("/ingest/queue")
async def ingest_queue_stats(current_user: User = Depends(ScopedAuth("read"))):
    """
    Derivation-queue health: pending/done/error counts and the age of the oldest
    pending task. A growing `pending` or a large `oldest_pending_age_seconds`
    means the worker is falling behind; a non-zero `error` needs a look.
    """
    return await run_in_threadpool(derivation_queue.queue_stats)
