"""Ingest routes - upload and batch ingest data files."""
from pathlib import Path

from fastapi import APIRouter, Depends, Form, File, UploadFile, HTTPException

from src.middleware.logging_config import get_logger
from src.core.database import User
from src.services.pipeline import ingest_single_file, ingest_tick_file, parse_filename_meta, DEFAULT_STAGING
from src.services.validators import validate_instrument, validate_timeframe
from src.auth.auth import ScopedAuth

logger = get_logger(__name__)
router = APIRouter()


@router.post("/ingest")
async def ingest_file_api(
    file: UploadFile = File(...),
    instrument: str = Form(...),
    timeframe: str = Form(...),
    current_user: User = Depends(ScopedAuth("write")),
):
    """Ingest a single CSV/Excel file into DuckDB. Requires write scope."""
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    tmp_path = DEFAULT_STAGING / file.filename
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp_path, "wb") as f:
        f.write(await file.read())

    try:
        ingest_single_file(tmp_path, instrument, timeframe)
        return {"status": "ok", "instrument": instrument, "timeframe": timeframe}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ingest-batch")
async def ingest_batch_api(
    folder: Path = Form(DEFAULT_STAGING),
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
            ingest_single_file(f, instrument, timeframe)
            results.append({"file": f.name, "status": "ok"})
        except Exception as e:
            results.append({"file": f.name, "status": "error", "error": str(e)})

    return {"results": results}


@router.post("/ingest/ticks")
async def ingest_tick_file_api(
    file: UploadFile = File(...),
    instrument: str = Form(...),
    current_user: User = Depends(ScopedAuth("write")),
):
    """Ingest a single tick CSV file into DuckDB. Requires write scope."""
    instrument = validate_instrument(instrument)

    tmp_path = DEFAULT_STAGING / file.filename
    tmp_path.parent.mkdir(parents=True, exist_ok=True)
    with open(tmp_path, "wb") as f:
        f.write(await file.read())

    try:
        rows = ingest_tick_file(tmp_path, instrument)
        return {"status": "ok", "instrument": instrument, "rows_inserted": rows}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/ingest-batch/ticks")
async def ingest_tick_batch_api(
    folder: Path = Form(DEFAULT_STAGING),
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
            rows = ingest_tick_file(f, instrument)
            results.append({"file": f.name, "status": "ok", "rows_inserted": rows})
        except Exception as e:
            results.append({"file": f.name, "status": "error", "error": str(e)})

    return {"results": results}
