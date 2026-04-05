"""Instruments routes - list instruments and timeframes from DuckDB."""
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException

from src.config import ALLOW_PUBLIC_READS
from src.core.database import User
from src.core.datalake import list_instruments, list_timeframes, get_data_range
from src.services.validators import validate_instrument
from src.auth.auth import ScopedAuth

router = APIRouter()


@router.get("/instruments")
def get_instruments(
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """List all instruments that have data in the datalake."""
    return {"instruments": list_instruments()}


@router.get("/instruments/{symbol}")
def get_instrument_detail(
    symbol: str,
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """Get data coverage for a specific instrument across all timeframes."""
    symbol = validate_instrument(symbol)

    timeframes = list_timeframes(symbol)
    if not timeframes:
        raise HTTPException(status_code=404, detail=f"Instrument '{symbol}' not found")

    coverage = []
    for tf in timeframes:
        data_range = get_data_range(symbol, tf)
        if data_range:
            coverage.append({
                "timeframe": tf,
                "min_date": str(data_range["min_date"]) if data_range["min_date"] else None,
                "max_date": str(data_range["max_date"]) if data_range["max_date"] else None,
                "record_count": data_range["count"],
            })

    return {"symbol": symbol, "timeframes": coverage}


@router.get("/timeframes")
def get_timeframes(
    instrument: str | None = Query(None),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """List available timeframes, optionally filtered by instrument."""
    if instrument:
        instrument = validate_instrument(instrument)
    return {"timeframes": list_timeframes(instrument)}
