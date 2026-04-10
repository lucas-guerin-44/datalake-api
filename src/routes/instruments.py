"""Instruments routes - list instruments and timeframes from DuckDB."""
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException

from src.config import ALLOW_PUBLIC_READS
from src.core.database import User
from src.core.datalake import list_instruments, list_timeframes, get_data_range, list_tick_instruments, get_tick_coverage
from src.services.validators import validate_instrument
from src.auth.auth import ScopedAuth

router = APIRouter()


@router.get("/instruments")
def get_instruments(
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """List all instruments that have data in the datalake."""
    ohlc = set(list_instruments())
    tick = set(list_tick_instruments())
    all_instruments = sorted(ohlc | tick)
    return {"instruments": all_instruments}


@router.get("/instruments/{symbol}")
def get_instrument_detail(
    symbol: str,
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """Get data coverage for a specific instrument across all timeframes."""
    symbol = validate_instrument(symbol)

    timeframes = list_timeframes(symbol)

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

    # Include tick coverage if available
    tick_cov = get_tick_coverage(symbol)
    if tick_cov:
        coverage.append({
            "timeframe": "TICK",
            "min_date": str(tick_cov["min_date"]) if tick_cov["min_date"] else None,
            "max_date": str(tick_cov["max_date"]) if tick_cov["max_date"] else None,
            "record_count": tick_cov["count"],
        })

    if not timeframes and not tick_cov:
        raise HTTPException(status_code=404, detail=f"Instrument '{symbol}' not found")

    return {"symbol": symbol, "timeframes": coverage}


@router.get("/timeframes")
def get_timeframes(
    instrument: str | None = Query(None),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """List available timeframes, optionally filtered by instrument."""
    if instrument:
        instrument = validate_instrument(instrument)
    tfs = list_timeframes(instrument)

    # Include TICK if tick data exists for the instrument (or any instrument)
    if instrument:
        if get_tick_coverage(instrument):
            tfs.append("TICK")
    else:
        if list_tick_instruments():
            tfs.append("TICK")

    return {"timeframes": tfs}
