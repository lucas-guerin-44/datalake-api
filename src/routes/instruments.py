"""Instruments routes - list instruments and timeframes from DuckDB."""
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, Query, HTTPException

from src.config import ALLOW_PUBLIC_READS
from src.core.database import User
from src.core import cache
from src.core.queries import (
    list_instruments,
    list_timeframes,
    get_data_range,
    list_tick_instruments,
    get_tick_coverage,
)
from src.core.writes import (
    delete_ohlc_data,
    delete_tick_data,
)
from src.services.validators import validate_instrument, validate_timeframe
from src.auth.auth import ScopedAuth

router = APIRouter()


@router.get("/instruments")
def get_instruments(
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """List all instruments that have data in the datalake (cached, self-invalidating on write)."""
    def _build():
        all_instruments = sorted(set(list_instruments()) | set(list_tick_instruments()))
        return {"instruments": all_instruments}

    return cache.get_or_compute("instruments", _build)


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
                "sources": data_range.get("sources", []),
            })

    # Include tick coverage if available
    tick_cov = get_tick_coverage(symbol)
    if tick_cov:
        coverage.append({
            "timeframe": "TICK",
            "min_date": str(tick_cov["min_date"]) if tick_cov["min_date"] else None,
            "max_date": str(tick_cov["max_date"]) if tick_cov["max_date"] else None,
            "record_count": tick_cov["count"],
            "sources": ["raw"],
        })

    if not timeframes and not tick_cov:
        raise HTTPException(status_code=404, detail=f"Instrument '{symbol}' not found")

    return {"symbol": symbol, "timeframes": coverage}


@router.delete("/instruments/{symbol}")
def delete_instrument_data(
    symbol: str,
    timeframe: Optional[str] = Query(None, description="If set, only delete this timeframe (or 'TICK' for ticks)."),
    start: Optional[datetime] = Query(None, description="Half-open window start (UTC). Inclusive."),
    end: Optional[datetime] = Query(None, description="Half-open window end (UTC). Exclusive."),
    include_ticks: bool = Query(True, description="Also delete tick_data when timeframe is unset."),
    confirm: bool = Query(False, description="Must be true — guard against accidental DELETE."),
    current_user: User = Depends(ScopedAuth("admin")),
):
    """
    Delete OHLC and/or tick rows for a symbol. Admin-only.

    - No `timeframe`: wipes every OHLC timeframe for the symbol; also wipes ticks
      unless `include_ticks=false`.
    - `timeframe=TICK`: only ticks.
    - `timeframe=M1` (etc): only that OHLC timeframe.
    - `start`/`end` scope by window; omit for full range.
    """
    if not confirm:
        raise HTTPException(status_code=400, detail="Pass confirm=true to actually delete.")

    symbol = validate_instrument(symbol)

    deleted = {}
    if timeframe is None:
        deleted["ohlc"] = delete_ohlc_data(symbol, start=start, end=end)
        if include_ticks:
            deleted["ticks"] = delete_tick_data(symbol, start=start, end=end)
    elif timeframe.upper() == "TICK":
        deleted["ticks"] = delete_tick_data(symbol, start=start, end=end)
    else:
        tf = validate_timeframe(timeframe)
        deleted["ohlc"] = delete_ohlc_data(symbol, timeframe=tf, start=start, end=end)

    return {
        "status": "ok",
        "symbol": symbol,
        "timeframe": timeframe,
        "window": {
            "start": start.isoformat() if start else None,
            "end": end.isoformat() if end else None,
        },
        "deleted": deleted,
    }


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
