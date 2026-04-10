"""Query routes - query and download OHLC and tick data from DuckDB."""
import asyncio
import csv
import io
from typing import Optional

from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse, StreamingResponse

from src.config import ALLOW_PUBLIC_READS
from src.core.database import User
from src.core.pagination import encode_cursor, decode_cursor
from src.core.datalake import get_db_connection
from src.services.validators import validate_instrument, validate_timeframe
from src.auth.auth import ScopedAuth

router = APIRouter()


@router.get("/query")
def query_api(
    instrument: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=10000, description="Page size (1-10000)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """Query OHLC data from DuckDB with cursor-based pagination."""
    if instrument:
        instrument = validate_instrument(instrument)
    if timeframe:
        timeframe = validate_timeframe(timeframe)

    cursor_timestamp = None
    if cursor:
        cursor_timestamp = decode_cursor(cursor, instrument, timeframe)

    # Build query
    conditions = ["1=1"]
    params = []

    if instrument:
        conditions.append("instrument = ?")
        params.append(instrument)
    if timeframe:
        conditions.append("timeframe = ?")
        params.append(timeframe)
    if cursor_timestamp:
        conditions.append("timestamp > ?::TIMESTAMP")
        params.append(cursor_timestamp)
    elif start:
        conditions.append("timestamp >= ?::TIMESTAMP")
        params.append(start)
    if end:
        conditions.append("timestamp <= ?::TIMESTAMP")
        params.append(end)

    fetch_limit = limit + 1
    sql = f"""
    SELECT instrument, timeframe, timestamp, open, high, low, close
    FROM ohlc_data
    WHERE {' AND '.join(conditions)}
    ORDER BY timestamp
    LIMIT {fetch_limit}
    """

    with get_db_connection() as con:
        df = con.execute(sql, params).fetchdf()

    has_more = len(df) > limit
    if has_more:
        df = df.head(limit)

    df["timestamp"] = df["timestamp"].astype(str)
    result = df.to_dict(orient="records")

    next_cursor = None
    if has_more and result:
        next_cursor = encode_cursor(result[-1]["timestamp"], instrument, timeframe)

    response = {
        "data": result,
        "pagination": {"limit": limit, "count": len(result), "has_more": has_more},
    }
    if next_cursor:
        response["pagination"]["next_cursor"] = next_cursor

    return JSONResponse(content=response)


@router.get("/download")
async def download_data(
    instrument: str,
    timeframe: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """Download OHLC data as a streaming CSV file."""
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    conditions = ["instrument = ?", "timeframe = ?"]
    params = [instrument, timeframe]
    if start:
        conditions.append("timestamp >= ?::TIMESTAMP")
        params.append(start)
    if end:
        conditions.append("timestamp <= ?::TIMESTAMP")
        params.append(end)

    sql = f"""
    SELECT instrument, timeframe, timestamp, open, high, low, close
    FROM ohlc_data
    WHERE {' AND '.join(conditions)}
    ORDER BY timestamp
    """

    async def csv_generator():
        with get_db_connection() as con:
            db_cursor = con.execute(sql, params)
            header = [desc[0] for desc in db_cursor.description]

            output = io.StringIO()
            writer = csv.writer(output, lineterminator="\n")

            writer.writerow(header)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

            for row in db_cursor.fetchall():
                writer.writerow(row)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
                await asyncio.sleep(0)

    return StreamingResponse(
        csv_generator(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={instrument}_{timeframe}_data.csv"},
    )


@router.get("/ticks")
def query_ticks_api(
    instrument: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    limit: int = Query(10000, ge=1, le=100000, description="Page size (1-100000)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """Query tick data from DuckDB with cursor-based pagination."""
    instrument = validate_instrument(instrument)

    cursor_timestamp = None
    if cursor:
        cursor_timestamp = decode_cursor(cursor, instrument, None)

    conditions = ["instrument = ?"]
    params = [instrument]

    if cursor_timestamp:
        conditions.append("timestamp > ?::TIMESTAMP")
        params.append(cursor_timestamp)
    elif start:
        conditions.append("timestamp >= ?::TIMESTAMP")
        params.append(start)
    if end:
        conditions.append("timestamp <= ?::TIMESTAMP")
        params.append(end)

    fetch_limit = limit + 1
    sql = f"""
    SELECT timestamp, price, volume, bid, ask
    FROM tick_data
    WHERE {' AND '.join(conditions)}
    ORDER BY timestamp
    LIMIT {fetch_limit}
    """

    with get_db_connection() as con:
        df = con.execute(sql, params).fetchdf()

    has_more = len(df) > limit
    if has_more:
        df = df.head(limit)

    df["timestamp"] = df["timestamp"].astype(str)
    result = df.to_dict(orient="records")

    next_cursor = None
    if has_more and result:
        next_cursor = encode_cursor(result[-1]["timestamp"], instrument, None)

    response = {
        "data": result,
        "pagination": {"limit": limit, "count": len(result), "has_more": has_more},
    }
    if next_cursor:
        response["pagination"]["next_cursor"] = next_cursor

    return JSONResponse(content=response)


@router.get("/ticks/download")
async def download_ticks(
    instrument: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
):
    """Download tick data as a streaming CSV file."""
    instrument = validate_instrument(instrument)

    conditions = ["instrument = ?"]
    params = [instrument]
    if start:
        conditions.append("timestamp >= ?::TIMESTAMP")
        params.append(start)
    if end:
        conditions.append("timestamp <= ?::TIMESTAMP")
        params.append(end)

    sql = f"""
    SELECT timestamp, price, volume, bid, ask
    FROM tick_data
    WHERE {' AND '.join(conditions)}
    ORDER BY timestamp
    """

    async def csv_generator():
        with get_db_connection() as con:
            db_cursor = con.execute(sql, params)
            header = [desc[0] for desc in db_cursor.description]

            output = io.StringIO()
            writer = csv.writer(output, lineterminator="\n")

            writer.writerow(header)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

            for row in db_cursor.fetchall():
                writer.writerow(row)
                yield output.getvalue()
                output.seek(0)
                output.truncate(0)
                await asyncio.sleep(0)

    return StreamingResponse(
        csv_generator(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={instrument}_TICK_data.csv"},
    )
