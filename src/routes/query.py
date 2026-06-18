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
from src.core.datalake import read_connection
from src.core.concurrency import query_slot
from src.services.validators import validate_instrument, validate_timeframe
from src.auth.auth import ScopedAuth

router = APIRouter()

# Table schemas: (table, columns_sql, has_timeframe_column)
OHLC = ("ohlc_data", "instrument, timeframe, timestamp, open, high, low, close", True)
TICKS = ("tick_data", "timestamp, price, volume, bid, ask", False)

CSV_BATCH_SIZE = 1000


# ── shared helpers ──────────────────────────────────────────────────────────


def _build_conditions(
    schema,
    instrument: Optional[str],
    timeframe: Optional[str],
    start: Optional[str],
    end: Optional[str],
    cursor_timestamp: Optional[str] = None,
    require_instrument: bool = False,
):
    """Build WHERE conditions and params list for OHLC or tick queries."""
    _, _, has_tf = schema
    conditions = []
    params: list = []

    if require_instrument or instrument:
        conditions.append("instrument = ?")
        params.append(instrument)

    if has_tf and timeframe:
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

    return conditions, params


def _build_paginated_query(
    schema,
    instrument: Optional[str],
    timeframe: Optional[str],
    start: Optional[str],
    end: Optional[str],
    cursor_timestamp: Optional[str],
    limit: int,
):
    """Build a paginated SELECT for either OHLC or tick data."""
    table, columns, _ = schema
    conditions, params = _build_conditions(
        schema, instrument, timeframe, start, end, cursor_timestamp,
    )
    fetch_limit = limit + 1
    sql = f"""
    SELECT {columns}
    FROM {table}
    WHERE {' AND '.join(conditions)}
    ORDER BY timestamp
    LIMIT {fetch_limit}
    """
    return sql, params


def _paginate(sql, params, limit, instrument, timeframe, schema):
    """Execute a paginated query and return the JSONResponse envelope."""
    _, _, has_tf = schema
    tf_key = timeframe if has_tf else None

    with read_connection() as con:
        df = con.execute(sql, params).fetchdf()

    has_more = len(df) > limit
    if has_more:
        df = df.head(limit)

    df["timestamp"] = df["timestamp"].astype(str)
    result = df.to_dict(orient="records")

    next_cursor = None
    if has_more and result:
        next_cursor = encode_cursor(result[-1]["timestamp"], instrument, tf_key)

    response = {
        "data": result,
        "pagination": {"limit": limit, "count": len(result), "has_more": has_more},
    }
    if next_cursor:
        response["pagination"]["next_cursor"] = next_cursor

    return JSONResponse(content=response)


async def _csv_stream(sql, params, filename: str):
    """Stream a query result as CSV using fetchmany for memory efficiency."""
    async def generate():
        with read_connection() as con:
            db_cursor = con.execute(sql, params)
            header = [desc[0] for desc in db_cursor.description]

            output = io.StringIO()
            writer = csv.writer(output, lineterminator="\n")

            writer.writerow(header)
            yield output.getvalue()
            output.seek(0)
            output.truncate(0)

            while True:
                rows = db_cursor.fetchmany(CSV_BATCH_SIZE)
                if not rows:
                    break
                for row in rows:
                    writer.writerow(row)
                    yield output.getvalue()
                    output.seek(0)
                    output.truncate(0)
                await asyncio.sleep(0)

    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


def _build_download_sql(schema, instrument, timeframe, start, end):
    """Build a non-paginated SELECT for CSV download."""
    table, columns, _ = schema
    conditions, params = _build_conditions(
        schema, instrument, timeframe, start, end, require_instrument=True,
    )
    sql = f"""
    SELECT {columns}
    FROM {table}
    WHERE {' AND '.join(conditions)}
    ORDER BY timestamp
    """
    return sql, params


# ── OHLC endpoints ──────────────────────────────────────────────────────────


@router.get("/query")
def query_api(
    instrument: Optional[str] = Query(None),
    timeframe: Optional[str] = Query(None),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=10000, description="Page size (1-10000)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
    _slot: None = Depends(query_slot),
):
    """Query OHLC data from DuckDB with cursor-based pagination."""
    if instrument:
        instrument = validate_instrument(instrument)
    if timeframe:
        timeframe = validate_timeframe(timeframe)

    cursor_timestamp = decode_cursor(cursor, instrument, timeframe) if cursor else None
    sql, params = _build_paginated_query(OHLC, instrument, timeframe, start, end, cursor_timestamp, limit)
    return _paginate(sql, params, limit, instrument, timeframe, OHLC)


@router.get("/download")
async def download_data(
    instrument: str,
    timeframe: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
    _slot: None = Depends(query_slot),
):
    """Download OHLC data as a streaming CSV file."""
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)
    sql, params = _build_download_sql(OHLC, instrument, timeframe, start, end)
    return await _csv_stream(sql, params, f"{instrument}_{timeframe}_data.csv")


# ── Tick endpoints ──────────────────────────────────────────────────────────


@router.get("/ticks")
def query_ticks_api(
    instrument: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    limit: int = Query(10000, ge=1, le=100000, description="Page size (1-100000)"),
    cursor: Optional[str] = Query(None, description="Pagination cursor from previous response"),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
    _slot: None = Depends(query_slot),
):
    """Query tick data from DuckDB with cursor-based pagination."""
    instrument = validate_instrument(instrument)
    cursor_timestamp = decode_cursor(cursor, instrument, None) if cursor else None
    sql, params = _build_paginated_query(TICKS, instrument, None, start, end, cursor_timestamp, limit)
    return _paginate(sql, params, limit, instrument, None, TICKS)


@router.get("/ticks/download")
async def download_ticks(
    instrument: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    current_user: Optional[User] = Depends(ScopedAuth("read", allow_public=ALLOW_PUBLIC_READS)),
    _slot: None = Depends(query_slot),
):
    """Download tick data as a streaming CSV file."""
    instrument = validate_instrument(instrument)
    sql, params = _build_download_sql(TICKS, instrument, None, start, end)
    return await _csv_stream(sql, params, f"{instrument}_TICK_data.csv")
