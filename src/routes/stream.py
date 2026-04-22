"""WebSocket streaming routes — replay historical ticks and bars at real-time speed."""
import asyncio
import json
import os
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Depends, WebSocket, WebSocketDisconnect, Query
from sqlalchemy.orm import Session

from src.auth.auth import ws_require_auth, _ws_close_with_reason
from src.config import ALLOW_PUBLIC_READS
from src.core.database import get_db
from src.core.datalake import get_db_connection
from src.services.validators import validate_instrument, validate_timeframe
from src.middleware.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()

# Per-client concurrent-connection cap. Prevents an authed client (or leaked API
# key) from opening unlimited WS connections and exhausting the event loop.
MAX_WS_PER_CLIENT = int(os.getenv("MAX_WS_PER_CLIENT", "5"))

_ws_connections: dict[str, int] = defaultdict(int)
_ws_lock = asyncio.Lock()


def _ws_client_key(ws: WebSocket) -> str:
    """Prefer the real client IP from X-Forwarded-For (set by Caddy)."""
    xff = ws.headers.get("x-forwarded-for", "")
    if xff:
        return xff.split(",")[0].strip()
    return ws.client.host if ws.client else "unknown"


@asynccontextmanager
async def _ws_slot(ws: WebSocket):
    """Reserve one of the per-client connection slots; raise RuntimeError if full."""
    key = _ws_client_key(ws)
    async with _ws_lock:
        if _ws_connections[key] >= MAX_WS_PER_CLIENT:
            raise RuntimeError(f"ws-limit:{key}")
        _ws_connections[key] += 1
    try:
        yield
    finally:
        async with _ws_lock:
            _ws_connections[key] = max(0, _ws_connections[key] - 1)


def _parse_ts(ts) -> datetime:
    """Convert a DuckDB timestamp value to a Python datetime."""
    if isinstance(ts, datetime):
        return ts
    return datetime.fromisoformat(str(ts))


async def _stream_rows(
    ws: WebSocket,
    table: str,
    columns: list[str],
    conditions: list[str],
    params: list,
    speed: float,
    max_delay: float = 10.0,
):
    """
    Stream query results over a WebSocket using internal cursor-based
    pagination. Each page is a small LIMIT query that returns instantly
    (indexed lookup), so the first message goes out in milliseconds
    regardless of total dataset size.

    Args:
        table: Table name to query (tick_data or ohlc_data).
        columns: Column names to SELECT.
        conditions: WHERE clause fragments (joined with AND).
        params: Bind parameters for the conditions.
        speed: Playback speed multiplier.
        max_delay: Upper bound (seconds) on sleep between messages (0 = burst).

    Each message is a JSON object with the column names as keys.
    Sends a final {"done": true} when replay is complete.
    """
    PAGE_SIZE = 5000
    prev_ts: Optional[datetime] = None
    last_ts = None

    while True:
        # Build a paginated query — use cursor from previous page
        page_conditions = list(conditions)
        page_params = list(params)
        if last_ts is not None:
            page_conditions.append("timestamp > ?::TIMESTAMP")
            page_params.append(last_ts)

        sql = f"""
        SELECT {', '.join(columns)}
        FROM {table}
        WHERE {' AND '.join(page_conditions)}
        ORDER BY timestamp
        LIMIT {PAGE_SIZE}
        """

        with get_db_connection() as con:
            rows = con.execute(sql, page_params).fetchall()

        if not rows:
            break

        for row in rows:
            record = dict(zip(columns, row))

            ts = _parse_ts(record["timestamp"])
            record["timestamp"] = ts.isoformat()
            last_ts = ts.isoformat()

            if prev_ts is not None and speed > 0 and max_delay > 0:
                delta = (ts - prev_ts).total_seconds() / speed
                if delta > 0:
                    await asyncio.sleep(min(delta, max_delay))

            prev_ts = ts
            await ws.send_text(json.dumps(record))

        if len(rows) < PAGE_SIZE:
            break

    await ws.send_text(json.dumps({"done": True}))


@router.websocket("/ws/ticks")
async def stream_ticks(
    ws: WebSocket,
    instrument: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    speed: float = Query(1.0, gt=0),
    max_delay: float = Query(10.0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Stream historical tick data at real-time speed (or multiplied).

    Auth: send credentials via headers only — `Authorization: Bearer <jwt>` or
    `X-API-Key: dk_...`. Query-string auth is not supported (leaks into logs).

    Query params:
        instrument — required, e.g. XAUUSD
        start / end — optional ISO-8601 timestamp bounds
        speed — playback multiplier (1.0 = real-time, 10.0 = 10x fast)
        max_delay — upper bound in seconds on sleep between messages (0 = burst)
    """
    await ws.accept()
    try:
        async with _ws_slot(ws):
            if not await ws_require_auth(ws, db, "read", ALLOW_PUBLIC_READS):
                return
            instrument = validate_instrument(instrument)

            conditions = ["instrument = ?"]
            params = [instrument]
            if start:
                conditions.append("timestamp >= ?::TIMESTAMP")
                params.append(start)
            if end:
                conditions.append("timestamp <= ?::TIMESTAMP")
                params.append(end)

            columns = ["timestamp", "price", "volume", "bid", "ask"]
            await _stream_rows(ws, "tick_data", columns, conditions, params, speed, max_delay)
    except RuntimeError as e:
        if str(e).startswith("ws-limit:"):
            await _ws_close_with_reason(ws, 4429, "too many concurrent connections")
            return
        raise
    except WebSocketDisconnect:
        logger.info("Tick stream client disconnected", extra={"instrument": instrument})
    except Exception as e:
        logger.error("Tick stream error", exc_info=True)
        try:
            await ws.send_text(json.dumps({"error": str(e)}))
        except Exception:
            pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass


@router.websocket("/ws/bars")
async def stream_bars(
    ws: WebSocket,
    instrument: str = Query(...),
    timeframe: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    speed: float = Query(1.0, gt=0),
    max_delay: float = Query(10.0, ge=0),
    db: Session = Depends(get_db),
):
    """
    Stream historical OHLC bars at real-time speed (or multiplied).

    Auth: send credentials via headers only — `Authorization: Bearer <jwt>` or
    `X-API-Key: dk_...`. Query-string auth is not supported (leaks into logs).

    Query params:
        instrument — required, e.g. XAUUSD
        timeframe — required, e.g. M5, H1
        start / end — optional ISO-8601 timestamp bounds
        speed — playback multiplier (1.0 = real-time, 60.0 = 1 bar/sec for M1)
        max_delay — upper bound in seconds on sleep between messages (0 = burst)
    """
    await ws.accept()
    try:
        async with _ws_slot(ws):
            if not await ws_require_auth(ws, db, "read", ALLOW_PUBLIC_READS):
                return
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

            columns = ["timestamp", "open", "high", "low", "close"]
            await _stream_rows(ws, "ohlc_data", columns, conditions, params, speed, max_delay)
    except RuntimeError as e:
        if str(e).startswith("ws-limit:"):
            await _ws_close_with_reason(ws, 4429, "too many concurrent connections")
            return
        raise
    except WebSocketDisconnect:
        logger.info("Bar stream client disconnected", extra={"instrument": instrument})
    except Exception as e:
        logger.error("Bar stream error", exc_info=True)
        try:
            await ws.send_text(json.dumps({"error": str(e)}))
        except Exception:
            pass
    finally:
        try:
            await ws.close()
        except Exception:
            pass
