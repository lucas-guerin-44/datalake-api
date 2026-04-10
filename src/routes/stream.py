"""WebSocket streaming routes — replay historical ticks and bars at real-time speed."""
import asyncio
import json
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, WebSocket, WebSocketDisconnect, Query

from src.core.datalake import get_db_connection
from src.services.validators import validate_instrument, validate_timeframe
from src.middleware.logging_config import get_logger

logger = get_logger(__name__)
router = APIRouter()


def _parse_ts(ts) -> datetime:
    """Convert a DuckDB timestamp value to a Python datetime."""
    if isinstance(ts, datetime):
        return ts
    return datetime.fromisoformat(str(ts))


async def _stream_rows(
    ws: WebSocket, sql: str, params: list, speed: float, columns: list[str], max_delay: float = 10.0,
):
    """
    Stream query results over a WebSocket, pacing sends by the real-time
    delta between consecutive timestamps (scaled by speed multiplier).

    Args:
        max_delay: Upper bound (seconds) on the sleep between any two messages.
                   Use a small value (e.g. 0.1) when the consumer handles its
                   own presentation pacing and just wants fast delivery.
                   Set to 0 for burst mode (no pacing at all).

    Each message is a JSON object with the column names as keys.
    Sends a final {"done": true} when replay is complete.
    """
    prev_ts: Optional[datetime] = None
    BATCH_SIZE = 1000

    with get_db_connection() as con:
        db_cursor = con.execute(sql, params)

        while True:
            batch = db_cursor.fetchmany(BATCH_SIZE)
            if not batch:
                break

            for row in batch:
                record = dict(zip(columns, row))

                ts = _parse_ts(record["timestamp"])
                record["timestamp"] = ts.isoformat()

                # Pace by real-time delta between rows, capped by max_delay
                if prev_ts is not None and speed > 0 and max_delay > 0:
                    delta = (ts - prev_ts).total_seconds() / speed
                    if delta > 0:
                        await asyncio.sleep(min(delta, max_delay))

                prev_ts = ts
                await ws.send_text(json.dumps(record))

    await ws.send_text(json.dumps({"done": True}))


@router.websocket("/ws/ticks")
async def stream_ticks(
    ws: WebSocket,
    instrument: str = Query(...),
    start: Optional[str] = Query(None),
    end: Optional[str] = Query(None),
    speed: float = Query(1.0, gt=0),
    max_delay: float = Query(10.0, ge=0),
):
    """
    Stream historical tick data at real-time speed (or multiplied).

    Query params:
        instrument — required, e.g. XAUUSD
        start / end — optional ISO-8601 timestamp bounds
        speed — playback multiplier (1.0 = real-time, 10.0 = 10x fast)
        max_delay — upper bound in seconds on sleep between messages (0 = burst)
    """
    await ws.accept()
    try:
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

        columns = ["timestamp", "price", "volume", "bid", "ask"]
        await _stream_rows(ws, sql, params, speed, columns, max_delay)

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
):
    """
    Stream historical OHLC bars at real-time speed (or multiplied).

    Query params:
        instrument — required, e.g. XAUUSD
        timeframe — required, e.g. M5, H1
        start / end — optional ISO-8601 timestamp bounds
        speed — playback multiplier (1.0 = real-time, 60.0 = 1 bar/sec for M1)
        max_delay — upper bound in seconds on sleep between messages (0 = burst)
    """
    await ws.accept()
    try:
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
        SELECT timestamp, open, high, low, close
        FROM ohlc_data
        WHERE {' AND '.join(conditions)}
        ORDER BY timestamp
        """

        columns = ["timestamp", "open", "high", "low", "close"]
        await _stream_rows(ws, sql, params, speed, columns, max_delay)

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
