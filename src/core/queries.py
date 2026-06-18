"""
Read-only query operations against DuckDB.
All functions use read_connection() for non-blocking concurrent reads.
"""
from typing import Optional, List

import pandas as pd

from src.middleware.logging_config import get_logger
from src.services.validators import validate_instrument, validate_timeframe
from src.config import DUCKDB_PATH
from src.core.datalake import read_connection, _TF_SECONDS

logger = get_logger(__name__)


def list_instruments() -> List[str]:
    """List all unique instruments in the database."""
    with read_connection() as con:
        rows = con.execute("SELECT DISTINCT instrument FROM ohlc_data ORDER BY instrument").fetchall()
        return [r[0] for r in rows]


def list_timeframes(instrument: Optional[str] = None) -> List[str]:
    """List unique timeframes, optionally filtered by instrument."""
    if instrument:
        instrument = validate_instrument(instrument)

    with read_connection() as con:
        if instrument:
            rows = con.execute(
                "SELECT DISTINCT timeframe FROM ohlc_data WHERE instrument = ? ORDER BY timeframe",
                [instrument],
            ).fetchall()
        else:
            rows = con.execute("SELECT DISTINCT timeframe FROM ohlc_data ORDER BY timeframe").fetchall()
        return [r[0] for r in rows]


def snap_to_canonical_bucket(series: pd.Series, timeframe: str) -> pd.Series:
    """
    Snap UTC timestamps to the canonical bucket boundary for the timeframe.

    Different brokers stamp aggregated bars (daily / 4-hour) at different hours
    depending on their server timezone and DST policy. Snapping to a canonical
    UTC-anchored boundary lets the (instrument, timeframe, timestamp) PK collapse
    offset-shifted duplicates via INSERT OR REPLACE instead of storing N copies
    of the same logical bar.
    """
    tf = timeframe.upper()
    # Extract leading unit letters and trailing digits (e.g. "M5" -> "M", 5).
    unit = ""
    for ch in tf:
        if ch.isalpha():
            unit += ch
        else:
            break
    n_str = tf[len(unit):]
    n = int(n_str) if n_str else 1

    if unit == "M":
        return series.dt.floor(f"{n}min")
    if unit == "H":
        return series.dt.floor(f"{n}h")
    if unit == "D":
        return series.dt.floor(f"{n}D")
    if unit == "W":
        daily = series.dt.floor("D")
        return daily - pd.to_timedelta(daily.dt.weekday, unit="D")
    if unit == "MN":
        return series.dt.to_period("M").dt.start_time.dt.tz_localize(series.dt.tz)
    return series


def get_data_range(instrument: str, timeframe: str) -> Optional[dict]:
    """Get min/max date and row count for an instrument/timeframe pair."""
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    with read_connection() as con:
        result = con.execute("""
            SELECT MIN(timestamp), MAX(timestamp), COUNT(*), list(DISTINCT source)
            FROM ohlc_data
            WHERE instrument = ? AND timeframe = ?
        """, [instrument, timeframe]).fetchone()

    if result and result[2] > 0:
        return {
            "min_date": result[0],
            "max_date": result[1],
            "count": result[2],
            "sources": sorted(result[3] or []),
        }
    return None


def get_database_stats() -> dict:
    """Get overall database statistics."""
    with read_connection() as con:
        total_rows = con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0]

        instruments = con.execute("""
            SELECT instrument, COUNT(*) as count FROM ohlc_data
            GROUP BY instrument ORDER BY count DESC
        """).fetchall()

        timeframes = con.execute("""
            SELECT timeframe, COUNT(*) as count FROM ohlc_data
            GROUP BY timeframe ORDER BY count DESC
        """).fetchall()

        date_range = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM ohlc_data").fetchone()

    return {
        "database_path": str(DUCKDB_PATH),
        "total_rows": total_rows,
        "instruments": [{"instrument": r[0], "count": r[1]} for r in instruments],
        "timeframes": [{"timeframe": r[0], "count": r[1]} for r in timeframes],
        "date_range": {
            "min": str(date_range[0]) if date_range[0] else None,
            "max": str(date_range[1]) if date_range[1] else None,
        },
    }


def find_gaps(
    instrument: str,
    timeframe: str,
    start=None,
    end=None,
    min_gap_seconds: Optional[int] = None,
    limit: int = 100,
) -> List[dict]:
    """
    Locate unusually-large gaps between consecutive bars in `ohlc_data`.

    Returns entries like {gap_start, gap_end, duration_seconds, missing_bars, is_weekend}
    sorted by duration descending. The default threshold flags any gap > 2x the bar size.
    """
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    tf_sec = _TF_SECONDS.get(timeframe)
    if tf_sec is None:
        # W1/MN1/TICK — gap semantics don't apply cleanly.
        return []

    threshold = int(min_gap_seconds) if min_gap_seconds is not None else tf_sec * 2

    params = [instrument, timeframe, start, start, end, end, threshold, int(limit)]
    sql = """
        WITH ordered AS (
            SELECT timestamp,
                   LAG(timestamp) OVER (ORDER BY timestamp) AS prev_ts
            FROM ohlc_data
            WHERE instrument = ?
              AND timeframe = ?
              AND (? IS NULL OR timestamp >= ?)
              AND (? IS NULL OR timestamp < ?)
        )
        SELECT prev_ts AS gap_start,
               timestamp AS gap_end,
               EXTRACT(EPOCH FROM (timestamp - prev_ts)) AS duration_seconds
        FROM ordered
        WHERE prev_ts IS NOT NULL
          AND EXTRACT(EPOCH FROM (timestamp - prev_ts)) > ?
        ORDER BY duration_seconds DESC
        LIMIT ?
    """
    with read_connection() as con:
        rows = con.execute(sql, params).fetchall()

    result = []
    for gap_start, gap_end, duration_seconds in rows:
        duration_seconds = int(duration_seconds)
        missing_bars = max(0, (duration_seconds // tf_sec) - 1)
        # Weekend heuristic: gap begins late Fri UTC and ends before ~Mon 00:00,
        # with duration in the 36h..75h window typical of FX market close.
        is_weekend = False
        if gap_start is not None:
            weekday = gap_start.weekday()  # Mon=0 .. Sun=6
            if weekday in (4, 5) and 36 * 3600 <= duration_seconds <= 75 * 3600:
                is_weekend = True
        result.append({
            "gap_start": gap_start,
            "gap_end": gap_end,
            "duration_seconds": duration_seconds,
            "missing_bars": int(missing_bars),
            "is_weekend": is_weekend,
        })
    return result


# --- Tick data query functions ---


def list_tick_instruments() -> List[str]:
    """List all unique instruments in the tick_data table."""
    with read_connection() as con:
        rows = con.execute("SELECT DISTINCT instrument FROM tick_data ORDER BY instrument").fetchall()
        return [r[0] for r in rows]


def get_tick_coverage(instrument: str) -> Optional[dict]:
    """Get min/max timestamp and tick count for an instrument."""
    instrument = validate_instrument(instrument)

    with read_connection() as con:
        result = con.execute("""
            SELECT MIN(timestamp), MAX(timestamp), COUNT(*)
            FROM tick_data
            WHERE instrument = ?
        """, [instrument]).fetchone()

    if result and result[2] > 0:
        return {"min_date": result[0], "max_date": result[1], "count": result[2]}
    return None


def get_tick_database_stats() -> dict:
    """Get tick_data table statistics."""
    with read_connection() as con:
        total_rows = con.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0]

        instruments = con.execute("""
            SELECT instrument, COUNT(*) as count FROM tick_data
            GROUP BY instrument ORDER BY count DESC
        """).fetchall()

        date_range = con.execute("SELECT MIN(timestamp), MAX(timestamp) FROM tick_data").fetchone()

    return {
        "total_ticks": total_rows,
        "instruments": [{"instrument": r[0], "count": r[1]} for r in instruments],
        "date_range": {
            "min": str(date_range[0]) if date_range[0] else None,
            "max": str(date_range[1]) if date_range[1] else None,
        },
    }
