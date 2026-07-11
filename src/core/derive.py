"""
OHLC timeframe derivation.
Builds higher-timeframe bars from lower-timeframe source data.
"""
import os
from contextlib import contextmanager
from typing import Optional

import pandas as pd

from src.middleware.logging_config import get_logger
from src.services.validators import validate_instrument, validate_timeframe
from src.core.datalake import get_db_connection, _TF_SECONDS, to_naive_utc
from src.core.queries import snap_to_canonical_bucket

logger = get_logger(__name__)

# Canonical target timeframes for auto-derivation. W1/MN1 deliberately excluded —
# their bucket alignment is calendar-dependent and better handled by re-export.
DERIVATION_TARGETS = ["M5", "M15", "M30", "H1", "H4", "D1"]

_TF_INTERVAL = {
    "M1": "1 minute", "M5": "5 minutes", "M15": "15 minutes", "M30": "30 minutes",
    "H1": "1 hour", "H4": "4 hours", "D1": "1 day",
}


def _safe_interval(timeframe: str) -> str:
    """Return the SQL INTERVAL string for a timeframe, raising on unknown."""
    if timeframe not in _TF_INTERVAL:
        raise ValueError(f"Unknown timeframe for interval: {timeframe}")
    return _TF_INTERVAL[timeframe]


def _derivation_targets_for(source_seconds: int):
    for tf in DERIVATION_TARGETS:
        tgt = _TF_SECONDS[tf]
        if tgt > source_seconds and tgt % source_seconds == 0:
            yield tf, _safe_interval(tf)


def _pad_window_to_day(start, end):
    """
    Expand [start, end) outwards to UTC day boundaries so day-sized buckets are complete.
    Returns naive-UTC timestamps to match DuckDB's TIMESTAMP column type.
    """
    s = pd.Timestamp(start)
    e = pd.Timestamp(end)
    if s.tz is None:
        s = s.tz_localize("UTC")
    else:
        s = s.tz_convert("UTC")
    if e.tz is None:
        e = e.tz_localize("UTC")
    else:
        e = e.tz_convert("UTC")
    s = s.floor("D")
    e = e.ceil("D") if e != e.floor("D") else e + pd.Timedelta(days=1)
    return to_naive_utc(s), to_naive_utc(e)


_VALID_SOURCE_TABLES = frozenset({"ohlc_data", "tick_data"})


def _derive_bars(
    con, target: str, interval: str, instrument: str,
    source_table: str, source_where: str, source_params: list,
    open_col: str = "open", high_col: str = "high",
    low_col: str = "low", close_col: str = "close",
) -> int:
    """
    Shared derivation SQL: aggregate from source_table into target timeframe,
    skipping windows that already have a raw row. Returns rows written.
    """
    if source_table not in _VALID_SOURCE_TABLES:
        raise ValueError(f"Invalid source_table: {source_table!r}")
    sql = f"""
        INSERT INTO ohlc_data
        (instrument, timeframe, timestamp, open, high, low, close, source)
        SELECT * FROM (
            SELECT
                instrument,
                ? AS timeframe,
                time_bucket(INTERVAL '{interval}', timestamp) AS timestamp,
                arg_min({open_col}, timestamp) AS open,
                max({high_col}) AS high,
                min({low_col}) AS low,
                arg_max({close_col}, timestamp) AS close,
                'derived' AS source
            FROM {source_table}
            WHERE {source_where}
            GROUP BY instrument, time_bucket(INTERVAL '{interval}', timestamp)
        ) derived_bars
        WHERE NOT EXISTS (
            SELECT 1 FROM ohlc_data existing
            WHERE existing.instrument = derived_bars.instrument
              AND existing.timeframe = derived_bars.timeframe
              AND existing.timestamp = derived_bars.timestamp
              AND existing.source = 'raw'
        )
        ON CONFLICT (instrument, timeframe, timestamp) DO UPDATE SET
            open = excluded.open,
            high = excluded.high,
            low  = excluded.low,
            close = excluded.close,
            source = excluded.source
    """
    con.execute(sql, [target, *source_params])
    return con.execute(
        """SELECT COUNT(*) FROM ohlc_data
           WHERE instrument=? AND timeframe=? AND source='derived'
             AND timestamp >= ? AND timestamp < ?""",
        [instrument, target, source_params[-2], source_params[-1]],
    ).fetchone()[0]


DERIVATION_THREADS = int(os.getenv("DERIVATION_THREADS", "2"))


@contextmanager
def _derivation_session(con):
    """
    Context manager: tune DuckDB session for memory-efficient derivation,
    then restore original settings. Must run inside an existing write transaction.
    """
    orig_io = con.execute("SELECT current_setting('preserve_insertion_order')").fetchone()[0]
    orig_threads = con.execute("SELECT current_setting('threads')").fetchone()[0]
    con.execute("SET preserve_insertion_order=false")
    con.execute(f"SET threads={DERIVATION_THREADS}")
    try:
        yield
    finally:
        con.execute(f"SET preserve_insertion_order={orig_io}")
        con.execute(f"SET threads={orig_threads}")


def derive_ohlc_timeframes(instrument: str, source_timeframe: str, start, end) -> dict:
    """
    Rebuild derived OHLC bars for all target timeframes larger than source_timeframe,
    covering [start, end] (inclusive on start, exclusive on end after padding).
    Idempotent via INSERT OR REPLACE. Never clobbers existing source='raw' rows.
    Returns {target_tf: rows_written}.
    """
    instrument = validate_instrument(instrument)
    source_timeframe = validate_timeframe(source_timeframe)

    src_sec = _TF_SECONDS.get(source_timeframe)
    if src_sec is None:
        return {}

    start_utc, end_utc = _pad_window_to_day(start, end)
    results: dict = {}

    with get_db_connection() as con:
        with _derivation_session(con):
            for target, interval in _derivation_targets_for(src_sec):
                source_where = "instrument = ? AND timeframe = ? AND timestamp >= ? AND timestamp < ?"
                source_params = [instrument, source_timeframe, start_utc, end_utc]
                cnt = _derive_bars(con, target, interval, instrument,
                                   "ohlc_data", source_where, source_params)
                results[target] = cnt

    if results:
        logger.info("Derived OHLC timeframes", extra={
            "instrument": instrument,
            "source_timeframe": source_timeframe,
            "targets": results,
        })
    return results


def derive_ohlc_from_ticks(instrument: str, start, end) -> dict:
    """
    Build OHLC bars from tick data for all canonical timeframes, covering [start, end].
    Idempotent via INSERT OR REPLACE. Never clobbers existing source='raw' rows.
    Returns {target_tf: rows_written}.
    """
    instrument = validate_instrument(instrument)
    start_utc, end_utc = _pad_window_to_day(start, end)
    results: dict = {}

    tick_targets = ["M1"] + DERIVATION_TARGETS
    with get_db_connection() as con:
        with _derivation_session(con):
            for target in tick_targets:
                interval = _safe_interval(target)
                source_where = "instrument = ? AND timestamp >= ? AND timestamp < ?"
                source_params = [instrument, start_utc, end_utc]
                cnt = _derive_bars(con, target, interval, instrument,
                                   "tick_data", source_where, source_params,
                                   open_col="price", high_col="price",
                                   low_col="price", close_col="price")
                results[target] = cnt

    if results:
        logger.info("Derived OHLC from ticks", extra={
            "instrument": instrument,
            "targets": results,
        })
    return results
