"""
Write operations for OHLC and tick data in DuckDB.
"""
import os
from typing import Optional

import pandas as pd

from src.middleware.logging_config import get_logger
from src.services.validators import validate_instrument, validate_timeframe
from src.core.datalake import get_db_connection, write_transaction, bump_data_version, _write_tx_lock, _get_shared_connection, MIGRATION_MEMORY_LIMIT, to_naive_utc
from src.core.queries import snap_to_canonical_bucket

logger = get_logger(__name__)


def upsert_ohlc_data(df: pd.DataFrame, instrument: str, timeframe: str, source: str = "raw") -> int:
    """
    Upsert OHLC data — updates existing rows, inserts new ones.
    Returns the number of rows affected.
    """
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    if df.empty:
        return 0

    required = ["timestamp", "open", "high", "low", "close"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    insert_df = df[required].copy()
    insert_df["instrument"] = instrument
    insert_df["timeframe"] = timeframe
    insert_df["source"] = source
    insert_df["timestamp"] = pd.to_datetime(insert_df["timestamp"], utc=True)
    insert_df["timestamp"] = snap_to_canonical_bucket(insert_df["timestamp"], timeframe)
    # DuckDB's TIMESTAMP column is naive; inserting tz-aware values silently
    # converts to the host's local tz. Strip tz while keeping UTC wall-clock so
    # stored values are canonical UTC regardless of where the API runs.
    insert_df["timestamp"] = insert_df["timestamp"].dt.tz_localize(None)
    # Collapse offset-shifted rows within the same batch before hitting the PK.
    insert_df = insert_df.drop_duplicates(subset=["timestamp"], keep="last")

    with get_db_connection() as con:
        con.execute("""
            INSERT INTO ohlc_data
            (instrument, timeframe, timestamp, open, high, low, close, source)
            SELECT instrument, timeframe, timestamp, open, high, low, close, source
            FROM insert_df
            ON CONFLICT (instrument, timeframe, timestamp) DO UPDATE SET
                open = excluded.open,
                high = excluded.high,
                low  = excluded.low,
                close = excluded.close,
                source = excluded.source
        """)

    return len(insert_df)


def upsert_tick_data(df: pd.DataFrame, instrument: str) -> int:
    """
    Upsert tick data — updates existing rows, inserts new ones.
    Returns the number of rows affected.
    """
    instrument = validate_instrument(instrument)

    if df.empty:
        return 0

    required = ["timestamp", "price"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    cols = ["timestamp", "price"]
    for optional_col in ["volume", "bid", "ask"]:
        if optional_col in df.columns:
            cols.append(optional_col)

    insert_df = df[cols].copy()
    insert_df["instrument"] = instrument
    insert_df["timestamp"] = pd.to_datetime(insert_df["timestamp"], utc=True)
    # Store as naive UTC (see upsert_ohlc_data for rationale).
    insert_df["timestamp"] = insert_df["timestamp"].dt.tz_localize(None)

    # Fill missing optional columns with defaults
    if "volume" not in insert_df.columns:
        insert_df["volume"] = 0.0
    if "bid" not in insert_df.columns:
        insert_df["bid"] = None
    if "ask" not in insert_df.columns:
        insert_df["ask"] = None

    with get_db_connection() as con:
        con.execute("""
            INSERT OR REPLACE INTO tick_data
            (instrument, timestamp, price, volume, bid, ask)
            SELECT instrument, timestamp, price, volume, bid, ask
            FROM insert_df
        """)

    return len(insert_df)


def delete_ohlc_data(
    instrument: str,
    timeframe: Optional[str] = None,
    start=None,
    end=None,
) -> int:
    """
    Delete rows from ohlc_data matching instrument [+ timeframe] [+ window).
    Window is half-open [start, end). Returns rows deleted.
    """
    instrument = validate_instrument(instrument)
    if timeframe is not None:
        timeframe = validate_timeframe(timeframe)

    clauses = ["instrument = ?"]
    params: list = [instrument]
    if timeframe is not None:
        clauses.append("timeframe = ?")
        params.append(timeframe)
    if start is not None:
        clauses.append("timestamp >= ?")
        params.append(to_naive_utc(start))
    if end is not None:
        clauses.append("timestamp < ?")
        params.append(to_naive_utc(end))
    where = " AND ".join(clauses)

    with write_transaction() as con:
        before = con.execute(f"SELECT COUNT(*) FROM ohlc_data WHERE {where}", params).fetchone()[0]
        con.execute(f"DELETE FROM ohlc_data WHERE {where}", params)
    logger.info("Deleted OHLC rows", extra={
        "instrument": instrument, "timeframe": timeframe,
        "start": str(start) if start else None, "end": str(end) if end else None,
        "rows": before,
    })
    return before


def delete_tick_data(instrument: str, start=None, end=None) -> int:
    """Delete rows from tick_data matching instrument [+ window). Returns rows deleted."""
    instrument = validate_instrument(instrument)

    clauses = ["instrument = ?"]
    params: list = [instrument]
    if start is not None:
        clauses.append("timestamp >= ?")
        params.append(to_naive_utc(start))
    if end is not None:
        clauses.append("timestamp < ?")
        params.append(to_naive_utc(end))
    where = " AND ".join(clauses)

    with write_transaction() as con:
        before = con.execute(f"SELECT COUNT(*) FROM tick_data WHERE {where}", params).fetchone()[0]
        con.execute(f"DELETE FROM tick_data WHERE {where}", params)
    logger.info("Deleted tick rows", extra={
        "instrument": instrument,
        "start": str(start) if start else None, "end": str(end) if end else None,
        "rows": before,
    })
    return before


def shift_timestamps_to_utc(source_timezone: str) -> dict:
    """
    One-shot data fix: re-interpret existing timestamps as being in
    `source_timezone`, shift them to UTC wall-clock, and store naive.

    Uses a temp-table + GROUP BY approach so that collisions are deduplicated
    deterministically. TAKE A BACKUP before running.
    """
    try:
        from zoneinfo import ZoneInfo
        ZoneInfo(source_timezone)  # raises on invalid tz name
    except Exception as e:
        raise ValueError(f"Invalid timezone '{source_timezone}': {e}")

    # ZoneInfo validation above makes string interpolation safe here.
    shift_expr = (
        f"CAST(timestamp AT TIME ZONE '{source_timezone}' AT TIME ZONE 'UTC' AS TIMESTAMP)"
    )

    with _write_tx_lock:
        con = _get_shared_connection()
        original_limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        con.execute(f"SET memory_limit='{MIGRATION_MEMORY_LIMIT}'")
        con.execute("SET preserve_insertion_order=false")

        try:
            # OHLC: per (instrument, timeframe)
            before_ohlc = con.execute("SELECT COUNT(*) FROM ohlc_data").fetchone()[0]
            pairs = con.execute(
                "SELECT DISTINCT instrument, timeframe FROM ohlc_data"
            ).fetchall()
            after_ohlc = 0
            for instrument_val, timeframe_val in pairs:
                con.execute("BEGIN TRANSACTION")
                try:
                    con.execute(f"""
                        CREATE TEMP TABLE _ohlc_batch AS
                        SELECT instrument, timeframe, new_ts AS timestamp,
                               arg_max(open, orig_ts)   AS open,
                               arg_max(high, orig_ts)   AS high,
                               arg_max(low,  orig_ts)   AS low,
                               arg_max(close, orig_ts)  AS close,
                               arg_max(source, orig_ts) AS source
                        FROM (
                            SELECT *, timestamp AS orig_ts, {shift_expr} AS new_ts
                            FROM ohlc_data
                            WHERE instrument = ? AND timeframe = ?
                        )
                        GROUP BY instrument, timeframe, new_ts
                    """, [instrument_val, timeframe_val])
                    batch_after = con.execute("SELECT COUNT(*) FROM _ohlc_batch").fetchone()[0]
                    con.execute(
                        "DELETE FROM ohlc_data WHERE instrument = ? AND timeframe = ?",
                        [instrument_val, timeframe_val],
                    )
                    con.execute("INSERT INTO ohlc_data SELECT * FROM _ohlc_batch")
                    con.execute("DROP TABLE _ohlc_batch")
                    con.execute("COMMIT")
                    after_ohlc += batch_after
                    logger.info("Migrated OHLC batch", extra={
                        "instrument": instrument_val, "timeframe": timeframe_val,
                        "rows_after": batch_after,
                    })
                except Exception:
                    con.execute("ROLLBACK")
                    raise

            # Ticks: per instrument
            before_ticks = con.execute("SELECT COUNT(*) FROM tick_data").fetchone()[0]
            tick_instruments = con.execute(
                "SELECT DISTINCT instrument FROM tick_data"
            ).fetchall()
            after_ticks = 0
            for (instrument_val,) in tick_instruments:
                con.execute("BEGIN TRANSACTION")
                try:
                    con.execute(f"""
                        CREATE TEMP TABLE _ticks_batch AS
                        SELECT instrument, new_ts AS timestamp,
                               arg_max(price,  orig_ts) AS price,
                               arg_max(volume, orig_ts) AS volume,
                               arg_max(bid,    orig_ts) AS bid,
                               arg_max(ask,    orig_ts) AS ask
                        FROM (
                            SELECT *, timestamp AS orig_ts, {shift_expr} AS new_ts
                            FROM tick_data
                            WHERE instrument = ?
                        )
                        GROUP BY instrument, new_ts
                    """, [instrument_val])
                    batch_after = con.execute("SELECT COUNT(*) FROM _ticks_batch").fetchone()[0]
                    con.execute(
                        "DELETE FROM tick_data WHERE instrument = ?",
                        [instrument_val],
                    )
                    con.execute("INSERT INTO tick_data SELECT * FROM _ticks_batch")
                    con.execute("DROP TABLE _ticks_batch")
                    con.execute("COMMIT")
                    after_ticks += batch_after
                except Exception:
                    con.execute("ROLLBACK")
                    raise
        finally:
            try:
                con.execute(f"SET memory_limit='{original_limit}'")
            except Exception as cleanup_err:
                logger.warning(
                    "Could not restore memory_limit after migration; stays elevated until restart",
                    extra={"error": str(cleanup_err), "elevated_limit": MIGRATION_MEMORY_LIMIT},
                )

    bump_data_version()
    result = {
        "ohlc_rows_before": before_ohlc,
        "ohlc_rows_after": after_ohlc,
        "ohlc_rows_deduplicated": before_ohlc - after_ohlc,
        "tick_rows_before": before_ticks,
        "tick_rows_after": after_ticks,
        "tick_rows_deduplicated": before_ticks - after_ticks,
    }
    logger.info("Shifted timestamps to UTC", extra={"source_timezone": source_timezone, **result})
    return result
