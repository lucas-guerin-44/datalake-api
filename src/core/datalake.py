"""
DuckDB-based OHLC and tick data storage.
All data lives in a single embedded database file.
"""
import re
import threading

import duckdb
import pandas as pd
from typing import Optional, List
from contextlib import contextmanager

from src.middleware.logging_config import get_logger
from src.services.validators import validate_instrument, validate_timeframe
from src.config import DUCKDB_PATH, DUCKDB_MEMORY_LIMIT
from src.core.migrations import run_migrations

logger = get_logger(__name__)

DUCKDB_PATH.parent.mkdir(parents=True, exist_ok=True)

# Shared connection — DuckDB supports concurrent reads from a single connection.
# Writes acquire an internal lock automatically.
_db_lock = threading.Lock()
_db_connection: Optional[duckdb.DuckDBPyConnection] = None


def _get_shared_connection() -> duckdb.DuckDBPyConnection:
    """Get or create the shared DuckDB connection."""
    global _db_connection
    if _db_connection is None:
        with _db_lock:
            if _db_connection is None:
                _db_connection = duckdb.connect(str(DUCKDB_PATH))
                _db_connection.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
                logger.info("DuckDB connection opened", extra={
                    "path": str(DUCKDB_PATH),
                    "memory_limit": DUCKDB_MEMORY_LIMIT,
                })
    return _db_connection


@contextmanager
def get_db_connection():
    """Context manager for DuckDB connections. Uses a shared connection."""
    con = _get_shared_connection()
    yield con


# Serializes all write transactions so BEGIN/COMMIT on the shared connection is safe.
# DuckDB already single-writes at the file level; this just makes it explicit at the
# Python layer and prevents two concurrent requests from interleaving statements inside
# each other's transactions.
_write_tx_lock = threading.Lock()


@contextmanager
def write_transaction():
    """
    Wrap a group of writes in an atomic transaction. Rolls back on any exception
    so ingest + derive can't leave the datalake half-written.
    """
    with _write_tx_lock:
        con = _get_shared_connection()
        con.execute("BEGIN TRANSACTION")
        try:
            yield con
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise


def init_duckdb():
    """Create the ohlc_data and tick_data tables and indexes if they don't exist."""
    with get_db_connection() as con:
        con.execute("""
            CREATE TABLE IF NOT EXISTS ohlc_data (
                instrument VARCHAR NOT NULL,
                timeframe VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                open DOUBLE NOT NULL,
                high DOUBLE NOT NULL,
                low DOUBLE NOT NULL,
                close DOUBLE NOT NULL,
                source VARCHAR NOT NULL DEFAULT 'raw',
                PRIMARY KEY (instrument, timeframe, timestamp)
            )
        """)
        # Legacy DBs predate the PRIMARY KEY in CREATE TABLE and rely on this named
        # unique index for their only uniqueness guarantee. Keep it — INSERT paths use
        # explicit ON CONFLICT targets, so having both this and a PK is harmless.
        con.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_ohlc_pk ON ohlc_data(instrument, timeframe, timestamp)")

        # Apply any pending schema migrations.
        run_migrations(con)
        con.execute("CREATE INDEX IF NOT EXISTS idx_instrument_timeframe ON ohlc_data(instrument, timeframe)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_timestamp ON ohlc_data(timestamp)")

        con.execute("""
            CREATE TABLE IF NOT EXISTS tick_data (
                instrument VARCHAR NOT NULL,
                timestamp TIMESTAMP NOT NULL,
                price DOUBLE NOT NULL,
                volume DOUBLE DEFAULT 0.0,
                bid DOUBLE,
                ask DOUBLE,
                PRIMARY KEY (instrument, timestamp)
            )
        """)
        con.execute("CREATE INDEX IF NOT EXISTS idx_tick_instrument ON tick_data(instrument)")
        con.execute("CREATE INDEX IF NOT EXISTS idx_tick_ts ON tick_data(instrument, timestamp)")

        logger.info("DuckDB initialized", extra={"path": str(DUCKDB_PATH)})


def list_instruments() -> List[str]:
    """List all unique instruments in the database."""
    with get_db_connection() as con:
        rows = con.execute("SELECT DISTINCT instrument FROM ohlc_data ORDER BY instrument").fetchall()
        return [r[0] for r in rows]


def list_timeframes(instrument: Optional[str] = None) -> List[str]:
    """List unique timeframes, optionally filtered by instrument."""
    if instrument:
        instrument = validate_instrument(instrument)

    with get_db_connection() as con:
        if instrument:
            rows = con.execute(
                "SELECT DISTINCT timeframe FROM ohlc_data WHERE instrument = ? ORDER BY timeframe",
                [instrument],
            ).fetchall()
        else:
            rows = con.execute("SELECT DISTINCT timeframe FROM ohlc_data ORDER BY timeframe").fetchall()
        return [r[0] for r in rows]


_TF_RE = re.compile(r"^(M|H|D|W|MN)(\d+)?$", re.IGNORECASE)


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
    m = _TF_RE.match(tf)
    if not m:
        return series
    unit, n = m.group(1), m.group(2)
    n = int(n) if n else 1

    if unit == "M":
        return series.dt.floor(f"{n}min")
    if unit == "H":
        return series.dt.floor(f"{n}h")
    if unit == "D":
        return series.dt.floor(f"{n}D")
    if unit == "W":
        # Monday-anchored ISO week
        daily = series.dt.floor("D")
        return daily - pd.to_timedelta(daily.dt.weekday, unit="D")
    if unit == "MN":
        # First day of month at 00:00, preserving tz
        return series.dt.to_period("M").dt.start_time.dt.tz_localize(series.dt.tz)
    return series


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


# --- Timeframe derivation ---

# Canonical target timeframes for auto-derivation. W1/MN1 deliberately excluded —
# their bucket alignment is calendar-dependent and better handled by re-export.
DERIVATION_TARGETS = ["M5", "M15", "M30", "H1", "H4", "D1"]

_TF_SECONDS = {
    "M1": 60, "M5": 300, "M15": 900, "M30": 1800,
    "H1": 3600, "H4": 14400, "D1": 86400,
}

_TF_INTERVAL = {
    "M1": "1 minute", "M5": "5 minutes", "M15": "15 minutes", "M30": "30 minutes",
    "H1": "1 hour", "H4": "4 hours", "D1": "1 day",
}


def _derivation_targets_for(source_seconds: int):
    for tf in DERIVATION_TARGETS:
        tgt = _TF_SECONDS[tf]
        if tgt > source_seconds and tgt % source_seconds == 0:
            yield tf, _TF_INTERVAL[tf]


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
    return s.tz_localize(None), e.tz_localize(None)


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
        for target, interval in _derivation_targets_for(src_sec):
            sql = f"""
                INSERT INTO ohlc_data
                (instrument, timeframe, timestamp, open, high, low, close, source)
                SELECT * FROM (
                    SELECT
                        instrument,
                        ? AS timeframe,
                        time_bucket(INTERVAL '{interval}', timestamp) AS timestamp,
                        arg_min(open, timestamp) AS open,
                        max(high) AS high,
                        min(low) AS low,
                        arg_max(close, timestamp) AS close,
                        'derived' AS source
                    FROM ohlc_data
                    WHERE instrument = ?
                      AND timeframe = ?
                      AND timestamp >= ?
                      AND timestamp < ?
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
            con.execute(sql, [target, instrument, source_timeframe, start_utc, end_utc])
            cnt = con.execute(
                """SELECT COUNT(*) FROM ohlc_data
                   WHERE instrument=? AND timeframe=? AND source='derived'
                     AND timestamp >= ? AND timestamp < ?""",
                [instrument, target, start_utc, end_utc],
            ).fetchone()[0]
            results[target] = cnt

    if results:
        logger.info("Derived OHLC timeframes", extra={
            "instrument": instrument,
            "source_timeframe": source_timeframe,
            "targets": results,
        })
    return results


def shift_timestamps_to_utc(source_timezone: str) -> dict:
    """
    One-shot data fix: re-interpret existing `ohlc_data` and `tick_data` timestamps
    as being in `source_timezone`, shift them to UTC wall-clock, and store naive.

    Uses a temp-table + GROUP BY approach so that collisions (two original rows
    shifting to the same final PK — typical of mixed pre/post-UTC-fix state) are
    deduplicated deterministically, keeping the row whose ORIGINAL timestamp was
    latest. Reports how many rows were dropped to dedup — non-zero means your DB
    had inconsistent timezone state. Take a backup before running.
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

    # Hold the write lock for the whole migration, but commit per batch so DuckDB
    # can free MVCC state between pairs. NOT atomic across batches — a crash
    # mid-run leaves earlier pairs migrated, later pairs untouched. TAKE A BACKUP.
    with _write_tx_lock:
        con = _get_shared_connection()
        original_limit = con.execute("SELECT current_setting('memory_limit')").fetchone()[0]
        con.execute("SET memory_limit='8GB'")
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
            # Try to restore the original memory cap — but if DuckDB has allocated
            # past the default, lowering the limit can itself throw. Don't let
            # cleanup mask migration success; leave the limit elevated until the
            # next API restart.
            try:
                con.execute(f"SET memory_limit='{original_limit}'")
            except Exception as cleanup_err:
                logger.warning(
                    "Could not restore memory_limit after migration; stays elevated until restart",
                    extra={"error": str(cleanup_err), "elevated_limit": "8GB"},
                )

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
    sorted by duration descending. The default threshold flags any gap > 2× the bar size.
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
    with get_db_connection() as con:
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
        for target in tick_targets:
            interval = _TF_INTERVAL[target]
            sql = f"""
                INSERT INTO ohlc_data
                (instrument, timeframe, timestamp, open, high, low, close, source)
                SELECT * FROM (
                    SELECT
                        instrument,
                        ? AS timeframe,
                        time_bucket(INTERVAL '{interval}', timestamp) AS timestamp,
                        arg_min(price, timestamp) AS open,
                        max(price) AS high,
                        min(price) AS low,
                        arg_max(price, timestamp) AS close,
                        'derived' AS source
                    FROM tick_data
                    WHERE instrument = ?
                      AND timestamp >= ?
                      AND timestamp < ?
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
            con.execute(sql, [target, instrument, start_utc, end_utc])
            cnt = con.execute(
                """SELECT COUNT(*) FROM ohlc_data
                   WHERE instrument=? AND timeframe=? AND source='derived'
                     AND timestamp >= ? AND timestamp < ?""",
                [instrument, target, start_utc, end_utc],
            ).fetchone()[0]
            results[target] = cnt

    if results:
        logger.info("Derived OHLC from ticks", extra={
            "instrument": instrument,
            "targets": results,
        })
    return results


def get_data_range(instrument: str, timeframe: str) -> Optional[dict]:
    """Get min/max date and row count for an instrument/timeframe pair."""
    instrument = validate_instrument(instrument)
    timeframe = validate_timeframe(timeframe)

    with get_db_connection() as con:
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
    with get_db_connection() as con:
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


# --- Tick data functions ---


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


def list_tick_instruments() -> List[str]:
    """List all unique instruments in the tick_data table."""
    with get_db_connection() as con:
        rows = con.execute("SELECT DISTINCT instrument FROM tick_data ORDER BY instrument").fetchall()
        return [r[0] for r in rows]


def get_tick_coverage(instrument: str) -> Optional[dict]:
    """Get min/max timestamp and tick count for an instrument."""
    instrument = validate_instrument(instrument)

    with get_db_connection() as con:
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
    with get_db_connection() as con:
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
